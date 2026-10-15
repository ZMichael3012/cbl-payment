//go:build integration

package usecase_test

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
	"go.uber.org/zap"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	eventsv1 "github.com/cbl/cbl-protos/gen/go/billing/events/v1"

	"github.com/cbl/cbl-kit/inbox"
	"github.com/cbl/cbl-kit/storagex"

	"github.com/cbl/cbl-payment/internal/domain"
	"github.com/cbl/cbl-payment/internal/storage"
	"github.com/cbl/cbl-payment/internal/usecase"
)

func setupConsumerDB(t *testing.T) *gorm.DB {
	t.Helper()
	ctx := context.Background()
	container, err := tcpostgres.Run(ctx,
		"postgres:16-alpine",
		tcpostgres.WithDatabase("payment_consumer"),
		tcpostgres.WithUsername("cbl"),
		tcpostgres.WithPassword("cbl"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(90*time.Second),
		),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = container.Terminate(context.Background()) })

	dsn, err := container.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	require.NoError(t, err)
	require.NoError(t, storagex.Migrate(ctx, db, filepath.Join("..", "..", "migrations")))
	return db
}

// pendingPayment создает платеж и доводит его до состояния pending.
func pendingPayment(t *testing.T, repo *storage.Repository) storage.Payment {
	t.Helper()
	payment := storage.Payment{
		ID:               canonicalPaymentID,
		MerchantID:       "m-canonical",
		IdempotencyKey:   "canonical-1",
		RequestHash:      "hash-canonical",
		ExpectedAmount:   100_000,
		ExpectedAsset:    "USDT",
		ExpectedDecimals: 2,
		Network:          "trc20",
	}
	require.NoError(t, repo.Create(context.Background(), payment))
	payment, err := repo.Transition(context.Background(), payment.ID, 1, domain.StateCreated, storage.TransitionPatch{})
	require.NoError(t, err)
	payment, err = repo.Transition(context.Background(), payment.ID, uint64(payment.StateVersion), domain.StatePending, storage.TransitionPatch{})
	require.NoError(t, err)
	return payment
}

func TestIntegrationCanonicalConsumerAppliesEvents(t *testing.T) {
	db := setupConsumerDB(t)
	repo := storage.NewRepository(db)
	consumer := usecase.NewCanonicalConsumer(repo, zap.NewNop())
	ctx := context.Background()

	pendingPayment(t, repo)

	require.NoError(t, consumer.Handle(ctx, nil, canonicalMessage(t, detectedEvent())))
	payment, _, err := repo.Get(ctx, canonicalPaymentID)
	require.NoError(t, err)
	assert.Equal(t, string(domain.StateDetected), payment.State)
	require.NotNil(t, payment.ReceivedAmount)
	assert.Equal(t, int64(100_000), *payment.ReceivedAmount)
	require.NotNil(t, payment.TxID)
	assert.Equal(t, "tx-1", *payment.TxID)

	confirmed := detectedEvent()
	confirmed.Type = eventsv1.CanonicalPaymentEventType_CANONICAL_PAYMENT_EVENT_TYPE_CONFIRMED
	confirmed.Confirmations = 4
	require.NoError(t, consumer.Handle(ctx, nil, canonicalMessage(t, confirmed)))
	payment, history, err := repo.Get(ctx, canonicalPaymentID)
	require.NoError(t, err)
	assert.Equal(t, string(domain.StateConfirming), payment.State)
	assert.Equal(t, 4, payment.Confirmations)
	assert.Len(t, history, 5, "создание, created, pending, detected, confirming")

	// Устаревшее событие после терминального состояния - no-op без ошибки.
	completed, err := repo.Transition(ctx, canonicalPaymentID, uint64(payment.StateVersion), domain.StateCompleted, storage.TransitionPatch{})
	require.NoError(t, err)
	require.NoError(t, consumer.Handle(ctx, nil, canonicalMessage(t, detectedEvent())))
	payment, history, err = repo.Get(ctx, canonicalPaymentID)
	require.NoError(t, err)
	assert.Equal(t, string(domain.StateCompleted), payment.State)
	assert.Equal(t, completed.StateVersion, payment.StateVersion, "устаревшее событие не растит версию")
	assert.Len(t, history, 6)
}

func TestIntegrationCanonicalConsumerConfirmedFirstChain(t *testing.T) {
	db := setupConsumerDB(t)
	repo := storage.NewRepository(db)
	consumer := usecase.NewCanonicalConsumer(repo, zap.NewNop())
	ctx := context.Background()

	pendingPayment(t, repo)

	confirmed := detectedEvent()
	confirmed.Type = eventsv1.CanonicalPaymentEventType_CANONICAL_PAYMENT_EVENT_TYPE_CONFIRMED
	confirmed.Confirmations = 2
	confirmed.Network = "erc20"
	require.NoError(t, consumer.Handle(ctx, nil, canonicalMessage(t, confirmed)))

	payment, history, err := repo.Get(ctx, canonicalPaymentID)
	require.NoError(t, err)
	assert.Equal(t, string(domain.StateConfirming), payment.State)
	assert.Equal(t, 2, payment.Confirmations)
	assert.Equal(t, "erc20", payment.Network, "network события фиксируется в патче")
	assert.Len(t, history, 5, "creating, created, pending, detected, confirming")
}

func TestIntegrationCanonicalConsumerDuplicateViaGuard(t *testing.T) {
	db := setupConsumerDB(t)
	repo := storage.NewRepository(db)
	consumer := usecase.NewCanonicalConsumer(repo, zap.NewNop())
	ctx := context.Background()

	pendingPayment(t, repo)

	eventID := "01a13d53-6bf5-707f-a944-c0d73586547e"
	guard := inbox.NewGuard()
	msg := canonicalMessage(t, detectedEvent())

	processed, err := guard.Process(ctx, db, usecase.ConsumerGroupCanonical, eventID, func(tx *gorm.DB) error {
		return consumer.Handle(ctx, tx, msg)
	})
	require.NoError(t, err)
	require.True(t, processed)

	afterFirst, _, err := repo.Get(ctx, canonicalPaymentID)
	require.NoError(t, err)

	processed, err = guard.Process(ctx, db, usecase.ConsumerGroupCanonical, eventID, func(tx *gorm.DB) error {
		return consumer.Handle(ctx, tx, msg)
	})
	require.NoError(t, err)
	assert.False(t, processed, "повторный event_id не обрабатывается")

	afterSecond, _, err := repo.Get(ctx, canonicalPaymentID)
	require.NoError(t, err)
	assert.Equal(t, afterFirst.StateVersion, afterSecond.StateVersion, "дубликат не растит версию")
}

// failingStore ломает второй переход: проверяет атомарность с inbox.
type failingStore struct {
	repo  *storage.Repository
	calls int
}

// PaymentTx делегирует в репозиторий.
func (f *failingStore) PaymentTx(ctx context.Context, tx *gorm.DB, id string) (storage.Payment, error) {
	return f.repo.PaymentTx(ctx, tx, id)
}

// TransitionTx падает на втором вызове.
func (f *failingStore) TransitionTx(ctx context.Context, tx *gorm.DB, id string, expectedVersion uint64, to domain.State, patch storage.TransitionPatch) (storage.Payment, error) {
	f.calls++
	if f.calls == 2 {
		return storage.Payment{}, errors.New("test failure")
	}
	return f.repo.TransitionTx(ctx, tx, id, expectedVersion, to, patch)
}

func TestIntegrationCanonicalConsumerTransactionalAtomicity(t *testing.T) {
	db := setupConsumerDB(t)
	repo := storage.NewRepository(db)
	consumer := usecase.NewCanonicalConsumer(&failingStore{repo: repo}, zap.NewNop())
	ctx := context.Background()

	pendingPayment(t, repo)
	before, _, err := repo.Get(ctx, canonicalPaymentID)
	require.NoError(t, err)

	confirmed := detectedEvent()
	confirmed.Type = eventsv1.CanonicalPaymentEventType_CANONICAL_PAYMENT_EVENT_TYPE_CONFIRMED
	confirmed.Confirmations = 1
	guard := inbox.NewGuard()
	eventID := "01a13d53-6bf5-707f-a944-c0d73586547f"

	processed, err := guard.Process(ctx, db, usecase.ConsumerGroupCanonical, eventID, func(tx *gorm.DB) error {
		return consumer.Handle(ctx, tx, canonicalMessage(t, confirmed))
	})
	require.Error(t, err, "сбой второго перехода обязан провалить обработку")
	assert.False(t, processed)

	after, history, err := repo.Get(ctx, canonicalPaymentID)
	require.NoError(t, err)
	assert.Equal(t, before.State, after.State, "переходы откатились вместе с inbox")
	assert.Equal(t, before.StateVersion, after.StateVersion)
	assert.Len(t, history, int(before.StateVersion), "журнал без следов отката")

	var inboxRows int64
	require.NoError(t, db.Table("inbox").Where("event_id = ?", eventID).Count(&inboxRows).Error)
	assert.Zero(t, inboxRows, "inbox-строка откатилась вместе с переходами")
}

func TestIntegrationCanonicalConsumerUnknownPayment(t *testing.T) {
	db := setupConsumerDB(t)
	repo := storage.NewRepository(db)
	consumer := usecase.NewCanonicalConsumer(repo, zap.NewNop())

	err := consumer.Handle(context.Background(), nil, canonicalMessage(t, detectedEvent()))
	require.ErrorIs(t, err, storage.ErrNotFound)
}
