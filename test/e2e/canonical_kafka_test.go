//go:build e2e

// Пакет e2e содержит сквозной сценарий consumer канонических событий payment:
// события gateway.events.canonical из Kafka переводят автомат платежа в
// PostgreSQL; дубликаты не применяются повторно, устаревшие события - no-op.
// Стенд: реальные Kafka и PostgreSQL.
package e2e

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/kafka"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	commonv1 "github.com/cbl/cbl-protos/gen/go/billing/common/v1"
	eventsv1 "github.com/cbl/cbl-protos/gen/go/billing/events/v1"

	"github.com/cbl/cbl-kit/inbox"
	"github.com/cbl/cbl-kit/inbox/kafkaconsumer"
	"github.com/cbl/cbl-kit/outbox"
	"github.com/cbl/cbl-kit/outbox/kafkapub"
	"github.com/cbl/cbl-kit/storagex"

	"github.com/cbl/cbl-payment/internal/domain"
	"github.com/cbl/cbl-payment/internal/storage"
	"github.com/cbl/cbl-payment/internal/usecase"
)

const canonicalE2EPaymentID = "01a13d53-6bf5-707f-a944-c0d7358654f0"

func setupE2EPostgres(t *testing.T) *gorm.DB {
	t.Helper()
	ctx := context.Background()
	container, err := tcpostgres.Run(ctx,
		"postgres:16-alpine",
		tcpostgres.WithDatabase("payment_e2e"),
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

func setupE2EKafka(t *testing.T) []string {
	t.Helper()
	ctx := context.Background()
	container, err := kafka.Run(ctx,
		"confluentinc/cp-kafka:7.6.1",
		kafka.WithClusterID("cbl-payment-e2e"),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = container.Terminate(context.Background()) })

	brokers, err := container.Brokers(ctx)
	require.NoError(t, err)

	adminClient, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	require.NoError(t, err)
	defer adminClient.Close()
	admin := kadm.NewClient(adminClient)
	topic := usecase.TopicGatewayEventsCanonical
	topics := []string{
		topic, topic + inbox.RetrySuffix5s, topic + inbox.RetrySuffix1m,
		topic + inbox.RetrySuffix10m, topic + inbox.DLQSuffix,
	}
	for _, name := range topics {
		_, err := admin.CreateTopic(ctx, 1, 1, nil, name)
		require.NoError(t, err, name)
	}
	return brokers
}

// canonicalRecord собирает Kafka-запись канонического события.
func canonicalRecord(t *testing.T, eventType eventsv1.CanonicalPaymentEventType, confirmations uint32) *kgo.Record {
	t.Helper()
	event := &eventsv1.CanonicalPaymentEvent{
		PaymentId:      canonicalE2EPaymentID,
		GatewayId:      "g-e2e",
		Provider:       "mock",
		Type:           eventType,
		ReceivedAmount: &commonv1.Money{MinorUnits: 100_000, Asset: "USDT", Decimals: 2},
		Confirmations:  confirmations,
		Txid:           "tx-e2e",
		Network:        "trc20",
	}
	body, err := proto.Marshal(event)
	require.NoError(t, err)
	return &kgo.Record{
		Topic: usecase.TopicGatewayEventsCanonical,
		Key:   []byte(canonicalE2EPaymentID),
		Value: body,
		Headers: []kgo.RecordHeader{
			{Key: outbox.HeaderEventID, Value: []byte(uuid.Must(uuid.NewV7()).String())},
			{Key: outbox.HeaderEventType, Value: []byte("gateway.payment")},
			{Key: outbox.HeaderProducer, Value: []byte("cbl-connector")},
		},
	}
}

// waitForState ждет состояния платежа или падает по таймауту контекста.
func waitForState(t *testing.T, ctx context.Context, repo *storage.Repository, want domain.State) storage.Payment {
	t.Helper()
	for {
		payment, _, err := repo.Get(ctx, canonicalE2EPaymentID)
		require.NoError(t, err)
		if payment.State == string(want) {
			return payment
		}
		select {
		case <-ctx.Done():
			require.FailNow(t, "payment state not reached", string(want), payment.State)
		default:
			time.Sleep(200 * time.Millisecond)
		}
	}
}

func TestCanonicalConsumerKafkaFlow(t *testing.T) {
	db := setupE2EPostgres(t)
	brokers := setupE2EKafka(t)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	repo := storage.NewRepository(db)
	payment := storage.Payment{
		ID:               canonicalE2EPaymentID,
		MerchantID:       "m-e2e",
		IdempotencyKey:   "e2e-1",
		RequestHash:      "hash-e2e",
		ExpectedAmount:   100_000,
		ExpectedAsset:    "USDT",
		ExpectedDecimals: 2,
		Network:          "trc20",
	}
	require.NoError(t, repo.Create(ctx, payment))
	payment, err := repo.Transition(ctx, payment.ID, 1, domain.StateCreated, storage.TransitionPatch{})
	require.NoError(t, err)
	payment, err = repo.Transition(ctx, payment.ID, uint64(payment.StateVersion), domain.StatePending, storage.TransitionPatch{})
	require.NoError(t, err)

	publisher, err := kafkapub.New(brokers)
	require.NoError(t, err)
	defer publisher.Close()

	policy, err := domain.ParseAmountPolicy(`{"USDT":[{"below_minor":1000000,"confirmations":1}]}`, 0, 0)
	require.NoError(t, err)
	consumer := usecase.NewCanonicalConsumer(repo, zap.NewNop(), usecase.WithAmountPolicy(policy))
	processor := inbox.NewProcessor(usecase.ConsumerGroupCanonical, db, inbox.NewGuard(), consumer, publisher,
		inbox.NewRouter(usecase.TopicGatewayEventsCanonical, 3), zap.NewNop())
	topic := usecase.TopicGatewayEventsCanonical
	topics := []string{
		topic, topic + inbox.RetrySuffix5s, topic + inbox.RetrySuffix1m, topic + inbox.RetrySuffix10m,
	}
	runner, err := kafkaconsumer.New(brokers, usecase.ConsumerGroupCanonical, topics, processor, zap.NewNop())
	require.NoError(t, err)
	defer runner.Close()
	go func() { _ = runner.Run(ctx) }()

	producer, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	require.NoError(t, err)
	defer producer.Close()

	detected := canonicalRecord(t, eventsv1.CanonicalPaymentEventType_CANONICAL_PAYMENT_EVENT_TYPE_DETECTED, 1)
	require.NoError(t, producer.ProduceSync(ctx, detected).FirstErr())
	detectedPayment := waitForState(t, ctx, repo, domain.StateDetected)
	require.NotNil(t, detectedPayment.TxID)
	assert.Equal(t, "tx-e2e", *detectedPayment.TxID)

	// Дубликат той же записи: inbox не применяет событие повторно. Проверка
	// детерминирована порядком партиции: confirmed приходит после дубликата,
	// поэтому лишний self-переход дубликата увеличил бы версию дважды.
	require.NoError(t, producer.ProduceSync(ctx, detected).FirstErr())
	confirmed := canonicalRecord(t, eventsv1.CanonicalPaymentEventType_CANONICAL_PAYMENT_EVENT_TYPE_CONFIRMED, 3)
	require.NoError(t, producer.ProduceSync(ctx, confirmed).FirstErr())
	completedPayment := waitForState(t, ctx, repo, domain.StateCompleted)
	assert.Equal(t, 3, completedPayment.Confirmations)
	assert.Equal(t, detectedPayment.StateVersion+2, completedPayment.StateVersion,
		"дубликат не создал лишний переход; подтверждение завершило платеж по политике")

	// Устаревшее detected после терминального состояния - no-op.
	stale := canonicalRecord(t, eventsv1.CanonicalPaymentEventType_CANONICAL_PAYMENT_EVENT_TYPE_DETECTED, 1)
	require.NoError(t, producer.ProduceSync(ctx, stale).FirstErr())
	waitForInboxCount(t, ctx, db, 3)
	after, history, err := repo.Get(ctx, canonicalE2EPaymentID)
	require.NoError(t, err)
	assert.Equal(t, string(domain.StateCompleted), after.State)
	assert.Equal(t, completedPayment.StateVersion, after.StateVersion, "устаревшее событие не меняет состояние")
	assert.Len(t, history, 6, fmt.Sprintf("история: %d", len(history)))
}

// waitForInboxCount ждет появления ожидаемого числа inbox-строк: маркер того,
// что consumer забрал и обработал события (sleep не верифицирует обработку).
func waitForInboxCount(t *testing.T, ctx context.Context, db *gorm.DB, want int64) {
	t.Helper()
	for {
		var count int64
		require.NoError(t, db.Table("inbox").Count(&count).Error)
		if count >= want {
			return
		}
		select {
		case <-ctx.Done():
			require.FailNow(t, "inbox rows not reached", fmt.Sprintf("want=%d got=%d", want, count))
		default:
			time.Sleep(100 * time.Millisecond)
		}
	}
}
