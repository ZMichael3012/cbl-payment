//go:build integration

package storage_test

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	"github.com/cbl/cbl-kit/storagex"

	"github.com/cbl/cbl-payment/internal/domain"
	"github.com/cbl/cbl-payment/internal/storage"
)

// allStates задает канонические состояния автомата для перебора матрицы.
var allStates = []domain.State{
	domain.StateCreating,
	domain.StateCreated,
	domain.StatePending,
	domain.StateDetected,
	domain.StateConfirming,
	domain.StateCompleted,
	domain.StateExpired,
	domain.StateUnderpaid,
	domain.StateOverpaid,
	domain.StateFailed,
	domain.StateCancelled,
}

func setupPaymentDB(t *testing.T) *gorm.DB {
	t.Helper()
	ctx := context.Background()
	container, err := tcpostgres.Run(ctx,
		"postgres:16-alpine",
		tcpostgres.WithDatabase("payment"),
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

// newPayment возвращает валидный платеж в состоянии создания.
func newPayment(t *testing.T, key string) storage.Payment {
	t.Helper()
	id, err := uuid.NewV7()
	require.NoError(t, err)
	return storage.Payment{
		ID:               id.String(),
		MerchantID:       "m-1",
		IdempotencyKey:   key,
		RequestHash:      "hash-" + key,
		ExpectedAmount:   100_000,
		ExpectedAsset:    "USDT",
		ExpectedDecimals: 2,
		Network:          "trc20",
	}
}

// transition выполняет переход и возвращает обновленный платеж.
func transition(t *testing.T, repo *storage.Repository, id string, version uint64, to domain.State, patch storage.TransitionPatch) storage.Payment {
	t.Helper()
	payment, err := repo.Transition(context.Background(), id, version, to, patch)
	require.NoError(t, err)
	return payment
}

// paymentAtState доводит новый платеж до состояния chain.
func paymentAtState(t *testing.T, repo *storage.Repository, key string, chain ...domain.State) storage.Payment {
	t.Helper()
	payment := newPayment(t, key)
	require.NoError(t, repo.Create(context.Background(), payment))
	current := payment
	current.State = string(domain.StateCreating)
	current.StateVersion = 1
	for _, state := range chain {
		current = transition(t, repo, payment.ID, uint64(current.StateVersion), state, storage.TransitionPatch{})
	}
	return current
}

func TestIntegrationCreateAndHistory(t *testing.T) {
	db := setupPaymentDB(t)
	repo := storage.NewRepository(db)

	payment := newPayment(t, "key-1")
	require.NoError(t, repo.Create(context.Background(), payment))

	loaded, history, err := repo.Get(context.Background(), payment.ID)
	require.NoError(t, err)
	assert.Equal(t, string(domain.StateCreating), loaded.State)
	assert.Equal(t, int64(1), loaded.StateVersion)
	assert.Equal(t, int64(100_000), loaded.ExpectedAmount)
	assert.Equal(t, "USDT", loaded.ExpectedAsset)
	require.Len(t, history, 1)
	assert.Equal(t, "", history[0].FromState)
	assert.Equal(t, string(domain.StateCreating), history[0].ToState)
	assert.Equal(t, int64(1), history[0].StateVersion)

	_, _, err = repo.Get(context.Background(), uuid.Must(uuid.NewV7()).String())
	require.ErrorIs(t, err, storage.ErrNotFound)
}

func TestIntegrationDuplicateIdempotencyKey(t *testing.T) {
	db := setupPaymentDB(t)
	repo := storage.NewRepository(db)

	first := newPayment(t, "same-key")
	require.NoError(t, repo.Create(context.Background(), first))

	second := newPayment(t, "same-key")
	err := repo.Create(context.Background(), second)
	require.ErrorIs(t, err, storage.ErrDuplicateKey)
}

func TestIntegrationTransitionChain(t *testing.T) {
	db := setupPaymentDB(t)
	repo := storage.NewRepository(db)

	completed := time.Now().UTC().Truncate(time.Millisecond)
	payment := paymentAtState(t, repo, "chain-1",
		domain.StateCreated, domain.StatePending, domain.StateDetected, domain.StateConfirming)
	payment = transition(t, repo, payment.ID, uint64(payment.StateVersion), domain.StateCompleted, storage.TransitionPatch{
		CompletedAt: &completed,
	})
	assert.Equal(t, string(domain.StateCompleted), payment.State)
	assert.Equal(t, int64(6), payment.StateVersion)
	require.NotNil(t, payment.CompletedAt)

	_, history, err := repo.Get(context.Background(), payment.ID)
	require.NoError(t, err)
	require.Len(t, history, 6, "создание плюс пять переходов")
	for i, item := range history {
		assert.Equalf(t, int64(i+1), item.StateVersion, "версия записи %d", i)
	}
}

func TestIntegrationConcurrentTransitionOnlyOneWins(t *testing.T) {
	db := setupPaymentDB(t)
	repo := storage.NewRepository(db)

	payment := paymentAtState(t, repo, "race-1", domain.StateCreated)
	start := make(chan struct{})
	results := make([]error, 2)
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			<-start
			_, err := repo.Transition(context.Background(), payment.ID, uint64(payment.StateVersion), domain.StatePending, storage.TransitionPatch{})
			results[index] = err
		}(i)
	}
	close(start)
	wg.Wait()

	successes := 0
	conflicts := 0
	for _, err := range results {
		switch {
		case err == nil:
			successes++
		case errors.Is(err, domain.ErrStateConflict):
			conflicts++
		default:
			t.Fatalf("неожиданная ошибка: %v", err)
		}
	}
	assert.Equal(t, 1, successes, "ровно один переход выигрывает гонку")
	assert.Equal(t, 1, conflicts)

	loaded, history, err := repo.Get(context.Background(), payment.ID)
	require.NoError(t, err)
	assert.Equal(t, int64(3), loaded.StateVersion, "версия выросла один раз")
	assert.Len(t, history, 3)
}

func TestIntegrationStaleVersionRejected(t *testing.T) {
	db := setupPaymentDB(t)
	repo := storage.NewRepository(db)

	payment := paymentAtState(t, repo, "stale-1", domain.StateCreated, domain.StatePending)

	_, err := repo.Transition(context.Background(), payment.ID, 1, domain.StateDetected, storage.TransitionPatch{})
	require.ErrorIs(t, err, domain.ErrStateConflict, "устаревшая версия отклонена")

	loaded, history, err := repo.Get(context.Background(), payment.ID)
	require.NoError(t, err)
	assert.Equal(t, payment.StateVersion, loaded.StateVersion, "версия после отказа не выросла")
	assert.Len(t, history, int(payment.StateVersion), "журнал не получил лишних записей")
}

func TestIntegrationForbiddenAndTerminalTransitions(t *testing.T) {
	db := setupPaymentDB(t)
	repo := storage.NewRepository(db)

	payment := paymentAtState(t, repo, "forbid-1",
		domain.StateCreated, domain.StatePending, domain.StateDetected, domain.StateConfirming)

	_, err := repo.Transition(context.Background(), payment.ID, uint64(payment.StateVersion), domain.StateDetected, storage.TransitionPatch{})
	require.ErrorIs(t, err, domain.ErrForbiddenTransition, "переход назад запрещен")

	completed := paymentAtState(t, repo, "terminal-1",
		domain.StateCreated, domain.StatePending, domain.StateDetected, domain.StateConfirming, domain.StateCompleted)
	_, err = repo.Transition(context.Background(), completed.ID, uint64(completed.StateVersion), domain.StateFailed, storage.TransitionPatch{})
	require.ErrorIs(t, err, domain.ErrTerminalState, "из терминального перехода нет")
}

func TestIntegrationSelfTransitionUpdatable(t *testing.T) {
	db := setupPaymentDB(t)
	repo := storage.NewRepository(db)

	payment := paymentAtState(t, repo, "self-1",
		domain.StateCreated, domain.StatePending, domain.StateDetected, domain.StateConfirming)

	confirmations := 2
	updated := transition(t, repo, payment.ID, uint64(payment.StateVersion), domain.StateConfirming, storage.TransitionPatch{
		Confirmations: &confirmations,
	})
	assert.Equal(t, string(domain.StateConfirming), updated.State)
	assert.Equal(t, int64(6), updated.StateVersion, "самопереход увеличивает версию")
	assert.Equal(t, 2, updated.Confirmations)

	_, history, err := repo.Get(context.Background(), payment.ID)
	require.NoError(t, err)
	require.Len(t, history, 6)
	assert.Equal(t, string(domain.StateConfirming), history[5].FromState)
	assert.Equal(t, string(domain.StateConfirming), history[5].ToState)
}

func TestIntegrationDatabaseTriggerSecondBarrier(t *testing.T) {
	db := setupPaymentDB(t)
	repo := storage.NewRepository(db)

	payment := paymentAtState(t, repo, "barrier-1",
		domain.StateCreated, domain.StatePending, domain.StateDetected)

	// Прямой SQL в обход CAS и домена: переход назад обязан отклонить триггер.
	backward := db.Exec("UPDATE payments SET state = 'pending', state_version = state_version + 1 WHERE id = ?", payment.ID)
	require.Error(t, backward.Error)
	assert.Contains(t, backward.Error.Error(), "forbidden")

	// Перескок версии через прямой SQL отклоняется.
	skipped := db.Exec("UPDATE payments SET state = 'confirming', state_version = state_version + 5 WHERE id = ?", payment.ID)
	require.Error(t, skipped.Error)
	assert.Contains(t, skipped.Error.Error(), "state_version")

	// Неизменяемые поля защищены.
	immutable := db.Exec("UPDATE payments SET merchant_id = 'm-2', state = 'confirming', state_version = state_version + 1 WHERE id = ?", payment.ID)
	require.Error(t, immutable.Error)
	assert.Contains(t, immutable.Error.Error(), "immutable")
}

func TestIntegrationTerminalStateImmutableInDatabase(t *testing.T) {
	db := setupPaymentDB(t)
	repo := storage.NewRepository(db)

	completed := paymentAtState(t, repo, "frozen-1",
		domain.StateCreated, domain.StatePending, domain.StateDetected, domain.StateConfirming, domain.StateCompleted)

	// Самопереход в терминальном состоянии запрещен даже через прямой SQL.
	frozen := db.Exec("UPDATE payments SET confirmations = confirmations + 1, state_version = state_version + 1 WHERE id = ?", completed.ID)
	require.Error(t, frozen.Error)
	assert.Contains(t, frozen.Error.Error(), "is immutable")
}

func TestIntegrationTransitionsAppendOnly(t *testing.T) {
	db := setupPaymentDB(t)
	repo := storage.NewRepository(db)

	payment := paymentAtState(t, repo, "append-1", domain.StateCreated)

	updateErr := db.Exec("UPDATE payment_transitions SET reason = 'rewritten' WHERE payment_id = ?", payment.ID).Error
	require.Error(t, updateErr)
	assert.Contains(t, updateErr.Error(), "append-only")

	deleteErr := db.Exec("DELETE FROM payment_transitions WHERE payment_id = ?", payment.ID).Error
	require.Error(t, deleteErr)
	assert.Contains(t, deleteErr.Error(), "append-only")

	truncateErr := db.Exec("TRUNCATE payment_transitions").Error
	require.Error(t, truncateErr)
	assert.Contains(t, truncateErr.Error(), "append-only")
}

func TestIntegrationListWithCursorAndFilter(t *testing.T) {
	db := setupPaymentDB(t)
	repo := storage.NewRepository(db)
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		payment := newPayment(t, "list-key-"+string(rune('a'+i)))
		require.NoError(t, repo.Create(ctx, payment))
		time.Sleep(2 * time.Millisecond)
	}
	other := newPayment(t, "other-key")
	other.MerchantID = "m-2"
	require.NoError(t, repo.Create(ctx, other))

	first, cursor, err := repo.List(ctx, storage.ListFilter{MerchantID: "m-1", Limit: 2})
	require.NoError(t, err)
	require.Len(t, first, 2)
	require.NotEmpty(t, cursor)

	second, cursor2, err := repo.List(ctx, storage.ListFilter{MerchantID: "m-1", Limit: 2, Cursor: cursor})
	require.NoError(t, err)
	require.Len(t, second, 1)
	assert.Empty(t, cursor2)

	ids := map[string]struct{}{}
	for _, payment := range append(first, second...) {
		ids[payment.ID] = struct{}{}
		assert.Equal(t, "m-1", payment.MerchantID)
	}
	assert.Len(t, ids, 3, "страницы не пересекаются")

	state := domain.StateCreating
	filtered, _, err := repo.List(ctx, storage.ListFilter{MerchantID: "m-1", State: &state})
	require.NoError(t, err)
	assert.Len(t, filtered, 3)

	_, _, err = repo.List(ctx, storage.ListFilter{MerchantID: "m-1", Cursor: "not-base64!!"})
	require.Error(t, err)
}

func TestIntegrationFindExpiredAndStuck(t *testing.T) {
	db := setupPaymentDB(t)
	repo := storage.NewRepository(db)
	ctx := context.Background()

	past := time.Now().UTC().Add(-time.Minute)
	future := time.Now().UTC().Add(time.Hour)

	expired := newPayment(t, "exp-1")
	expired.ExpiresAt = &past
	require.NoError(t, repo.Create(ctx, expired))
	expired = transition(t, repo, expired.ID, 1, domain.StateCreated, storage.TransitionPatch{})

	alive := newPayment(t, "exp-2")
	alive.ExpiresAt = &future
	require.NoError(t, repo.Create(ctx, alive))

	found, err := repo.FindExpired(ctx, time.Now().UTC(), 10)
	require.NoError(t, err)
	require.Len(t, found, 1)
	assert.Equal(t, expired.ID, found[0].ID)

	stuck := paymentAtState(t, repo, "stuck-1", domain.StateCreated, domain.StatePending, domain.StateDetected)

	// updated_at ведет триггер переходов, поэтому «зависшесть» проверяется
	// порогом в будущем: платеж в detected старше любого прошлого порога.
	hung, err := repo.FindStuck(ctx, time.Now().UTC().Add(time.Hour), 10)
	require.NoError(t, err)
	require.Len(t, hung, 1)
	assert.Equal(t, stuck.ID, hung[0].ID)

	fresh, err := repo.FindStuck(ctx, time.Now().UTC().Add(-time.Hour), 10)
	require.NoError(t, err)
	assert.Empty(t, fresh, "свежий платеж не считается зависшим")
}

func TestIntegrationFindCreating(t *testing.T) {
	db := setupPaymentDB(t)
	repo := storage.NewRepository(db)
	ctx := context.Background()

	fresh := newPayment(t, "creating-fresh")
	require.NoError(t, repo.Create(ctx, fresh))

	stuck := newPayment(t, "creating-stuck")
	require.NoError(t, db.Exec(`INSERT INTO payments
		(id, merchant_id, idempotency_key, request_hash, state, state_version,
		 expected_amount, expected_asset, expected_decimals, network, created_at, updated_at)
		VALUES (?, 'm-1', 'creating-stuck', 'hash', 'creating', 1, 100000, 'USDT', 2, 'trc20',
		        now() - interval '10 minutes', now() - interval '10 minutes')`, stuck.ID).Error)

	advanced := paymentAtState(t, repo, "creating-advanced", domain.StateCreated)

	found, err := repo.FindCreating(ctx, time.Now().UTC().Add(-5*time.Minute), 10)
	require.NoError(t, err)
	require.Len(t, found, 1)
	assert.Equal(t, stuck.ID, found[0].ID)

	none, err := repo.FindCreating(ctx, time.Now().UTC().Add(-time.Hour), 10)
	require.NoError(t, err)
	assert.Empty(t, none)

	found, err = repo.FindCreating(ctx, time.Now().UTC().Add(time.Minute), 10)
	require.NoError(t, err)
	require.Len(t, found, 2, "свежее и зависшее создания")
	for _, row := range found {
		assert.NotEqual(t, advanced.ID, row.ID, "нетерминальное создание вне creating не попадает в выборку")
	}
}

// TestIntegrationFindCreatingBatchBeyondListLimit проверяет, что пакет фоновых
// выборок не урезается потолком пользовательского списка (200).
func TestIntegrationFindCreatingBatchBeyondListLimit(t *testing.T) {
	db := setupPaymentDB(t)
	repo := storage.NewRepository(db)
	ctx := context.Background()

	require.NoError(t, db.Exec(`INSERT INTO payments
		(id, merchant_id, idempotency_key, request_hash, state, state_version,
		 expected_amount, expected_asset, expected_decimals, network, created_at, updated_at)
		SELECT gen_random_uuid(), 'm-batch', 'batch-' || i, 'hash', 'creating', 1, 100000, 'USDT', 2, 'trc20',
		       now() - interval '1 hour', now() - interval '1 hour'
		FROM generate_series(1, 205) AS i`).Error)

	found, err := repo.FindCreating(ctx, time.Now().UTC(), 250)
	require.NoError(t, err)
	assert.Len(t, found, 205, "пакет фоновых выборок не режется лимитом списка")

	page, err := repo.FindCreating(ctx, time.Now().UTC(), 50)
	require.NoError(t, err)
	assert.Len(t, page, 50)
}

func TestIntegrationConstraints(t *testing.T) {
	db := setupPaymentDB(t)
	repo := storage.NewRepository(db)

	zero := newPayment(t, "zero-1")
	zero.ExpectedAmount = 0
	require.Error(t, repo.Create(context.Background(), zero), "нулевая сумма отклоняется CHECK-ом")

	badAsset := newPayment(t, "asset-1")
	badAsset.ExpectedAsset = "usdt"
	require.Error(t, repo.Create(context.Background(), badAsset), "актив в нижнем регистре отклоняется")

	// Create принудительно возвращает платеж в creating/версию 1.
	forced := newPayment(t, "forced-1")
	forced.State = string(domain.StateCompleted)
	forced.StateVersion = 7
	require.NoError(t, repo.Create(context.Background(), forced))
	loaded, history, err := repo.Get(context.Background(), forced.ID)
	require.NoError(t, err)
	assert.Equal(t, string(domain.StateCreating), loaded.State)
	assert.Equal(t, int64(1), loaded.StateVersion)
	require.Len(t, history, 1)

	// Прямой INSERT в обход автомата отклоняет BEFORE INSERT триггер.
	rawID, err := uuid.NewV7()
	require.NoError(t, err)
	raw := db.Exec(`INSERT INTO payments (id, merchant_id, idempotency_key, request_hash, state, state_version,
		expected_amount, expected_asset, expected_decimals, network)
		VALUES (?, 'm-1', 'raw-1', 'hash-raw', 'completed', 7, 100, 'USDT', 2, 'trc20')`, rawID.String())
	require.Error(t, raw.Error)
	assert.Contains(t, raw.Error.Error(), "creating")

	// Границы CHECK: шкала вне диапазона и отрицательная полученная сумма.
	badDecimals := newPayment(t, "decimals-1")
	badDecimals.ExpectedDecimals = 19
	require.Error(t, repo.Create(context.Background(), badDecimals), "decimals вне 0 - 18 отклоняются")

	negative := paymentAtState(t, repo, "negative-1", domain.StateCreated)
	negativeAmount := int64(-1)
	_, err = repo.Transition(context.Background(), negative.ID, uint64(negative.StateVersion), domain.StatePending, storage.TransitionPatch{
		ReceivedAmount: &negativeAmount,
	})
	require.Error(t, err, "отрицательная полученная сумма отклоняется CHECK-ом")
}

func TestIntegrationCompletedAtOnlyTerminalSuccess(t *testing.T) {
	db := setupPaymentDB(t)
	repo := storage.NewRepository(db)

	payment := paymentAtState(t, repo, "done-1", domain.StateCreated, domain.StatePending)
	completedAt := time.Now().UTC()
	_, err := repo.Transition(context.Background(), payment.ID, uint64(payment.StateVersion), domain.StateFailed, storage.TransitionPatch{
		CompletedAt: &completedAt,
	})
	require.Error(t, err, "completed_at допустим только в completed/underpaid/overpaid")

	ok := paymentAtState(t, repo, "done-2", domain.StateCreated, domain.StatePending, domain.StateDetected, domain.StateConfirming)
	updated := transition(t, repo, ok.ID, uint64(ok.StateVersion), domain.StateCompleted, storage.TransitionPatch{CompletedAt: &completedAt})
	require.NotNil(t, updated.CompletedAt)
}

func TestIntegrationTransitionsDictionary(t *testing.T) {
	db := setupPaymentDB(t)

	payment := newPayment(t, "dict-1")
	repo := storage.NewRepository(db)
	require.NoError(t, repo.Create(context.Background(), payment))

	fake := db.Exec(`INSERT INTO payment_transitions (payment_id, from_state, to_state, state_version)
		VALUES (?, '', 'nonsense', 2)`, payment.ID)
	require.Error(t, fake.Error, "неизвестное состояние не проходит словарь журнала")
}

// TestIntegrationTriggerMirrorsMatrix перебирает все пары состояний против
// триггера БД: разрешенные парой матрицы (и самопереходы в нетерминальных)
// должны проходить, остальные - отклоняться. Ловит дрейф Go-матрицы и SQL.
func TestIntegrationTriggerMirrorsMatrix(t *testing.T) {
	db := setupPaymentDB(t)
	repo := storage.NewRepository(db)

	chains := map[domain.State][]domain.State{
		domain.StateCreating:   {},
		domain.StateCreated:    {domain.StateCreated},
		domain.StatePending:    {domain.StateCreated, domain.StatePending},
		domain.StateDetected:   {domain.StateCreated, domain.StatePending, domain.StateDetected},
		domain.StateConfirming: {domain.StateCreated, domain.StatePending, domain.StateDetected, domain.StateConfirming},
		domain.StateCompleted:  {domain.StateCreated, domain.StatePending, domain.StateDetected, domain.StateConfirming, domain.StateCompleted},
		domain.StateExpired:    {domain.StateCreated, domain.StateExpired},
		domain.StateUnderpaid:  {domain.StateCreated, domain.StatePending, domain.StateDetected, domain.StateUnderpaid},
		domain.StateOverpaid:   {domain.StateCreated, domain.StatePending, domain.StateDetected, domain.StateOverpaid},
		domain.StateFailed:     {domain.StateFailed},
		domain.StateCancelled:  {domain.StateCancelled},
	}
	index := 0
	for _, from := range allStates {
		for _, to := range allStates {
			index++
			payment := paymentAtState(t, repo, fmt.Sprintf("mx-%d", index), chains[from]...)
			require.Equalf(t, string(from), payment.State, "цепочка доводит до %s", from)
			res := db.Exec("UPDATE payments SET state = ?, state_version = state_version + 1 WHERE id = ?", string(to), payment.ID)
			want := domain.AllowedTransition(from, to) || (from == to && !domain.Terminal(from))
			if want {
				require.NoErrorf(t, res.Error, "SQL-матрица должна разрешать %s -> %s", from, to)
			} else {
				require.Errorf(t, res.Error, "SQL-матрица должна запрещать %s -> %s", from, to)
			}
		}
	}
}
