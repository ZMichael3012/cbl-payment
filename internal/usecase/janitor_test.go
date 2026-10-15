package usecase_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/cbl/cbl-payment/internal/domain"
	"github.com/cbl/cbl-payment/internal/storage"
	"github.com/cbl/cbl-payment/internal/usecase"
)

// janitorFixedNow задает фиксированные часы юнит-тестов janitor-а.
var janitorFixedNow = time.Date(2026, 10, 15, 12, 0, 0, 0, time.UTC)

// janitorTransition описывает зафиксированный переход фейкового хранилища.
type janitorTransition struct {
	id              string
	expectedVersion uint64
	to              domain.State
	reason          string
}

// janitorFindCall описывает аргументы выборки фейкового хранилища.
type janitorFindCall struct {
	section string
	cutoff  time.Time
	limit   int
}

// fakeJanitorStore подменяет выборки и переходы janitor-а.
type fakeJanitorStore struct {
	expired       []storage.Payment
	creating      []storage.Payment
	stuck         []storage.Payment
	findErr       map[string]error
	transitionErr map[string]error
	transitions   []janitorTransition
	findCalls     []janitorFindCall
}

// FindExpired возвращает подготовленные счета с истекшим сроком.
func (f *fakeJanitorStore) FindExpired(_ context.Context, before time.Time, limit int) ([]storage.Payment, error) {
	f.findCalls = append(f.findCalls, janitorFindCall{section: "expired", cutoff: before, limit: limit})
	if err := f.findErr["expired"]; err != nil {
		return nil, err
	}
	return f.expired, nil
}

// FindCreating возвращает подготовленные зависшие создания.
func (f *fakeJanitorStore) FindCreating(_ context.Context, olderThan time.Time, limit int) ([]storage.Payment, error) {
	f.findCalls = append(f.findCalls, janitorFindCall{section: "creating", cutoff: olderThan, limit: limit})
	if err := f.findErr["creating"]; err != nil {
		return nil, err
	}
	return f.creating, nil
}

// FindStuck возвращает подготовленные зависшие платежи.
func (f *fakeJanitorStore) FindStuck(_ context.Context, olderThan time.Time, limit int) ([]storage.Payment, error) {
	f.findCalls = append(f.findCalls, janitorFindCall{section: "stuck", cutoff: olderThan, limit: limit})
	if err := f.findErr["stuck"]; err != nil {
		return nil, err
	}
	return f.stuck, nil
}

// Transition фиксирует переход или возвращает заданную ошибку.
func (f *fakeJanitorStore) Transition(_ context.Context, id string, expectedVersion uint64, to domain.State, patch storage.TransitionPatch) (storage.Payment, error) {
	if err := f.transitionErr[id]; err != nil {
		return storage.Payment{}, err
	}
	f.transitions = append(f.transitions, janitorTransition{
		id:              id,
		expectedVersion: expectedVersion,
		to:              to,
		reason:          patch.Reason,
	})
	return storage.Payment{ID: id, State: string(to), StateVersion: int64(expectedVersion) + 1}, nil //nolint:gosec // версии в тесте малы
}

// newJanitor собирает janitor с фиксированными часами.
func newJanitor(t *testing.T, store usecase.JanitorStore) *usecase.Janitor {
	t.Helper()
	janitor, err := usecase.NewJanitor(store, zap.NewNop(), usecase.JanitorConfig{
		Clock: func() time.Time { return janitorFixedNow },
	})
	require.NoError(t, err)
	return janitor
}

// duePayment собирает платеж в заданном состоянии с указанным сроком.
func duePayment(id string, state domain.State, expiresAt *time.Time) storage.Payment {
	return storage.Payment{
		ID:             id,
		State:          string(state),
		StateVersion:   3,
		ExpiresAt:      expiresAt,
		ExpectedAmount: 100_000,
		ExpectedAsset:  "USDT",
	}
}

func TestJanitorConfigValidation(t *testing.T) {
	t.Parallel()

	store := &fakeJanitorStore{}
	_, err := usecase.NewJanitor(store, nil, usecase.JanitorConfig{Interval: -time.Second})
	require.ErrorIs(t, err, usecase.ErrInvalidJanitorConfig)

	_, err = usecase.NewJanitor(store, nil, usecase.JanitorConfig{Batch: 5000})
	require.ErrorIs(t, err, usecase.ErrInvalidJanitorConfig)

	_, err = usecase.NewJanitor(store, nil, usecase.JanitorConfig{CreatingAfter: -time.Minute})
	require.ErrorIs(t, err, usecase.ErrInvalidJanitorConfig)

	_, err = usecase.NewJanitor(store, nil, usecase.JanitorConfig{StuckAfter: -time.Hour})
	require.ErrorIs(t, err, usecase.ErrInvalidJanitorConfig)

	_, err = usecase.NewJanitor(store, nil, usecase.JanitorConfig{})
	require.NoError(t, err, "нулевая конфигурация - значения по умолчанию")
}

func TestJanitorSweepExpiresDuePayments(t *testing.T) {
	t.Parallel()

	past := janitorFixedNow.Add(-time.Minute)
	store := &fakeJanitorStore{
		expired: []storage.Payment{
			duePayment("p-1", domain.StateCreated, &past),
			duePayment("p-2", domain.StatePending, &past),
		},
	}
	result := newJanitor(t, store).Sweep(context.Background())

	assert.Equal(t, 2, result.Expired)
	assert.Zero(t, result.Errors)
	require.Len(t, store.transitions, 2)
	for _, transition := range store.transitions {
		assert.Equal(t, domain.StateExpired, transition.to)
		assert.Equal(t, "janitor expired", transition.reason)
		assert.Equal(t, uint64(3), transition.expectedVersion)
	}
}

func TestJanitorSweepSkipsSnapshotNotDue(t *testing.T) {
	t.Parallel()

	future := janitorFixedNow.Add(time.Minute)
	past := janitorFixedNow.Add(-time.Minute)
	store := &fakeJanitorStore{
		expired: []storage.Payment{
			duePayment("p-null", domain.StatePending, nil),
			duePayment("p-future", domain.StateCreated, &future),
			duePayment("p-detected", domain.StateDetected, &past),
		},
	}
	result := newJanitor(t, store).Sweep(context.Background())

	assert.Zero(t, result.Expired)
	assert.Zero(t, result.Errors)
	assert.Empty(t, store.transitions, "двойная проверка ShouldExpire отсекает снимок")
}

func TestJanitorSweepBoundaryEquality(t *testing.T) {
	t.Parallel()

	store := &fakeJanitorStore{
		// expires_at ровно now - счет считается истекшим.
		expired: []storage.Payment{duePayment("p-eq", domain.StatePending, &janitorFixedNow)},
		creating: []storage.Payment{
			// created_at ровно на границе таймаута (5m) - считается зависшим.
			{ID: "p-bound", State: string(domain.StateCreating), StateVersion: 1,
				CreatedAt: janitorFixedNow.Add(-5 * time.Minute)},
		},
	}
	result := newJanitor(t, store).Sweep(context.Background())

	assert.Equal(t, 1, result.Expired)
	assert.Equal(t, 1, result.CreatingFailed)
}

func TestJanitorSweepFinderArguments(t *testing.T) {
	t.Parallel()

	store := &fakeJanitorStore{}
	newJanitor(t, store).Sweep(context.Background())

	require.Len(t, store.findCalls, 3)
	assert.Equal(t, janitorFindCall{section: "expired", cutoff: janitorFixedNow, limit: usecase.DefaultJanitorBatch}, store.findCalls[0])
	assert.Equal(t, janitorFindCall{section: "creating", cutoff: janitorFixedNow.Add(-usecase.DefaultCreatingTimeout), limit: usecase.DefaultJanitorBatch}, store.findCalls[1])
	assert.Equal(t, janitorFindCall{section: "stuck", cutoff: janitorFixedNow.Add(-usecase.DefaultStuckTimeout), limit: usecase.DefaultJanitorBatch}, store.findCalls[2])
}

func TestJanitorSweepExpireConflictSkipped(t *testing.T) {
	t.Parallel()

	past := janitorFixedNow.Add(-time.Minute)
	store := &fakeJanitorStore{
		expired: []storage.Payment{
			duePayment("p-race", domain.StatePending, &past),
			duePayment("p-ok", domain.StatePending, &past),
		},
		transitionErr: map[string]error{"p-race": domain.ErrStateConflict},
	}
	result := newJanitor(t, store).Sweep(context.Background())

	assert.Equal(t, 1, result.Expired)
	assert.Equal(t, 1, result.Conflicts)
	assert.Zero(t, result.Errors, "конфликт версии - не ошибка прохода")
	require.Len(t, store.transitions, 1)
	assert.Equal(t, "p-ok", store.transitions[0].id)
}

func TestJanitorSweepTransitionErrorContinues(t *testing.T) {
	t.Parallel()

	past := janitorFixedNow.Add(-time.Minute)
	boom := errors.New("boom")
	store := &fakeJanitorStore{
		expired: []storage.Payment{
			duePayment("p-fail", domain.StatePending, &past),
			duePayment("p-ok", domain.StatePending, &past),
		},
		transitionErr: map[string]error{"p-fail": boom},
	}
	result := newJanitor(t, store).Sweep(context.Background())

	assert.Equal(t, 1, result.Expired)
	assert.Equal(t, 1, result.Errors)
	require.Len(t, store.transitions, 1)
	assert.Equal(t, "p-ok", store.transitions[0].id, "проход продолжается после ошибки платежа")
}

func TestJanitorSweepFailsStuckCreating(t *testing.T) {
	t.Parallel()

	store := &fakeJanitorStore{
		creating: []storage.Payment{
			{ID: "p-stuck", State: string(domain.StateCreating), StateVersion: 1, CreatedAt: janitorFixedNow.Add(-10 * time.Minute)},
			{ID: "p-fresh", State: string(domain.StateCreating), StateVersion: 1, CreatedAt: janitorFixedNow.Add(-time.Minute)},
		},
	}
	result := newJanitor(t, store).Sweep(context.Background())

	assert.Equal(t, 1, result.CreatingFailed)
	assert.Zero(t, result.Errors)
	require.Len(t, store.transitions, 1)
	assert.Equal(t, "p-stuck", store.transitions[0].id, "свежее создание не закрывается")
	assert.Equal(t, domain.StateFailed, store.transitions[0].to)
	assert.Equal(t, "janitor creating timeout", store.transitions[0].reason)
}

func TestJanitorSweepReportsStuckWithoutTransitions(t *testing.T) {
	t.Parallel()

	store := &fakeJanitorStore{
		stuck: []storage.Payment{
			{ID: "p-detected", State: string(domain.StateDetected), StateVersion: 4},
			{ID: "p-confirming", State: string(domain.StateConfirming), StateVersion: 5},
		},
	}
	result := newJanitor(t, store).Sweep(context.Background())

	assert.Equal(t, 2, result.Stuck)
	assert.Zero(t, result.Errors)
	assert.Empty(t, store.transitions, "денежные состояния не терминализируются автоматически")
}

func TestJanitorSweepFindErrorSkipsSection(t *testing.T) {
	t.Parallel()

	store := &fakeJanitorStore{
		findErr:  map[string]error{"expired": errors.New("db down")},
		creating: []storage.Payment{{ID: "p-stuck", State: string(domain.StateCreating), StateVersion: 1, CreatedAt: janitorFixedNow.Add(-10 * time.Minute)}},
	}
	result := newJanitor(t, store).Sweep(context.Background())

	assert.Equal(t, 1, result.Errors, "ошибка выборки учтена")
	assert.Zero(t, result.Expired)
	assert.Equal(t, 1, result.CreatingFailed, "следующие секции прохода выполняются")
}

func TestJanitorSweepCancelledContext(t *testing.T) {
	t.Parallel()

	store := &fakeJanitorStore{
		expired: []storage.Payment{duePayment("p-1", domain.StatePending, ptrTime(janitorFixedNow.Add(-time.Minute)))},
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	result := newJanitor(t, store).Sweep(ctx)

	assert.Equal(t, usecase.JanitorResult{}, result)
	assert.Empty(t, store.transitions)
}

func TestJanitorRunStopsOnContextCancel(t *testing.T) {
	t.Parallel()

	janitor := newJanitor(t, &fakeJanitorStore{})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	done := make(chan struct{})
	go func() {
		janitor.Run(ctx)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("janitor не остановился по отмене контекста")
	}
}

// ptrTime возвращает указатель на время.
func ptrTime(value time.Time) *time.Time { return &value }
