//go:build integration

package usecase_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/cbl/cbl-payment/internal/domain"
	"github.com/cbl/cbl-payment/internal/storage"
	"github.com/cbl/cbl-payment/internal/usecase"
)

// newJanitorIntegrationPayment creates a payment and returns the storage record.
func newJanitorIntegrationPayment(t *testing.T, repo *storage.Repository, key string) storage.Payment {
	t.Helper()
	id, err := uuid.NewV7()
	require.NoError(t, err)
	payment := storage.Payment{
		ID:               id.String(),
		MerchantID:       "m-janitor",
		IdempotencyKey:   key,
		RequestHash:      "hash-" + key,
		ExpectedAmount:   100_000,
		ExpectedAsset:    "USDT",
		ExpectedDecimals: 2,
		Network:          "trc20",
	}
	require.NoError(t, repo.Create(context.Background(), payment))
	return payment
}

// janitorAt assembles a janitor with a clock shifted relative to real time.
func janitorAt(t *testing.T, repo *storage.Repository, now time.Time) *usecase.Janitor {
	t.Helper()
	janitor, err := usecase.NewJanitor(repo, zap.NewNop(), usecase.JanitorConfig{
		Clock: func() time.Time { return now },
	})
	require.NoError(t, err)
	return janitor
}

func TestIntegrationJanitorExpiresPayment(t *testing.T) {
	db := setupConsumerDB(t)
	repo := storage.NewRepository(db)
	ctx := context.Background()

	past := time.Now().UTC().Add(-time.Minute)
	payment := newJanitorIntegrationPayment(t, repo, "janitor-exp-1")
	payment, err := repo.Transition(ctx, payment.ID, 1, domain.StateCreated, storage.TransitionPatch{ExpiresAt: &past})
	require.NoError(t, err)

	janitor := janitorAt(t, repo, time.Now().UTC())
	result := janitor.Sweep(ctx)
	assert.Equal(t, 1, result.Expired)
	assert.Zero(t, result.Errors)

	updated, history, err := repo.Get(ctx, payment.ID)
	require.NoError(t, err)
	assert.Equal(t, string(domain.StateExpired), updated.State)
	last := history[len(history)-1]
	assert.Equal(t, "janitor expired", last.Reason)
	assert.Equal(t, string(domain.StateCreated), last.FromState)

	// A repeated sweep is a no-op: the selection by state does not return the expired payment.
	assert.Zero(t, janitorAt(t, repo, time.Now().UTC()).Sweep(ctx).Expired)
}

func TestIntegrationJanitorSkipsDetectedWithPastExpiry(t *testing.T) {
	db := setupConsumerDB(t)
	repo := storage.NewRepository(db)
	ctx := context.Background()

	past := time.Now().UTC().Add(-time.Minute)
	payment := newJanitorIntegrationPayment(t, repo, "janitor-det-1")
	payment, err := repo.Transition(ctx, payment.ID, 1, domain.StateCreated, storage.TransitionPatch{ExpiresAt: &past})
	require.NoError(t, err)
	payment, err = repo.Transition(ctx, payment.ID, uint64(payment.StateVersion), domain.StatePending, storage.TransitionPatch{})
	require.NoError(t, err)
	payment, err = repo.Transition(ctx, payment.ID, uint64(payment.StateVersion), domain.StateDetected, storage.TransitionPatch{})
	require.NoError(t, err)

	janitor := janitorAt(t, repo, time.Now().UTC())
	result := janitor.Sweep(ctx)
	assert.Zero(t, result.Expired, "a detected payment does not expire (KI-payment-08)")

	updated, _, err := repo.Get(ctx, payment.ID)
	require.NoError(t, err)
	assert.Equal(t, string(domain.StateDetected), updated.State)
	assert.Equal(t, payment.StateVersion, updated.StateVersion)
}

func TestIntegrationJanitorFailsStuckCreating(t *testing.T) {
	db := setupConsumerDB(t)
	repo := storage.NewRepository(db)
	ctx := context.Background()

	// Stuck creation: backdating created_at is allowed on INSERT (the trigger
	// protects immutable fields only for UPDATE).
	stuckID, err := uuid.NewV7()
	require.NoError(t, err)
	require.NoError(t, db.Exec(`INSERT INTO payments
		(id, merchant_id, idempotency_key, request_hash, state, state_version,
		 expected_amount, expected_asset, expected_decimals, network, created_at, updated_at)
		VALUES (?, 'm-janitor', 'janitor-stuck-1', 'hash', 'creating', 1, 100000, 'USDT', 2, 'trc20',
		        now() - interval '10 minutes', now() - interval '10 minutes')`, stuckID.String()).Error)

	fresh := newJanitorIntegrationPayment(t, repo, "janitor-fresh-1")

	janitor := janitorAt(t, repo, time.Now().UTC())
	result := janitor.Sweep(ctx)
	assert.Equal(t, 1, result.CreatingFailed)
	assert.Zero(t, result.Errors)

	updated, history, err := repo.Get(ctx, stuckID.String())
	require.NoError(t, err)
	assert.Equal(t, string(domain.StateFailed), updated.State)
	require.NotEmpty(t, history)
	assert.Equal(t, "janitor creating timeout", history[len(history)-1].Reason)

	stillFresh, _, err := repo.Get(ctx, fresh.ID)
	require.NoError(t, err)
	assert.Equal(t, string(domain.StateCreating), stillFresh.State, "a fresh creation is not closed")
}

func TestIntegrationJanitorStuckScanRespectsThreshold(t *testing.T) {
	db := setupConsumerDB(t)
	repo := storage.NewRepository(db)
	ctx := context.Background()

	detected := newJanitorIntegrationPayment(t, repo, "janitor-hung-1")
	detected, err := repo.Transition(ctx, detected.ID, 1, domain.StateCreated, storage.TransitionPatch{})
	require.NoError(t, err)
	detected, err = repo.Transition(ctx, detected.ID, uint64(detected.StateVersion), domain.StatePending, storage.TransitionPatch{})
	require.NoError(t, err)
	detected, err = repo.Transition(ctx, detected.ID, uint64(detected.StateVersion), domain.StateDetected, storage.TransitionPatch{})
	require.NoError(t, err)

	confirming := newJanitorIntegrationPayment(t, repo, "janitor-hung-2")
	confirming, err = repo.Transition(ctx, confirming.ID, 1, domain.StateCreated, storage.TransitionPatch{})
	require.NoError(t, err)
	confirming, err = repo.Transition(ctx, confirming.ID, uint64(confirming.StateVersion), domain.StatePending, storage.TransitionPatch{})
	require.NoError(t, err)
	confirming, err = repo.Transition(ctx, confirming.ID, uint64(confirming.StateVersion), domain.StateDetected, storage.TransitionPatch{})
	require.NoError(t, err)
	confirming, err = repo.Transition(ctx, confirming.ID, uint64(confirming.StateVersion), domain.StateConfirming, storage.TransitionPatch{})
	require.NoError(t, err)

	assert.Zero(t, janitorAt(t, repo, time.Now().UTC()).Sweep(ctx).Stuck,
		"fresh detected/confirming payments are not considered stuck")

	future := time.Now().UTC().Add(7 * time.Hour)
	result := janitorAt(t, repo, future).Sweep(ctx)
	assert.Equal(t, 2, result.Stuck, "updates older than the 6h threshold are considered stuck")

	for _, id := range []string{detected.ID, confirming.ID} {
		updated, _, err := repo.Get(ctx, id)
		require.NoError(t, err)
		assert.Equal(t, id, updated.ID)
		assert.NotEqual(t, string(domain.StateFailed), updated.State, "observation does not change the state")
	}
}

func TestIntegrationJanitorDoubleSweepSingleEffect(t *testing.T) {
	db := setupConsumerDB(t)
	repo := storage.NewRepository(db)
	ctx := context.Background()

	past := time.Now().UTC().Add(-time.Minute)
	payment := newJanitorIntegrationPayment(t, repo, "janitor-race-1")
	payment, err := repo.Transition(ctx, payment.ID, 1, domain.StateCreated, storage.TransitionPatch{ExpiresAt: &past})
	require.NoError(t, err)

	// Two sweeps in parallel: CAS guarantees a single effect regardless of
	// whether both instances saw the same snapshot.
	janitors := make([]*usecase.Janitor, 0, 2)
	for range 2 {
		janitors = append(janitors, janitorAt(t, repo, time.Now().UTC()))
	}
	results := make(chan usecase.JanitorResult, 2)
	for _, janitor := range janitors {
		go func() {
			results <- janitor.Sweep(ctx)
		}()
	}
	total := 0
	for range 2 {
		total += (<-results).Expired
	}
	assert.Equal(t, 1, total)

	_, history, err := repo.Get(ctx, payment.ID)
	require.NoError(t, err)
	expiredRows := 0
	for _, row := range history {
		if row.ToState == string(domain.StateExpired) {
			expiredRows++
		}
	}
	assert.Equal(t, 1, expiredRows)
}
