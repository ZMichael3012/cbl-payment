//go:build integration

package storage_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cbl/cbl-payment/internal/domain"
	"github.com/cbl/cbl-payment/internal/storage"
)

func TestIntegrationByIdempotencyKey(t *testing.T) {
	db := setupPaymentDB(t)
	repo := storage.NewRepository(db)
	ctx := context.Background()
	expires := time.Now().UTC().Add(time.Hour)
	payment := storage.Payment{
		ID:               uuid.Must(uuid.NewV7()).String(),
		MerchantID:       "merchant-1",
		IdempotencyKey:   "key-1",
		RequestHash:      "hash-1",
		ExpectedAmount:   10_000_000,
		ExpectedAsset:    "USDT",
		ExpectedDecimals: 6,
		Network:          "usdt_trc20",
		ExpiresAt:        &expires,
	}
	require.NoError(t, repo.Create(ctx, payment))

	stored, err := repo.ByIdempotencyKey(ctx, "merchant-1", "key-1")
	require.NoError(t, err)
	assert.Equal(t, payment.ID, stored.ID)
	assert.Equal(t, string(domain.StateCreating), stored.State)
	assert.EqualValues(t, 1, stored.StateVersion)
	assert.Equal(t, int64(10_000_000), stored.ExpectedAmount, "the placeholder carries the request amount")

	_, err = repo.ByIdempotencyKey(ctx, "merchant-1", "other")
	require.ErrorIs(t, err, storage.ErrNotFound)

	// The creating -> created transition fills the locked pay amount and the
	// quote fields (the single expected fill of migration 00007).
	quoteID, rate := "q-1", "1.5"
	lockedUntil := time.Now().UTC().Add(5 * time.Minute)
	expectedAmount, expectedAsset, expectedDecimals := int64(15_000_000), "USDT", 6
	updated, err := repo.Transition(ctx, payment.ID, uint64(stored.StateVersion), domain.StateCreated, storage.TransitionPatch{
		QuoteID:          &quoteID,
		Rate:             &rate,
		QuoteLockedUntil: &lockedUntil,
		ExpectedAmount:   &expectedAmount,
		ExpectedAsset:    &expectedAsset,
		ExpectedDecimals: &expectedDecimals,
		Reason:           "quote locked",
	})
	require.NoError(t, err)
	assert.Equal(t, string(domain.StateCreated), updated.State)
	assert.Equal(t, int64(15_000_000), updated.ExpectedAmount)
	require.NotNil(t, updated.QuoteID)
	assert.Equal(t, "q-1", *updated.QuoteID)

	// Any other transition keeps the expected fields immutable.
	otherAmount := int64(1)
	address, gatewayID, provider := "addr-1", "gw-1", "coingate"
	_, err = repo.Transition(ctx, payment.ID, uint64(updated.StateVersion), domain.StatePending, storage.TransitionPatch{
		ExpectedAmount: &otherAmount,
		PayAddress:     &address,
		GatewayID:      &gatewayID,
		Provider:       &provider,
		Reason:         "invoice created",
	})
	require.Error(t, err, "the guard forbids expected changes outside creating -> created")

	final, err := repo.Transition(ctx, payment.ID, uint64(updated.StateVersion), domain.StatePending, storage.TransitionPatch{
		PayAddress: &address,
		GatewayID:  &gatewayID,
		Provider:   &provider,
		Reason:     "invoice created",
	})
	require.NoError(t, err)
	assert.Equal(t, string(domain.StatePending), final.State)
	require.NotNil(t, final.PayAddress)
	assert.Equal(t, "addr-1", *final.PayAddress)
}

func TestIntegrationTransitionGuardKeepsParentImmutable(t *testing.T) {
	db := setupPaymentDB(t)
	repo := storage.NewRepository(db)
	ctx := context.Background()
	expires := time.Now().UTC().Add(time.Hour)
	first := storage.Payment{
		ID: uuid.Must(uuid.NewV7()).String(), MerchantID: "merchant-1", IdempotencyKey: "parent-1",
		RequestHash: "hash-1", ExpectedAmount: 1_000_000, ExpectedAsset: "USDT", ExpectedDecimals: 6,
		Network: "usdt_trc20", ExpiresAt: &expires,
	}
	second := first
	second.ID = uuid.Must(uuid.NewV7()).String()
	second.IdempotencyKey = "parent-2"
	require.NoError(t, repo.Create(ctx, first))
	require.NoError(t, repo.Create(ctx, second))

	// The 00007 guard rebuild keeps the 00006 parent-link immutability: a
	// version-bumping update may not re-parent a payment.
	err := db.Exec("UPDATE payments SET parent_payment_id = ?, state_version = state_version + 1 WHERE id = ?",
		second.ID, first.ID).Error
	require.Error(t, err, "the parent link is immutable (00006 clause preserved by 00007)")
	assert.Contains(t, err.Error(), "immutable fields")
}
