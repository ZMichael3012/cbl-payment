//go:build integration

package storage_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cbl/cbl-payment/internal/domain"
	"github.com/cbl/cbl-payment/internal/storage"
)

func TestIntegrationQuoteFieldsRoundTrip(t *testing.T) {
	db := setupPaymentDB(t)
	repo := storage.NewRepository(db)
	ctx := context.Background()

	payment := newPayment(t, "quote-1")
	require.NoError(t, repo.Create(ctx, payment))

	quoteID := uuid.Must(uuid.NewV7()).String()
	rate := "78.125"
	lockedUntil := time.Now().UTC().Add(15 * time.Minute).Truncate(time.Microsecond)
	expiresAt := time.Now().UTC().Add(30 * time.Minute).Truncate(time.Microsecond)
	transition(t, repo, payment.ID, 1, domain.StateCreated, storage.TransitionPatch{
		QuoteID:          &quoteID,
		Rate:             &rate,
		QuoteLockedUntil: &lockedUntil,
		ExpiresAt:        &expiresAt,
	})

	loaded, history, err := repo.Get(ctx, payment.ID)
	require.NoError(t, err)
	require.NotNil(t, loaded.Rate)
	assert.Equal(t, rate, *loaded.Rate)
	require.NotNil(t, loaded.QuoteID)
	assert.Equal(t, quoteID, *loaded.QuoteID)
	require.NotNil(t, loaded.QuoteLockedUntil)
	assert.True(t, loaded.QuoteLockedUntil.Equal(lockedUntil), "locked_until survives the round-trip")
	require.Len(t, history, 2)
}

func TestIntegrationQuoteFieldsImmutableAfterFill(t *testing.T) {
	db := setupPaymentDB(t)
	repo := storage.NewRepository(db)
	ctx := context.Background()

	payment := newPayment(t, "quote-2")
	require.NoError(t, repo.Create(ctx, payment))

	quoteID := uuid.Must(uuid.NewV7()).String()
	rate := "1.5"
	lockedUntil := time.Now().UTC().Add(10 * time.Minute).Truncate(time.Microsecond)
	transition(t, repo, payment.ID, 1, domain.StateCreated, storage.TransitionPatch{
		QuoteID:          &quoteID,
		Rate:             &rate,
		QuoteLockedUntil: &lockedUntil,
	})

	// Once filled, the quote fields are immutable (fill-once, 4.8).
	otherRate := "1.6"
	_, err := repo.Transition(ctx, payment.ID, 2, domain.StateCreated, storage.TransitionPatch{Rate: &otherRate})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "immutable fields cannot change")

	otherLocked := lockedUntil.Add(time.Minute)
	_, err = repo.Transition(ctx, payment.ID, 2, domain.StateCreated, storage.TransitionPatch{QuoteLockedUntil: &otherLocked})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "immutable fields cannot change")

	// An unfilled quote field is filled later (a 4.2 creation may reach
	// created without locked_until).
	second := newPayment(t, "quote-3")
	require.NoError(t, repo.Create(ctx, second))
	transition(t, repo, second.ID, 1, domain.StateCreated, storage.TransitionPatch{PayAddress: strPtr("Taddr")})
	late := time.Now().UTC().Add(5 * time.Minute).Truncate(time.Microsecond)
	updated := transition(t, repo, second.ID, 2, domain.StateCreated, storage.TransitionPatch{QuoteLockedUntil: &late})
	require.NotNil(t, updated.QuoteLockedUntil)
	assert.True(t, updated.QuoteLockedUntil.Equal(late))
}

func TestIntegrationListPaymentsPeriodAndCursor(t *testing.T) {
	db := setupPaymentDB(t)
	repo := storage.NewRepository(db)
	ctx := context.Background()

	base := time.Now().UTC().Add(-3 * time.Hour).Truncate(time.Microsecond)
	ids := make([]string, 3)
	for i := range ids {
		payment := newPayment(t, fmt.Sprintf("period-%d", i))
		payment.MerchantID = "m-period"
		payment.State = string(domain.StateCreating)
		payment.StateVersion = 1
		payment.CreatedAt = base.Add(time.Duration(i) * time.Hour)
		require.NoError(t, db.Create(&payment).Error, "direct insert: created_at is immutable")
		ids[i] = payment.ID
	}
	other := newPayment(t, "period-other")
	other.MerchantID = "m-other"
	other.State = string(domain.StateCreating)
	other.StateVersion = 1
	other.CreatedAt = base
	require.NoError(t, db.Create(&other).Error)

	from := base.Add(30 * time.Minute)
	to := base.Add(2*time.Hour + 30*time.Minute)
	page, cursor, err := repo.List(ctx, storage.ListFilter{MerchantID: "m-period", CreatedFrom: &from, CreatedTo: &to})
	require.NoError(t, err)
	require.Len(t, page, 2, "period bounds: from inclusive, to exclusive")
	assert.Empty(t, cursor)
	assert.Equal(t, ids[2], page[0].ID, "created_at DESC order")
	assert.Equal(t, ids[1], page[1].ID)

	page, cursor, err = repo.List(ctx, storage.ListFilter{MerchantID: "m-period", CreatedFrom: &from, CreatedTo: &to, Limit: 1})
	require.NoError(t, err)
	require.Len(t, page, 1)
	require.NotEmpty(t, cursor)
	page, cursor, err = repo.List(ctx, storage.ListFilter{MerchantID: "m-period", CreatedFrom: &from, CreatedTo: &to, Limit: 1, Cursor: cursor})
	require.NoError(t, err)
	require.Len(t, page, 1)
	assert.Equal(t, ids[1], page[0].ID, "the pages do not overlap")
	assert.Empty(t, cursor)

	atFrom := base
	page, _, err = repo.List(ctx, storage.ListFilter{MerchantID: "m-period", CreatedFrom: &atFrom})
	require.NoError(t, err)
	require.Len(t, page, 3)

	atTo := base
	page, _, err = repo.List(ctx, storage.ListFilter{MerchantID: "m-period", CreatedTo: &atTo})
	require.NoError(t, err)
	assert.Empty(t, page, "an entry exactly on the to boundary is not included")

	all, _, err := repo.List(ctx, storage.ListFilter{MerchantID: "m-period"})
	require.NoError(t, err)
	require.Len(t, all, 3, "other merchants are not included in the list")
	for _, listed := range all {
		assert.Equal(t, "m-period", listed.MerchantID)
	}

	// A cursor from another query is accepted: the filter is built on
	// (created_at, id).
	firstPage, crossCursor, err := repo.List(ctx, storage.ListFilter{MerchantID: "m-period", Limit: 1})
	require.NoError(t, err)
	require.Len(t, firstPage, 1)
	require.NotEmpty(t, crossCursor)
	_, _, err = repo.List(ctx, storage.ListFilter{MerchantID: "m-other", Cursor: crossCursor})
	require.NoError(t, err, "a cursor from another query is not rejected")

	_, _, err = repo.List(ctx, storage.ListFilter{MerchantID: "m-period", Cursor: "junk"})
	require.ErrorIs(t, err, storage.ErrInvalidCursor)
}

// TestIntegrationGetTimelineLimited checks the server history limit: Get
// returns the last 500 transitions (protection against an unbounded response).
func TestIntegrationGetTimelineLimited(t *testing.T) {
	db := setupPaymentDB(t)
	repo := storage.NewRepository(db)
	ctx := context.Background()

	payment := newPayment(t, "timeline-1")
	payment.State = string(domain.StateCreating)
	payment.StateVersion = 1
	require.NoError(t, db.Create(&payment).Error)
	require.NoError(t, db.Create(&storage.Transition{
		PaymentID: payment.ID, FromState: "", ToState: "creating", StateVersion: 1, Reason: "created",
	}).Error)
	for version := int64(2); version <= 510; version++ {
		require.NoError(t, db.Create(&storage.Transition{
			PaymentID: payment.ID, FromState: "creating", ToState: "creating",
			StateVersion: version, Reason: "confirmations update",
		}).Error)
	}

	_, history, err := repo.Get(ctx, payment.ID)
	require.NoError(t, err)
	require.Len(t, history, 500, "the history is limited to the last 500 transitions")
	assert.Equal(t, int64(11), history[0].StateVersion, "the latest journal entries are returned")
	assert.Equal(t, int64(510), history[len(history)-1].StateVersion, "ascending version order")
}

// TestIntegrationListPaymentsLimitClamp checks the page size clamping.
func TestIntegrationListPaymentsLimitClamp(t *testing.T) {
	db := setupPaymentDB(t)
	repo := storage.NewRepository(db)
	ctx := context.Background()

	for i := 0; i < 205; i++ {
		payment := newPayment(t, fmt.Sprintf("clamp-%d", i))
		payment.MerchantID = "m-clamp"
		payment.State = string(domain.StateCreating)
		payment.StateVersion = 1
		require.NoError(t, db.Create(&payment).Error)
	}

	page, cursor, err := repo.List(ctx, storage.ListFilter{MerchantID: "m-clamp", Limit: 5000})
	require.NoError(t, err)
	require.Len(t, page, 200, "limit is clamped by the 200 ceiling")
	assert.NotEmpty(t, cursor, "there is a continuation after the page")
}

// strPtr returns a pointer to the string for transition patches.
func strPtr(value string) *string { return &value }
