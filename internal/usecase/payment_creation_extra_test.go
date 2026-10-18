package usecase_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cbl/cbl-payment/internal/domain"
	"github.com/cbl/cbl-payment/internal/storage"
	"github.com/cbl/cbl-payment/internal/usecase"
)

func TestCreatePaymentStoreFailures(t *testing.T) {
	t.Parallel()

	t.Run("lookup unavailable", func(t *testing.T) {
		fixture := newCreationFixture(t)
		fixture.store.lookupErr = errors.New("db down")
		_, err := fixture.creation.Create(context.Background(), creationRequest())
		require.Error(t, err)
		assert.Empty(t, fixture.quote.calls)
	})

	t.Run("insert failure", func(t *testing.T) {
		fixture := newCreationFixture(t)
		fixture.store.createErr = errors.New("db down")
		_, err := fixture.creation.Create(context.Background(), creationRequest())
		require.Error(t, err)
	})

	t.Run("reload after insert fails", func(t *testing.T) {
		fixture := newCreationFixture(t)
		fixture.store.paymentErr = errors.New("db down")
		_, err := fixture.creation.Create(context.Background(), creationRequest())
		require.Error(t, err)
	})

	t.Run("duplicate race lookup fails", func(t *testing.T) {
		fixture := newCreationFixture(t)
		fixture.store.createErr = storage.ErrDuplicateKey
		fixture.store.lookupErr = errors.New("db down")
		_, err := fixture.creation.Create(context.Background(), creationRequest())
		require.Error(t, err)
	})
}

func TestCreatePaymentInvoiceEdgeCases(t *testing.T) {
	t.Parallel()
	createdRow := storage.Payment{
		ID: createPayment, MerchantID: createMerchant, IdempotencyKey: "key-1", RequestHash: testCreationHash(),
		State: string(domain.StateCreated), StateVersion: 2,
		ExpectedAmount: 15_000_000, ExpectedAsset: "USDT", ExpectedDecimals: 6, Network: "usdt_trc20",
	}

	t.Run("provider answer without destination", func(t *testing.T) {
		fixture := newCreationFixture(t)
		fixture.invoices.empty = true
		_, err := fixture.creation.Create(context.Background(), creationRequest())
		require.ErrorIs(t, err, domain.ErrDependencyTemporary)
		stored, err := fixture.store.Payment(context.Background(), createPayment)
		require.NoError(t, err)
		assert.Equal(t, string(domain.StateCreated), stored.State)
	})

	t.Run("pending transition conflict resolves to the fresh pending row", func(t *testing.T) {
		fixture := newCreationFixture(t)
		fixture.store.payments[createPayment] = createdRow
		fixture.store.byKey[createMerchant+"|key-1"] = createPayment
		fixture.store.transitionErrs = []error{domain.ErrStateConflict}
		pending := createdRow
		pending.State = string(domain.StatePending)
		pending.StateVersion = 3
		fixture.store.paymentOverride = &pending

		payment, err := fixture.creation.Create(context.Background(), creationRequest())
		require.NoError(t, err)
		assert.Equal(t, string(domain.StatePending), payment.State)
	})

	t.Run("pending transition conflict unresolved", func(t *testing.T) {
		fixture := newCreationFixture(t)
		fixture.store.payments[createPayment] = createdRow
		fixture.store.byKey[createMerchant+"|key-1"] = createPayment
		fixture.store.transitionErrs = []error{domain.ErrStateConflict}
		_, err := fixture.creation.Create(context.Background(), creationRequest())
		require.ErrorIs(t, err, domain.ErrStateConflict)
	})
}

func TestQuoteTransitionFailures(t *testing.T) {
	t.Parallel()
	creatingRow := storage.Payment{
		ID: createPayment, MerchantID: createMerchant, IdempotencyKey: "key-1", RequestHash: testCreationHash(),
		State: string(domain.StateCreating), StateVersion: 1,
		ExpectedAmount: 10_000_000, ExpectedAsset: "USDT", ExpectedDecimals: 6, Network: "usdt_trc20",
	}

	t.Run("conflict reloads and continues", func(t *testing.T) {
		fixture := newCreationFixture(t)
		// The initial read sees the creating row; the conflict reload (store.Payment)
		// sees the advanced created row of the concurrent writer.
		created := creatingRow
		created.State = string(domain.StateCreated)
		created.StateVersion = 2
		fixture.store.payments[createPayment] = created
		fixture.store.byKeyOverride = &creatingRow
		fixture.store.byKey[createMerchant+"|key-1"] = createPayment
		fixture.store.transitionErrs = []error{domain.ErrStateConflict}

		payment, err := fixture.creation.Create(context.Background(), creationRequest())
		require.NoError(t, err)
		assert.Equal(t, string(domain.StatePending), payment.State)
	})

	t.Run("generic transition failure", func(t *testing.T) {
		fixture := newCreationFixture(t)
		fixture.store.payments[createPayment] = creatingRow
		fixture.store.byKey[createMerchant+"|key-1"] = createPayment
		fixture.store.transitionErrs = []error{errors.New("db down")}
		_, err := fixture.creation.Create(context.Background(), creationRequest())
		require.Error(t, err)
	})
}

func TestCancelPaymentRetriesAndFailures(t *testing.T) {
	t.Parallel()

	t.Run("conflict retried", func(t *testing.T) {
		fixture := newCreationFixture(t)
		pending := storage.Payment{
			ID: createPayment, MerchantID: createMerchant, IdempotencyKey: "key-1", RequestHash: "hash",
			State: string(domain.StatePending), StateVersion: 3,
		}
		fixture.store.payments[createPayment] = pending
		fixture.store.transitionErrs = []error{domain.ErrStateConflict}
		cancelled, err := fixture.creation.Cancel(context.Background(), createPayment, "cancel-1")
		require.NoError(t, err)
		assert.Equal(t, string(domain.StateCancelled), cancelled.State)
	})

	t.Run("attempts exhausted", func(t *testing.T) {
		fixture := newCreationFixture(t)
		pending := storage.Payment{
			ID: createPayment, MerchantID: createMerchant, IdempotencyKey: "key-1", RequestHash: "hash",
			State: string(domain.StatePending), StateVersion: 3,
		}
		fixture.store.payments[createPayment] = pending
		fixture.store.transitionErrs = []error{domain.ErrStateConflict, domain.ErrStateConflict, domain.ErrStateConflict}
		_, err := fixture.creation.Cancel(context.Background(), createPayment, "cancel-1")
		require.ErrorIs(t, err, domain.ErrStateConflict)
	})

	t.Run("store failure", func(t *testing.T) {
		fixture := newCreationFixture(t)
		fixture.store.paymentErr = errors.New("db down")
		_, err := fixture.creation.Cancel(context.Background(), createPayment, "cancel-1")
		require.Error(t, err)
	})

	t.Run("invalid input", func(t *testing.T) {
		fixture := newCreationFixture(t)
		_, err := fixture.creation.Cancel(context.Background(), "not-a-uuid", "cancel-1")
		require.ErrorIs(t, err, domain.ErrInvalidCreation)
		_, err = fixture.creation.Cancel(context.Background(), createPayment, "  ")
		require.ErrorIs(t, err, domain.ErrInvalidCreation)
	})
}

func TestQuoteTTLFromExpiry(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 10, 18, 12, 0, 0, 0, time.UTC)

	// Short expiry: the lock is clamped to the minimum.
	fixture := newCreationFixture(t)
	request := creationRequest()
	request.ExpiresAt = now.Add(30 * time.Second)
	_, err := fixture.creation.Create(context.Background(), request)
	require.NoError(t, err)
	assert.Equal(t, uint32(60), fixture.quote.calls[0].ttlSeconds)

	// Mid expiry: the remaining seconds travel as-is.
	fixture = newCreationFixture(t)
	request = creationRequest()
	request.ExpiresAt = now.Add(5 * time.Minute)
	_, err = fixture.creation.Create(context.Background(), request)
	require.NoError(t, err)
	assert.Equal(t, uint32(300), fixture.quote.calls[0].ttlSeconds)

	// Long expiry: clamped to the quote service cap.
	fixture = newCreationFixture(t)
	request = creationRequest()
	request.ExpiresAt = now.Add(6 * time.Hour)
	_, err = fixture.creation.Create(context.Background(), request)
	require.NoError(t, err)
	assert.Equal(t, uint32(900), fixture.quote.calls[0].ttlSeconds)
}

func TestFailPaymentTransitionRetries(t *testing.T) {
	t.Parallel()

	t.Run("conflict retried against the fresh row", func(t *testing.T) {
		fixture := newCreationFixture(t)
		fixture.quote.errs = []error{domain.ErrQuoteRejected}
		fixture.store.transitionErrs = []error{domain.ErrStateConflict}
		_, err := fixture.creation.Create(context.Background(), creationRequest())
		require.ErrorIs(t, err, domain.ErrQuoteRejected)
		stored, err := fixture.store.Payment(context.Background(), createPayment)
		require.NoError(t, err)
		assert.Equal(t, string(domain.StateFailed), stored.State, "the failed transition is retried after the conflict")
	})

	t.Run("non-conflict failure leaves the row for the janitor", func(t *testing.T) {
		fixture := newCreationFixture(t)
		fixture.quote.errs = []error{domain.ErrQuoteRejected}
		fixture.store.transitionErrs = []error{errors.New("db down")}
		_, err := fixture.creation.Create(context.Background(), creationRequest())
		require.ErrorIs(t, err, domain.ErrQuoteRejected)
		stored, err := fixture.store.Payment(context.Background(), createPayment)
		require.NoError(t, err)
		assert.Equal(t, string(domain.StateCreating), stored.State)
	})
}

// testCreationHash is the request hash of the standard creation request of the
// fixtures.
func testCreationHash() string {
	request := creationRequest()
	return domain.RequestHash(createMerchant, request.Amount, request.PayAsset, request.Network, request.ExpiresAt, 6)
}

func TestCancelPaymentFromCreatingAndCreated(t *testing.T) {
	t.Parallel()
	for _, state := range []domain.State{domain.StateCreating, domain.StateCreated} {
		fixture := newCreationFixture(t)
		fixture.store.payments[createPayment] = storage.Payment{
			ID: createPayment, MerchantID: createMerchant, IdempotencyKey: "key-1", RequestHash: testCreationHash(),
			State: string(state), StateVersion: 2,
		}
		cancelled, err := fixture.creation.Cancel(context.Background(), createPayment, "cancel-1")
		require.NoError(t, err, state)
		assert.Equal(t, string(domain.StateCancelled), cancelled.State, state)
	}
}

func TestCreatePaymentCanonicalizesMerchant(t *testing.T) {
	t.Parallel()
	fixture := newCreationFixture(t)
	upper := creationRequest()
	upper.MerchantID = "01A14F70-0000-7000-8000-0000000000AA"
	_, err := fixture.creation.Create(context.Background(), upper)
	require.NoError(t, err)
	stored, err := fixture.store.Payment(context.Background(), createPayment)
	require.NoError(t, err)
	assert.Equal(t, strings.ToLower(upper.MerchantID), stored.MerchantID, "the merchant id is stored canonical")

	replay, err := fixture.creation.Create(context.Background(), upper)
	require.NoError(t, err)
	assert.Equal(t, createPayment, replay.ID)
	assert.Len(t, fixture.invoices.calls, 1)
}

func TestCreatePaymentScaleConfigChangeConflicts(t *testing.T) {
	t.Parallel()
	fixture := newCreationFixture(t)
	request := creationRequest()
	request.PayAsset = "BTC"
	_, err := fixture.creation.Create(context.Background(), request)
	require.NoError(t, err)

	// The same store served by a service with a different pay scale: the
	// request hash no longer matches and the replay fails closed.
	other, err := usecase.NewPaymentCreation(fixture.store, fixture.quote, fixture.invoices, usecase.CreationConfig{
		AssetScales: map[string]uint32{"USDT": 6, "BTC": 7},
		Now:         func() time.Time { return fixture.now },
		NewID:       func() string { return createPayment },
	})
	require.NoError(t, err)
	_, err = other.Create(context.Background(), request)
	require.ErrorIs(t, err, domain.ErrIdempotencyConflict)
}

func TestCreatePaymentInvoiceRejectionNoAdopt(t *testing.T) {
	t.Parallel()
	fixture := newCreationFixture(t)
	// The payment already reached `created`; the invoice rejection hits a CAS
	// conflict and must NOT adopt the fresh row (a concurrent attempt may be
	// mid-invoice), so the row stays created.
	created := storage.Payment{
		ID: createPayment, MerchantID: createMerchant, IdempotencyKey: "key-1", RequestHash: testCreationHash(),
		State: string(domain.StateCreated), StateVersion: 2,
		ExpectedAmount: 15_000_000, ExpectedAsset: "USDT", ExpectedDecimals: 6, Network: "usdt_trc20",
	}
	fixture.store.payments[createPayment] = created
	fixture.store.byKey[createMerchant+"|key-1"] = createPayment
	fixture.invoices.errs = []error{domain.ErrInvoiceRejected}
	fixture.store.transitionErrs = []error{domain.ErrStateConflict}

	_, err := fixture.creation.Create(context.Background(), creationRequest())
	require.ErrorIs(t, err, domain.ErrInvoiceRejected)
	stored, err := fixture.store.Payment(context.Background(), createPayment)
	require.NoError(t, err)
	assert.Equal(t, string(domain.StateCreated), stored.State,
		"the rejected invoice never adopts the fresh row: a concurrent attempt may be mid-invoice")
}

func TestCreatePaymentPrefersCheckoutDestination(t *testing.T) {
	t.Parallel()
	fixture := newCreationFixture(t)
	fixture.invoices.both = true

	payment, err := fixture.creation.Create(context.Background(), creationRequest())
	require.NoError(t, err)
	assert.Nil(t, payment.PayAddress, "exactly one destination reaches the merchant")
	require.NotNil(t, payment.CheckoutURL)
	assert.Equal(t, "https://checkout.example/1", *payment.CheckoutURL)
}

func TestCancelCanonicalizesPaymentID(t *testing.T) {
	t.Parallel()
	fixture := newCreationFixture(t)
	fixture.store.payments[createPayment] = storage.Payment{
		ID: createPayment, MerchantID: createMerchant, IdempotencyKey: "key-1", RequestHash: testCreationHash(),
		State: string(domain.StatePending), StateVersion: 3,
	}
	cancelled, err := fixture.creation.Cancel(context.Background(), strings.ToUpper(createPayment), "cancel-1")
	require.NoError(t, err)
	assert.Equal(t, string(domain.StateCancelled), cancelled.State)
}
