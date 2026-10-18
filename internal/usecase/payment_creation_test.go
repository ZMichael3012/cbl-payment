package usecase_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cbl/cbl-kit/money"

	"github.com/cbl/cbl-payment/internal/domain"
	"github.com/cbl/cbl-payment/internal/storage"
	"github.com/cbl/cbl-payment/internal/usecase"
)

const (
	createMerchant = "01a14f70-0000-7000-8000-0000000000aa"
	createPayment  = "01a14f70-0000-7000-8000-000000000701"
)

// lookupResult scripts one ByIdempotencyKey answer.
type lookupResult struct {
	payment storage.Payment
	err     error
}

// fakeCreationStore is an in-memory payment store with the idempotency
// semantics of the real schema.
type fakeCreationStore struct {
	payments        map[string]storage.Payment
	byKey           map[string]string
	transits        []string
	createErr       error
	lookupErr       error
	paymentErr      error
	transitionErrs  []error
	paymentOverride *storage.Payment
	byKeyOverride   *storage.Payment
	lookupQueue     []lookupResult
}

func newFakeCreationStore() *fakeCreationStore {
	return &fakeCreationStore{payments: map[string]storage.Payment{}, byKey: map[string]string{}}
}

func (f *fakeCreationStore) Create(_ context.Context, payment storage.Payment) error {
	if f.createErr != nil {
		err := f.createErr
		f.createErr = nil
		return err
	}
	key := payment.MerchantID + "|" + payment.IdempotencyKey
	if _, taken := f.byKey[key]; taken {
		return storage.ErrDuplicateKey
	}
	payment.State = string(domain.StateCreating)
	payment.StateVersion = 1
	payment.CreatedAt = time.Now().UTC()
	payment.UpdatedAt = payment.CreatedAt
	f.payments[payment.ID] = payment
	f.byKey[key] = payment.ID
	return nil
}

func (f *fakeCreationStore) ByIdempotencyKey(_ context.Context, merchantID, key string) (storage.Payment, error) {
	if f.lookupErr != nil {
		return storage.Payment{}, f.lookupErr
	}
	if len(f.lookupQueue) > 0 {
		next := f.lookupQueue[0]
		f.lookupQueue = f.lookupQueue[1:]
		return next.payment, next.err
	}
	if f.byKeyOverride != nil {
		return *f.byKeyOverride, nil
	}
	id, ok := f.byKey[merchantID+"|"+key]
	if !ok {
		return storage.Payment{}, storage.ErrNotFound
	}
	return f.payments[id], nil
}

func (f *fakeCreationStore) Payment(_ context.Context, id string) (storage.Payment, error) {
	if f.paymentErr != nil {
		return storage.Payment{}, f.paymentErr
	}
	if f.paymentOverride != nil {
		return *f.paymentOverride, nil
	}
	payment, ok := f.payments[id]
	if !ok {
		return storage.Payment{}, storage.ErrNotFound
	}
	return payment, nil
}

func (f *fakeCreationStore) Transition(_ context.Context, id string, expectedVersion uint64, to domain.State, patch storage.TransitionPatch) (storage.Payment, error) {
	if len(f.transitionErrs) > 0 {
		err := f.transitionErrs[0]
		f.transitionErrs = f.transitionErrs[1:]
		if err != nil {
			return storage.Payment{}, err
		}
	}
	payment, ok := f.payments[id]
	if !ok {
		return storage.Payment{}, storage.ErrNotFound
	}
	if uint64(payment.StateVersion) != expectedVersion { //nolint:gosec // state_version is constrained by CHECK (>= 1)
		return storage.Payment{}, domain.ErrStateConflict
	}
	payment.State = string(to)
	payment.StateVersion++
	if patch.QuoteID != nil {
		payment.QuoteID = patch.QuoteID
	}
	if patch.Rate != nil {
		payment.Rate = patch.Rate
	}
	if patch.QuoteLockedUntil != nil {
		payment.QuoteLockedUntil = patch.QuoteLockedUntil
	}
	if patch.ExpectedAmount != nil {
		payment.ExpectedAmount = *patch.ExpectedAmount
	}
	if patch.ExpectedAsset != nil {
		payment.ExpectedAsset = *patch.ExpectedAsset
	}
	if patch.ExpectedDecimals != nil {
		payment.ExpectedDecimals = *patch.ExpectedDecimals
	}
	if patch.PayAddress != nil {
		payment.PayAddress = patch.PayAddress
	}
	if patch.GatewayID != nil {
		payment.GatewayID = patch.GatewayID
	}
	if patch.Provider != nil {
		payment.Provider = patch.Provider
	}
	if patch.CheckoutURL != nil {
		payment.CheckoutURL = patch.CheckoutURL
	}
	f.payments[id] = payment
	f.transits = append(f.transits, payment.State+"#"+patch.Reason)
	return payment, nil
}

type quoteCall struct {
	from, to   string
	amount     money.Money
	ttlSeconds uint32
}

type fakeQuote struct {
	calls  []quoteCall
	lock   usecase.QuoteLock
	err    error
	errs   []error
	nextID int
}

func (f *fakeQuote) LockQuote(_ context.Context, req usecase.QuoteLockRequest) (usecase.QuoteLock, error) {
	f.calls = append(f.calls, quoteCall{req.FromAsset, req.ToAsset, req.Amount, req.TTLSeconds})
	if len(f.errs) > 0 {
		err := f.errs[0]
		f.errs = f.errs[1:]
		if err != nil {
			return usecase.QuoteLock{}, err
		}
	}
	if f.err != nil {
		return usecase.QuoteLock{}, f.err
	}
	f.nextID++
	return f.lock, nil
}

type invoiceCall struct {
	paymentID string
	amount    money.Money
	network   string
}

type fakeInvoices struct {
	calls []invoiceCall
	errs  []error
	err   error
	empty bool
	both  bool
}

func (f *fakeInvoices) CreateInvoice(_ context.Context, req usecase.InvoiceRequest) (usecase.InvoiceResult, error) {
	f.calls = append(f.calls, invoiceCall{req.PaymentID, req.Amount, req.Network})
	if len(f.errs) > 0 {
		err := f.errs[0]
		f.errs = f.errs[1:]
		if err != nil {
			return usecase.InvoiceResult{}, err
		}
	}
	if f.err != nil {
		return usecase.InvoiceResult{}, f.err
	}
	if f.empty {
		return usecase.InvoiceResult{GatewayInvoiceID: "gw-inv-1"}, nil
	}
	if f.both {
		return usecase.InvoiceResult{
			GatewayInvoiceID: "gw-inv-1",
			Provider:         "coingate",
			Address:          "TE2eAddress",
			CheckoutURL:      "https://checkout.example/1",
		}, nil
	}
	return usecase.InvoiceResult{
		GatewayInvoiceID: "gw-inv-1",
		Provider:         "coingate",
		CheckoutURL:      "https://checkout.example/1",
	}, nil
}

type creationFixture struct {
	creation *usecase.PaymentCreation
	store    *fakeCreationStore
	quote    *fakeQuote
	invoices *fakeInvoices
	now      time.Time
}

func newCreationFixture(t *testing.T) *creationFixture {
	t.Helper()
	fixture := &creationFixture{
		store:    newFakeCreationStore(),
		quote:    &fakeQuote{lock: usecase.QuoteLock{QuoteID: "q-1", Rate: "1.5"}},
		invoices: &fakeInvoices{},
		now:      time.Date(2026, 10, 18, 12, 0, 0, 0, time.UTC),
	}
	fixture.quote.lock.LockedUntil = fixture.now.Add(5 * time.Minute)
	creation, err := usecase.NewPaymentCreation(fixture.store, fixture.quote, fixture.invoices, usecase.CreationConfig{
		AssetScales: map[string]uint32{"USDT": 6, "BTC": 8},
		MaxExpiry:   7 * 24 * time.Hour,
		Now:         func() time.Time { return fixture.now },
		NewID:       func() string { return createPayment },
	})
	require.NoError(t, err)
	fixture.creation = creation
	return fixture
}

func creationRequest() usecase.CreatePaymentRequest {
	return usecase.CreatePaymentRequest{
		MerchantID:     createMerchant,
		IdempotencyKey: "key-1",
		Amount:         money.MustParse("10", "USDT", 6),
		PayAsset:       "USDT",
		Network:        "usdt_trc20",
		ExpiresAt:      time.Date(2026, 10, 18, 13, 0, 0, 0, time.UTC),
	}
}

func TestCreatePaymentHappyPath(t *testing.T) {
	t.Parallel()
	fixture := newCreationFixture(t)

	payment, err := fixture.creation.Create(context.Background(), creationRequest())
	require.NoError(t, err)
	assert.Equal(t, createPayment, payment.ID)
	assert.Equal(t, string(domain.StatePending), payment.State)
	assert.Equal(t, int64(15_000_000), payment.ExpectedAmount, "the locked pay amount replaces the request placeholder")
	assert.Equal(t, "USDT", payment.ExpectedAsset)
	require.NotNil(t, payment.QuoteID)
	assert.Equal(t, "q-1", *payment.QuoteID)
	require.NotNil(t, payment.Rate)
	assert.Equal(t, "1.5", *payment.Rate)
	require.NotNil(t, payment.GatewayID)
	assert.Equal(t, "gw-inv-1", *payment.GatewayID)
	require.NotNil(t, payment.Provider)
	assert.Equal(t, "coingate", *payment.Provider)
	require.NotNil(t, payment.CheckoutURL)
	assert.Equal(t, "https://checkout.example/1", *payment.CheckoutURL)
	assert.Equal(t, []string{"created#quote locked", "pending#invoice created"}, fixture.store.transits)

	require.Len(t, fixture.quote.calls, 1)
	assert.Equal(t, "USDT", fixture.quote.calls[0].from)
	assert.Equal(t, "USDT", fixture.quote.calls[0].to)
	require.Len(t, fixture.invoices.calls, 1)
	assert.Equal(t, createPayment, fixture.invoices.calls[0].paymentID)
	assert.Equal(t, int64(15_000_000), fixture.invoices.calls[0].amount.MinorUnits)
	assert.Equal(t, "usdt_trc20", fixture.invoices.calls[0].network)
}

func TestCreatePaymentReplay(t *testing.T) {
	t.Parallel()
	fixture := newCreationFixture(t)
	first, err := fixture.creation.Create(context.Background(), creationRequest())
	require.NoError(t, err)

	second, err := fixture.creation.Create(context.Background(), creationRequest())
	require.NoError(t, err)
	assert.Equal(t, first.ID, second.ID)
	assert.Len(t, fixture.quote.calls, 1, "a replay never locks a second quote")
	assert.Len(t, fixture.invoices.calls, 1, "a replay never creates a second invoice")
}

func TestCreatePaymentResumesAfterConnectorTimeout(t *testing.T) {
	t.Parallel()
	fixture := newCreationFixture(t)
	fixture.invoices.errs = []error{domain.ErrDependencyTemporary}

	_, err := fixture.creation.Create(context.Background(), creationRequest())
	require.ErrorIs(t, err, domain.ErrDependencyTemporary)
	stored, err := fixture.store.Payment(context.Background(), createPayment)
	require.NoError(t, err)
	assert.Equal(t, string(domain.StateCreated), stored.State, "the quote is locked, the address is expected")

	payment, err := fixture.creation.Create(context.Background(), creationRequest())
	require.NoError(t, err)
	assert.Equal(t, string(domain.StatePending), payment.State)
	assert.Len(t, fixture.quote.calls, 1, "the quote is not locked twice")
	assert.Len(t, fixture.invoices.calls, 2, "only the invoice step is retried")
}

func TestCreatePaymentResumesAfterQuoteTimeout(t *testing.T) {
	t.Parallel()
	fixture := newCreationFixture(t)
	fixture.quote.errs = []error{domain.ErrDependencyTemporary}

	_, err := fixture.creation.Create(context.Background(), creationRequest())
	require.ErrorIs(t, err, domain.ErrDependencyTemporary)
	stored, err := fixture.store.Payment(context.Background(), createPayment)
	require.NoError(t, err)
	assert.Equal(t, string(domain.StateCreating), stored.State, "the quote did not make it in time")
	assert.Equal(t, int64(10_000_000), stored.ExpectedAmount, "the placeholder carries the request amount until the quote")

	payment, err := fixture.creation.Create(context.Background(), creationRequest())
	require.NoError(t, err)
	assert.Equal(t, string(domain.StatePending), payment.State)
	assert.Equal(t, int64(15_000_000), payment.ExpectedAmount)
	assert.Len(t, fixture.quote.calls, 2, "the retry locks the quote again")
	assert.Len(t, fixture.invoices.calls, 1)
}

func TestCreatePaymentKeyReuseConflict(t *testing.T) {
	t.Parallel()
	fixture := newCreationFixture(t)
	_, err := fixture.creation.Create(context.Background(), creationRequest())
	require.NoError(t, err)

	different := creationRequest()
	different.Amount = money.MustParse("11", "USDT", 6)
	_, err = fixture.creation.Create(context.Background(), different)
	require.ErrorIs(t, err, domain.ErrIdempotencyConflict)
	assert.Len(t, fixture.quote.calls, 1)
	assert.Len(t, fixture.invoices.calls, 1)
}

func TestCreatePaymentQuoteRejectionFailsPayment(t *testing.T) {
	t.Parallel()
	fixture := newCreationFixture(t)
	fixture.quote.errs = []error{domain.ErrQuoteRejected}

	_, err := fixture.creation.Create(context.Background(), creationRequest())
	require.ErrorIs(t, err, domain.ErrQuoteRejected)
	stored, err := fixture.store.Payment(context.Background(), createPayment)
	require.NoError(t, err)
	assert.Equal(t, string(domain.StateFailed), stored.State)
	assert.Contains(t, fixture.store.transits, "failed#quote rejected", "the failure carries the fixed reason")

	payment, err := fixture.creation.Create(context.Background(), creationRequest())
	require.NoError(t, err)
	assert.Equal(t, string(domain.StateFailed), payment.State, "a failed creation replays as is")
	assert.Len(t, fixture.quote.calls, 1, "no retry revives a failed creation")
}

func TestCreatePaymentInvoiceRejectionFailsPayment(t *testing.T) {
	t.Parallel()
	fixture := newCreationFixture(t)
	fixture.invoices.errs = []error{domain.ErrInvoiceRejected}

	_, err := fixture.creation.Create(context.Background(), creationRequest())
	require.ErrorIs(t, err, domain.ErrInvoiceRejected)
	stored, err := fixture.store.Payment(context.Background(), createPayment)
	require.NoError(t, err)
	assert.Equal(t, string(domain.StateFailed), stored.State)
	assert.Contains(t, fixture.store.transits, "failed#invoice rejected", "the failure carries the fixed reason")
}

func TestCreatePaymentDuplicateRaceResumes(t *testing.T) {
	t.Parallel()
	fixture := newCreationFixture(t)
	// The initial lookup misses; the insert loses the race (the unique index
	// fires); the concurrent lookup returns the winner's created row and the
	// flow resumes at the invoice step.
	createdRow := storage.Payment{
		ID: createPayment, MerchantID: createMerchant, IdempotencyKey: "key-1",
		RequestHash: testCreationHash(),
		State:       string(domain.StateCreated), StateVersion: 2,
		ExpectedAmount: 15_000_000, ExpectedAsset: "USDT", ExpectedDecimals: 6,
		Network: "usdt_trc20",
	}
	fixture.store.createErr = storage.ErrDuplicateKey
	fixture.store.lookupQueue = []lookupResult{
		{err: storage.ErrNotFound},
		{payment: createdRow},
	}
	fixture.store.payments[createPayment] = createdRow
	fixture.store.byKey[createMerchant+"|key-1"] = createPayment

	payment, err := fixture.creation.Create(context.Background(), creationRequest())
	require.NoError(t, err)
	assert.Equal(t, string(domain.StatePending), payment.State)
	assert.Empty(t, fixture.quote.calls, "the quote is already locked")
	assert.Len(t, fixture.invoices.calls, 1)
}

func TestCreatePaymentRejectsBrokenRate(t *testing.T) {
	t.Parallel()
	fixture := newCreationFixture(t)
	fixture.quote.lock.Rate = "0"

	_, err := fixture.creation.Create(context.Background(), creationRequest())
	require.ErrorIs(t, err, domain.ErrInvalidRate)
	stored, err := fixture.store.Payment(context.Background(), createPayment)
	require.NoError(t, err)
	assert.Equal(t, string(domain.StateCreating), stored.State, "a broken rate leaves the payment in creating")
	assert.Empty(t, fixture.invoices.calls)
}

func TestCreatePaymentUnknownPayAssetScale(t *testing.T) {
	t.Parallel()
	fixture := newCreationFixture(t)
	request := creationRequest()
	request.PayAsset = "XRP"

	_, err := fixture.creation.Create(context.Background(), request)
	require.ErrorIs(t, err, domain.ErrUnknownAssetScale)
	assert.Empty(t, fixture.quote.calls)
	assert.Empty(t, fixture.store.payments, "an unknown asset must not create a row")
}

func TestCreatePaymentValidation(t *testing.T) {
	t.Parallel()
	fixture := newCreationFixture(t)
	cases := map[string]func(*usecase.CreatePaymentRequest){
		"missing merchant": func(r *usecase.CreatePaymentRequest) { r.MerchantID = "" },
		"missing key":      func(r *usecase.CreatePaymentRequest) { r.IdempotencyKey = "" },
		"zero amount":      func(r *usecase.CreatePaymentRequest) { r.Amount = money.Money{} },
		"bad network":      func(r *usecase.CreatePaymentRequest) { r.Network = "" },
		"expired":          func(r *usecase.CreatePaymentRequest) { r.ExpiresAt = fixture.now.Add(-time.Minute) },
		"expiry too far":   func(r *usecase.CreatePaymentRequest) { r.ExpiresAt = fixture.now.Add(8 * 24 * time.Hour) },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			request := creationRequest()
			mutate(&request)
			_, err := fixture.creation.Create(context.Background(), request)
			require.Error(t, err, name)
			assert.Empty(t, fixture.quote.calls, name)
			assert.Empty(t, fixture.store.payments, name)
		})
	}
}

func TestCreatePaymentHashNormalizationReplays(t *testing.T) {
	t.Parallel()
	fixture := newCreationFixture(t)
	first := creationRequest()
	first.PayAsset = " btc "
	first.Network = " USDT_TRC20 "
	_, err := fixture.creation.Create(context.Background(), first)
	require.NoError(t, err)

	second := creationRequest()
	second.PayAsset = "btc"
	second.Network = "usdt_trc20"
	payment, err := fixture.creation.Create(context.Background(), second)
	require.NoError(t, err)
	assert.Equal(t, createPayment, payment.ID)
	assert.Len(t, fixture.invoices.calls, 1, "casing and whitespace variations replay")
}

func TestCancelPaymentFlow(t *testing.T) {
	t.Parallel()
	fixture := newCreationFixture(t)
	_, err := fixture.creation.Create(context.Background(), creationRequest())
	require.NoError(t, err)

	cancelled, err := fixture.creation.Cancel(context.Background(), createPayment, "cancel-1")
	require.NoError(t, err)
	assert.Equal(t, string(domain.StateCancelled), cancelled.State)

	again, err := fixture.creation.Cancel(context.Background(), createPayment, "cancel-2")
	require.NoError(t, err)
	assert.Equal(t, string(domain.StateCancelled), again.State)
	assert.Equal(t, cancelled.StateVersion, again.StateVersion, "a repeated cancel does not transition")
}

func TestCancelPaymentAfterDetectionRejected(t *testing.T) {
	t.Parallel()
	fixture := newCreationFixture(t)
	payment := storage.Payment{
		ID: createPayment, MerchantID: createMerchant, IdempotencyKey: "key-1",
		RequestHash: "hash", State: string(domain.StateDetected), StateVersion: 5,
		ExpectedAmount: 10_000_000, ExpectedAsset: "USDT", ExpectedDecimals: 6,
		Network: "usdt_trc20",
	}
	fixture.store.payments[createPayment] = payment
	fixture.store.byKey[createMerchant+"|key-1"] = createPayment

	_, err := fixture.creation.Cancel(context.Background(), createPayment, "cancel-1")
	require.ErrorIs(t, err, domain.ErrInvoiceNotCancellable)
	assert.Empty(t, fixture.store.transits)
}

func TestCancelPaymentMissing(t *testing.T) {
	t.Parallel()
	fixture := newCreationFixture(t)
	_, err := fixture.creation.Cancel(context.Background(), "01a14f70-0000-7000-8000-0000000007ff", "cancel-1")
	require.ErrorIs(t, err, storage.ErrNotFound)
}
