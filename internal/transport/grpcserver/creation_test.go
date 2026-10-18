package grpcserver

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/cbl/cbl-protos/gen/go/billing/common/v1"
	paymentv1 "github.com/cbl/cbl-protos/gen/go/billing/payment/v1"

	"github.com/cbl/cbl-kit/grpcmw"
	kitmoney "github.com/cbl/cbl-kit/money"

	"github.com/cbl/cbl-payment/internal/domain"
	"github.com/cbl/cbl-payment/internal/storage"
	"github.com/cbl/cbl-payment/internal/usecase"
)

const unitPaymentID = "01a14f70-0000-7000-8000-000000000701"

// unitStore is a minimal in-memory creation store.
type unitStore struct {
	payment storage.Payment
	missing bool
}

func (s *unitStore) Create(_ context.Context, payment storage.Payment) error {
	payment.State = string(domain.StateCreating)
	payment.StateVersion = 1
	s.payment = payment
	return nil
}

func (s *unitStore) ByIdempotencyKey(context.Context, string, string) (storage.Payment, error) {
	if s.missing || s.payment.ID == "" {
		return storage.Payment{}, storage.ErrNotFound
	}
	return s.payment, nil
}

func (s *unitStore) Payment(context.Context, string) (storage.Payment, error) {
	if s.missing || s.payment.ID == "" {
		return storage.Payment{}, storage.ErrNotFound
	}
	return s.payment, nil
}

func (s *unitStore) Transition(_ context.Context, _ string, _ uint64, to domain.State, patch storage.TransitionPatch) (storage.Payment, error) {
	s.payment.State = string(to)
	s.payment.StateVersion++
	if patch.QuoteID != nil {
		s.payment.QuoteID = patch.QuoteID
	}
	if patch.Rate != nil {
		s.payment.Rate = patch.Rate
	}
	if patch.QuoteLockedUntil != nil {
		s.payment.QuoteLockedUntil = patch.QuoteLockedUntil
	}
	if patch.ExpectedAmount != nil {
		s.payment.ExpectedAmount = *patch.ExpectedAmount
	}
	if patch.ExpectedAsset != nil {
		s.payment.ExpectedAsset = *patch.ExpectedAsset
	}
	if patch.ExpectedDecimals != nil {
		s.payment.ExpectedDecimals = *patch.ExpectedDecimals
	}
	if patch.PayAddress != nil {
		s.payment.PayAddress = patch.PayAddress
	}
	if patch.CheckoutURL != nil {
		s.payment.CheckoutURL = patch.CheckoutURL
	}
	if patch.GatewayID != nil {
		s.payment.GatewayID = patch.GatewayID
	}
	if patch.Provider != nil {
		s.payment.Provider = patch.Provider
	}
	return s.payment, nil
}

type unitQuote struct{}

func (unitQuote) LockQuote(context.Context, usecase.QuoteLockRequest) (usecase.QuoteLock, error) {
	return usecase.QuoteLock{QuoteID: "q-1", Rate: "1.5", LockedUntil: time.Now().UTC().Add(5 * time.Minute)}, nil
}

type unitInvoices struct{}

func (unitInvoices) CreateInvoice(context.Context, usecase.InvoiceRequest) (usecase.InvoiceResult, error) {
	return usecase.InvoiceResult{GatewayInvoiceID: "gw-1", Provider: "coingate", Address: "TE2eAddress"}, nil
}

func unitCreation(t *testing.T, store *unitStore) *Payment {
	t.Helper()
	creation, err := usecase.NewPaymentCreation(store, unitQuote{}, unitInvoices{}, usecase.CreationConfig{
		AssetScales: map[string]uint32{"USDT": 6},
		NewID:       func() string { return unitPaymentID },
	})
	require.NoError(t, err)
	return NewPayment(nil, creation)
}

func unitCreateRequest() *paymentv1.CreatePaymentRequest {
	return &paymentv1.CreatePaymentRequest{
		IdempotencyKey: "key-1",
		MerchantId:     "01a14f70-0000-7000-8000-0000000000aa",
		Amount:         &commonv1.Money{MinorUnits: 10_000_000, Asset: "USDT", Decimals: 6},
		PayAsset:       "USDT",
		Network:        "usdt_trc20",
		ExpiresAt:      timestamppb.New(time.Now().UTC().Add(time.Hour)),
	}
}

func TestCreatePaymentValidationBranches(t *testing.T) {
	t.Parallel()
	cases := map[string]func(*paymentv1.CreatePaymentRequest){
		"bad merchant": func(r *paymentv1.CreatePaymentRequest) { r.MerchantId = "nope" },
		"long merchant": func(r *paymentv1.CreatePaymentRequest) {
			r.MerchantId = "01a14f70-0000-7000-8000-0000000000aa" + string(make([]byte, 300))
		},
		"empty key":     func(r *paymentv1.CreatePaymentRequest) { r.IdempotencyKey = "" },
		"nil amount":    func(r *paymentv1.CreatePaymentRequest) { r.Amount = nil },
		"zero amount":   func(r *paymentv1.CreatePaymentRequest) { r.Amount.MinorUnits = 0 },
		"bad decimals":  func(r *paymentv1.CreatePaymentRequest) { r.Amount.Decimals = 19 },
		"bad asset":     func(r *paymentv1.CreatePaymentRequest) { r.Amount.Asset = "U" },
		"bad pay asset": func(r *paymentv1.CreatePaymentRequest) { r.PayAsset = "" },
		"empty network": func(r *paymentv1.CreatePaymentRequest) { r.Network = " " },
		"missing expiry": func(r *paymentv1.CreatePaymentRequest) {
			r.ExpiresAt = nil
		},
		"past expiry": func(r *paymentv1.CreatePaymentRequest) {
			r.ExpiresAt = timestamppb.New(time.Now().UTC().Add(-time.Minute))
		},
		"far expiry": func(r *paymentv1.CreatePaymentRequest) {
			r.ExpiresAt = timestamppb.New(time.Now().UTC().Add(8 * 24 * time.Hour))
		},
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			server := unitCreation(t, &unitStore{})
			request := unitCreateRequest()
			mutate(request)
			_, err := server.CreatePayment(context.Background(), request)
			require.Error(t, err, name)
			assert.Equal(t, codes.InvalidArgument, status.Code(grpcmw.MapError(err)), name)
		})
	}
}

func TestCreatePaymentDisabledWithoutWiring(t *testing.T) {
	t.Parallel()
	server := NewPayment(nil, nil)
	_, err := server.CreatePayment(context.Background(), unitCreateRequest())
	require.Equal(t, codes.Unavailable, status.Code(grpcmw.MapError(err)))
	_, err = server.CancelPayment(context.Background(), &paymentv1.CancelPaymentRequest{PaymentId: unitPaymentID, IdempotencyKey: "k"})
	require.Equal(t, codes.Unavailable, status.Code(grpcmw.MapError(err)))
}

func TestCreatePaymentHappyResponse(t *testing.T) {
	t.Parallel()
	server := unitCreation(t, &unitStore{})
	response, err := server.CreatePayment(context.Background(), unitCreateRequest())
	require.NoError(t, err)
	assert.Equal(t, unitPaymentID, response.GetPaymentId())
	assert.Equal(t, paymentv1.PaymentState_PAYMENT_STATE_PENDING, response.GetState())
	assert.Equal(t, int64(15_000_000), response.GetExpectedAmount().GetMinorUnits())
	assert.Equal(t, "q-1", response.GetQuoteId())
	assert.Equal(t, "1.5", response.GetRate())
	assert.NotNil(t, response.GetLockedUntil())
	assert.NotNil(t, response.GetExpiresAt())
}

func TestCancelPaymentBranches(t *testing.T) {
	t.Parallel()

	t.Run("invalid payment id", func(t *testing.T) {
		server := unitCreation(t, &unitStore{})
		_, err := server.CancelPayment(context.Background(), &paymentv1.CancelPaymentRequest{PaymentId: "nope", IdempotencyKey: "k"})
		assert.Equal(t, codes.InvalidArgument, status.Code(grpcmw.MapError(err)))
	})

	t.Run("invalid key", func(t *testing.T) {
		server := unitCreation(t, &unitStore{})
		_, err := server.CancelPayment(context.Background(), &paymentv1.CancelPaymentRequest{PaymentId: unitPaymentID, IdempotencyKey: ""})
		assert.Equal(t, codes.InvalidArgument, status.Code(grpcmw.MapError(err)))
	})

	t.Run("missing payment", func(t *testing.T) {
		server := unitCreation(t, &unitStore{missing: true})
		_, err := server.CancelPayment(context.Background(), &paymentv1.CancelPaymentRequest{PaymentId: unitPaymentID, IdempotencyKey: "k"})
		assert.Equal(t, codes.NotFound, status.Code(grpcmw.MapError(err)))
	})

	t.Run("cancelled payment replays", func(t *testing.T) {
		store := &unitStore{payment: storage.Payment{ID: unitPaymentID, State: string(domain.StateCancelled), StateVersion: 4}}
		server := unitCreation(t, store)
		response, err := server.CancelPayment(context.Background(), &paymentv1.CancelPaymentRequest{PaymentId: unitPaymentID, IdempotencyKey: "k"})
		require.NoError(t, err)
		assert.Equal(t, paymentv1.PaymentState_PAYMENT_STATE_CANCELLED, response.GetPayment().GetState())
	})

	t.Run("after detection rejected", func(t *testing.T) {
		store := &unitStore{payment: storage.Payment{ID: unitPaymentID, State: string(domain.StateDetected), StateVersion: 4}}
		server := unitCreation(t, store)
		_, err := server.CancelPayment(context.Background(), &paymentv1.CancelPaymentRequest{PaymentId: unitPaymentID, IdempotencyKey: "k"})
		assert.Equal(t, codes.FailedPrecondition, status.Code(grpcmw.MapError(err)))
	})
}

func TestCreatePaymentReplaysAfterExpiry(t *testing.T) {
	t.Parallel()
	// The stored original request has already expired: the replay must return
	// the stored payment instead of an invalid_expires_at (FR-2).
	expires := time.Now().UTC().Add(-time.Minute)
	request := unitCreateRequest()
	request.ExpiresAt = timestamppb.New(expires)
	store := &unitStore{payment: storage.Payment{
		ID:               unitPaymentID,
		MerchantID:       "01a14f70-0000-7000-8000-0000000000aa",
		IdempotencyKey:   "key-1",
		RequestHash:      domain.RequestHash("01a14f70-0000-7000-8000-0000000000aa", kitmoney.MustParse("10", "USDT", 6), "USDT", "usdt_trc20", expires, 6),
		State:            string(domain.StateExpired),
		StateVersion:     4,
		ExpectedAmount:   15_000_000,
		ExpectedAsset:    "USDT",
		ExpectedDecimals: 6,
		Network:          "usdt_trc20",
	}}
	server := unitCreation(t, store)
	response, err := server.CreatePayment(context.Background(), request)
	require.NoError(t, err)
	assert.Equal(t, paymentv1.PaymentState_PAYMENT_STATE_EXPIRED, response.GetState())
}

func TestMapCreationErrorCodes(t *testing.T) {
	t.Parallel()
	cases := []struct {
		err  error
		code codes.Code
	}{
		{storage.ErrNotFound, codes.NotFound},
		{domain.ErrIdempotencyConflict, codes.AlreadyExists},
		{domain.ErrQuoteRejected, codes.FailedPrecondition},
		{domain.ErrInvoiceRejected, codes.FailedPrecondition},
		{domain.ErrInvoiceNotCancellable, codes.FailedPrecondition},
		{domain.ErrPayAmount, codes.FailedPrecondition},
		{domain.ErrInvalidCreation, codes.InvalidArgument},
		{domain.ErrInvalidExpiry, codes.InvalidArgument},
		{domain.ErrUnknownAssetScale, codes.InvalidArgument},
		{domain.ErrInvalidRate, codes.Unavailable},
		{domain.ErrStateConflict, codes.Unavailable},
		{domain.ErrDependencyTemporary, codes.Unavailable},
		{domain.ErrOverloaded, codes.ResourceExhausted},
		{context.DeadlineExceeded, codes.Unavailable},
		{errors.New("unknown failure"), codes.Internal},
	}
	for _, tc := range cases {
		mapped := mapCreationError(tc.err)
		assert.Equal(t, tc.code, status.Code(grpcmw.MapError(mapped)), tc.err.Error())
	}
}
