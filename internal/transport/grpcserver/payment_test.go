package grpcserver

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	paymentv1 "github.com/ZMichael3012/cbl-protos/gen/go/billing/payment/v1"

	"github.com/ZMichael3012/cbl-kit/grpcmw"

	"github.com/cbl/cbl-payment/internal/domain"
	"github.com/cbl/cbl-payment/internal/storage"
)

func TestPaymentIdValidation(t *testing.T) {
	t.Parallel()

	server := NewPayment(nil, nil)
	ctx := context.Background()

	_, err := server.GetPayment(ctx, &paymentv1.GetPaymentRequest{})
	require.Error(t, err)
	assert.Equal(t, grpcmw.KindInvalidArgument, grpcmw.KindOf(err))

	_, err = server.GetPayment(ctx, &paymentv1.GetPaymentRequest{PaymentId: "not-a-uuid"})
	require.Error(t, err)
	assert.Equal(t, grpcmw.KindInvalidArgument, grpcmw.KindOf(err))
}

func TestListPaymentsValidation(t *testing.T) {
	t.Parallel()

	server := NewPayment(nil, nil)
	ctx := context.Background()

	_, err := server.ListPayments(ctx, &paymentv1.ListPaymentsRequest{})
	require.Error(t, err)
	assert.Equal(t, grpcmw.KindInvalidArgument, grpcmw.KindOf(err))

	_, err = server.ListPayments(ctx, &paymentv1.ListPaymentsRequest{
		MerchantId: strings.Repeat("m", maxMerchantIDLen+1),
	})
	require.Error(t, err)
	assert.Equal(t, grpcmw.KindInvalidArgument, grpcmw.KindOf(err))

	_, err = server.ListPayments(ctx, &paymentv1.ListPaymentsRequest{
		MerchantId:  "m-1",
		StateFilter: paymentv1.PaymentState(99),
	})
	require.Error(t, err)
	assert.Equal(t, grpcmw.KindInvalidArgument, grpcmw.KindOf(err))

	_, err = server.ListPayments(ctx, &paymentv1.ListPaymentsRequest{
		MerchantId: "m-1",
		Cursor:     strings.Repeat("c", maxCursorLen+1),
	})
	require.Error(t, err)
	assert.Equal(t, grpcmw.KindInvalidArgument, grpcmw.KindOf(err))

	_, err = server.ListPayments(ctx, &paymentv1.ListPaymentsRequest{
		MerchantId:  "m-1",
		CreatedFrom: &timestamppb.Timestamp{Seconds: 253402300800},
	})
	require.Error(t, err, "a timestamp outside the protobuf range")
	assert.Equal(t, grpcmw.KindInvalidArgument, grpcmw.KindOf(err))

	_, err = server.ListPayments(ctx, &paymentv1.ListPaymentsRequest{
		MerchantId:  "m-1",
		CreatedFrom: timestamppb.New(time.Date(2100, 1, 1, 0, 0, 0, 0, time.UTC)),
		CreatedTo:   timestamppb.New(time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)),
	})
	require.Error(t, err)
	assert.Equal(t, grpcmw.KindInvalidArgument, grpcmw.KindOf(err))

	_, err = server.ListPayments(ctx, &paymentv1.ListPaymentsRequest{
		MerchantId:  "m-1",
		CreatedFrom: timestamppb.New(time.Date(2100, 1, 1, 0, 0, 0, 0, time.UTC)),
		CreatedTo:   timestamppb.New(time.Date(2100, 1, 1, 0, 0, 0, 0, time.UTC)),
	})
	require.Error(t, err, "an empty period is rejected")
	assert.Equal(t, grpcmw.KindInvalidArgument, grpcmw.KindOf(err))
}

func TestStateMappingRoundTrip(t *testing.T) {
	t.Parallel()

	states := []domain.State{
		domain.StateCreating, domain.StateCreated, domain.StatePending, domain.StateDetected,
		domain.StateConfirming, domain.StateCompleted, domain.StateExpired, domain.StateUnderpaid,
		domain.StateOverpaid, domain.StateFailed, domain.StateCancelled,
	}
	for _, state := range states {
		mapped := stateToProto(state)
		assert.NotEqual(t, paymentv1.PaymentState_PAYMENT_STATE_UNSPECIFIED, mapped, string(state))
		assert.Equal(t, state, stateFromProto(mapped), string(state))
	}

	assert.Equal(t, paymentv1.PaymentState_PAYMENT_STATE_UNSPECIFIED, stateToProto(domain.State("unknown")))
	assert.Equal(t, domain.State(""), stateFromProto(paymentv1.PaymentState(99)))

	filter, err := stateFilter(paymentv1.PaymentState_PAYMENT_STATE_UNSPECIFIED)
	require.NoError(t, err)
	assert.Nil(t, filter, "UNSPECIFIED means no filter")

	filter, err = stateFilter(paymentv1.PaymentState_PAYMENT_STATE_COMPLETED)
	require.NoError(t, err)
	require.NotNil(t, filter)
	assert.Equal(t, domain.StateCompleted, *filter)

	_, err = stateFilter(paymentv1.PaymentState(99))
	require.Error(t, err)
	assert.Equal(t, grpcmw.KindInvalidArgument, grpcmw.KindOf(err))
}

func TestPaymentToProtoProviderFields(t *testing.T) {
	t.Parallel()
	provider := "bitpay"
	rawStatus := "paid"
	checkout := "https://pay.example/invoice/1"
	payment := storage.Payment{
		ID:               "01a13d53-6bf5-707f-a944-c0d73586547e",
		MerchantID:       "m-1",
		State:            string(domain.StatePending),
		StateVersion:     2,
		ExpectedAmount:   100_000,
		ExpectedAsset:    "USDT",
		ExpectedDecimals: 2,
		Network:          "trc20",
		Provider:         &provider,
		RawStatus:        &rawStatus,
		CheckoutURL:      &checkout,
	}
	result := paymentToProto(payment, nil)
	assert.Equal(t, provider, result.GetProvider())
	assert.Equal(t, rawStatus, result.GetRawStatus())
	assert.Equal(t, checkout, result.GetCheckoutUrl())
}

func TestMapPaymentError(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		err  error
		want grpcmw.Kind
	}{
		{name: "not found", err: storage.ErrNotFound, want: grpcmw.KindNotFound},
		{name: "invalid cursor", err: storage.ErrInvalidCursor, want: grpcmw.KindInvalidArgument},
		{name: "internal", err: assert.AnError, want: grpcmw.KindInternal},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			mapped := mapPaymentError(tt.err)
			assert.Equal(t, tt.want, grpcmw.KindOf(mapped))
		})
	}
}
