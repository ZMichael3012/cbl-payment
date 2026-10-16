package usecase_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	eventsv1 "github.com/cbl/cbl-protos/gen/go/billing/events/v1"

	"github.com/cbl/cbl-kit/outbox"

	"github.com/cbl/cbl-payment/internal/domain"
	"github.com/cbl/cbl-payment/internal/storage"
	"github.com/cbl/cbl-payment/internal/usecase"
)

// paymentTransitionEvent assembles a transition with a populated payment.
func paymentTransitionEvent(to domain.State) storage.TransitionEvent {
	received := int64(99_500)
	receivedAsset := "USDT"
	receivedDecimals := 2
	feeAmount := int64(500)
	feeAsset := "USDT"
	feeDecimals := 2
	quoteID := "q-1"
	rate := "1.0"
	txid := "tx-1"
	return storage.TransitionEvent{
		Payment: storage.Payment{
			ID:               canonicalPaymentID,
			MerchantID:       "m-1",
			State:            string(to),
			StateVersion:     5,
			ExpectedAmount:   100_000,
			ExpectedAsset:    "USDT",
			ExpectedDecimals: 2,
			ReceivedAmount:   &received,
			ReceivedAsset:    &receivedAsset,
			ReceivedDecimals: &receivedDecimals,
			FeeAmount:        &feeAmount,
			FeeAsset:         &feeAsset,
			FeeDecimals:      &feeDecimals,
			QuoteID:          &quoteID,
			Rate:             &rate,
			TxID:             &txid,
		},
		From:       domain.StateDetected,
		To:         to,
		OccurredAt: time.Date(2026, 10, 15, 12, 0, 0, 0, time.UTC),
	}
}

func TestBuildPaymentEventEnvelopeMapsStates(t *testing.T) {
	t.Parallel()

	tests := map[domain.State]eventsv1.PaymentEventType{
		domain.StateCreated:    eventsv1.PaymentEventType_PAYMENT_EVENT_TYPE_CREATED,
		domain.StateDetected:   eventsv1.PaymentEventType_PAYMENT_EVENT_TYPE_DETECTED,
		domain.StateConfirming: eventsv1.PaymentEventType_PAYMENT_EVENT_TYPE_CONFIRMING,
		domain.StateCompleted:  eventsv1.PaymentEventType_PAYMENT_EVENT_TYPE_COMPLETED,
		domain.StateExpired:    eventsv1.PaymentEventType_PAYMENT_EVENT_TYPE_EXPIRED,
		domain.StateUnderpaid:  eventsv1.PaymentEventType_PAYMENT_EVENT_TYPE_UNDERPAID,
		domain.StateOverpaid:   eventsv1.PaymentEventType_PAYMENT_EVENT_TYPE_OVERPAID,
		domain.StateFailed:     eventsv1.PaymentEventType_PAYMENT_EVENT_TYPE_FAILED,
	}
	for state, wantType := range tests {
		t.Run(string(state), func(t *testing.T) {
			t.Parallel()
			envelope, emit, err := usecase.BuildPaymentEventEnvelope(paymentTransitionEvent(state))
			require.NoError(t, err)
			require.True(t, emit)

			assert.Equal(t, usecase.TopicPaymentEvents, envelope.Topic)
			assert.Equal(t, canonicalPaymentID, envelope.Key)
			assert.Equal(t, "payment."+string(state), envelope.Headers[outbox.HeaderEventType])
			assert.Equal(t, usecase.ProducerPayment, envelope.Headers[outbox.HeaderProducer])
			assert.Equal(t, usecase.SchemaVersion, envelope.Headers[outbox.HeaderSchemaVersion])
			assert.NotEmpty(t, envelope.EventID)

			var payload eventsv1.PaymentEvent
			require.NoError(t, proto.Unmarshal(envelope.Payload, &payload))
			assert.Equal(t, wantType, payload.GetType())
			assert.Equal(t, canonicalPaymentID, payload.GetPaymentId())
			assert.Equal(t, "m-1", payload.GetMerchantId())
			assert.Equal(t, uint64(5), payload.GetStateVersion())
			assert.Equal(t, int64(100_000), payload.GetExpectedAmount().GetMinorUnits())
			assert.Equal(t, "USDT", payload.GetExpectedAmount().GetAsset())
			assert.Equal(t, uint32(2), payload.GetExpectedAmount().GetDecimals())
			assert.Equal(t, int64(99_500), payload.GetReceivedAmount().GetMinorUnits())
			assert.Equal(t, int64(500), payload.GetFee().GetMinorUnits())
			assert.Equal(t, "q-1", payload.GetQuoteId())
			assert.Equal(t, "1.0", payload.GetRate())
			assert.Equal(t, "tx-1", payload.GetTxid())
			assert.Equal(t, timestamppb.New(time.Date(2026, 10, 15, 12, 0, 0, 0, time.UTC)), payload.GetOccurredAt())
			assert.Equal(t, "2026-10-15T12:00:00Z", envelope.Headers[outbox.HeaderOccurredAt])
		})
	}
}

func TestBuildPaymentEventEnvelopeSkipsStatesWithoutType(t *testing.T) {
	t.Parallel()

	for _, state := range []domain.State{domain.StatePending, domain.StateCancelled, domain.StateCreating} {
		t.Run(string(state), func(t *testing.T) {
			t.Parallel()
			envelope, emit, err := usecase.BuildPaymentEventEnvelope(paymentTransitionEvent(state))
			require.NoError(t, err)
			assert.False(t, emit, "a state without a CF-2 type does not emit an event")
			assert.Equal(t, outbox.Envelope{}, envelope)
		})
	}
}

func TestBuildPaymentEventEnvelopeSelfTransition(t *testing.T) {
	t.Parallel()

	event := paymentTransitionEvent(domain.StateConfirming)
	event.From = domain.StateConfirming
	envelope, emit, err := usecase.BuildPaymentEventEnvelope(event)
	require.NoError(t, err)
	require.True(t, emit, "a self-transition records a new state version")

	var payload eventsv1.PaymentEvent
	require.NoError(t, proto.Unmarshal(envelope.Payload, &payload))
	assert.Equal(t, eventsv1.PaymentEventType_PAYMENT_EVENT_TYPE_CONFIRMING, payload.GetType())
}

func TestBuildPaymentEventEnvelopeOptionalMoney(t *testing.T) {
	t.Parallel()

	event := paymentTransitionEvent(domain.StateDetected)
	event.Payment.ReceivedAmount = nil
	event.Payment.ReceivedAsset = nil
	event.Payment.ReceivedDecimals = nil
	event.Payment.FeeAsset = nil
	event.Payment.QuoteID = nil
	event.Payment.Rate = nil
	event.Payment.TxID = nil
	envelope, emit, err := usecase.BuildPaymentEventEnvelope(event)
	require.NoError(t, err)
	require.True(t, emit)

	var payload eventsv1.PaymentEvent
	require.NoError(t, proto.Unmarshal(envelope.Payload, &payload))
	assert.Nil(t, payload.GetReceivedAmount(), "the received amount is not set - the field is omitted")
	assert.Nil(t, payload.GetFee(), "an incomplete fee triple does not form money")
	assert.Empty(t, payload.GetQuoteId())
	assert.Empty(t, payload.GetTxid())
}

func TestBuildPaymentEventEnvelopeTraceparent(t *testing.T) {
	t.Parallel()

	valid := "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
	event := paymentTransitionEvent(domain.StateCompleted)
	event.Traceparent = valid
	envelope, emit, err := usecase.BuildPaymentEventEnvelope(event)
	require.NoError(t, err)
	require.True(t, emit)
	assert.Equal(t, valid, envelope.Headers[outbox.HeaderTraceparent])

	event.Traceparent = ""
	envelope, emit, err = usecase.BuildPaymentEventEnvelope(event)
	require.NoError(t, err)
	require.True(t, emit)
	assert.NotContains(t, envelope.Headers, outbox.HeaderTraceparent)
}

func TestBuildPaymentEventEnvelopeDecimalsBoundaries(t *testing.T) {
	t.Parallel()

	for _, decimals := range []int{0, 18} {
		decimals := decimals
		event := paymentTransitionEvent(domain.StateDetected)
		event.Payment.ReceivedDecimals = &decimals
		event.Payment.ExpectedDecimals = decimals
		envelope, emit, err := usecase.BuildPaymentEventEnvelope(event)
		require.NoError(t, err)
		require.True(t, emit)

		var payload eventsv1.PaymentEvent
		require.NoError(t, proto.Unmarshal(envelope.Payload, &payload))
		assert.Equal(t, uint32(decimals), payload.GetExpectedAmount().GetDecimals()) //nolint:gosec // the 0 to 18 boundary is checked in the loop
		assert.Equal(t, uint32(decimals), payload.GetReceivedAmount().GetDecimals()) //nolint:gosec // the 0 to 18 boundary is checked in the loop
	}
}

func TestBuildPaymentEventEnvelopeZeroOccurredAt(t *testing.T) {
	t.Parallel()

	_, emit, err := usecase.BuildPaymentEventEnvelope(storage.TransitionEvent{
		Payment: storage.Payment{ID: canonicalPaymentID, State: string(domain.StateFailed), StateVersion: 2},
		To:      domain.StateFailed,
	})
	require.Error(t, err)
	assert.False(t, emit)
}
