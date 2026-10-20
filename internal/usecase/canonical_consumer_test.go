package usecase_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
	"google.golang.org/protobuf/proto"
	"gorm.io/gorm"

	commonv1 "github.com/ZMichael3012/cbl-protos/gen/go/billing/common/v1"
	eventsv1 "github.com/ZMichael3012/cbl-protos/gen/go/billing/events/v1"

	"github.com/ZMichael3012/cbl-kit/inbox"

	"github.com/cbl/cbl-payment/internal/domain"
	"github.com/cbl/cbl-payment/internal/storage"
	"github.com/cbl/cbl-payment/internal/usecase"
)

const canonicalPaymentID = "01a13d53-6bf5-707f-a944-c0d73586547e"

// fakeCanonicalStore implements usecase.CanonicalStore over memory.
type fakeCanonicalStore struct {
	payment       storage.Payment
	notFound      bool
	conflicts     int
	transitions   []domain.State
	patches       []storage.TransitionPatch
	derived       []storage.Payment
	topUpBaseline int64
}

// PaymentTx returns the state of the fake payment.
func (f *fakeCanonicalStore) PaymentTx(_ context.Context, _ *gorm.DB, _ string) (storage.Payment, error) {
	if f.notFound {
		return storage.Payment{}, fmt.Errorf("%w: test", storage.ErrNotFound)
	}
	return f.payment, nil
}

// newFakeStore creates a payment in the given state.
func newFakeStore(state domain.State, version int64) *fakeCanonicalStore {
	return &fakeCanonicalStore{payment: storage.Payment{
		ID:               canonicalPaymentID,
		State:            string(state),
		StateVersion:     version,
		ExpectedAmount:   100_000,
		ExpectedAsset:    "USDT",
		ExpectedDecimals: 2,
	}}
}

// TransitionTx performs a CAS transition over the fake payment.
func (f *fakeCanonicalStore) TransitionTx(_ context.Context, _ *gorm.DB, _ string, expectedVersion uint64, to domain.State, patch storage.TransitionPatch) (storage.Payment, error) {
	if f.conflicts > 0 {
		f.conflicts--
		return storage.Payment{}, fmt.Errorf("%w: test race", domain.ErrStateConflict)
	}
	if uint64(f.payment.StateVersion) != expectedVersion { //nolint:gosec // a test value
		return storage.Payment{}, fmt.Errorf("%w: test version", domain.ErrStateConflict)
	}
	from, err := domain.ParseState(f.payment.State)
	if err != nil {
		return storage.Payment{}, err
	}
	next, err := domain.NextVersion(from, to, expectedVersion)
	if err != nil {
		return storage.Payment{}, err
	}
	f.payment.State = string(to)
	f.payment.StateVersion = int64(next) //nolint:gosec // a test value
	if patch.ReceivedAmount != nil {
		f.payment.ReceivedAmount = patch.ReceivedAmount
	}
	if patch.ReceivedAsset != nil {
		f.payment.ReceivedAsset = patch.ReceivedAsset
	}
	if patch.ReceivedDecimals != nil {
		f.payment.ReceivedDecimals = patch.ReceivedDecimals
	}
	if patch.TxID != nil {
		f.payment.TxID = patch.TxID
	}
	if patch.Confirmations != nil {
		f.payment.Confirmations = *patch.Confirmations
	}
	if patch.CompletedAt != nil {
		f.payment.CompletedAt = patch.CompletedAt
	}
	f.transitions = append(f.transitions, to)
	f.patches = append(f.patches, patch)
	return f.payment, nil
}

// CreateDerivedTx records a derived top-up insert over the fake store.
func (f *fakeCanonicalStore) CreateDerivedTx(_ context.Context, _ *gorm.DB, payment storage.Payment, _ string) (storage.Payment, error) {
	f.derived = append(f.derived, payment)
	return payment, nil
}

// LockAndSumTopUpsTx returns the fixed test baseline when set; otherwise it
// derives the baseline from the recorded amount plus the accepted children.
func (f *fakeCanonicalStore) LockAndSumTopUpsTx(_ context.Context, _ *gorm.DB, _ string) (int64, error) {
	if f.topUpBaseline > 0 {
		return f.topUpBaseline, nil
	}
	var baseline int64
	if f.payment.ReceivedAmount != nil {
		baseline = *f.payment.ReceivedAmount
	}
	for _, child := range f.derived {
		if child.ReceivedAmount != nil {
			baseline += *child.ReceivedAmount
		}
	}
	return baseline, nil
}

// topUpConfirmed assembles a confirmed event with the explicit amount and txid.
func topUpConfirmed(amount int64, txid string) *eventsv1.CanonicalPaymentEvent {
	event := detectedEvent()
	event.Type = eventsv1.CanonicalPaymentEventType_CANONICAL_PAYMENT_EVENT_TYPE_CONFIRMED
	event.ReceivedAmount = &commonv1.Money{MinorUnits: amount, Asset: "USDT", Decimals: 2}
	event.Txid = txid
	return event
}

func TestCanonicalConsumerTopUpSuccessive(t *testing.T) {
	t.Parallel()
	store := newFakeStore(domain.StateCompleted, 4)
	received := int64(100_000)
	store.payment.ReceivedAmount = &received
	consumer := usecase.NewCanonicalConsumer(store, zap.NewNop())
	ctx := context.Background()

	require.NoError(t, consumer.Handle(ctx, nil, canonicalMessage(t, topUpConfirmed(150_000, "tx-a"))))
	require.Len(t, store.derived, 1)
	assert.Equal(t, int64(50_000), store.derived[0].ExpectedAmount)

	require.NoError(t, consumer.Handle(ctx, nil, canonicalMessage(t, topUpConfirmed(180_000, "tx-b"))))
	require.Len(t, store.derived, 2)
	assert.Equal(t, int64(30_000), store.derived[1].ExpectedAmount, "successive events credit only the new part")
}

func TestCanonicalConsumerTopUpWithoutTxid(t *testing.T) {
	t.Parallel()
	store := newFakeStore(domain.StateCompleted, 4)
	received := int64(100_000)
	store.payment.ReceivedAmount = &received
	consumer := usecase.NewCanonicalConsumer(store, zap.NewNop())
	ctx := context.Background()

	require.NoError(t, consumer.Handle(ctx, nil, canonicalMessage(t, topUpConfirmed(150_000, "  "))))
	assert.Empty(t, store.derived, "a deposit without a txid is not credited")
}

func TestCanonicalConsumerRawStatusRejectsControlCharacters(t *testing.T) {
	t.Parallel()
	for _, raw := range []string{"bad\nstatus", "bad\u0085status", "bad\u2028status", "raw\u007fstatus"} {
		raw := raw
		t.Run("raw", func(t *testing.T) {
			t.Parallel()
			store := newFakeStore(domain.StatePending, 2)
			consumer := usecase.NewCanonicalConsumer(store, zap.NewNop())
			ctx := context.Background()

			event := detectedEvent()
			event.RawStatus = raw
			err := consumer.Handle(ctx, nil, canonicalMessage(t, event))
			require.Error(t, err)
			assert.Contains(t, err.Error(), "raw_status is invalid")
		})
	}
}

// canonicalMessage assembles an inbox message with the serialized event.
func canonicalMessage(t *testing.T, event *eventsv1.CanonicalPaymentEvent) inbox.Message {
	t.Helper()
	body, err := proto.Marshal(event)
	require.NoError(t, err)
	return inbox.Message{Value: body}
}

// detectedEvent assembles a canonical detection event.
func detectedEvent() *eventsv1.CanonicalPaymentEvent {
	return &eventsv1.CanonicalPaymentEvent{
		PaymentId:      canonicalPaymentID,
		GatewayId:      "g-1",
		Provider:       "mock",
		Type:           eventsv1.CanonicalPaymentEventType_CANONICAL_PAYMENT_EVENT_TYPE_DETECTED,
		ReceivedAmount: &commonv1.Money{MinorUnits: 100_000, Asset: "USDT", Decimals: 2},
		Txid:           "tx-1",
		Network:        "trc20",
	}
}

func TestCanonicalConsumerDetected(t *testing.T) {
	t.Parallel()

	store := newFakeStore(domain.StatePending, 3)
	consumer := usecase.NewCanonicalConsumer(store, nil)

	require.NoError(t, consumer.Handle(context.Background(), nil, canonicalMessage(t, detectedEvent())))
	assert.Equal(t, string(domain.StateDetected), store.payment.State)
	assert.Equal(t, int64(4), store.payment.StateVersion)
	require.Equal(t, []domain.State{domain.StateDetected}, store.transitions)
	require.NotNil(t, store.payment.ReceivedAmount)
	assert.Equal(t, int64(100_000), *store.payment.ReceivedAmount)
	require.NotNil(t, store.payment.TxID)
	assert.Equal(t, "tx-1", *store.payment.TxID)
}

func TestCanonicalConsumerConfirmedFirstChain(t *testing.T) {
	t.Parallel()

	store := newFakeStore(domain.StatePending, 2)
	consumer := usecase.NewCanonicalConsumer(store, nil)

	event := detectedEvent()
	event.Type = eventsv1.CanonicalPaymentEventType_CANONICAL_PAYMENT_EVENT_TYPE_CONFIRMED
	event.Confirmations = 3
	require.NoError(t, consumer.Handle(context.Background(), nil, canonicalMessage(t, event)))

	assert.Equal(t, string(domain.StateConfirming), store.payment.State)
	assert.Equal(t, int64(4), store.payment.StateVersion)
	assert.Equal(t, []domain.State{domain.StateDetected, domain.StateConfirming}, store.transitions)
	assert.Equal(t, 3, store.payment.Confirmations)
	require.Len(t, store.patches, 2)
	assert.Nil(t, store.patches[0].ReceivedAmount, "event data is recorded on the target state")
	require.NotNil(t, store.patches[1].ReceivedAmount)
}

func TestCanonicalConsumerStaleAfterTerminal(t *testing.T) {
	t.Parallel()

	store := newFakeStore(domain.StateCompleted, 6)
	consumer := usecase.NewCanonicalConsumer(store, nil)

	require.NoError(t, consumer.Handle(context.Background(), nil, canonicalMessage(t, detectedEvent())))
	assert.Equal(t, string(domain.StateCompleted), store.payment.State)
	assert.Equal(t, int64(6), store.payment.StateVersion, "a stale event does not change the state")
	assert.Empty(t, store.transitions)
}

func TestCanonicalConsumerTooEarly(t *testing.T) {
	t.Parallel()

	store := newFakeStore(domain.StateCreating, 1)
	consumer := usecase.NewCanonicalConsumer(store, nil)

	err := consumer.Handle(context.Background(), nil, canonicalMessage(t, detectedEvent()))
	require.ErrorIs(t, err, domain.ErrEventTooEarly)
	assert.Empty(t, store.transitions)
}

func TestCanonicalConsumerUnknownPayment(t *testing.T) {
	t.Parallel()

	store := newFakeStore(domain.StatePending, 2)
	store.notFound = true
	consumer := usecase.NewCanonicalConsumer(store, nil)

	err := consumer.Handle(context.Background(), nil, canonicalMessage(t, detectedEvent()))
	require.ErrorIs(t, err, storage.ErrNotFound)
}

func TestCanonicalConsumerRetriesOnConflict(t *testing.T) {
	t.Parallel()

	store := newFakeStore(domain.StatePending, 2)
	store.conflicts = 1
	consumer := usecase.NewCanonicalConsumer(store, nil)

	require.NoError(t, consumer.Handle(context.Background(), nil, canonicalMessage(t, detectedEvent())))
	assert.Equal(t, string(domain.StateDetected), store.payment.State)
}

func TestCanonicalConsumerExhaustsConflictAttempts(t *testing.T) {
	t.Parallel()

	store := newFakeStore(domain.StatePending, 2)
	store.conflicts = 10
	consumer := usecase.NewCanonicalConsumer(store, nil)

	err := consumer.Handle(context.Background(), nil, canonicalMessage(t, detectedEvent()))
	require.ErrorIs(t, err, domain.ErrStateConflict)
}

func TestCanonicalConsumerSkipsUnknownAndEmptyTypes(t *testing.T) {
	t.Parallel()

	for name, eventType := range map[string]eventsv1.CanonicalPaymentEventType{
		"unspecified": eventsv1.CanonicalPaymentEventType_CANONICAL_PAYMENT_EVENT_TYPE_UNSPECIFIED,
		"unknown":     eventsv1.CanonicalPaymentEventType(99),
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			store := newFakeStore(domain.StatePending, 2)
			consumer := usecase.NewCanonicalConsumer(store, nil)
			event := detectedEvent()
			event.Type = eventType
			require.NoError(t, consumer.Handle(context.Background(), nil, canonicalMessage(t, event)))
			assert.Empty(t, store.transitions)
		})
	}
}

func TestCanonicalConsumerNormalizesAssetCase(t *testing.T) {
	t.Parallel()

	store := newFakeStore(domain.StatePending, 2)
	consumer := usecase.NewCanonicalConsumer(store, nil)

	event := detectedEvent()
	event.ReceivedAmount = &commonv1.Money{MinorUnits: 5, Asset: "usdt", Decimals: 2}
	require.NoError(t, consumer.Handle(context.Background(), nil, canonicalMessage(t, event)))

	require.NotNil(t, store.payment.ReceivedAsset)
	assert.Equal(t, "USDT", *store.payment.ReceivedAsset, "the asset is normalized to upper case")
}

func TestCanonicalConsumerRejectsExpectedMismatch(t *testing.T) {
	t.Parallel()

	tests := map[string]*commonv1.Money{
		"other asset":    {MinorUnits: 1, Asset: "BTC", Decimals: 2},
		"other decimals": {MinorUnits: 1, Asset: "USDT", Decimals: 6},
	}
	for name, amount := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			store := newFakeStore(domain.StatePending, 2)
			consumer := usecase.NewCanonicalConsumer(store, nil)
			event := detectedEvent()
			event.ReceivedAmount = amount
			require.Error(t, consumer.Handle(context.Background(), nil, canonicalMessage(t, event)))
			assert.Empty(t, store.transitions)
		})
	}
}

func TestCanonicalConsumerRequiresReceivedForMoneyEvents(t *testing.T) {
	t.Parallel()

	store := newFakeStore(domain.StatePending, 2)
	consumer := usecase.NewCanonicalConsumer(store, nil)
	event := detectedEvent()
	event.ReceivedAmount = nil
	require.Error(t, consumer.Handle(context.Background(), nil, canonicalMessage(t, event)))

	failed := newFakeStore(domain.StatePending, 2)
	failedConsumer := usecase.NewCanonicalConsumer(failed, nil)
	failedEvent := detectedEvent()
	failedEvent.ReceivedAmount = nil
	failedEvent.Type = eventsv1.CanonicalPaymentEventType_CANONICAL_PAYMENT_EVENT_TYPE_FAILED
	require.NoError(t, failedConsumer.Handle(context.Background(), nil, canonicalMessage(t, failedEvent)),
		"failed without an amount is allowed")
}

func TestCanonicalConsumerRejectsHugeConfirmations(t *testing.T) {
	t.Parallel()

	store := newFakeStore(domain.StatePending, 2)
	consumer := usecase.NewCanonicalConsumer(store, nil)
	event := detectedEvent()
	event.Type = eventsv1.CanonicalPaymentEventType_CANONICAL_PAYMENT_EVENT_TYPE_CONFIRMED
	event.Confirmations = 4_000_000_000
	require.Error(t, consumer.Handle(context.Background(), nil, canonicalMessage(t, event)))
}

func TestCanonicalConsumerAllEventTypes(t *testing.T) {
	t.Parallel()

	tests := map[eventsv1.CanonicalPaymentEventType]domain.State{
		eventsv1.CanonicalPaymentEventType_CANONICAL_PAYMENT_EVENT_TYPE_DETECTED:  domain.StateDetected,
		eventsv1.CanonicalPaymentEventType_CANONICAL_PAYMENT_EVENT_TYPE_CONFIRMED: domain.StateConfirming,
		eventsv1.CanonicalPaymentEventType_CANONICAL_PAYMENT_EVENT_TYPE_FAILED:    domain.StateFailed,
		eventsv1.CanonicalPaymentEventType_CANONICAL_PAYMENT_EVENT_TYPE_EXPIRED:   domain.StateExpired,
		eventsv1.CanonicalPaymentEventType_CANONICAL_PAYMENT_EVENT_TYPE_UNDERPAID: domain.StateUnderpaid,
		eventsv1.CanonicalPaymentEventType_CANONICAL_PAYMENT_EVENT_TYPE_OVERPAID:  domain.StateOverpaid,
	}
	for eventType, want := range tests {
		t.Run(eventType.String(), func(t *testing.T) {
			t.Parallel()

			store := newFakeStore(domain.StatePending, 2)
			consumer := usecase.NewCanonicalConsumer(store, nil)
			event := detectedEvent()
			event.Type = eventType
			switch eventType {
			case eventsv1.CanonicalPaymentEventType_CANONICAL_PAYMENT_EVENT_TYPE_UNDERPAID:
				event.ReceivedAmount = &commonv1.Money{MinorUnits: 99_000, Asset: "USDT", Decimals: 2}
			case eventsv1.CanonicalPaymentEventType_CANONICAL_PAYMENT_EVENT_TYPE_OVERPAID:
				event.ReceivedAmount = &commonv1.Money{MinorUnits: 101_000, Asset: "USDT", Decimals: 2}
			}
			require.NoError(t, consumer.Handle(context.Background(), nil, canonicalMessage(t, event)))
			assert.Equal(t, string(want), store.payment.State)
		})
	}
}

func TestCanonicalConsumerRejectsInvalidEvents(t *testing.T) {
	t.Parallel()

	badID := detectedEvent()
	badID.PaymentId = "not-a-uuid"

	negative := detectedEvent()
	negative.ReceivedAmount = &commonv1.Money{MinorUnits: -1, Asset: "USDT", Decimals: 2}

	badAsset := detectedEvent()
	badAsset.ReceivedAmount = &commonv1.Money{MinorUnits: 1, Asset: "usdt!", Decimals: 2}

	badDecimals := detectedEvent()
	badDecimals.ReceivedAmount = &commonv1.Money{MinorUnits: 1, Asset: "USDT", Decimals: 19}

	longTxID := detectedEvent()
	longTxID.Txid = strings.Repeat("a", 300)

	for name, event := range map[string]*eventsv1.CanonicalPaymentEvent{
		"bad id": badID, "negative": negative, "bad asset": badAsset,
		"bad decimals": badDecimals, "long txid": longTxID,
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			store := newFakeStore(domain.StatePending, 2)
			consumer := usecase.NewCanonicalConsumer(store, nil)
			require.Error(t, consumer.Handle(context.Background(), nil, canonicalMessage(t, event)))
			assert.Empty(t, store.transitions)
		})
	}

	garbage := inbox.Message{Value: []byte{0xff, 0x00, 0xff}}
	store := newFakeStore(domain.StatePending, 2)
	consumer := usecase.NewCanonicalConsumer(store, nil)
	require.Error(t, consumer.Handle(context.Background(), nil, garbage))
}

func TestCanonicalConsumerUnderpaidChainFromPending(t *testing.T) {
	t.Parallel()

	store := newFakeStore(domain.StatePending, 2)
	consumer := usecase.NewCanonicalConsumer(store, nil)

	event := detectedEvent()
	event.Type = eventsv1.CanonicalPaymentEventType_CANONICAL_PAYMENT_EVENT_TYPE_UNDERPAID
	event.ReceivedAmount = &commonv1.Money{MinorUnits: 99_000, Asset: "USDT", Decimals: 2}
	require.NoError(t, consumer.Handle(context.Background(), nil, canonicalMessage(t, event)))

	assert.Equal(t, string(domain.StateUnderpaid), store.payment.State)
	assert.Equal(t, []domain.State{domain.StateDetected, domain.StateUnderpaid}, store.transitions)
}

func TestCanonicalConsumerPolicyCompletesAtThreshold(t *testing.T) {
	t.Parallel()

	store := newFakeStore(domain.StateDetected, 4)
	policy, err := domain.ParseAmountPolicy(`{"USDT":[{"below_minor":1000000,"confirmations":1}]}`, 0, 0)
	require.NoError(t, err)
	consumer := usecase.NewCanonicalConsumer(store, nil, usecase.WithAmountPolicy(policy))

	event := detectedEvent()
	event.Type = eventsv1.CanonicalPaymentEventType_CANONICAL_PAYMENT_EVENT_TYPE_CONFIRMED
	event.Confirmations = 1
	require.NoError(t, consumer.Handle(context.Background(), nil, canonicalMessage(t, event)))

	assert.Equal(t, string(domain.StateCompleted), store.payment.State)
	assert.Equal(t, []domain.State{domain.StateConfirming, domain.StateCompleted}, store.transitions)
}

func TestCanonicalConsumerLateDetectedAfterExpiryAlerts(t *testing.T) {
	t.Parallel()

	store := newFakeStore(domain.StateExpired, 5)
	core, logs := observer.New(zap.WarnLevel)
	consumer := usecase.NewCanonicalConsumer(store, zap.New(core))

	require.NoError(t, consumer.Handle(context.Background(), nil, canonicalMessage(t, detectedEvent())))
	assert.Equal(t, string(domain.StateExpired), store.payment.State)
	assert.Equal(t, int64(5), store.payment.StateVersion, "a late event does not change the state")
	assert.Empty(t, store.transitions)

	require.Len(t, logs.All(), 1, "a late payment after expiry triggers a separate alert (KI-payment-08)")
	assert.Equal(t, "late canonical event after terminal state needs manual review", logs.All()[0].Message)
}

func TestCanonicalConsumerLateDetectedAfterCancellationAlerts(t *testing.T) {
	t.Parallel()

	store := newFakeStore(domain.StateCancelled, 5)
	core, logs := observer.New(zap.WarnLevel)
	consumer := usecase.NewCanonicalConsumer(store, zap.New(core))

	require.NoError(t, consumer.Handle(context.Background(), nil, canonicalMessage(t, detectedEvent())))
	assert.Equal(t, string(domain.StateCancelled), store.payment.State)
	assert.Empty(t, store.transitions)

	require.Len(t, logs.All(), 1, "a deposit after a cancellation needs manual review (KI-payment-09)")
	assert.Equal(t, "late canonical event after terminal state needs manual review", logs.All()[0].Message)
}

func TestCanonicalConsumerPolicyWaitsBelowThreshold(t *testing.T) {
	t.Parallel()

	store := newFakeStore(domain.StateDetected, 4)
	consumer := usecase.NewCanonicalConsumer(store, nil)

	event := detectedEvent()
	event.Type = eventsv1.CanonicalPaymentEventType_CANONICAL_PAYMENT_EVENT_TYPE_CONFIRMED
	event.Confirmations = 5
	require.NoError(t, consumer.Handle(context.Background(), nil, canonicalMessage(t, event)))

	assert.Equal(t, string(domain.StateConfirming), store.payment.State, "the default threshold is 6")
	assert.Equal(t, 5, store.payment.Confirmations)

	// A repeated confirmed below the threshold is a self-transition with growing confirmations.
	event.Confirmations = 6
	require.NoError(t, consumer.Handle(context.Background(), nil, canonicalMessage(t, event)))
	assert.Equal(t, string(domain.StateCompleted), store.payment.State, "reaching the threshold completes")
}

func TestCanonicalConsumerSelfTransitionConfirming(t *testing.T) {
	t.Parallel()

	store := newFakeStore(domain.StateConfirming, 5)
	consumer := usecase.NewCanonicalConsumer(store, nil)

	event := detectedEvent()
	event.Type = eventsv1.CanonicalPaymentEventType_CANONICAL_PAYMENT_EVENT_TYPE_CONFIRMED
	event.Confirmations = 3
	require.NoError(t, consumer.Handle(context.Background(), nil, canonicalMessage(t, event)))

	assert.Equal(t, string(domain.StateConfirming), store.payment.State)
	assert.Equal(t, int64(6), store.payment.StateVersion, "a self-transition grows the version")
	assert.Equal(t, 3, store.payment.Confirmations)
	assert.Equal(t, []domain.State{domain.StateConfirming}, store.transitions)
}

func TestCanonicalConsumerPolicyClassifiesByAmount(t *testing.T) {
	t.Parallel()

	// canonical confirmed with an amount below tolerance: the policy moves to underpaid.
	store := newFakeStore(domain.StatePending, 2)
	consumer := usecase.NewCanonicalConsumer(store, nil)
	event := detectedEvent()
	event.Type = eventsv1.CanonicalPaymentEventType_CANONICAL_PAYMENT_EVENT_TYPE_CONFIRMED
	event.ReceivedAmount = &commonv1.Money{MinorUnits: 99_000, Asset: "USDT", Decimals: 2}
	event.Confirmations = 6
	require.NoError(t, consumer.Handle(context.Background(), nil, canonicalMessage(t, event)))
	assert.Equal(t, string(domain.StateUnderpaid), store.payment.State)

	// Overpayment within the 2% tolerance - the policy completes to completed.
	policy, err := domain.ParseAmountPolicy("", 0, 200)
	require.NoError(t, err)
	overpaidStore := newFakeStore(domain.StateConfirming, 5)
	overpaidConsumer := usecase.NewCanonicalConsumer(overpaidStore, nil, usecase.WithAmountPolicy(policy))
	overpaid := detectedEvent()
	overpaid.Type = eventsv1.CanonicalPaymentEventType_CANONICAL_PAYMENT_EVENT_TYPE_OVERPAID
	overpaid.ReceivedAmount = &commonv1.Money{MinorUnits: 102_000, Asset: "USDT", Decimals: 2}
	overpaid.Confirmations = 6
	require.NoError(t, overpaidConsumer.Handle(context.Background(), nil, canonicalMessage(t, overpaid)))
	assert.Equal(t, string(domain.StateCompleted), overpaidStore.payment.State, "overpayment within tolerance is completed")
}

func TestCanonicalConsumerCompletedAtUsesClock(t *testing.T) {
	t.Parallel()

	fixed := time.Date(2026, 10, 15, 12, 0, 0, 0, time.UTC)
	policy, err := domain.ParseAmountPolicy(`{"USDT":[{"below_minor":1000000,"confirmations":1}]}`, 0, 0)
	require.NoError(t, err)
	store := newFakeStore(domain.StateDetected, 4)
	consumer := usecase.NewCanonicalConsumer(store, nil,
		usecase.WithAmountPolicy(policy),
		usecase.WithClock(func() time.Time { return fixed }))

	event := detectedEvent()
	event.Type = eventsv1.CanonicalPaymentEventType_CANONICAL_PAYMENT_EVENT_TYPE_CONFIRMED
	event.Confirmations = 1
	require.NoError(t, consumer.Handle(context.Background(), nil, canonicalMessage(t, event)))

	require.NotNil(t, store.payment.CompletedAt)
	assert.Equal(t, fixed, *store.payment.CompletedAt, "completed_at comes from the consumer clock")
}

func TestCanonicalConsumerPolicyToleranceWins(t *testing.T) {
	t.Parallel()

	policy, err := domain.ParseAmountPolicy("", 100, 0)
	require.NoError(t, err)

	// Underpayment within 1% at a reached threshold - the policy completes.
	store := newFakeStore(domain.StateDetected, 4)
	consumer := usecase.NewCanonicalConsumer(store, nil, usecase.WithAmountPolicy(policy))
	event := detectedEvent()
	event.Type = eventsv1.CanonicalPaymentEventType_CANONICAL_PAYMENT_EVENT_TYPE_UNDERPAID
	event.ReceivedAmount = &commonv1.Money{MinorUnits: 99_500, Asset: "USDT", Decimals: 2}
	event.Confirmations = 6
	require.NoError(t, consumer.Handle(context.Background(), nil, canonicalMessage(t, event)))
	assert.Equal(t, string(domain.StateCompleted), store.payment.State, "canonical underpaid within tolerance is completed by the policy")

	// Underpayment within tolerance without the confirmation threshold - waiting.
	waiting := newFakeStore(domain.StateDetected, 4)
	waitingConsumer := usecase.NewCanonicalConsumer(waiting, nil, usecase.WithAmountPolicy(policy))
	waitEvent := detectedEvent()
	waitEvent.Type = eventsv1.CanonicalPaymentEventType_CANONICAL_PAYMENT_EVENT_TYPE_CONFIRMED
	waitEvent.ReceivedAmount = &commonv1.Money{MinorUnits: 99_500, Asset: "USDT", Decimals: 2}
	waitEvent.Confirmations = 1
	require.NoError(t, waitingConsumer.Handle(context.Background(), nil, canonicalMessage(t, waitEvent)))
	assert.Equal(t, string(domain.StateConfirming), waiting.payment.State)
}
