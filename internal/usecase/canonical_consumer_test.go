package usecase_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"gorm.io/gorm"

	commonv1 "github.com/cbl/cbl-protos/gen/go/billing/common/v1"
	eventsv1 "github.com/cbl/cbl-protos/gen/go/billing/events/v1"

	"github.com/cbl/cbl-kit/inbox"

	"github.com/cbl/cbl-payment/internal/domain"
	"github.com/cbl/cbl-payment/internal/storage"
	"github.com/cbl/cbl-payment/internal/usecase"
)

const canonicalPaymentID = "01a13d53-6bf5-707f-a944-c0d73586547e"

// fakeCanonicalStore реализует usecase.CanonicalStore поверх памяти.
type fakeCanonicalStore struct {
	payment     storage.Payment
	notFound    bool
	conflicts   int
	transitions []domain.State
	patches     []storage.TransitionPatch
}

// PaymentTx возвращает состояние фейкового платежа.
func (f *fakeCanonicalStore) PaymentTx(_ context.Context, _ *gorm.DB, _ string) (storage.Payment, error) {
	if f.notFound {
		return storage.Payment{}, fmt.Errorf("%w: test", storage.ErrNotFound)
	}
	return f.payment, nil
}

// newFakeStore создает платеж в заданном состоянии.
func newFakeStore(state domain.State, version int64) *fakeCanonicalStore {
	return &fakeCanonicalStore{payment: storage.Payment{
		ID:               canonicalPaymentID,
		State:            string(state),
		StateVersion:     version,
		ExpectedAsset:    "USDT",
		ExpectedDecimals: 2,
	}}
}

// TransitionTx выполняет CAS-переход над фейковым платежом.
func (f *fakeCanonicalStore) TransitionTx(_ context.Context, _ *gorm.DB, _ string, expectedVersion uint64, to domain.State, patch storage.TransitionPatch) (storage.Payment, error) {
	if f.conflicts > 0 {
		f.conflicts--
		return storage.Payment{}, fmt.Errorf("%w: test race", domain.ErrStateConflict)
	}
	if uint64(f.payment.StateVersion) != expectedVersion { //nolint:gosec // тестовое значение
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
	f.payment.StateVersion = int64(next) //nolint:gosec // тестовое значение
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
	f.transitions = append(f.transitions, to)
	f.patches = append(f.patches, patch)
	return f.payment, nil
}

// canonicalMessage собирает inbox-сообщение с сериализованным событием.
func canonicalMessage(t *testing.T, event *eventsv1.CanonicalPaymentEvent) inbox.Message {
	t.Helper()
	body, err := proto.Marshal(event)
	require.NoError(t, err)
	return inbox.Message{Value: body}
}

// detectedEvent собирает каноническое событие обнаружения.
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
	assert.Nil(t, store.patches[0].ReceivedAmount, "данные события фиксируются на целевом состоянии")
	require.NotNil(t, store.patches[1].ReceivedAmount)
}

func TestCanonicalConsumerStaleAfterTerminal(t *testing.T) {
	t.Parallel()

	store := newFakeStore(domain.StateCompleted, 6)
	consumer := usecase.NewCanonicalConsumer(store, nil)

	require.NoError(t, consumer.Handle(context.Background(), nil, canonicalMessage(t, detectedEvent())))
	assert.Equal(t, string(domain.StateCompleted), store.payment.State)
	assert.Equal(t, int64(6), store.payment.StateVersion, "устаревшее событие не меняет состояние")
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
	assert.Equal(t, "USDT", *store.payment.ReceivedAsset, "актив нормализуется в верхний регистр")
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
		"failed без суммы допустим")
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
	require.NoError(t, consumer.Handle(context.Background(), nil, canonicalMessage(t, event)))

	assert.Equal(t, string(domain.StateUnderpaid), store.payment.State)
	assert.Equal(t, []domain.State{domain.StateDetected, domain.StateUnderpaid}, store.transitions)
}
