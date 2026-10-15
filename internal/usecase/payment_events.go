package usecase

import (
	"context"
	"fmt"
	"time"

	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
	"gorm.io/gorm"

	commonv1 "github.com/cbl/cbl-protos/gen/go/billing/common/v1"
	eventsv1 "github.com/cbl/cbl-protos/gen/go/billing/events/v1"

	"github.com/cbl/cbl-kit/outbox"

	"github.com/cbl/cbl-payment/internal/domain"
	"github.com/cbl/cbl-payment/internal/storage"
)

// Константы событий payment.events (CBL-003, раздел 6).
const (
	// ProducerPayment задает имя сервиса-продюсера событий платежа.
	ProducerPayment = "cbl-payment"
	// TopicPaymentEvents задает топик событий жизненного цикла платежа.
	TopicPaymentEvents = "payment.events"
	// SchemaVersion задает версию схемы событий платежа.
	SchemaVersion = "v1"
)

// PaymentEventSink собирает protobuf-конверты payment.events и пишет их в outbox
// в транзакции перехода (storage.TransitionEventSink).
type PaymentEventSink struct {
	store *outbox.Store
	log   *zap.Logger
}

// NewPaymentEventSink создает PaymentEventSink.
func NewPaymentEventSink(log *zap.Logger) *PaymentEventSink {
	if log == nil {
		log = zap.NewNop()
	}
	return &PaymentEventSink{store: outbox.NewStore(), log: log}
}

// AppendTransitionEvent собирает конверт события и записывает его в outbox.
// Переходы в состояния без типа в CF-2 (pending, cancelled, creating)
// пропускаются: контракт не имеет для них события.
func (s *PaymentEventSink) AppendTransitionEvent(ctx context.Context, tx *gorm.DB, event storage.TransitionEvent) error {
	envelope, emit, err := BuildPaymentEventEnvelope(event)
	if err != nil {
		return err
	}
	if !emit {
		return nil
	}
	if err := s.store.Append(ctx, tx, envelope); err != nil {
		return fmt.Errorf("usecase: append payment event: %w", err)
	}
	s.log.Debug("payment event queued in outbox",
		zap.String("event_id", envelope.EventID),
		zap.String("event_type", envelope.Headers[outbox.HeaderEventType]),
		zap.String("payment_id", event.Payment.ID),
		zap.Int64("state_version", event.Payment.StateVersion))
	return nil
}

// BuildPaymentEventEnvelope собирает конверт payment.events для перехода:
// protobuf-тело billing.events.v1.PaymentEvent и заголовки конверта CBL.
// Второе значение false означает, что состояние не имеет типа события в CF-2.
func BuildPaymentEventEnvelope(event storage.TransitionEvent) (outbox.Envelope, bool, error) {
	eventType, emit := paymentEventType(event.To)
	if !emit {
		return outbox.Envelope{}, false, nil
	}
	if event.OccurredAt.IsZero() {
		return outbox.Envelope{}, false, fmt.Errorf("usecase: transition %s has zero occurred_at", event.To)
	}
	payload := &eventsv1.PaymentEvent{
		PaymentId:  event.Payment.ID,
		MerchantId: event.Payment.MerchantID,
		Type:       eventType,
		ExpectedAmount: &commonv1.Money{
			MinorUnits: event.Payment.ExpectedAmount,
			Asset:      event.Payment.ExpectedAsset,
			Decimals:   decimalsToUint32(event.Payment.ExpectedDecimals),
		},
		QuoteId: derefString(event.Payment.QuoteID),
		Rate:    derefString(event.Payment.Rate),
		Txid:    derefString(event.Payment.TxID),
		// Версия гарантированно положительна (CHECK >= 1).
		StateVersion: uint64(event.Payment.StateVersion), //nolint:gosec
		OccurredAt:   timestamppb.New(event.OccurredAt.UTC()),
	}
	if received := moneyFrom(event.Payment.ReceivedAmount, event.Payment.ReceivedAsset, event.Payment.ReceivedDecimals); received != nil {
		payload.ReceivedAmount = received
	}
	if fee := moneyFrom(event.Payment.FeeAmount, event.Payment.FeeAsset, event.Payment.FeeDecimals); fee != nil {
		payload.Fee = fee
	}
	body, err := proto.Marshal(payload)
	if err != nil {
		return outbox.Envelope{}, false, fmt.Errorf("usecase: marshal payment event: %w", err)
	}
	headers := map[string]string{
		outbox.HeaderProducer:      ProducerPayment,
		outbox.HeaderSchemaVersion: SchemaVersion,
		outbox.HeaderOccurredAt:    event.OccurredAt.UTC().Format(time.RFC3339Nano),
	}
	if event.Traceparent != "" {
		headers[outbox.HeaderTraceparent] = event.Traceparent
	}
	eventTypeName := "payment." + string(event.To)
	envelope, err := outbox.NewEnvelope(eventTypeName, TopicPaymentEvents, event.Payment.ID, headers, body)
	if err != nil {
		return outbox.Envelope{}, false, fmt.Errorf("usecase: build payment envelope: %w", err)
	}
	return envelope, true, nil
}

// paymentEventType переводит целевое состояние перехода в контрактный тип;
// состояния pending, cancelled и creating типа не имеют (CF-2).
func paymentEventType(state domain.State) (eventsv1.PaymentEventType, bool) {
	switch state {
	case domain.StateCreated:
		return eventsv1.PaymentEventType_PAYMENT_EVENT_TYPE_CREATED, true
	case domain.StateDetected:
		return eventsv1.PaymentEventType_PAYMENT_EVENT_TYPE_DETECTED, true
	case domain.StateConfirming:
		return eventsv1.PaymentEventType_PAYMENT_EVENT_TYPE_CONFIRMING, true
	case domain.StateCompleted:
		return eventsv1.PaymentEventType_PAYMENT_EVENT_TYPE_COMPLETED, true
	case domain.StateExpired:
		return eventsv1.PaymentEventType_PAYMENT_EVENT_TYPE_EXPIRED, true
	case domain.StateUnderpaid:
		return eventsv1.PaymentEventType_PAYMENT_EVENT_TYPE_UNDERPAID, true
	case domain.StateOverpaid:
		return eventsv1.PaymentEventType_PAYMENT_EVENT_TYPE_OVERPAID, true
	case domain.StateFailed:
		return eventsv1.PaymentEventType_PAYMENT_EVENT_TYPE_FAILED, true
	default:
		return eventsv1.PaymentEventType_PAYMENT_EVENT_TYPE_UNSPECIFIED, false
	}
}

// moneyFrom собирает Money из необязательной тройки полей; неполная тройка
// (сумма без актива или шкалы) денег не образует и опускается.
func moneyFrom(amount *int64, asset *string, decimals *int) *commonv1.Money {
	if amount == nil || asset == nil || decimals == nil {
		return nil
	}
	return &commonv1.Money{
		MinorUnits: *amount,
		Asset:      *asset,
		Decimals:   decimalsToUint32(*decimals),
	}
}

// derefString разыменовывает необязательную строку (nil - пустая).
func derefString(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}

// decimalsToUint32 безопасно переводит шкалу в uint32 контракта.
func decimalsToUint32(decimals int) uint32 {
	if decimals < 0 {
		return 0
	}
	return uint32(decimals) //nolint:gosec // CHECK 0 - 18
}
