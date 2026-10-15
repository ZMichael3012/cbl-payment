package usecase

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
	"gorm.io/gorm"

	eventsv1 "github.com/cbl/cbl-protos/gen/go/billing/events/v1"

	"github.com/cbl/cbl-kit/inbox"
	"github.com/cbl/cbl-kit/outbox"

	"github.com/cbl/cbl-payment/internal/domain"
	"github.com/cbl/cbl-payment/internal/storage"
)

// Константы consumer-конвейера канонических событий.
const (
	// TopicGatewayEventsCanonical задает топик канонических событий шлюза.
	TopicGatewayEventsCanonical = "gateway.events.canonical"
	// ConsumerGroupCanonical задает группу консьюмера канонических событий.
	ConsumerGroupCanonical = "cbl-payment-canonical"
	// maxConflictAttempts задает число попыток применения события при
	// конфликте версии состояния (гонка с другими переходами).
	maxConflictAttempts = 3
	// maxConfirmations ограничивает подтверждения недоверенного события:
	// пороги сетей на порядки меньше, подделанное 2^32-1 не должно проходить.
	maxConfirmations = 1_000_000
	// maxIdentifierBytes ограничивает идентификаторы события в БД и логах.
	maxIdentifierBytes = 256
)

// errInvalidCanonicalEvent сообщает о некорректном событии: такой вход штатно
// уходит в retry-топики и DLQ, а не обрабатывается бесконечно.
var errInvalidCanonicalEvent = errors.New("usecase: invalid canonical event")

// CanonicalStore описывает операции платежей, нужные consumer-у. Методы
// принимают транзакцию Guard (tx=nil - собственная транзакция репозитория):
// переходы и inbox-запись коммитятся атомарно (CBL-003, раздел 6).
type CanonicalStore interface {
	// PaymentTx возвращает платеж без истории переходов.
	PaymentTx(ctx context.Context, tx *gorm.DB, id string) (storage.Payment, error)
	// TransitionTx выполняет оптимистичный переход состояния.
	TransitionTx(ctx context.Context, tx *gorm.DB, id string, expectedVersion uint64, to domain.State, patch storage.TransitionPatch) (storage.Payment, error)
}

// CanonicalConsumer переводит канонические события шлюза в переходы автомата
// платежа. Дубли отсекает inbox (kit Processor), устаревшие события и события
// не по порядку - доменный план (PlanEvent) и CAS по state_version; финальные
// состояния completed/underpaid/overpaid определяет политика платежа (4.5).
type CanonicalConsumer struct {
	store    CanonicalStore
	policy   domain.AmountPolicy
	log      *zap.Logger
	attempts int
	clock    func() time.Time
}

// Option настраивает CanonicalConsumer.
type Option func(*CanonicalConsumer)

// WithAmountPolicy задает политику порогов подтверждений и толерансов.
func WithAmountPolicy(policy domain.AmountPolicy) Option {
	return func(c *CanonicalConsumer) { c.policy = policy }
}

// WithClock задает источник времени (терминальные состояния фиксируют completed_at).
func WithClock(clock func() time.Time) Option {
	return func(c *CanonicalConsumer) {
		if clock != nil {
			c.clock = clock
		}
	}
}

// NewCanonicalConsumer создает CanonicalConsumer с политикой по умолчанию.
func NewCanonicalConsumer(store CanonicalStore, log *zap.Logger, opts ...Option) *CanonicalConsumer {
	if log == nil {
		log = zap.NewNop()
	}
	consumer := &CanonicalConsumer{
		store:    store,
		policy:   domain.DefaultAmountPolicy(),
		log:      log,
		attempts: maxConflictAttempts,
		clock:    time.Now,
	}
	for _, opt := range opts {
		opt(consumer)
	}
	return consumer
}

// Handle обрабатывает каноническое событие в транзакции Guard: проверяет вход,
// строит план переходов и применяет его с повторами при конфликте версии.
func (c *CanonicalConsumer) Handle(ctx context.Context, tx *gorm.DB, msg inbox.Message) error {
	traceparent := msg.Headers[outbox.HeaderTraceparent]
	var event eventsv1.CanonicalPaymentEvent
	if err := proto.Unmarshal(msg.Value, &event); err != nil {
		return fmt.Errorf("%w: unmarshal: %w", errInvalidCanonicalEvent, err)
	}
	eventType, skip := canonicalType(event.GetType())
	if skip {
		c.log.Warn("canonical event without known type skipped",
			zap.String("payment_id", shortID(event.GetPaymentId())),
			zap.String("type", event.GetType().String()),
			zap.String("traceparent", traceparent),
		)
		return nil
	}
	if err := validateEventCommon(&event, eventType); err != nil {
		return err
	}
	paymentID := canonicalID(event.GetPaymentId())

	for attempt := 0; attempt < c.attempts; attempt++ {
		payment, getErr := c.store.PaymentTx(ctx, tx, paymentID)
		if getErr != nil {
			// Событие не может появиться раньше транзакции создания (4.2):
			// короткая гонка закрывается ретраями, постоянная - DLQ.
			return fmt.Errorf("usecase: canonical event: load payment %s: %w", shortID(event.GetPaymentId()), getErr)
		}
		received, err := validateReceived(&event, payment)
		if err != nil {
			return err
		}
		current, err := domain.ParseState(payment.State)
		if err != nil {
			return fmt.Errorf("usecase: canonical payment %s: %w", payment.ID, err)
		}
		plan, planErr := domain.PlanEvent(current, eventType)
		if errors.Is(planErr, domain.ErrEventTooEarly) {
			return fmt.Errorf("usecase: canonical event too early for %s: %w", payment.ID, planErr)
		}
		if planErr != nil {
			return fmt.Errorf("usecase: canonical plan for %s: %w", payment.ID, planErr)
		}
		if len(plan) == 0 {
			// Деньги в сети после истечения счета требуют ручного разбора
			// (KI-payment-08): событие в порядке обработки опоздало, но
			// оплату нельзя просто отбросить - отдельное сообщение для
			// алертов по логам (метрики - 0.7).
			if current == domain.StateExpired &&
				(eventType == domain.CanonicalDetected || eventType == domain.CanonicalConfirmed) {
				c.log.Warn("late canonical event after expiry needs manual review",
					zap.String("payment_id", payment.ID),
					zap.String("type", string(eventType)),
					zap.String("state", string(current)),
					zap.Int64("state_version", payment.StateVersion),
					zap.String("traceparent", traceparent))
				return nil
			}
			c.log.Warn("stale canonical event skipped",
				zap.String("payment_id", payment.ID),
				zap.String("type", string(eventType)),
				zap.String("state", string(current)),
				zap.String("traceparent", traceparent))
			return nil
		}
		if override, overrideErr := c.settlementPlan(payment, current, &event, eventType); overrideErr != nil {
			return overrideErr
		} else if override != nil {
			plan = override
		}

		version := uint64(payment.StateVersion) //nolint:gosec // state_version ограничен CHECK (>= 1)
		applied := true
		for i, target := range plan {
			patch := storage.TransitionPatch{Reason: "canonical " + string(eventType)}
			if i == len(plan)-1 {
				// Данные события фиксируются на целевом состоянии (FR-3).
				patch = mergePatches(patch, received, canonicalPatch(&event, eventType))
				if target == domain.StateCompleted || target == domain.StateUnderpaid || target == domain.StateOverpaid {
					completedAt := c.clock().UTC()
					patch.CompletedAt = &completedAt
				}
			}
			updated, transitionErr := c.store.TransitionTx(ctx, tx, payment.ID, version, target, patch)
			if errors.Is(transitionErr, domain.ErrStateConflict) {
				applied = false
				break
			}
			if transitionErr != nil {
				return fmt.Errorf("usecase: apply canonical %s -> %s: %w", payment.ID, target, transitionErr)
			}
			version = uint64(updated.StateVersion) //nolint:gosec // state_version ограничен CHECK
		}
		if applied {
			c.log.Info("canonical event applied",
				zap.String("payment_id", payment.ID),
				zap.String("type", string(eventType)),
				zap.String("state", string(plan[len(plan)-1])),
				zap.Uint64("state_version", version),
				zap.Bool("conflict_retry", attempt > 0),
				zap.String("traceparent", traceparent))
			return nil
		}
	}
	return fmt.Errorf("usecase: canonical event for %s: %w", shortID(event.GetPaymentId()), domain.ErrStateConflict)
}

// canonicalID канонизирует payment_id (нижний регистр) для запросов к БД.
func canonicalID(raw string) string {
	parsed, err := uuid.Parse(raw)
	if err != nil {
		return raw
	}
	return parsed.String()
}

// shortID усекает идентификатор для ошибок и логов: недоверенные значения
// ограниченной длины не должны раздувать сообщения DLQ.
func shortID(raw string) string {
	if len(raw) <= 64 {
		return raw
	}
	return raw[:64] + "..."
}

// settlementPlan пересчитывает план для денежных событий по политике (4.5):
// политика платформы главнее классификации Connector. Возвращает nil, если
// событие не влияет на сумму (detected/failed/expired).
func (c *CanonicalConsumer) settlementPlan(payment storage.Payment, current domain.State, event *eventsv1.CanonicalPaymentEvent, eventType domain.CanonicalEventType) ([]domain.State, error) {
	switch eventType {
	case domain.CanonicalConfirmed, domain.CanonicalUnderpaid, domain.CanonicalOverpaid:
	default:
		return nil, nil
	}
	received := event.GetReceivedAmount().GetMinorUnits()
	required := c.policy.RequiredConfirmations(payment.ExpectedAsset, payment.ExpectedAmount)
	decision := c.policy.Decide(received, payment.ExpectedAmount, int(event.GetConfirmations()), required)
	target := domain.State(decision)
	chain, err := domain.ChainTo(current, target)
	if err != nil {
		return nil, fmt.Errorf("usecase: canonical %s settlement: %w", payment.ID, err)
	}
	if len(chain) == 0 {
		// Целевое совпало с текущим: self-переход фиксирует данные события.
		chain = []domain.State{current}
	}
	return chain, nil
}

// canonicalType переводит proto-enum в доменный тип; skip=true для
// UNSPECIFIED и неизвестных значений (подтверждаются без эффекта).
func canonicalType(value eventsv1.CanonicalPaymentEventType) (domain.CanonicalEventType, bool) {
	switch value {
	case eventsv1.CanonicalPaymentEventType_CANONICAL_PAYMENT_EVENT_TYPE_DETECTED:
		return domain.CanonicalDetected, false
	case eventsv1.CanonicalPaymentEventType_CANONICAL_PAYMENT_EVENT_TYPE_CONFIRMED:
		return domain.CanonicalConfirmed, false
	case eventsv1.CanonicalPaymentEventType_CANONICAL_PAYMENT_EVENT_TYPE_FAILED:
		return domain.CanonicalFailed, false
	case eventsv1.CanonicalPaymentEventType_CANONICAL_PAYMENT_EVENT_TYPE_EXPIRED:
		return domain.CanonicalExpired, false
	case eventsv1.CanonicalPaymentEventType_CANONICAL_PAYMENT_EVENT_TYPE_UNDERPAID:
		return domain.CanonicalUnderpaid, false
	case eventsv1.CanonicalPaymentEventType_CANONICAL_PAYMENT_EVENT_TYPE_OVERPAID:
		return domain.CanonicalOverpaid, false
	default:
		return "", true
	}
}

// validateReceived проверяет недоверенную полученную сумму события и сверяет
// ее реквизиты с ожидаемыми реквизитами платежа (актив и шкала).
func validateReceived(event *eventsv1.CanonicalPaymentEvent, payment storage.Payment) (*storage.TransitionPatch, error) {
	received := event.GetReceivedAmount()
	if received == nil {
		return nil, nil
	}
	if received.GetMinorUnits() < 0 {
		return nil, fmt.Errorf("%w: negative received amount", errInvalidCanonicalEvent)
	}
	asset := strings.ToUpper(strings.TrimSpace(received.GetAsset()))
	if !validAsset(asset) {
		return nil, fmt.Errorf("%w: received asset %q", errInvalidCanonicalEvent, shortID(received.GetAsset()))
	}
	if asset != payment.ExpectedAsset || int(received.GetDecimals()) != payment.ExpectedDecimals {
		return nil, fmt.Errorf("%w: received %s/%d does not match expected %s/%d",
			errInvalidCanonicalEvent, asset, received.GetDecimals(), payment.ExpectedAsset, payment.ExpectedDecimals)
	}
	amount := received.GetMinorUnits()
	decimals := int(received.GetDecimals())
	return &storage.TransitionPatch{
		ReceivedAmount:   &amount,
		ReceivedAsset:    &asset,
		ReceivedDecimals: &decimals,
	}, nil
}

// validateEvent общие проверки недоверенного события: идентификатор,
// подтверждения, длины строк.
func validateEventCommon(event *eventsv1.CanonicalPaymentEvent, eventType domain.CanonicalEventType) error {
	if _, err := uuid.Parse(event.GetPaymentId()); err != nil {
		return fmt.Errorf("%w: payment_id %q", errInvalidCanonicalEvent, shortID(event.GetPaymentId()))
	}
	if event.GetConfirmations() > maxConfirmations {
		return fmt.Errorf("%w: confirmations %d", errInvalidCanonicalEvent, event.GetConfirmations())
	}
	if eventType == domain.CanonicalDetected || eventType == domain.CanonicalConfirmed ||
		eventType == domain.CanonicalUnderpaid || eventType == domain.CanonicalOverpaid {
		if event.GetReceivedAmount() == nil {
			return fmt.Errorf("%w: received_amount is required for %s", errInvalidCanonicalEvent, eventType)
		}
	}
	for name, value := range map[string]string{
		"txid":       event.GetTxid(),
		"gateway_id": event.GetGatewayId(),
		"provider":   event.GetProvider(),
		"network":    event.GetNetwork(),
	} {
		if len(value) > maxIdentifierBytes {
			return fmt.Errorf("%w: %s is too long", errInvalidCanonicalEvent, name)
		}
	}
	return nil
}

// canonicalPatch собирает патч данных события (идентификаторы, подтверждения).
func canonicalPatch(event *eventsv1.CanonicalPaymentEvent, eventType domain.CanonicalEventType) storage.TransitionPatch {
	patch := storage.TransitionPatch{}
	if txid := strings.TrimSpace(event.GetTxid()); txid != "" {
		patch.TxID = &txid
	}
	if gatewayID := strings.TrimSpace(event.GetGatewayId()); gatewayID != "" {
		patch.GatewayID = &gatewayID
	}
	if provider := strings.TrimSpace(event.GetProvider()); provider != "" {
		patch.Provider = &provider
	}
	if network := strings.TrimSpace(event.GetNetwork()); network != "" {
		patch.Network = &network
	}
	if eventType == domain.CanonicalConfirmed || eventType == domain.CanonicalDetected {
		confirmations := int(event.GetConfirmations())
		patch.Confirmations = &confirmations
	}
	return patch
}

// mergePatches объединяет патчи, не изменяя исходные.
func mergePatches(base storage.TransitionPatch, extra *storage.TransitionPatch, event storage.TransitionPatch) storage.TransitionPatch {
	merged := base
	if extra != nil {
		merged.ReceivedAmount = extra.ReceivedAmount
		merged.ReceivedAsset = extra.ReceivedAsset
		merged.ReceivedDecimals = extra.ReceivedDecimals
	}
	if event.TxID != nil {
		merged.TxID = event.TxID
	}
	if event.GatewayID != nil {
		merged.GatewayID = event.GatewayID
	}
	if event.Provider != nil {
		merged.Provider = event.Provider
	}
	if event.Network != nil {
		merged.Network = event.Network
	}
	if event.Confirmations != nil {
		merged.Confirmations = event.Confirmations
	}
	return merged
}

// validAsset проверяет нормализованный код актива: 2 - 16 символов латиницы
// и цифр в верхнем регистре (та же граница, что в CHECK схемы платежа).
func validAsset(asset string) bool {
	if len(asset) < 2 || len(asset) > 16 {
		return false
	}
	for _, r := range asset {
		if (r < 'A' || r > 'Z') && (r < '0' || r > '9') {
			return false
		}
	}
	return true
}
