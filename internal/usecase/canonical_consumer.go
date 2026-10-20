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

	eventsv1 "github.com/ZMichael3012/cbl-protos/gen/go/billing/events/v1"

	"github.com/ZMichael3012/cbl-kit/inbox"
	"github.com/ZMichael3012/cbl-kit/outbox"

	commonv1 "github.com/ZMichael3012/cbl-protos/gen/go/billing/common/v1"

	"github.com/cbl/cbl-payment/internal/domain"
	"github.com/cbl/cbl-payment/internal/storage"
)

// Constants of the canonical event consumer pipeline.
const (
	// TopicGatewayEventsCanonical defines the gateway canonical events topic.
	TopicGatewayEventsCanonical = "gateway.events.canonical"
	// ConsumerGroupCanonical defines the canonical events consumer group.
	ConsumerGroupCanonical = "cbl-payment-canonical"
	// maxConflictAttempts defines the number of event application attempts on
	// a state version conflict (race with other transitions).
	maxConflictAttempts = 3
	// maxConfirmations bounds the confirmations of an untrusted event:
	// network thresholds are orders of magnitude smaller, a forged 2^32-1 must not pass.
	maxConfirmations = 1_000_000
	// maxIdentifierBytes bounds event identifiers in the DB and logs.
	maxIdentifierBytes = 256
)

// errInvalidCanonicalEvent reports an invalid event: such input goes to retry
// topics and the DLQ as designed, and is not processed forever.
var errInvalidCanonicalEvent = errors.New("usecase: invalid canonical event")

// CanonicalStore describes the payment operations the consumer needs. The methods
// accept the Guard transaction (tx=nil means the repository's own transaction):
// transitions and the inbox record commit atomically (CBL-003, section 6).
type CanonicalStore interface {
	// PaymentTx returns the payment without transition history.
	PaymentTx(ctx context.Context, tx *gorm.DB, id string) (storage.Payment, error)
	// TransitionTx performs an optimistic state transition.
	TransitionTx(ctx context.Context, tx *gorm.DB, id string, expectedVersion uint64, to domain.State, patch storage.TransitionPatch) (storage.Payment, error)
	// CreateDerivedTx inserts a derived completed top-up payment (17.10).
	CreateDerivedTx(ctx context.Context, tx *gorm.DB, payment storage.Payment, reason string) (storage.Payment, error)
	// LockAndSumTopUpsTx locks the parent row and returns the baseline already
	// attributed to it (the recorded amount plus the derived children).
	LockAndSumTopUpsTx(ctx context.Context, tx *gorm.DB, parentID string) (int64, error)
}

// CanonicalConsumer translates gateway canonical events into payment state
// machine transitions. Duplicates are filtered by the inbox (kit Processor),
// stale and out-of-order events by the domain plan (PlanEvent) and CAS on
// state_version; the final states completed/underpaid/overpaid are decided
// by the payment policy (4.5).
type CanonicalConsumer struct {
	store    CanonicalStore
	policy   domain.AmountPolicy
	log      *zap.Logger
	attempts int
	clock    func() time.Time
}

// Option configures CanonicalConsumer.
type Option func(*CanonicalConsumer)

// WithAmountPolicy sets the confirmation threshold and tolerance policy.
func WithAmountPolicy(policy domain.AmountPolicy) Option {
	return func(c *CanonicalConsumer) { c.policy = policy }
}

// WithClock sets the time source (terminal states record completed_at).
func WithClock(clock func() time.Time) Option {
	return func(c *CanonicalConsumer) {
		if clock != nil {
			c.clock = clock
		}
	}
}

// NewCanonicalConsumer creates a CanonicalConsumer with the default policy.
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

// Handle processes a canonical event in the Guard transaction: it validates
// the input, builds the transition plan and applies it with retries on version
// conflict.
func (c *CanonicalConsumer) Handle(ctx context.Context, tx *gorm.DB, msg inbox.Message) error {
	traceparent := msg.Headers[outbox.HeaderTraceparent]
	// The source tracing is carried into the context: the payment.events event
	// inherits the traceparent of the canonical event (4.7).
	ctx = storage.WithTraceparent(ctx, traceparent)
	var event eventsv1.CanonicalPaymentEvent
	if err := proto.Unmarshal(msg.Value, &event); err != nil {
		return fmt.Errorf("%w: unmarshal: %w", errInvalidCanonicalEvent, err)
	}
	eventType, skip := canonicalType(event.GetType())
	if skip {
		c.log.Warn("canonical event without known type skipped",
			zap.String("payment_id", shortID(event.GetPaymentId())),
			zap.String("type", event.GetType().String()),
			zap.String("traceparent", shortID(traceparent)),
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
			// The event cannot appear before the creation transaction (4.2):
			// a short race is closed by retries, a permanent one by the DLQ.
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
			handled, topUpErr := c.handleTopUp(ctx, tx, payment, current, &event, eventType)
			if topUpErr != nil {
				return topUpErr
			}
			if handled {
				return nil
			}
			// Money on the network after invoice expiry or merchant
			// cancellation needs manual review (KI-payment-08, KI-payment-09):
			// the event arrived late in processing order, but the payment
			// cannot simply be dropped - a separate message for log-based
			// alerts (metrics - 0.7).
			if (current == domain.StateExpired || current == domain.StateCancelled) &&
				(eventType == domain.CanonicalDetected || eventType == domain.CanonicalConfirmed) {
				c.log.Warn("late canonical event after terminal state needs manual review",
					zap.String("payment_id", payment.ID),
					zap.String("type", string(eventType)),
					zap.String("state", string(current)),
					zap.Int64("state_version", payment.StateVersion),
					zap.String("traceparent", shortID(traceparent)))
				return nil
			}
			c.log.Warn("stale canonical event skipped",
				zap.String("payment_id", payment.ID),
				zap.String("type", string(eventType)),
				zap.String("state", string(current)),
				zap.String("traceparent", shortID(traceparent)))
			return nil
		}
		if override, overrideErr := c.settlementPlan(payment, current, &event, eventType); overrideErr != nil {
			return overrideErr
		} else if override != nil {
			plan = override
		}

		version := uint64(payment.StateVersion) //nolint:gosec // state_version is bounded by CHECK (>= 1)
		applied := true
		for i, target := range plan {
			patch := storage.TransitionPatch{Reason: "canonical " + string(eventType)}
			if i == len(plan)-1 {
				// Event data is recorded on the target state (FR-3).
				feeInfo, feeErr := feePatch(&event, payment)
				if feeErr != nil {
					return feeErr
				}
				patch = mergePatches(patch, received, canonicalPatch(&event, eventType))
				patch = mergePatches(patch, nil, feeInfo)
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
			version = uint64(updated.StateVersion) //nolint:gosec // state_version is bounded by CHECK
		}
		if applied {
			c.log.Info("canonical event applied",
				zap.String("payment_id", payment.ID),
				zap.String("type", string(eventType)),
				zap.String("state", string(plan[len(plan)-1])),
				zap.Uint64("state_version", version),
				zap.Bool("conflict_retry", attempt > 0),
				zap.String("traceparent", shortID(traceparent)))
			return nil
		}
	}
	return fmt.Errorf("usecase: canonical event for %s: %w", shortID(event.GetPaymentId()), domain.ErrStateConflict)
}

// canonicalID canonicalizes payment_id (lower case) for DB queries.
func canonicalID(raw string) string {
	parsed, err := uuid.Parse(raw)
	if err != nil {
		return raw
	}
	return parsed.String()
}

// shortID truncates an identifier for errors and logs: untrusted values of
// bounded length must not inflate DLQ messages.
func shortID(raw string) string {
	if len(raw) <= 64 {
		return raw
	}
	return raw[:64] + "..."
}

// settlementPlan recalculates the plan for money events according to the
// policy (4.5): the platform policy prevails over the Connector classification.
// It returns nil if the event does not affect the amount (detected/failed/expired).
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
		// The target equals the current state: a self-transition records the event data.
		chain = []domain.State{current}
	}
	return chain, nil
}

// canonicalType translates the proto enum into the domain type; skip=true for
// UNSPECIFIED and unknown values (acknowledged with no effect).
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

// validateReceived validates the untrusted received amount of the event and
// matches its attributes against the expected payment attributes (asset and scale).
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
		return nil, fmt.Errorf("%w: wrong asset deposit: received %s/%d does not match expected %s/%d",
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

// validateEventCommon performs the common checks of an untrusted event:
// identifier, confirmations, string lengths.
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
	// The raw provider status is persisted and re-emitted: the bound matches the
	// payments.raw_status CHECK (17.10), and non-printable characters are
	// rejected so the value cannot inject into logs or downstream output (E-8).
	if rawStatus := event.GetRawStatus(); len(rawStatus) > 64 || !printableASCII(rawStatus) {
		return fmt.Errorf("%w: raw_status is invalid", errInvalidCanonicalEvent)
	}
	return nil
}

// printableASCII reports that the string contains only printable ASCII
// characters (0x20 - 0x7E): control characters (C0 and C1, DEL) and Unicode
// separators are rejected so raw_status cannot inject into logs or downstream
// consumers (E-8).
func printableASCII(value string) bool {
	for _, r := range value {
		if r < 0x20 || r > 0x7E {
			return false
		}
	}
	return true
}

// canonicalPatch assembles the event data patch (identifiers, confirmations).
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
	if rawStatus := strings.TrimSpace(event.GetRawStatus()); rawStatus != "" {
		patch.RawStatus = &rawStatus
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

// mergePatches merges patches without modifying the originals.
func mergePatches(base storage.TransitionPatch, extra *storage.TransitionPatch, event storage.TransitionPatch) storage.TransitionPatch {
	merged := base
	if extra != nil {
		merged.ReceivedAmount = extra.ReceivedAmount
		merged.ReceivedAsset = extra.ReceivedAsset
		merged.ReceivedDecimals = extra.ReceivedDecimals
	}
	if event.FeeAmount != nil {
		merged.FeeAmount = event.FeeAmount
	}
	if event.FeeAsset != nil {
		merged.FeeAsset = event.FeeAsset
	}
	if event.FeeDecimals != nil {
		merged.FeeDecimals = event.FeeDecimals
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
	if event.RawStatus != nil {
		merged.RawStatus = event.RawStatus
	}
	if event.CheckoutURL != nil {
		merged.CheckoutURL = event.CheckoutURL
	}
	if event.GatewayFee != nil {
		merged.GatewayFee = event.GatewayFee
	}
	if event.NetworkFee != nil {
		merged.NetworkFee = event.NetworkFee
	}
	if event.FxSpread != nil {
		merged.FxSpread = event.FxSpread
	}
	if event.Network != nil {
		merged.Network = event.Network
	}
	if event.Confirmations != nil {
		merged.Confirmations = event.Confirmations
	}
	return merged
}

// validAsset validates a normalized asset code: 2 to 16 upper-case Latin
// letters and digits (the same bound as in the payment schema CHECK).
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

// handleTopUp credits a repeated deposit of a terminal-success payment as a
// derived completed child (17.10, decisions 9/10): only a confirmed event above
// the baseline already attributed to the parent (the recorded amount plus the
// existing derived children, read under the parent row lock) with a txid forms
// a credited delta; a missing txid keeps the deposit on the manual-review path.
// The second value reports that the event was consumed (either credited or
// deliberately left for review).
func (c *CanonicalConsumer) handleTopUp(ctx context.Context, tx *gorm.DB, payment storage.Payment, current domain.State, event *eventsv1.CanonicalPaymentEvent, eventType domain.CanonicalEventType) (bool, error) {
	if eventType != domain.CanonicalConfirmed || payment.ReceivedAmount == nil {
		return false, nil
	}
	switch current {
	case domain.StateCompleted, domain.StateUnderpaid, domain.StateOverpaid:
	default:
		return false, nil
	}
	baseline, err := c.store.LockAndSumTopUpsTx(ctx, tx, payment.ID)
	if err != nil {
		return false, fmt.Errorf("usecase: topup baseline of %s: %w", payment.ID, err)
	}
	eventReceived := event.GetReceivedAmount().GetMinorUnits()
	txid := domain.CanonicalTxID(event.GetTxid())
	delta, outcome := domain.TopUpDelta(current, eventType, &baseline, eventReceived, txid)
	switch outcome {
	case domain.TopUpSkip:
		return false, nil
	case domain.TopUpManualReview:
		// A deposit without a transaction hash cannot be deduplicated against a
		// repeated delivery: it must not be credited automatically (KI-payment-10).
		c.log.Warn("topup deposit without txid needs manual review",
			zap.String("payment_id", payment.ID),
			zap.Int64("baseline", baseline),
			zap.Int64("event_received", eventReceived))
		return true, nil
	}
	childID, err := uuid.NewV7()
	if err != nil {
		return false, fmt.Errorf("usecase: topup payment id: %w", err)
	}
	received := delta
	asset := payment.ExpectedAsset
	decimals := payment.ExpectedDecimals
	child := storage.Payment{
		ID:               childID.String(),
		MerchantID:       payment.MerchantID,
		IdempotencyKey:   "topup:" + payment.ID + ":" + txid,
		RequestHash:      "topup:" + payment.ID + ":" + txid,
		ExpectedAmount:   delta,
		ExpectedAsset:    asset,
		ExpectedDecimals: decimals,
		ReceivedAmount:   &received,
		ReceivedAsset:    &asset,
		ReceivedDecimals: &decimals,
		Network:          payment.Network,
		TxID:             &txid,
		Confirmations:    int(event.GetConfirmations()),
		ParentPaymentID:  &payment.ID,
	}
	if provider := strings.TrimSpace(event.GetProvider()); provider != "" {
		child.Provider = &provider
	} else if payment.Provider != nil {
		child.Provider = payment.Provider
	}
	if gatewayID := strings.TrimSpace(event.GetGatewayId()); gatewayID != "" {
		child.GatewayID = &gatewayID
	} else if payment.GatewayID != nil {
		child.GatewayID = payment.GatewayID
	}
	if rawStatus := strings.TrimSpace(event.GetRawStatus()); rawStatus != "" {
		child.RawStatus = &rawStatus
	}
	reason := "topup deposit of " + payment.ID + " txid " + txid
	created, err := c.store.CreateDerivedTx(ctx, tx, child, reason)
	if errors.Is(err, storage.ErrDuplicateKey) {
		c.log.Warn("topup duplicate skipped",
			zap.String("payment_id", payment.ID),
			zap.String("txid", shortID(txid)))
		return true, nil
	}
	if err != nil {
		return false, fmt.Errorf("usecase: topup deposit of %s: %w", payment.ID, err)
	}
	c.log.Info("topup deposit credited",
		zap.String("parent_payment_id", payment.ID),
		zap.String("child_payment_id", created.ID),
		zap.Int64("delta", delta),
		zap.String("txid", shortID(txid)))
	return true, nil
}

// feePatch validates the provider fee split of a money event and assembles the
// storage patch: the canonical contract carries only the components (cf-4.1),
// the total is their sum following FeeTotal, and the components are persisted
// for the payment.events payload (17.10, D5).
func feePatch(event *eventsv1.CanonicalPaymentEvent, payment storage.Payment) (storage.TransitionPatch, error) {
	gateway := event.GetGatewayFee()
	network := event.GetNetworkFee()
	fx := event.GetFxSpread()
	if gateway == nil && network == nil && fx == nil {
		return storage.TransitionPatch{}, nil
	}
	if event.GetReceivedAmount() == nil {
		return storage.TransitionPatch{}, fmt.Errorf("%w: fee split without received_amount", errInvalidCanonicalEvent)
	}
	for name, part := range map[string]*commonv1.Money{
		"gateway_fee": gateway,
		"network_fee": network,
		"fx_spread":   fx,
	} {
		if part == nil {
			continue
		}
		if part.GetMinorUnits() < 0 {
			return storage.TransitionPatch{}, fmt.Errorf("%w: negative %s", errInvalidCanonicalEvent, name)
		}
		// A zero component is treated as absent; a non-zero one must carry the
		// exact payment asset and scale (E-5), otherwise the money is rejected.
		if part.GetMinorUnits() == 0 {
			continue
		}
		if asset := strings.ToUpper(strings.TrimSpace(part.GetAsset())); asset != payment.ExpectedAsset {
			return storage.TransitionPatch{}, fmt.Errorf("%w: %s asset does not match the payment", errInvalidCanonicalEvent, name)
		}
		if int(part.GetDecimals()) != payment.ExpectedDecimals {
			return storage.TransitionPatch{}, fmt.Errorf("%w: %s scale does not match the payment", errInvalidCanonicalEvent, name)
		}
	}
	received := event.GetReceivedAmount().GetMinorUnits()
	total, err := domain.FeeTotal(0, moneyAmount(gateway), moneyAmount(network), moneyAmount(fx), received)
	if err != nil {
		return storage.TransitionPatch{}, fmt.Errorf("%w: %w", errInvalidCanonicalEvent, err)
	}
	asset := payment.ExpectedAsset
	decimals := payment.ExpectedDecimals
	patch := storage.TransitionPatch{FeeAmount: &total, FeeAsset: &asset, FeeDecimals: &decimals}
	if gateway != nil {
		value := gateway.GetMinorUnits()
		patch.GatewayFee = &value
	}
	if network != nil {
		value := network.GetMinorUnits()
		patch.NetworkFee = &value
	}
	if fx != nil {
		value := fx.GetMinorUnits()
		patch.FxSpread = &value
	}
	return patch, nil
}

// moneyAmount returns the minor units of an optional money (nil means zero).
func moneyAmount(value *commonv1.Money) int64 {
	if value == nil {
		return 0
	}
	return value.GetMinorUnits()
}
