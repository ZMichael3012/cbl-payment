package storage

import (
	"context"
	"regexp"
	"time"

	"gorm.io/gorm"

	"github.com/cbl/cbl-payment/internal/domain"
)

// TransitionEvent describes an applied state transition for publication to
// payment.events (4.7): a payment snapshot after the transition and journal service fields.
type TransitionEvent struct {
	// Payment holds the payment snapshot after the transition (including the new version).
	Payment Payment
	// From holds the state before the transition.
	From domain.State
	// To holds the state after the transition.
	To domain.State
	// OccurredAt holds the transition time (the journal row).
	OccurredAt time.Time
	// Traceparent holds the W3C trace context for the envelope (if known).
	Traceparent string
}

// TransitionEventSink writes a payment.events event to the outbox within the
// transition transaction: the event and the business change commit or roll back together.
type TransitionEventSink interface {
	// AppendTransitionEvent appends the event envelope within the given transaction.
	AppendTransitionEvent(ctx context.Context, tx *gorm.DB, event TransitionEvent) error
}

// WithEventSink enables payment.events writing; without a sink no events are written.
func WithEventSink(sink TransitionEventSink) Option {
	return func(r *Repository) { r.eventSink = sink }
}

// emitTransitionEvent passes the event to the sink when it is configured; an
// explicitly set traceparent is not overwritten by the value from the context.
func (r *Repository) emitTransitionEvent(ctx context.Context, tx *gorm.DB, event TransitionEvent) error {
	if r.eventSink == nil {
		return nil
	}
	if event.Traceparent == "" {
		event.Traceparent = traceparentFromContext(ctx)
	}
	return r.eventSink.AppendTransitionEvent(ctx, tx, event)
}

// traceparentKey holds the context key for the W3C traceparent.
type traceparentKey struct{}

// traceparentPattern holds the W3C trace context format; an arbitrary string
// from a Kafka header must not reach envelopes and downstream logs.
var traceparentPattern = regexp.MustCompile(`^[0-9a-f]{2}-[0-9a-f]{32}-[0-9a-f]{16}-[0-9a-f]{2}$`)

// WithTraceparent stores the traceparent in the context: the consumer passes the
// canonical event header, and the payment event inherits the source tracing.
// A header with an invalid W3C format is ignored.
func WithTraceparent(ctx context.Context, traceparent string) context.Context {
	if !traceparentPattern.MatchString(traceparent) {
		return ctx
	}
	return context.WithValue(ctx, traceparentKey{}, traceparent)
}

// traceparentFromContext extracts the traceparent from the context.
func traceparentFromContext(ctx context.Context) string {
	value, _ := ctx.Value(traceparentKey{}).(string)
	return value
}
