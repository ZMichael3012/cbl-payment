package domain

import (
	"errors"
	"fmt"
)

// CanonicalEventType defines the type of a canonical gateway event.
type CanonicalEventType string

// Canonical event types correspond to billing.events.v1.CanonicalPaymentEvent.
const (
	// CanonicalDetected defines detection of a payment on the network.
	CanonicalDetected CanonicalEventType = "detected"
	// CanonicalConfirmed defines confirmation of the payment by the network.
	CanonicalConfirmed CanonicalEventType = "confirmed"
	// CanonicalFailed defines a payment failure at the gateway or on the network.
	CanonicalFailed CanonicalEventType = "failed"
	// CanonicalExpired defines expiry of the invoice without payment.
	CanonicalExpired CanonicalEventType = "expired"
	// CanonicalUnderpaid defines a payment smaller than the expected amount.
	CanonicalUnderpaid CanonicalEventType = "underpaid"
	// CanonicalOverpaid defines a payment larger than the expected amount.
	CanonicalOverpaid CanonicalEventType = "overpaid"
)

// Errors of canonical event handling.
var (
	// ErrEventTooEarly reports that the event arrived before invoice creation:
	// handling must be retried after the payment transitions to pending.
	ErrEventTooEarly = errors.New("payment: canonical event arrived too early")
	// ErrUnknownEvent reports an unknown canonical event type.
	ErrUnknownEvent = errors.New("payment: unknown canonical event type")
)

// PlanEvent builds the state machine transition chain for a canonical event.
// An empty chain without an error means a stale event (idempotent no-op):
// the current state is already ahead of the event or terminal. ErrEventTooEarly
// is returned for creating/created: the event will be retried through retry topics.
func PlanEvent(current State, event CanonicalEventType) ([]State, error) {
	switch event {
	case CanonicalDetected, CanonicalConfirmed, CanonicalFailed,
		CanonicalExpired, CanonicalUnderpaid, CanonicalOverpaid:
	default:
		return nil, fmt.Errorf("%w: %q", ErrUnknownEvent, event)
	}
	if current == StateCreating || current == StateCreated {
		return nil, fmt.Errorf("%w: state %s, event %s", ErrEventTooEarly, current, event)
	}
	if Terminal(current) {
		return nil, nil
	}
	switch event {
	case CanonicalDetected:
		switch current {
		case StatePending, StateDetected:
			return []State{StateDetected}, nil
		default:
			return nil, nil
		}
	case CanonicalConfirmed:
		switch current {
		case StatePending:
			// Confirmation before detection: apply the minimal
			// allowed chain detected -> confirming.
			return []State{StateDetected, StateConfirming}, nil
		case StateDetected, StateConfirming:
			return []State{StateConfirming}, nil
		default:
			return nil, nil
		}
	case CanonicalFailed:
		return []State{StateFailed}, nil
	case CanonicalExpired:
		if current == StatePending {
			return []State{StateExpired}, nil
		}
		// The payment has already been detected: the invoice expiry is stale.
		return nil, nil
	case CanonicalUnderpaid:
		if current == StatePending {
			return []State{StateDetected, StateUnderpaid}, nil
		}
		return []State{StateUnderpaid}, nil
	case CanonicalOverpaid:
		if current == StatePending {
			return []State{StateDetected, StateOverpaid}, nil
		}
		return []State{StateOverpaid}, nil
	}
	return nil, fmt.Errorf("%w: %q", ErrUnknownEvent, event)
}
