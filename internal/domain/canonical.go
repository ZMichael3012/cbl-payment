package domain

import (
	"errors"
	"fmt"
)

// CanonicalEventType задает тип канонического события шлюза.
type CanonicalEventType string

// Канонические типы событий соответствуют billing.events.v1.CanonicalPaymentEvent.
const (
	// CanonicalDetected задает обнаружение оплаты в сети.
	CanonicalDetected CanonicalEventType = "detected"
	// CanonicalConfirmed задает подтверждение оплаты сетью.
	CanonicalConfirmed CanonicalEventType = "confirmed"
	// CanonicalFailed задает неуспех платежа у шлюза или в сети.
	CanonicalFailed CanonicalEventType = "failed"
	// CanonicalExpired задает истечение срока счета без оплаты.
	CanonicalExpired CanonicalEventType = "expired"
	// CanonicalUnderpaid задает оплату меньше ожидаемой суммы.
	CanonicalUnderpaid CanonicalEventType = "underpaid"
	// CanonicalOverpaid задает оплату больше ожидаемой суммы.
	CanonicalOverpaid CanonicalEventType = "overpaid"
)

// Ошибки обработки канонических событий.
var (
	// ErrEventTooEarly сообщает, что событие опередило создание счета:
	// обработку нужно повторить после перехода платежа в pending.
	ErrEventTooEarly = errors.New("payment: canonical event arrived too early")
	// ErrUnknownEvent сообщает о неизвестном типе канонического события.
	ErrUnknownEvent = errors.New("payment: unknown canonical event type")
)

// PlanEvent строит цепочку переходов автомата для канонического события.
// Пустая цепочка без ошибки означает устаревшее событие (идемпотентный no-op):
// текущее состояние уже дальше события или терминально. ErrEventTooEarly
// возвращается для creating/created: событие повторится через retry-топики.
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
			// Подтверждение раньше обнаружения: применяем минимальную
			// допустимую цепочку detected -> confirming.
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
		// Оплата уже обнаружена: истечение счета устарело.
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
