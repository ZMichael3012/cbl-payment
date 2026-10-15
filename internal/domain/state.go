package domain

import (
	"errors"
	"fmt"
	"sort"
)

// Ошибки автомата состояний платежа.
var (
	// ErrInvalidState сообщает о неизвестном состоянии.
	ErrInvalidState = errors.New("payment: invalid state")
	// ErrForbiddenTransition сообщает о переходе, запрещенном матрицей.
	ErrForbiddenTransition = errors.New("payment: forbidden transition")
	// ErrTerminalState сообщает о попытке изменить терминальное состояние.
	ErrTerminalState = errors.New("payment: terminal state is immutable")
	// ErrStateConflict сообщает о конфликте версии состояния (оптимистичная блокировка).
	ErrStateConflict = errors.New("payment: state version conflict")
)

// State задает состояние конечного автомата платежа.
type State string

// Канонические состояния соответствуют значениям billing.payment.v1.PaymentState
// (CF-1): имена совпадают по нижнему регистру с суффиксами enum.
const (
	// StateCreating - запись платежа создана, котировка фиксируется,
	// адрес шлюза еще не получен (внешние вызовы 4.2).
	StateCreating State = "creating"
	// StateCreated - котировка зафиксирована, адрес шлюза ожидается.
	StateCreated State = "created"
	// StatePending - счет создан в шлюзе, выдан адрес, ожидается оплата.
	StatePending State = "pending"
	// StateDetected - оплата обнаружена в сети.
	StateDetected State = "detected"
	// StateConfirming - оплата набирает подтверждения.
	StateConfirming State = "confirming"
	// StateCompleted - подтверждений достаточно, платеж завершен.
	StateCompleted State = "completed"
	// StateExpired - срок счета истек без оплаты.
	StateExpired State = "expired"
	// StateUnderpaid - оплата меньше ожидаемой суммы.
	StateUnderpaid State = "underpaid"
	// StateOverpaid - оплата больше ожидаемой суммы.
	StateOverpaid State = "overpaid"
	// StateFailed - платеж неуспешен (отказ сети или шлюза).
	StateFailed State = "failed"
	// StateCancelled - платеж отменен до обнаружения оплаты.
	StateCancelled State = "cancelled"
)

// transitions задает матрицу разрешенных переходов автомата: единственный
// источник правил для приложения и основа для второго барьера БД (4.1).
var transitions = map[State]map[State]struct{}{
	StateCreating: {
		StateCreated:   {},
		StateCancelled: {},
		StateFailed:    {},
	},
	StateCreated: {
		StatePending:   {},
		StateCancelled: {},
		StateExpired:   {},
		StateFailed:    {},
	},
	StatePending: {
		StateDetected:  {},
		StateCancelled: {},
		StateExpired:   {},
		StateFailed:    {},
	},
	StateDetected: {
		StateConfirming: {},
		StateUnderpaid:  {},
		StateOverpaid:   {},
		StateFailed:     {},
	},
	StateConfirming: {
		StateCompleted: {},
		StateUnderpaid: {},
		StateOverpaid:  {},
		StateFailed:    {},
	},
}

// terminalStates задает состояния без исходящих переходов.
var terminalStates = map[State]struct{}{
	StateCompleted: {},
	StateExpired:   {},
	StateUnderpaid: {},
	StateOverpaid:  {},
	StateFailed:    {},
	StateCancelled: {},
}

// ParseState разбирает строку в состояние автомата.
func ParseState(raw string) (State, error) {
	state := State(raw)
	if _, ok := transitions[state]; ok {
		return state, nil
	}
	if _, ok := terminalStates[state]; ok {
		return state, nil
	}
	return "", fmt.Errorf("%w: %q", ErrInvalidState, raw)
}

// AllowedTransition сообщает, разрешен ли переход матрицей.
func AllowedTransition(from, to State) bool {
	next, ok := transitions[from]
	if !ok {
		return false
	}
	_, ok = next[to]
	return ok
}

// Terminal сообщает, является ли состояние терминальным.
func Terminal(state State) bool {
	_, ok := terminalStates[state]
	return ok
}

// ChainTo возвращает кратчайшую цепочку переходов из состояния from в to
// по матрице автомата; пустая цепочка означает совпадение состояний.
func ChainTo(from, to State) ([]State, error) {
	if _, err := ParseState(string(from)); err != nil {
		return nil, err
	}
	if _, err := ParseState(string(to)); err != nil {
		return nil, err
	}
	if from == to {
		return nil, nil
	}
	if Terminal(from) {
		return nil, fmt.Errorf("%w: %s", ErrTerminalState, from)
	}
	type node struct {
		state State
		path  []State
	}
	queue := []node{{state: from}}
	visited := map[State]struct{}{from: {}}
	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]
		nexts := make([]State, 0, len(transitions[current.state]))
		for target := range transitions[current.state] {
			nexts = append(nexts, target)
		}
		sort.Slice(nexts, func(i, j int) bool { return nexts[i] < nexts[j] })
		for _, target := range nexts {
			path := append(append([]State{}, current.path...), target)
			if target == to {
				return path, nil
			}
			if _, ok := visited[target]; !ok {
				visited[target] = struct{}{}
				queue = append(queue, node{state: target, path: path})
			}
		}
	}
	return nil, fmt.Errorf("%w: %s -> %s", ErrForbiddenTransition, from, to)
}

// NextVersion проверяет переход и возвращает новую версию состояния.
// Самопереход (обновление confirmations и т.п.) допустим только в
// нетерминальных состояниях; терминальные состояния неизменяемы.
func NextVersion(from, to State, version uint64) (uint64, error) {
	if version == 0 {
		return 0, fmt.Errorf("%w: zero version", ErrStateConflict)
	}
	if _, err := ParseState(string(from)); err != nil {
		return 0, err
	}
	if _, err := ParseState(string(to)); err != nil {
		return 0, err
	}
	if Terminal(from) {
		return 0, fmt.Errorf("%w: %s", ErrTerminalState, from)
	}
	if from == to {
		return version + 1, nil
	}
	if !AllowedTransition(from, to) {
		return 0, fmt.Errorf("%w: %s -> %s", ErrForbiddenTransition, from, to)
	}
	return version + 1, nil
}
