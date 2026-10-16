package domain

import (
	"errors"
	"fmt"
	"sort"
)

// Errors of the payment state machine.
var (
	// ErrInvalidState reports an unknown state.
	ErrInvalidState = errors.New("payment: invalid state")
	// ErrForbiddenTransition reports a transition forbidden by the matrix.
	ErrForbiddenTransition = errors.New("payment: forbidden transition")
	// ErrTerminalState reports an attempt to change a terminal state.
	ErrTerminalState = errors.New("payment: terminal state is immutable")
	// ErrStateConflict reports a state version conflict (optimistic lock).
	ErrStateConflict = errors.New("payment: state version conflict")
)

// State defines a state of the payment finite state machine.
type State string

// Canonical states correspond to billing.payment.v1.PaymentState values
// (CF-1): the names match the enum suffixes in lower case.
const (
	// StateCreating - the payment record is created, the quote is being locked,
	// the gateway address has not been received yet (external calls 4.2).
	StateCreating State = "creating"
	// StateCreated - the quote is locked, the gateway address is awaited.
	StateCreated State = "created"
	// StatePending - the invoice is created at the gateway, the address is issued, payment is awaited.
	StatePending State = "pending"
	// StateDetected - the payment is detected on the network.
	StateDetected State = "detected"
	// StateConfirming - the payment is accumulating confirmations.
	StateConfirming State = "confirming"
	// StateCompleted - enough confirmations, the payment is completed.
	StateCompleted State = "completed"
	// StateExpired - the invoice expired without payment.
	StateExpired State = "expired"
	// StateUnderpaid - the payment is smaller than the expected amount.
	StateUnderpaid State = "underpaid"
	// StateOverpaid - the payment is larger than the expected amount.
	StateOverpaid State = "overpaid"
	// StateFailed - the payment failed (network or gateway decline).
	StateFailed State = "failed"
	// StateCancelled - the payment was cancelled before payment detection.
	StateCancelled State = "cancelled"
)

// transitions defines the matrix of allowed state machine transitions: the single
// source of rules for the application and the basis for the second DB barrier (4.1).
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

// terminalStates defines states without outgoing transitions.
var terminalStates = map[State]struct{}{
	StateCompleted: {},
	StateExpired:   {},
	StateUnderpaid: {},
	StateOverpaid:  {},
	StateFailed:    {},
	StateCancelled: {},
}

// ParseState parses a string into a state machine state.
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

// AllowedTransition reports whether the transition is allowed by the matrix.
func AllowedTransition(from, to State) bool {
	next, ok := transitions[from]
	if !ok {
		return false
	}
	_, ok = next[to]
	return ok
}

// Terminal reports whether the state is terminal.
func Terminal(state State) bool {
	_, ok := terminalStates[state]
	return ok
}

// ChainTo returns the shortest chain of transitions from state from to state to
// along the state machine matrix; an empty chain means the states are equal.
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

// NextVersion validates the transition and returns the new state version.
// A self-transition (updating confirmations, etc.) is allowed only in
// non-terminal states; terminal states are immutable.
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
