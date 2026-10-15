package domain_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	paymentv1 "github.com/cbl/cbl-protos/gen/go/billing/payment/v1"

	"github.com/cbl/cbl-payment/internal/domain"
)

// allStates задает канонические состояния автомата.
var allStates = []domain.State{
	domain.StateCreating,
	domain.StateCreated,
	domain.StatePending,
	domain.StateDetected,
	domain.StateConfirming,
	domain.StateCompleted,
	domain.StateExpired,
	domain.StateUnderpaid,
	domain.StateOverpaid,
	domain.StateFailed,
	domain.StateCancelled,
}

// allowedPairs задает матрицу переходов (FR-3) в виде пар "from>to".
var allowedPairs = map[string]struct{}{
	"creating>created":     {},
	"creating>cancelled":   {},
	"creating>failed":      {},
	"created>pending":      {},
	"created>cancelled":    {},
	"created>expired":      {},
	"created>failed":       {},
	"pending>detected":     {},
	"pending>cancelled":    {},
	"pending>expired":      {},
	"pending>failed":       {},
	"detected>confirming":  {},
	"detected>underpaid":   {},
	"detected>overpaid":    {},
	"detected>failed":      {},
	"confirming>completed": {},
	"confirming>underpaid": {},
	"confirming>overpaid":  {},
	"confirming>failed":    {},
}

func TestParseState(t *testing.T) {
	t.Parallel()

	for _, state := range allStates {
		parsed, err := domain.ParseState(string(state))
		require.NoErrorf(t, err, "состояние %s", state)
		assert.Equal(t, state, parsed)
	}
	_, err := domain.ParseState("unknown")
	require.ErrorIs(t, err, domain.ErrInvalidState)
	_, err = domain.ParseState("")
	require.ErrorIs(t, err, domain.ErrInvalidState)
}

func TestTransitionMatrix(t *testing.T) {
	t.Parallel()

	for _, from := range allStates {
		for _, to := range allStates {
			_, want := allowedPairs[string(from)+">"+string(to)]
			assert.Equalf(t, want, domain.AllowedTransition(from, to),
				"переход %s -> %s", from, to)
		}
	}
}

func TestTerminal(t *testing.T) {
	t.Parallel()

	terminal := []domain.State{
		domain.StateCompleted, domain.StateExpired, domain.StateUnderpaid,
		domain.StateOverpaid, domain.StateFailed, domain.StateCancelled,
	}
	for _, state := range terminal {
		assert.Truef(t, domain.Terminal(state), "%s должен быть терминальным", state)
	}
	active := []domain.State{
		domain.StateCreating, domain.StateCreated, domain.StatePending,
		domain.StateDetected, domain.StateConfirming,
	}
	for _, state := range active {
		assert.Falsef(t, domain.Terminal(state), "%s не терминальный", state)
	}
}

func TestNextVersion(t *testing.T) {
	t.Parallel()

	version, err := domain.NextVersion(domain.StateCreating, domain.StateCreated, 1)
	require.NoError(t, err)
	assert.Equal(t, uint64(2), version)

	version, err = domain.NextVersion(domain.StateConfirming, domain.StateConfirming, 5)
	require.NoError(t, err)
	assert.Equal(t, uint64(6), version, "самопереход увеличивает версию")

	_, err = domain.NextVersion(domain.StateConfirming, domain.StateDetected, 5)
	require.ErrorIs(t, err, domain.ErrForbiddenTransition, "переход назад запрещен")

	_, err = domain.NextVersion(domain.StateCompleted, domain.StateCompleted, 5)
	require.ErrorIs(t, err, domain.ErrTerminalState, "терминальное состояние неизменяемо")

	_, err = domain.NextVersion(domain.StatePending, domain.StateCompleted, 2)
	require.ErrorIs(t, err, domain.ErrForbiddenTransition, "перескок через состояние запрещен")

	_, err = domain.NextVersion(domain.StateCreating, domain.StateCreated, 0)
	require.ErrorIs(t, err, domain.ErrStateConflict)

	_, err = domain.NextVersion("unknown", "unknown", 1)
	require.ErrorIs(t, err, domain.ErrInvalidState, "неизвестное состояние не проходит самопереходом")

	_, err = domain.NextVersion(domain.StatePending, "unknown", 2)
	require.ErrorIs(t, err, domain.ErrInvalidState)
}

func TestStateMatchesProtoEnum(t *testing.T) {
	t.Parallel()

	protoStates := make(map[string]struct{})
	for _, name := range paymentv1.PaymentState_name {
		if strings.HasSuffix(name, "_UNSPECIFIED") {
			continue
		}
		protoStates[strings.ToLower(strings.TrimPrefix(name, "PAYMENT_STATE_"))] = struct{}{}
	}
	require.Len(t, protoStates, len(allStates))
	for _, state := range allStates {
		_, ok := protoStates[string(state)]
		assert.Truef(t, ok, "состояние %s отсутствует в billing.payment.v1.PaymentState", state)
	}
	assert.Equal(t, paymentv1.PaymentState_PAYMENT_STATE_CONFIRMING,
		paymentv1.PaymentState(paymentv1.PaymentState_value["PAYMENT_STATE_"+strings.ToUpper(string(domain.StateConfirming))]),
		"маппинг состояний 1:1 по именам")
}
