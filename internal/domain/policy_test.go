package domain_test

import (
	"math"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cbl/cbl-payment/internal/domain"
)

const policyJSON = `{"USDT":[{"below_minor":10000,"confirmations":1},{"below_minor":100000,"confirmations":3}],"default":6}`

func mustPolicy(t *testing.T, raw string, underBPS, overBPS int) domain.AmountPolicy {
	t.Helper()
	policy, err := domain.ParseAmountPolicy(raw, underBPS, overBPS)
	require.NoError(t, err)
	return policy
}

func TestAmountPolicyRequiredConfirmations(t *testing.T) {
	t.Parallel()

	policy := mustPolicy(t, policyJSON, 0, 0)
	assert.Equal(t, 1, policy.RequiredConfirmations("USDT", 9_999), "первый ярус: менее 100.00")
	assert.Equal(t, 3, policy.RequiredConfirmations("USDT", 10_000), "граница яруса переходит к следующему")
	assert.Equal(t, 3, policy.RequiredConfirmations("usdt", 99_999))
	assert.Equal(t, 6, policy.RequiredConfirmations("USDT", 100_000), "выше ярусов - fallback")
	assert.Equal(t, 6, policy.RequiredConfirmations("BTC", 1), "актив без ярусов - fallback")
}

func TestAmountPolicyDecide(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		policy        domain.AmountPolicy
		received      int64
		expected      int64
		confirmations int
		required      int
		want          domain.Decision
	}{
		"exact and enough":      {policy: mustPolicy(t, "", 0, 0), received: 100_000, expected: 100_000, confirmations: 6, required: 6, want: domain.DecisionCompleted},
		"exact below threshold": {policy: mustPolicy(t, "", 0, 0), received: 100_000, expected: 100_000, confirmations: 5, required: 6, want: domain.DecisionConfirming},
		"zero tolerance unpaid": {policy: mustPolicy(t, "", 0, 0), received: 99_999, expected: 100_000, confirmations: 6, required: 6, want: domain.DecisionUnderpaid},
		"zero tolerance paid":   {policy: mustPolicy(t, "", 0, 0), received: 100_001, expected: 100_000, confirmations: 6, required: 6, want: domain.DecisionOverpaid},
		"underpaid in 1pct":     {policy: mustPolicy(t, "", 100, 0), received: 99_000, expected: 100_000, confirmations: 6, required: 6, want: domain.DecisionCompleted},
		"underpaid 1bp over":    {policy: mustPolicy(t, "", 100, 0), received: 98_999, expected: 100_000, confirmations: 6, required: 6, want: domain.DecisionUnderpaid},
		"odd amount at edge":    {policy: mustPolicy(t, "", 100, 0), received: 9_901, expected: 10_001, confirmations: 6, required: 6, want: domain.DecisionCompleted},
		"odd amount 1bp over":   {policy: mustPolicy(t, "", 100, 0), received: 9_900, expected: 10_001, confirmations: 6, required: 6, want: domain.DecisionUnderpaid},
		"odd one bp tolerance":  {policy: mustPolicy(t, "", 1, 0), received: 10_000, expected: 10_001, confirmations: 6, required: 6, want: domain.DecisionCompleted},
		"odd one bp 1bp over":   {policy: mustPolicy(t, "", 1, 0), received: 9_999, expected: 10_001, confirmations: 6, required: 6, want: domain.DecisionUnderpaid},
		"overpaid in 2pct":      {policy: mustPolicy(t, "", 0, 200), received: 102_000, expected: 100_000, confirmations: 6, required: 6, want: domain.DecisionCompleted},
		"overpaid 1bp over":     {policy: mustPolicy(t, "", 0, 200), received: 102_001, expected: 100_000, confirmations: 6, required: 6, want: domain.DecisionOverpaid},
		"in tolerance low conf": {policy: mustPolicy(t, "", 100, 0), received: 99_500, expected: 100_000, confirmations: 0, required: 1, want: domain.DecisionConfirming},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, tc.policy.Decide(tc.received, tc.expected, tc.confirmations, tc.required))
		})
	}
}

func TestAmountPolicyHugeAmountNoOverflow(t *testing.T) {
	t.Parallel()

	policy := mustPolicy(t, "", 10_000, 10_000)
	// При сумме около предела int64 границы не переполняются.
	assert.Equal(t, domain.DecisionCompleted, policy.Decide(1<<62, 1<<62, 1, 1))
	assert.Equal(t, domain.DecisionCompleted, policy.Decide(0, 1<<62, 1, 1),
		"100% толеранс принимает любую полученную сумму")

	strict := mustPolicy(t, "", 0, 0)
	assert.Equal(t, domain.DecisionUnderpaid, strict.Decide(0, 1<<62, 1, 1))
	assert.Equal(t, domain.DecisionOverpaid, strict.Decide(math.MaxInt64, 1<<62, 1, 1))
}

func TestParseAmountPolicyErrors(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		raw      string
		underBPS int
		overBPS  int
	}{
		"bad json":           {raw: "{", underBPS: 0, overBPS: 0},
		"null config":        {raw: "null", underBPS: 0, overBPS: 0},
		"default zero":       {raw: `{"default":0}`, underBPS: 0, overBPS: 0},
		"default too big":    {raw: `{"default":1001}`, underBPS: 0, overBPS: 0},
		"empty tiers":        {raw: `{"USDT":[]}`, underBPS: 0, overBPS: 0},
		"negative below":     {raw: `{"USDT":[{"below_minor":-1,"confirmations":1}]}`, underBPS: 0, overBPS: 0},
		"bad confirmations":  {raw: `{"USDT":[{"below_minor":1,"confirmations":0}]}`, underBPS: 0, overBPS: 0},
		"duplicate below":    {raw: `{"USDT":[{"below_minor":10,"confirmations":1},{"below_minor":10,"confirmations":2}]}`, underBPS: 0, overBPS: 0},
		"duplicate assets":   {raw: `{"usdt":[{"below_minor":10,"confirmations":1}],"USDT":[{"below_minor":20,"confirmations":2}]}`, underBPS: 0, overBPS: 0},
		"bad asset":          {raw: `{"us!":[{"below_minor":1,"confirmations":1}]}`, underBPS: 0, overBPS: 0},
		"under bps negative": {raw: "", underBPS: -1, overBPS: 0},
		"over bps too big":   {raw: "", underBPS: 0, overBPS: 10_001},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			_, err := domain.ParseAmountPolicy(tc.raw, tc.underBPS, tc.overBPS)
			require.ErrorIs(t, err, domain.ErrInvalidPolicy)
		})
	}

	policy := mustPolicy(t, "", 0, 0)
	assert.Equal(t, 6, policy.RequiredConfirmations("USDT", 1), "пустая конфигурация - политика по умолчанию")

	// Ключ "Default" нормализуется в fallback; описание политики детерминировано.
	described := mustPolicy(t, `{"usdt":[{"below_minor":100,"confirmations":2}],"Default":7}`, 50, 10)
	assert.Equal(t, 7, described.RequiredConfirmations("USDT", 1_000))
	assert.Equal(t, 2, described.RequiredConfirmations("USDT", 50))
	assert.Contains(t, described.Describe(), "USDT=[<100:2]")
	assert.Contains(t, described.Describe(), "default=7,underpaid_bps=50,overpaid_bps=10")
	assert.True(t, strings.Contains(policyJSON, "default"))
}

func TestAmountPolicyZeroValueGuard(t *testing.T) {
	t.Parallel()

	var empty domain.AmountPolicy
	assert.Equal(t, 6, empty.RequiredConfirmations("USDT", 1), "нулевая политика не завершает без подтверждений")
	assert.Equal(t, 6, empty.RequiredConfirmations("USDT", 1_000_000))
}

func TestShouldExpire(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 10, 15, 12, 0, 0, 0, time.UTC)
	past := now.Add(-time.Minute)
	future := now.Add(time.Minute)

	assert.True(t, domain.ShouldExpire(domain.StatePending, &past, now))
	assert.True(t, domain.ShouldExpire(domain.StateCreated, &past, now))
	assert.False(t, domain.ShouldExpire(domain.StateCreated, &future, now))
	assert.False(t, domain.ShouldExpire(domain.StatePending, nil, now), "без срока истечения нет")
	assert.False(t, domain.ShouldExpire(domain.StateCreating, &past, now), "creating доводится фоном")
	assert.False(t, domain.ShouldExpire(domain.StateDetected, &past, now), "оплата обнаружена - не истекает")
	assert.False(t, domain.ShouldExpire(domain.StateConfirming, &past, now))
	assert.False(t, domain.ShouldExpire(domain.StateCompleted, &past, now))
}

func TestChainTo(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		from domain.State
		to   domain.State
		want []domain.State
	}{
		"pending to completed":  {from: domain.StatePending, to: domain.StateCompleted, want: []domain.State{domain.StateDetected, domain.StateConfirming, domain.StateCompleted}},
		"pending to underpaid":  {from: domain.StatePending, to: domain.StateUnderpaid, want: []domain.State{domain.StateDetected, domain.StateUnderpaid}},
		"pending to overpaid":   {from: domain.StatePending, to: domain.StateOverpaid, want: []domain.State{domain.StateDetected, domain.StateOverpaid}},
		"detected to completed": {from: domain.StateDetected, to: domain.StateCompleted, want: []domain.State{domain.StateConfirming, domain.StateCompleted}},
		"confirming terminal":   {from: domain.StateConfirming, to: domain.StateUnderpaid, want: []domain.State{domain.StateUnderpaid}},
		"same state":            {from: domain.StatePending, to: domain.StatePending, want: nil},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			path, err := domain.ChainTo(tc.from, tc.to)
			require.NoError(t, err)
			assert.Equal(t, tc.want, path)
		})
	}

	_, err := domain.ChainTo(domain.StateCompleted, domain.StateFailed)
	require.ErrorIs(t, err, domain.ErrTerminalState)

	_, err = domain.ChainTo("unknown", domain.StatePending)
	require.ErrorIs(t, err, domain.ErrInvalidState)

	_, err = domain.ChainTo(domain.StatePending, "unknown")
	require.ErrorIs(t, err, domain.ErrInvalidState)
}
