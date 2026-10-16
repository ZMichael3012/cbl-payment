package domain

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"
)

// Errors of the payment settlement policy.
var (
	// ErrInvalidPolicy reports an invalid policy configuration.
	ErrInvalidPolicy = errors.New("payment: invalid amount policy")
)

// Policy bounds.
const (
	// defaultConfirmations defines the confirmation threshold without configuration.
	defaultConfirmations = 6
	// maxConfirmationsPolicy defines the upper bound of the confirmation threshold.
	maxConfirmationsPolicy = 1000
	// maxToleranceBPS defines the upper tolerance bound (100%).
	maxToleranceBPS = 10000
)

// Decision defines the policy decision for a payment.
type Decision string

// Policy decisions correspond to target states of the state machine.
const (
	// DecisionConfirming defines continuation of confirmation accumulation.
	DecisionConfirming Decision = "confirming"
	// DecisionCompleted defines completion of the payment.
	DecisionCompleted Decision = "completed"
	// DecisionUnderpaid defines an insufficient payment.
	DecisionUnderpaid Decision = "underpaid"
	// DecisionOverpaid defines an excess payment.
	DecisionOverpaid Decision = "overpaid"
)

// ConfirmationTier defines a confirmation threshold tier by asset amount.
type ConfirmationTier struct {
	// BelowMinor defines the exclusive upper bound of the tier in minor units.
	BelowMinor int64 `json:"below_minor"`
	// Confirmations defines the confirmation threshold of the tier.
	Confirmations int `json:"confirmations"`
}

// AmountPolicy defines the settlement policy: confirmation thresholds by asset
// and amount, underpayment and overpayment tolerances in basis points.
type AmountPolicy struct {
	tiers    map[string][]ConfirmationTier
	fallback int
	underBPS int
	overBPS  int
}

// DefaultAmountPolicy creates the default policy: a single threshold of 6
// and zero tolerances.
func DefaultAmountPolicy() AmountPolicy {
	return AmountPolicy{fallback: defaultConfirmations}
}

// ParseAmountPolicy parses the JSON configuration of thresholds and the tolerances.
// An empty string means the default policy; tiers are sorted by amount.
func ParseAmountPolicy(raw string, underBPS, overBPS int) (AmountPolicy, error) {
	if underBPS < 0 || underBPS > maxToleranceBPS {
		return AmountPolicy{}, fmt.Errorf("%w: underpaid bps %d", ErrInvalidPolicy, underBPS)
	}
	if overBPS < 0 || overBPS > maxToleranceBPS {
		return AmountPolicy{}, fmt.Errorf("%w: overpaid bps %d", ErrInvalidPolicy, overBPS)
	}
	policy := AmountPolicy{fallback: defaultConfirmations, underBPS: underBPS, overBPS: overBPS}
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return policy, nil
	}
	if raw == "null" {
		return AmountPolicy{}, fmt.Errorf("%w: null config", ErrInvalidPolicy)
	}
	var generic map[string]json.RawMessage
	if err := json.Unmarshal([]byte(raw), &generic); err != nil {
		return AmountPolicy{}, fmt.Errorf("%w: %w", ErrInvalidPolicy, err)
	}
	policy.tiers = make(map[string][]ConfirmationTier, len(generic))
	seenAssets := make(map[string]struct{}, len(generic))
	for name, value := range generic {
		if strings.ToLower(strings.TrimSpace(name)) == "default" {
			var def int
			if err := json.Unmarshal(value, &def); err != nil {
				return AmountPolicy{}, fmt.Errorf("%w: default: %w", ErrInvalidPolicy, err)
			}
			if def < 1 || def > maxConfirmationsPolicy {
				return AmountPolicy{}, fmt.Errorf("%w: default %d", ErrInvalidPolicy, def)
			}
			policy.fallback = def
			continue
		}
		var tiers []ConfirmationTier
		if err := json.Unmarshal(value, &tiers); err != nil {
			return AmountPolicy{}, fmt.Errorf("%w: asset %q: %w", ErrInvalidPolicy, name, err)
		}
		if len(tiers) == 0 {
			return AmountPolicy{}, fmt.Errorf("%w: asset %q has no tiers", ErrInvalidPolicy, name)
		}
		asset := strings.ToUpper(strings.TrimSpace(name))
		if !validPolicyAsset(asset) {
			return AmountPolicy{}, fmt.Errorf("%w: asset %q", ErrInvalidPolicy, name)
		}
		if _, dup := seenAssets[asset]; dup {
			return AmountPolicy{}, fmt.Errorf("%w: duplicate asset %q", ErrInvalidPolicy, name)
		}
		seenAssets[asset] = struct{}{}
		sort.Slice(tiers, func(i, j int) bool { return tiers[i].BelowMinor < tiers[j].BelowMinor })
		seen := make(map[int64]struct{}, len(tiers))
		for _, tier := range tiers {
			if tier.BelowMinor <= 0 {
				return AmountPolicy{}, fmt.Errorf("%w: asset %q below_minor %d", ErrInvalidPolicy, name, tier.BelowMinor)
			}
			if tier.Confirmations < 1 || tier.Confirmations > maxConfirmationsPolicy {
				return AmountPolicy{}, fmt.Errorf("%w: asset %q confirmations %d", ErrInvalidPolicy, name, tier.Confirmations)
			}
			if _, dup := seen[tier.BelowMinor]; dup {
				return AmountPolicy{}, fmt.Errorf("%w: asset %q duplicate below_minor %d", ErrInvalidPolicy, name, tier.BelowMinor)
			}
			seen[tier.BelowMinor] = struct{}{}
		}
		policy.tiers[asset] = tiers
	}
	return policy, nil
}

// Describe returns a deterministic description of the effective policy for
// the startup configuration log (without secrets).
func (p AmountPolicy) Describe() string {
	assets := make([]string, 0, len(p.tiers))
	for asset := range p.tiers {
		assets = append(assets, asset)
	}
	sort.Strings(assets)
	parts := make([]string, 0, len(assets)+1)
	for _, asset := range assets {
		tiers := p.tiers[asset]
		bounds := make([]string, 0, len(tiers))
		for _, tier := range tiers {
			bounds = append(bounds, fmt.Sprintf("<%d:%d", tier.BelowMinor, tier.Confirmations))
		}
		parts = append(parts, asset+"=["+strings.Join(bounds, ",")+"]")
	}
	parts = append(parts, fmt.Sprintf("default=%d,underpaid_bps=%d,overpaid_bps=%d",
		p.fallback, p.underBPS, p.overBPS))
	return strings.Join(parts, ";")
}

// RequiredConfirmations returns the confirmation threshold for the expected
// asset and amount: the first tier where the amount is strictly below the
// bound, otherwise the fallback. A zero policy value is protected by the
// default value.
func (p AmountPolicy) RequiredConfirmations(asset string, expectedMinor int64) int {
	fallback := p.fallback
	if fallback < 1 {
		fallback = defaultConfirmations
	}
	tiers, ok := p.tiers[strings.ToUpper(asset)]
	if !ok {
		return fallback
	}
	for _, tier := range tiers {
		if expectedMinor < tier.BelowMinor {
			return tier.Confirmations
		}
	}
	return fallback
}

// Decide makes the settlement decision: an insufficient amount is decided
// immediately (underpayment beyond tolerance), an accepted amount completes
// when the confirmation threshold is reached and stays in confirming while short.
func (p AmountPolicy) Decide(received, expected int64, confirmations, required int) Decision {
	underLimit := expected - bpsFloor(expected, p.underBPS)
	if received < underLimit {
		return DecisionUnderpaid
	}
	overLimit := expected
	if extra := bpsFloor(expected, p.overBPS); extra > 0 {
		if expected > math.MaxInt64-extra {
			overLimit = math.MaxInt64
		} else {
			overLimit = expected + extra
		}
	}
	if received > overLimit {
		return DecisionOverpaid
	}
	if confirmations >= required {
		return DecisionCompleted
	}
	return DecisionConfirming
}

// bpsFloor returns floor(amount * bps / 10000) without float; the multiplication
// is decomposed into parts against int64 overflow.
func bpsFloor(amount int64, bps int) int64 {
	if bps <= 0 {
		return 0
	}
	whole := amount / 10000
	frac := amount % 10000
	result := whole * int64(bps)
	extra := frac * int64(bps) / 10000
	if result > math.MaxInt64-extra {
		return math.MaxInt64
	}
	return result + extra
}

// validPolicyAsset validates a policy asset code: 2 to 16 characters of Latin letters and digits.
func validPolicyAsset(asset string) bool {
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

// ShouldExpire reports that the invoice has expired: the deadline is set and
// reached, the state is created or awaiting payment (creating does not expire,
// it is settled by 4.6).
func ShouldExpire(state State, expiresAt *time.Time, now time.Time) bool {
	if expiresAt == nil {
		return false
	}
	if state != StateCreated && state != StatePending {
		return false
	}
	return !now.UTC().Before(expiresAt.UTC())
}
