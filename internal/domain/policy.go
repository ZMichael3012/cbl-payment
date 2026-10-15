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

// Ошибки политики завершения платежей.
var (
	// ErrInvalidPolicy сообщает о некорректной конфигурации политики.
	ErrInvalidPolicy = errors.New("payment: invalid amount policy")
)

// Границы политики.
const (
	// defaultConfirmations задает порог подтверждений без настройки.
	defaultConfirmations = 6
	// maxConfirmationsPolicy задает верхнюю границу порога подтверждений.
	maxConfirmationsPolicy = 1000
	// maxToleranceBPS задает верхнюю границу толеранса (100%).
	maxToleranceBPS = 10000
)

// Decision задает решение политики по платежу.
type Decision string

// Решения политики соответствуют целевым состояниям автомата.
const (
	// DecisionConfirming задает продолжение набора подтверждений.
	DecisionConfirming Decision = "confirming"
	// DecisionCompleted задает завершение платежа.
	DecisionCompleted Decision = "completed"
	// DecisionUnderpaid задает недостаточную оплату.
	DecisionUnderpaid Decision = "underpaid"
	// DecisionOverpaid задает избыточную оплату.
	DecisionOverpaid Decision = "overpaid"
)

// ConfirmationTier задает ярус порога подтверждений по сумме актива.
type ConfirmationTier struct {
	// BelowMinor задает верхнюю границу яруса (не включительно) в минимальных единицах.
	BelowMinor int64 `json:"below_minor"`
	// Confirmations задает порог подтверждений яруса.
	Confirmations int `json:"confirmations"`
}

// AmountPolicy задает политику завершения: пороги подтверждений по активу и
// сумме, толерансы недоплаты и переплаты в базисных пунктах.
type AmountPolicy struct {
	tiers    map[string][]ConfirmationTier
	fallback int
	underBPS int
	overBPS  int
}

// DefaultAmountPolicy создает политику по умолчанию: единый порог 6,
// нулевые толерансы.
func DefaultAmountPolicy() AmountPolicy {
	return AmountPolicy{fallback: defaultConfirmations}
}

// ParseAmountPolicy разбирает JSON-конфигурацию порогов и толерансы.
// Пустая строка - политика по умолчанию; ярусы сортируются по сумме.
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

// Describe возвращает детерминированное описание эффективной политики для
// лога конфигурации при старте (без секретов).
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

// RequiredConfirmations возвращает порог подтверждений для ожидаемого актива и
// суммы: первый ярус, где сумма строго меньше границы, иначе fallback.
// Нулевое значение политики защищено значением по умолчанию.
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

// Decide принимает решение по оплате: недостаточная сумма определяется
// немедленно (недоплата сверх толеранса), принятая сумма завершается при
// достижении порога подтверждений и остается в confirming при недостатке.
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

// bpsFloor возвращает floor(amount * bps / 10000) без float; умножение
// разложено на части против переполнения int64.
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

// validPolicyAsset проверяет код актива политики: 2 - 16 символов латиницы и цифр.
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

// ShouldExpire сообщает, что счет истек: срок задан и наступил, состояние
// создано или ожидает оплату (creating не истекает - доводится 4.6).
func ShouldExpire(state State, expiresAt *time.Time, now time.Time) bool {
	if expiresAt == nil {
		return false
	}
	if state != StateCreated && state != StatePending {
		return false
	}
	return !now.UTC().Before(expiresAt.UTC())
}
