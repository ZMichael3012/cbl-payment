package domain

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"math/big"
	"strings"
	"time"

	"github.com/cbl/cbl-kit/money"
)

// Creation errors report the outcome classes of CreatePayment and
// CancelPayment (4.2): business rejections of the quote or the connector call
// fail the payment closed, dependency failures stay retryable.
var (
	// ErrInvalidRate reports a malformed or non-positive rate string.
	ErrInvalidRate = errors.New("payment: invalid rate")
	// ErrPayAmount reports that the converted pay amount is not positive or
	// does not fit the int64 minor-unit domain.
	ErrPayAmount = errors.New("payment: converted pay amount is invalid")
	// ErrQuoteRejected reports a quote business rejection (pair, minimum
	// amount, asset): the payment fails closed.
	ErrQuoteRejected = errors.New("payment: quote rejected")
	// ErrInvoiceRejected reports a connector business rejection: the payment
	// fails closed.
	ErrInvoiceRejected = errors.New("payment: invoice rejected")
	// ErrDependencyTemporary reports a retryable quote or connector failure.
	ErrDependencyTemporary = errors.New("payment: dependency temporarily unavailable")
	// ErrOverloaded reports a dependency overload (provider rate limit class).
	ErrOverloaded = errors.New("payment: dependency overloaded")
	// ErrInvoiceNotCancellable reports a cancel attempt after detection.
	ErrInvoiceNotCancellable = errors.New("payment: invoice not cancellable")
	// ErrIdempotencyConflict reports a reused idempotency key with a different
	// request body.
	ErrIdempotencyConflict = errors.New("payment: idempotency key reused with a different request")
	// ErrUnknownAssetScale reports a pay asset without a configured scale.
	ErrUnknownAssetScale = errors.New("payment: unknown asset scale")
	// ErrInvalidExpiry reports a missing, past or over-horizon expiry.
	ErrInvalidExpiry = errors.New("payment: invalid expiry")
	// ErrInvalidCreation reports a malformed creation or cancel request.
	ErrInvalidCreation = errors.New("payment: invalid creation request")
)

// maxRateLen bounds the rate string (the contract Money pattern limit).
const maxRateLen = 40

// ConvertRate converts a pricing amount in minor units into the pay asset with
// the locked rate (pay units per one pricing unit): floor integer math, no
// float. A conversion below one pay minor unit fails closed instead of
// invoicing a zero amount.
func ConvertRate(amountMinor int64, rate string, fromDecimals, toDecimals uint32) (int64, error) {
	if amountMinor <= 0 {
		return 0, fmt.Errorf("%w: non-positive amount", ErrPayAmount)
	}
	mantissa, scale, err := parseRate(rate)
	if err != nil {
		return 0, err
	}
	numerator := new(big.Int).Mul(big.NewInt(amountMinor), mantissa)
	denominator := big.NewInt(1)
	if toDecimals >= fromDecimals {
		numerator.Mul(numerator, pow10(toDecimals-fromDecimals))
	} else {
		denominator = pow10(fromDecimals - toDecimals)
	}
	denominator.Mul(denominator, pow10(scale))
	converted := new(big.Int).Quo(numerator, denominator)
	if !converted.IsInt64() || converted.Sign() <= 0 {
		return 0, fmt.Errorf("%w: the conversion floors to zero", ErrPayAmount)
	}
	return converted.Int64(), nil
}

// parseRate validates the decimal rate string and returns its mantissa and
// scale; a zero, exponent, sign or over-long value is invalid.
func parseRate(rate string) (*big.Int, uint32, error) {
	trimmed := strings.TrimSpace(rate)
	if trimmed == "" || len(trimmed) > maxRateLen {
		return nil, 0, fmt.Errorf("%w: %q", ErrInvalidRate, rate)
	}
	parts := strings.Split(trimmed, ".")
	if len(parts) > 2 {
		return nil, 0, fmt.Errorf("%w: %q", ErrInvalidRate, rate)
	}
	whole, fraction := parts[0], ""
	if len(parts) == 2 {
		fraction = parts[1]
	}
	if whole == "" || (len(parts) == 2 && fraction == "") {
		return nil, 0, fmt.Errorf("%w: %q", ErrInvalidRate, rate)
	}
	for _, r := range whole + fraction {
		if r < '0' || r > '9' {
			return nil, 0, fmt.Errorf("%w: %q", ErrInvalidRate, rate)
		}
	}
	mantissa, ok := new(big.Int).SetString(whole+fraction, 10)
	if !ok || mantissa.Sign() <= 0 {
		return nil, 0, fmt.Errorf("%w: %q", ErrInvalidRate, rate)
	}
	return mantissa, uint32(len(fraction)), nil //nolint:gosec // the rate length is bounded by maxRateLen (40)
}

// pow10 returns 10^n for n within the contract bounds (decimals up to 18 and
// the rate scale up to 40).
func pow10(exponent uint32) *big.Int {
	return new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(exponent)), nil)
}

// RequestHash fingerprints the normalized creation request for the idempotent
// replay: whitespace and casing differences replay instead of conflicting, and
// the pay asset scale participates so a configuration change cannot silently
// respin the locked amount on a resume.
func RequestHash(merchantID string, amount money.Money, payAsset, network string, expiresAt time.Time, payDecimals uint32) string {
	raw := fmt.Sprintf("%s|%s|%d|%d|%s|%s|%d|%s",
		strings.TrimSpace(merchantID),
		strings.ToUpper(strings.TrimSpace(amount.Asset)),
		amount.MinorUnits,
		amount.Decimals,
		strings.ToUpper(strings.TrimSpace(payAsset)),
		strings.ToLower(strings.TrimSpace(network)),
		payDecimals,
		expiresAt.UTC().Format(time.RFC3339Nano),
	)
	sum := sha256.Sum256([]byte(raw))
	return hex.EncodeToString(sum[:])
}

// ValidNetwork reports whether the value matches the payment network pattern
// (2 to 64 Latin letters, digits, dashes or underscores; casing is folded).
func ValidNetwork(network string) bool {
	trimmed := strings.ToLower(strings.TrimSpace(network))
	if len(trimmed) < 2 || len(trimmed) > 64 {
		return false
	}
	for _, r := range trimmed {
		if (r < 'a' || r > 'z') && (r < '0' || r > '9') && r != '_' && r != '-' {
			return false
		}
	}
	return true
}

// ValidAssetCode reports whether the value matches the payment asset pattern
// (2 to 16 Latin letters or digits, case-insensitive).
func ValidAssetCode(asset string) bool {
	trimmed := strings.TrimSpace(asset)
	if len(trimmed) < 2 || len(trimmed) > 16 {
		return false
	}
	for _, r := range strings.ToUpper(trimmed) {
		if (r < 'A' || r > 'Z') && (r < '0' || r > '9') {
			return false
		}
	}
	return true
}
