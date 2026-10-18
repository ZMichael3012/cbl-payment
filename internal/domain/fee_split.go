package domain

import (
	"errors"
	"fmt"
	"math"
)

// ErrInvalidFee reports an inconsistent provider fee split.
var ErrInvalidFee = errors.New("payment: invalid fee split")

// FeeTotal computes the total fee from the provider aggregate and the split
// components (17.10): components take precedence when any of them is non-zero
// and must agree with a non-zero aggregate; a zero component means the provider
// did not report it. The total may not exceed the received amount.
func FeeTotal(fee, gatewayFee, networkFee, fxSpread, received int64) (int64, error) {
	if fee < 0 || gatewayFee < 0 || networkFee < 0 || fxSpread < 0 {
		return 0, fmt.Errorf("%w: a negative component", ErrInvalidFee)
	}
	sum := int64(0)
	for _, part := range []int64{gatewayFee, networkFee, fxSpread} {
		if part > received || sum > math.MaxInt64-part {
			return 0, fmt.Errorf("%w: a component exceeds the received amount", ErrInvalidFee)
		}
		sum += part
	}
	if sum > 0 {
		if sum > received {
			return 0, fmt.Errorf("%w: the split total %d exceeds the received %d", ErrInvalidFee, sum, received)
		}
		if fee > 0 && fee != sum {
			return 0, fmt.Errorf("%w: the aggregate %d does not match the components %d", ErrInvalidFee, fee, sum)
		}
		return sum, nil
	}
	if fee > received {
		return 0, fmt.Errorf("%w: the fee %d exceeds the received %d", ErrInvalidFee, fee, received)
	}
	return fee, nil
}
