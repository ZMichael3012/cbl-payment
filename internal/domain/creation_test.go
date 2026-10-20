package domain_test

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ZMichael3012/cbl-kit/money"

	"github.com/cbl/cbl-payment/internal/domain"
)

func TestConvertRate(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name     string
		minor    int64
		rate     string
		fromDec  uint32
		toDec    uint32
		expected int64
	}{
		{"same decimals", 10_000_000, "1.5", 6, 6, 15_000_000},
		{"scale up pay decimals", 5_000_000, "0.00001234", 6, 8, 6170},
		{"scale down pay decimals", 123, "0.01", 2, 6, 12300},
		{"floor stays floor", 1, "0.5", 6, 6, 0},
		{"tiny rate under one minor", 3, "0.0000001", 6, 6, 0},
		{"large integers", 1_000_000_000_000, "2", 6, 6, 2_000_000_000_000},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			converted, err := domain.ConvertRate(tc.minor, tc.rate, tc.fromDec, tc.toDec)
			if tc.expected == 0 {
				require.ErrorIs(t, err, domain.ErrPayAmount)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.expected, converted)
		})
	}
}

func TestConvertRateRejectsBrokenRates(t *testing.T) {
	t.Parallel()
	// Floor to zero is the ErrPayAmount path covered above; these are the
	// invalid rate shapes.
	for _, rate := range []string{"", "0", "0.0", "-1", "+1", "1e5", "abc", "1.2.3", "0.00000000000000000000000000000000000000000", ".5", "5.", "1,5"} {
		_, err := domain.ConvertRate(1000, rate, 6, 6)
		require.Error(t, err, "rate %q", rate)
		assert.ErrorIs(t, err, domain.ErrInvalidRate, "rate %q", rate)
	}
	_, err := domain.ConvertRate(1_000_000, "0.0000001", 6, 6)
	require.ErrorIs(t, err, domain.ErrPayAmount, "a conversion below one pay minor unit fails closed")
}

func TestRequestHashStability(t *testing.T) {
	t.Parallel()
	expires := time.Date(2026, 10, 18, 12, 0, 0, 0, time.UTC)
	first := domain.RequestHash("01a14f70-0000-7000-8000-000000000001",
		money.MustParse("10", "USDT", 6), " BTC ", " usdt_trc20 ", expires, 8)
	second := domain.RequestHash("01a14f70-0000-7000-8000-000000000001",
		money.MustParse("10", "USDT", 6), "btc", "USDT_TRC20", expires, 8)
	assert.Equal(t, first, second, "the hash normalizes asset and network casing and whitespace")

	different := domain.RequestHash("01a14f70-0000-7000-8000-000000000001",
		money.MustParse("11", "USDT", 6), "btc", "usdt_trc20", expires, 8)
	assert.NotEqual(t, first, different)

	anotherExpiry := domain.RequestHash("01a14f70-0000-7000-8000-000000000001",
		money.MustParse("10", "USDT", 6), "btc", "usdt_trc20", expires.Add(time.Minute), 8)
	assert.NotEqual(t, first, anotherExpiry)

	anotherMerchant := domain.RequestHash("01a14f70-0000-7000-8000-000000000002",
		money.MustParse("10", "USDT", 6), "btc", "usdt_trc20", expires, 8)
	assert.NotEqual(t, first, anotherMerchant)

	anotherScale := domain.RequestHash("01a14f70-0000-7000-8000-000000000001",
		money.MustParse("10", "USDT", 6), "btc", "usdt_trc20", expires, 6)
	assert.NotEqual(t, first, anotherScale, "the pay scale participates in the hash")
}

func TestValidAssetCode(t *testing.T) {
	t.Parallel()
	assert.True(t, domain.ValidAssetCode("USDT"))
	assert.True(t, domain.ValidAssetCode("btc"))
	assert.False(t, domain.ValidAssetCode("U"))
	assert.False(t, domain.ValidAssetCode("TOO-LONG-ASSET-CODE"))
	assert.False(t, domain.ValidAssetCode("US DT"))
	assert.False(t, domain.ValidAssetCode(""))
}

func TestValidNetwork(t *testing.T) {
	t.Parallel()
	assert.True(t, domain.ValidNetwork("usdt_trc20"))
	assert.True(t, domain.ValidNetwork("btc-testnet"))
	assert.True(t, domain.ValidNetwork(" USDT_TRC20 "), "casing and whitespace are folded")
	assert.False(t, domain.ValidNetwork(""))
	assert.False(t, domain.ValidNetwork("usdt trc20"))
	assert.False(t, domain.ValidNetwork(strings.Repeat("a", 65)))
}
