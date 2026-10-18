package domain_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/cbl/cbl-payment/internal/domain"
)

func TestTopUpDelta(t *testing.T) {
	t.Parallel()
	parent := int64(100_000)
	credited := int64(150_000) // the baseline after the first 50k child

	cases := map[string]struct {
		current     domain.State
		event       domain.CanonicalEventType
		baseline    *int64
		amount      int64
		txid        string
		wantDelta   int64
		wantOutcome domain.TopUpOutcome
	}{
		"confirmed on completed":                     {current: domain.StateCompleted, event: domain.CanonicalConfirmed, baseline: &parent, amount: 150_000, txid: "tx-2", wantDelta: 50_000, wantOutcome: domain.TopUpCredit},
		"confirmed on underpaid":                     {current: domain.StateUnderpaid, event: domain.CanonicalConfirmed, baseline: &parent, amount: 150_000, txid: "tx-2", wantDelta: 50_000, wantOutcome: domain.TopUpCredit},
		"confirmed on overpaid":                      {current: domain.StateOverpaid, event: domain.CanonicalConfirmed, baseline: &parent, amount: 150_000, txid: "tx-2", wantDelta: 50_000, wantOutcome: domain.TopUpCredit},
		"successive event credits only the new part": {current: domain.StateCompleted, event: domain.CanonicalConfirmed, baseline: &credited, amount: 180_000, txid: "tx-3", wantDelta: 30_000, wantOutcome: domain.TopUpCredit},
		"detected is not a topup":                    {current: domain.StateCompleted, event: domain.CanonicalDetected, baseline: &parent, amount: 150_000, txid: "tx-2", wantOutcome: domain.TopUpSkip},
		"non-terminal state":                         {current: domain.StateConfirming, event: domain.CanonicalConfirmed, baseline: &parent, amount: 150_000, txid: "tx-2", wantOutcome: domain.TopUpSkip},
		"expired stays manual":                       {current: domain.StateExpired, event: domain.CanonicalConfirmed, baseline: &parent, amount: 150_000, txid: "tx-2", wantOutcome: domain.TopUpSkip},
		"empty txid cannot dedup":                    {current: domain.StateCompleted, event: domain.CanonicalConfirmed, baseline: &parent, amount: 150_000, txid: "  ", wantOutcome: domain.TopUpManualReview},
		"delta zero":                                 {current: domain.StateCompleted, event: domain.CanonicalConfirmed, baseline: &parent, amount: 100_000, txid: "tx-2", wantOutcome: domain.TopUpSkip},
		"delta negative":                             {current: domain.StateCompleted, event: domain.CanonicalConfirmed, baseline: &parent, amount: 90_000, txid: "tx-2", wantOutcome: domain.TopUpSkip},
		"below the credited baseline":                {current: domain.StateCompleted, event: domain.CanonicalConfirmed, baseline: &credited, amount: 150_000, txid: "tx-2", wantOutcome: domain.TopUpSkip},
		"unknown baseline":                           {current: domain.StateCompleted, event: domain.CanonicalConfirmed, baseline: nil, amount: 150_000, txid: "tx-2", wantOutcome: domain.TopUpSkip},
	}
	for name, tc := range cases {
		tc := tc
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			delta, outcome := domain.TopUpDelta(tc.current, tc.event, tc.baseline, tc.amount, tc.txid)
			assert.Equal(t, tc.wantOutcome, outcome)
			assert.Equal(t, tc.wantDelta, delta)
		})
	}
}

func TestCanonicalTxID(t *testing.T) {
	t.Parallel()
	cases := map[string]struct {
		in   string
		want string
	}{
		"hex without prefix is lowercased":           {"AB12CD34", "ab12cd34"},
		"the prefix is stripped and the body folded": {"0xAbC123", "abc123"},
		"the 0X spelling folds too":                  {"0XAbC123", "abc123"},
		"prefix and no-prefix aliases fold":          {"AbC123", "abc123"},
		"base58 case is preserved":                   {"5j7sVnQkLmNp2XyZ", "5j7sVnQkLmNp2XyZ"},
		"spaces are trimmed":                         {" 0xAB ", "ab"},
		"prefix only is empty":                       {"0x", ""},
		"empty stays empty":                          {"   ", ""},
	}
	for name, tc := range cases {
		tc := tc
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, domain.CanonicalTxID(tc.in))
		})
	}
}
