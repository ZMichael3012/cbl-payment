package domain

import "strings"

// TopUpOutcome is the decision of TopUpDelta for a canonical event of a
// terminal-success payment (17.10, decision 9).
type TopUpOutcome int

const (
	// TopUpSkip marks an event that is not a repeated-deposit signal.
	TopUpSkip TopUpOutcome = iota
	// TopUpManualReview marks a repeated deposit that cannot be deduplicated
	// automatically (an empty txid, KI-payment-10).
	TopUpManualReview
	// TopUpCredit marks a repeated deposit to credit as a derived child.
	TopUpCredit
)

// TopUpDelta decides whether a canonical event carries a repeated deposit
// (top-up) for a payment in a terminal success state (17.10, decision 9): the
// credited delta is measured against the baseline already attributed to the
// parent, that is the recorded received amount plus the sum of the existing
// derived children. Successive cumulative events therefore credit only the new
// part (100k parent, 150k, then 180k events credit 50k and 30k, B1). Only a
// confirmed event above the baseline with a non-empty txid forms a new delta;
// an empty txid cannot be deduplicated and goes to TopUpManualReview; expired,
// failed and cancelled parents are excluded.
func TopUpDelta(current State, event CanonicalEventType, baseline *int64, eventReceived int64, txid string) (int64, TopUpOutcome) {
	if event != CanonicalConfirmed {
		return 0, TopUpSkip
	}
	switch current {
	case StateCompleted, StateUnderpaid, StateOverpaid:
	default:
		return 0, TopUpSkip
	}
	if baseline == nil || eventReceived <= *baseline {
		return 0, TopUpSkip
	}
	if strings.TrimSpace(txid) == "" {
		return 0, TopUpManualReview
	}
	return eventReceived - *baseline, TopUpCredit
}

// CanonicalTxID normalizes a transaction hash for the top-up identity (17.10,
// M1): a hex-encoded hash is case-insensitive and its 0x/0X prefix is a
// spelling variant, so the canonical form is the lowercased hex body without
// the prefix, which folds prefix and case variants of one hash into a single
// identity. Base58 and other case-sensitive encodings are kept as-is; an
// empty or prefix-only value canonicalizes to an empty string, which keeps the
// deposit on the manual-review path. A case-sensitive encoding that happens to
// consist of hex characters only would fold too; for realistic hash lengths
// the chance is negligible, but such a fold would merge two identities (a
// missed credit), so the residual is recorded in the spec.
func CanonicalTxID(txid string) string {
	trimmed := strings.TrimSpace(txid)
	body := trimmed
	if len(body) >= 2 && body[0] == '0' && (body[1] == 'x' || body[1] == 'X') {
		body = body[2:]
	}
	if body == "" {
		return ""
	}
	for _, r := range body {
		if (r < '0' || r > '9') && (r < 'a' || r > 'f') && (r < 'A' || r > 'F') {
			return trimmed
		}
	}
	return strings.ToLower(body)
}
