# CBL-4.5. Rules: confirmation thresholds, underpayment, overpayment, expiration

| Field | Value |
| :-- | :-- |
| Epic | E4, Payment Service |
| Repository | `cbl-payment` |
| Size | M |
| Wave and parallel group | W2-PAYMENT |
| Depends on | 4.3 |
| Contract version | `cbl-protos@cf-2` (`billing/events/v1`), `CBL-000` section 7 |
| Status | done, review closed |

## 1. Context and goal

Canonical events are the input, but the final decision on `completed`/`underpaid`/
`overpaid` is made by the platform policy (`CBL-000`, section 7: "1 for less than $100,
3 for $100 - 1000, 6 for more than $1000"; "underpayment and overpayment are a separate
scenario"). The task adds a decision rule: a confirmation threshold by amount,
underpayment/overpayment tolerances, an expiration rule; consumer 4.3 starts completing
payments itself instead of leaving them in `confirming` forever.

## 2. Boundaries

In scope: the domain policy (thresholds by asset and amount, tolerances, expiration),
integration of the policy into consumer 4.3, configuration, tests.
Out of scope: the re-check of `GetGatewayPayment` before completion (4.4, KI-01),
background expiration tasks (4.6 - the predicate call of the policy), `payment.events`
publication (4.7), the USD rate as a threshold source (KI-01/0.6), the platform fee
(a separate decision, fee = 0 in 4.5).

## 3. Functional requirements

FR-1. Threshold configuration: `PAYMENT_CONFIRMATION_POLICY` - JSON of the form
`{"USDT":[{"below_minor":10000,"confirmations":1},{"below_minor":100000,"confirmations":3}],"default":6}`.
Tier boundaries are specified in minor units of the expected asset in its canonical
decimals (the example above is for an asset with two digits; for USDT with 6 digits the
tiers are scaled); the threshold is selected by the expected asset and the payment
amount: the first tier where `expected < below_minor`, otherwise `default` (6 by
default, range 1 - 1000; at a tier boundary `expected == below_minor` the next tier
applies). An invalid configuration (bad JSON, `null`, a threshold out of range,
duplicate assets after case normalization, empty tiers) - the service does not start;
the `default` key is recognized case-insensitively. The effective policy is logged once
at startup. For volatile assets tiers are specified by the operator in asset units
based on the USD equivalent at the time of configuration (`CBL-000`; an automatic feed -
KI-payment-06).

FR-2. Tolerances: `PAYMENT_UNDERPAID_TOLERANCE_BPS` and
`PAYMENT_OVERPAID_TOLERANCE_BPS` (basis points, 0 - 10000, 0 by default; an invalid
value - startup refusal). The admissible deviation is computed without float:
`underLimit = expected - floor(expected * underBPS / 10000)`,
`overLimit = expected + floor(expected * overBPS / 10000)`. An underpayment within the
tolerance is treated as full payment (the payment can complete as `completed`), an
overpayment within it likewise; beyond the tolerance - terminal `underpaid`/`overpaid`.
An underpayment tolerance of 10000 bps accepts any amount (a deliberate, documented
operator decision).

FR-3. Policy decision `Decide(received, expected, confirmations, required)`:
- `received < underLimit` -> `underpaid` (immediately, the confirmation threshold does
  not matter: the amount is insufficient);
- `received > overLimit` -> `overpaid` (immediately);
- otherwise the amount is accepted: `confirmations >= required` -> `completed`,
  otherwise `confirming` (the payment keeps collecting; a repeated event updates
  `confirmations` by a self-transition).

FR-4. Consumer 4.3 uses the policy for the events `confirmed`, `underpaid`, `overpaid`:
the platform policy takes precedence over the Connector classification (an underpayment
within the tolerance completes as `completed`; an overpayment within the tolerance
likewise `completed`). The events `detected`, `failed`, `expired` and the staleness/
too-early rules remain as in 4.3. The target state is reached by the shortest admissible
chain of the matrix (`pending -> detected -> confirming -> completed`, etc.), CAS and
the Guard transaction are preserved.

FR-5. Expiration: a payment expires when `expires_at` is set, `now >= expires_at`, and
the state is `created` or `pending` (the invoice is created, payment is not detected);
`creating` does not expire (it is advanced by 4.6 to `created` or `failed`); after
`detected` expiration does not apply. The domain provides the predicate
`ShouldExpire(state, expiresAt, now)`; the transitions themselves are performed by 4.6
(before expiring, the task rereads the state under CAS: a payment that became `detected`
does not expire).

FR-6. `completed_at` is set on a transition to a terminal outcome
(`completed`/`underpaid`/`overpaid`); `fee` remains NULL (treated as 0; the fee rule -
KI-payment-06).

## 4. Contracts

External contracts do not change: decisions are reflected by `PaymentState` states and
`payment.events` events (4.7).

## 5. Data model and migrations

No new migrations: the fields `received_amount/expected_amount`, `confirmations`,
`completed_at` are used (4.1).

## 6. State machines

Target states of the policy are reachable via the 4.1 matrix by shortest chains; the
policy adds no new transitions.

## 7. Edge cases

Tier boundaries (`expected == below_minor` - the next tier); an asset without tiers in
the policy (default); `confirmations == required` (completion is allowed);
`confirmations < required` with an accepted amount (we stay `confirming`, a
self-transition with confirmations growth, including repeatedly); an underpayment
exactly at the tolerance boundary (accepted, including indivisible amounts: the `floor`
boundary rounding); an underpayment 1 basis point beyond (terminal); an overpayment of 0
with a tolerance of 0 (exactly `expected` is accepted); `received > expected` beyond the
tolerance (overpaid immediately); canonical `underpaid` but the amount is within the
tolerance (the policy completes as `completed` at the threshold); canonical `confirmed`
with an amount below the tolerance (the policy moves to `underpaid`); `expires_at` in
the past for `pending` (expires by 4.6); detected before expiration (expiration does not
apply, 4.6 rereads the state); a late detected after expired (money on-chain -
KI-payment-08); a huge `confirmations` is already cut off by 4.3; bps math without float
and overflow (verified on MaxInt64).

## 8. Non-functional requirements

The policy decision is a pure function without I/O; the configuration is validated at
startup (including strict bps parsing: a non-numeric value - startup refusal); no new DB
calls per event (amounts and confirmations are already in the snapshot); the effective
policy (tiers, fallback, tolerances) is logged once at startup without secrets.

## 9. Test plan

| Level | Scenarios |
| :-- | :-- |
| Unit (domain) | the `Decide` table (amount/threshold/tolerances, boundaries and one bp beyond the boundary), tier selection by asset and amount, the default, configuration validation, `ShouldExpire` by states, bps without float |
| Unit (usecase) | confirmed below the threshold (self confirming), reaching the threshold (completed), underpaid beyond the tolerance, underpaid within the tolerance with and without a threshold, overpaid beyond/within the tolerance, canonical underpaid under a completed policy |
| Integration | real PostgreSQL: confirmed with threshold 1 completes the payment; a repeated confirmed below the threshold stays confirming; amount substitution at the boundary |
| E2E | Kafka + PostgreSQL: detected + confirmed (threshold 1) complete the payment automatically without a manual transition; a stale event after completed - no-op |
| Load | 14.1 |

## 10. Work plan and commit

Tests first (domain, usecase, integration), then domain/policy.go, canonical_consumer
edits, main configuration. Commit
`CBL-4.5: confirmation thresholds, underpayment/overpayment and expiration`; the spec is
in the same commit.

## 11. Open questions

- An automatic USD equivalent for thresholds (a price feed from Quote 6.x) -
  KI-payment-06; until then tiers are configured by the operator in asset units and
  require manual revision when the rate or peg drifts.
- The platform fee (`fee` in `payment.events` and the ledger posting) - a decision
  outside 4.5 (candidate: Billing API/tariffs 7.x); currently NULL, consumers treat it
  as 0 (KI-payment-06).
- The re-check of `GetGatewayPayment` before completion (4.4) will become an additional
  input of `Decide` (the extension point - `settlementPlan`) after KI-01.
- Monotonicity by `gateway_event_ts` (delegated from 4.3/T-CONNECTOR-05) is not
  implemented: a stale event from the retry loop can regress `confirmations` or
  terminalize a payment after a newer one - KI-payment-07.
- The race "expired committed before a late detected": 4.6 rereads the state before
  expiration, but a payment detected after expiration requires manual review/an alert -
  KI-payment-08 (related to 3.1/3.2).
- Reconciler reconciliation (9.x) must account for the policy tolerances: a discrepancy
  between the gateway and platform classification within the tolerance is not a
  `status_mismatch` (T-PAY-05).

## 12. Review (CBL-001, step 6)

Four independent passes (15.10.2026). No blockers. MAJORs are closed: atomicity/trust
model boundaries (KI-payment-04 extended to completing a payment from an unauthenticated
topic before 0.5/0.6/4.4), the missing effective policy log (`Describe` added),
incomplete self-transition and amount-classification scenarios (tests added),
`gateway_event_ts` monotonicity and the expired/detected race recorded in
KI-payment-07/08, the impact on Reconciler - T-PAY-05. The MINOR of tolerance boundaries
(the floor formula of FR-2) is fixed and covered by indivisible amounts; strict bps
parsing; duplicate keys and `null` are rejected; the zero-value policy guard; `%w` in
configuration errors.

| Role | Key findings | Closure |
| :-- | :-- | :-- |
| No context | MAJOR: the policy log is missing; the self-transition below the threshold and the amount boundaries are not covered; MINOR: the floor formula vs the code; silent getenvInt; case duplicates; incomplete ChainTo/case coverage; NIT: %w, a duplicate test, WithClock, the return code | the log added; tests for self/below the threshold/boundaries; the formula `expected - bpsFloor` implemented and tested (indivisible amounts); `parseToleranceBPS` with refusal; duplicates/`Default`/`null`; ChainTo and case tests; `%w`; the duplicate removed; WithClock covered |
| Architecture | MAJOR: gateway_event_ts not implemented (it fell between 4.3 and 4.5); the expired/late detected race; the impact on Reconciler not recorded; MINOR: decimals in FR-1, the $1000 boundary, feed references, the 4.4/4.5 conflict, the KI-04 wording, case duplicates, getenvInt, the log, zero-value, fee NULL, underBPS=10000, duplicate ShouldExpire/FindExpired states; NIT: the ChainTo warning, the env file | KI-payment-07/08; T-PAY-05 and a caveat for 9.x; FR-1 clarified (decimals, a case-insensitive key); the tier boundary documented; references to 6.x/KI-06; the 4.4 extension point recorded; KI-04 updated; duplicate validation; strict bps; the log; the guard; fee NULL=0 in FR-6/KI; 10000 bps documented; the duplication noted for 4.6; the ChainTo docstring |
| With context | MAJOR: AGENTS not updated (step 7); MINOR: silent bps; the log; boundary rounding; the self-transition without a test; the spec status; NIT: a duplicate test, WithClock, duplicate validAsset, key case, zero-value | AGENTS updated in this commit; parseToleranceBPS; the log; the formula+tests; tests; finalization; the duplicate removed; a WithClock test; the NIT duplication; case is rejected; the guard |
| Security | MAJOR: since 4.5 a forged confirmed (an amount within the tolerance + confirmations) completes a payment from an unauthenticated topic - KI-payment-04 updated and the ACL/mTLS requirement tightened; MINOR: the underpayment boundary (+1 unit), `null`/duplicates/`Default`, silent bps, the log; NIT: the return code, zero-value | KI-payment-04 and the spec updated (boundaries: until 4.4/0.6 do not publish the topic outside the trusted network); the floor formula; validation; strict bps; the log; the guard |
