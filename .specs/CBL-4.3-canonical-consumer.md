# CBL-4.3. Canonical event consumer: duplicates and ordering

| Field | Value |
| :-- | :-- |
| Epic | E4, Payment Service |
| Repository | `cbl-payment` |
| Size | M |
| Wave and parallel group | W2-PAYMENT |
| Depends on | 4.1, 3.5 |
| Contract version | `cbl-protos@cf-2` (`billing/events/v1`, `CanonicalPaymentEvent`) |
| Status | done, review closed |

## 1. Context and goal

Connector (3.5) publishes canonical gateway events to `gateway.events.canonical`.
Payment must move the state machine (4.1) to the state corresponding to the event:
`detected`, `confirmed`, `failed`, `expired`, `underpaid`, `overpaid`. Kafka provides
at-least-once delivery and ordering only within a partition: duplicates and out-of-order
event arrival (a confirmation before detection, a repeat after a terminal state) must be
safe.

## 2. Boundaries

In scope: the `cbl-payment-canonical` consumer group, mapping of canonical events to
transition chains, application with CAS and retries, protection against duplicates and
stale events, main wiring.
Out of scope: confirmation thresholds and `completed` by amount (4.5), status re-check
via `GetGatewayPayment` (4.4), `payment.events` publication (4.7), webhook normalization
(3.5).

## 3. Functional requirements

FR-1. Subscription to `gateway.events.canonical` (group `cbl-payment-canonical`) via the
kit `inbox.Processor` + `kafkaconsumer`: retry topics 5s/1m/10m and DLQ, parking via the
kit producer (as in 2.7). Processing is idempotent; the business effect is state machine
transitions only.

FR-2. Event plan (domain): the function `PlanEvent(current, event)` returns a chain of
transitions, a stale flag (an empty chain, a confirmation without an effect), or
`ErrEventTooEarly` (the event overtook invoice creation - a retry via the retry topics):

| Current | detected | confirmed | failed | expired | underpaid/overpaid |
| :-- | :-- | :-- | :-- | :-- | :-- |
| creating/created | too early | too early | too early | too early | too early |
| pending | detected | detected, confirming | failed | expired | detected, terminal |
| detected | detected (self) | confirming | failed | stale | terminal |
| confirming | stale | confirming (self) | failed | stale | terminal |
| terminal | stale | stale | stale | stale | stale |

Terminal: `completed`, `expired`, `underpaid`, `overpaid`, `failed`, `cancelled`.

FR-3. Application: `PaymentTx` (snapshot) -> `PlanEvent` -> sequential `TransitionTx`
with CAS; the event field patch (the received amount, `txid`, `gateway_id`, `provider`,
`network`, `confirmations`) is applied at the target (last) step of the chain;
`confirmations` are written only for `detected`/`confirmed`. Transitions and the inbox
record are committed in one Guard transaction (`CBL-003`, section 6): a processing
failure rolls back the effect together with the inbox row. `ErrStateConflict` (a race
with 4.2/4.4/background tasks) - a retry: reread, rebuild the plan (up to 3 attempts),
then return the error to the retry topics.

FR-4. Duplicates: `event_id` from the envelope is protected by the inbox table (kit
Processor); a repeated delivery of the same event creates no second effect. A stale
event (FR-2) is acknowledged (ack) with a WARN log: the version and journal do not
change.

FR-5. Input errors: an invalid event (broken protobuf, an invalid `payment_id`, a
negative amount, a scale outside 0 - 18, an empty or too long identifier,
`confirmations` above 1 000 000, a missing received amount for
`detected`/`confirmed`/`underpaid`/`overpaid`, an asset or scale mismatch with the
expected payment details) - `errInvalidCanonicalEvent` -> retry topics and DLQ; the
asset is normalized to uppercase; `UNSPECIFIED`/an unknown enum - WARN and ack with no
effect; the payment is not found - an error (a retry): the event cannot appear before
the creation transaction (the race with 4.2 is closed by retries). Untrusted identifiers
in errors are truncated (they do not inflate the DLQ envelope).

FR-6. main: with `KAFKA_BROKERS` set, the consumer starts (the group from
`KAFKA_CONSUMER_GROUP` or `cbl-payment-canonical`); without brokers - a warning, HTTP/
gRPC still work.

## 4. Contracts

The input is `billing.events.v1.CanonicalPaymentEvent` (CF-2): `payment_id`, `gateway_id`,
`provider`, `type`, `received_amount`, `confirmations`, `txid`, `network`,
`gateway_event_ts`. There are no outbound contracts.

## 5. Data model and migrations

No new tables: `payments`/`payment_transitions` (4.1) and inbox (00003) are used.

## 6. State machines

The FR-2 chains are applied strictly through the 4.1 matrix; an event cannot move a
payment to a state unreachable by the matrix.

## 7. Edge cases

A confirmation before detection (`pending -> detected -> confirming` in one command); a
repeat of `detected`/`confirmed` with a new `event_id` (a self-transition, version +1,
journal; duplicates of the same `event_id` are cut off by inbox); `detected` after
`completed` (stale, WARN, ack); `failed` after terminal (stale); `expired` after
detection (stale); `underpaid` from `pending` (a chain of two transitions); an event for
`creating`/`created` (too early, retries); an event for an unknown payment (retries,
then DLQ); a version conflict due to a concurrent transition (a plan retry, up to 3);
invalid protobuf/amount/asset (DLQ); a lowercase asset (normalized); an asset/scale
mismatch with the expected ones (DLQ); a missing amount for monetary types (DLQ);
`confirmations` above the limit (DLQ); overlong identifiers (DLQ, truncated in errors);
an unknown enum type (ack); a processing failure after the first transition of the chain
(a rollback of everything together with inbox); a terminal state + an unknown type
(ErrUnknownEvent in the domain).

## 8. Non-functional requirements

Lock-free processing: a snapshot read + CAS transitions (T-PAY-02); the number of DB
operations per event - 1 read + 1 - 2 transitions; repeated conflicts are bounded by 3
attempts with subsequent handover to the retry mechanics; transitions and the inbox row
are atomic (the Guard transaction); the raw event is not logged in full (only payment_id,
type, versions); traceparent from the envelope headers reaches the consumer logs
(end-to-end tracing - 0.7).

## 9. Test plan

| Level | Scenarios |
| :-- | :-- |
| Unit (domain) | the complete FR-2 table: every (state, event) pair - a chain/stale/too early; an unknown type |
| Unit (usecase) | fake storage: application of the chain and the patch, a stale ack, a too-early error, a retry after a version conflict, exhaustion of attempts, invalid events |
| Integration | real PostgreSQL: the handler applies detected and saves the fields; a confirmed-first chain; a duplicate does not change the state and journal; stale after completion |
| E2E | Kafka + PostgreSQL: canonical detected + confirmed from Kafka move the payment to `confirming`; a record duplicate does not grow the version; a stale detected after terminal - no-op |
| Load | 14.1 |

## 10. Work plan and commit

Tests first (domain, usecase, integration, e2e), then domain/canonical.go,
usecase/canonical_consumer.go, main wiring. Commit
`CBL-4.3: canonical event consumer with duplicate and ordering protection`; the spec is
in the same commit.

## 11. Open questions

- The confirmation threshold (4.5) will replace the `confirming` self-transition with
  `completed` by amount; 4.3 keeps `confirmations` (the limit 1 000 000 - protection
  against forgery).
- The re-check of `GetGatewayPayment` before `completed` - 4.4 (a Connector 3.2 call,
  blocked by KI-01): 4.3 does not call Connector.
- Kafka trust boundaries: events are not signed, deduplication is by `event_id` from the
  header; a participant with produce rights can move states up to `confirming` and
  terminal ones, but not `completed` (4.4/4.5); ACL/mTLS - 0.5/0.6 (KI-payment-04).
- A too-early event relies on nominal retry pauses 5s/1m/10m: if the payment stays in
  `creating`/`created` for a long time (a CreateInvoice timeout, 4.6), a valid event
  will go to the DLQ and require a manual replay (KI-payment-05, runbook).
- Deferred processing of retry topics (separate pauses instead of immediate reading by
  the same consumer) - a common kit topic (as in 2.7), out of 4.3.

## 12. Review (CBL-001, step 6)

Four independent passes (15.10.2026). No blockers. The MAJOR of all roles (atomicity of
inbox and transitions) is closed by moving to the Guard transaction: the consumer and
the repository got tx-aware methods (`PaymentTx`/`TransitionTx`), a rollback integration
test was added; MAJORs of untrusted input are closed by asset normalization,
reconciliation with the expected payment details, the amount requirement for monetary
types, the `confirmations` limit and identifier truncation in errors; the MAJOR of Kafka
trust boundaries is recorded in KI-payment-04; the MAJOR of the poison loop (x-error
inflation) is closed by truncation; MINOR/NIT are closed by tests (lowercase, mismatch,
missing received, huge confirmations, all enum types, confirmed-first, duplicates via
Guard, atomicity), the `make test-e2e` target, the DB pool, WARN for stale and
documentation updates.

| Role | Key findings | Closure |
| :-- | :-- | :-- |
| No context | MAJOR: the tx Guard is ignored (non-atomic); confirmations for underpaid/overpaid are lost; stale is logged at Info; traceparent not implemented; a missing amount is accepted; a lowercase asset fails on the DB CHECK; missing integration tests; e2e outside the Makefile; FR-3 vs the code; network outside FR-3; dead code; sleeps in e2e | tx-aware methods + an atomicity test; confirmations only for detected/confirmed (spec); Warn; traceparent in the logs; the amount is mandatory for monetary types; normalization + reconciliation; tests added; `make test-e2e`; FR-3 clarified; network in FR-3; dead code removed; e2e via inbox markers |
| Architecture | MAJOR: atomicity; lowercase asset; traceparent; ordering by gateway_event_ts not implemented (T-CONNECTOR-05); AGENTS not updated; MINOR: patch at the last step vs FR-3, Info, the integration plan, e2e outside make, the too-early budget; NIT: an unknown type in terminal, an extra timeline in Get, the DB pool | atomicity + T-PAY-04; normalization; traceparent; monotonicity by event time is delegated to 4.5 (spec §11), T-CONNECTOR-05 is reconciled; AGENTS updated; FR-3 aligned with the code; Warn; tests; make; KI-payment-05; unknown-first in PlanEvent; `PaymentTx` without history; the pool configured |
| With context | MAJOR: lowercase asset (the last barrier); step 7 documentation; MINOR: AGENTS content, WARN, FR-3/code/network/confirmations, integration confirmed-first, network without a differ test, canonicalType coverage, traceparent; NIT: dead code, duplicate helpers, sleep, a narrow interface, fmt.Sprintf | normalization + a mismatch test; Makefile/AGENTS; journal/T-PAY-04/KI/runbook; Warn; the spec is synchronized; tests added (network erc20, chain, duplicates); the type table; traceparent; removed; helpers kept (different packages, NIT); inbox markers; `PaymentTx`; `fmt.Sprintf` removed |
| Security | MAJOR: atomicity; MAJOR: amount/asset/network are not reconciled with the expected ones; MAJOR: Kafka trust boundaries; MAJOR: the poison loop of the x-error size; MINOR: confirmations without a limit, string lengths, self-transitions with a new event_id, boundary tests, nominal retries | atomicity; reconciliation of asset/decimals with expected (network is patched deliberately); KI-payment-04; identifier truncation (a short payment_id, limits 256); the confirmations limit 1e6; field limits; self-transitions documented (4.5 reads the amount before terminal); boundary tests; KI-payment-05 |
