# CBL-4.6. Background tasks: invoice expiration and stuck payments

| Field | Value |
| :-- | :-- |
| Epic | E4, Payment Service |
| Repository | `cbl-payment` |
| Size | M |
| Wave and parallel group | W2-PAYMENT |
| Depends on | 4.1, `ShouldExpire` predicate from 4.5 |
| Contract version | `cbl-protos@cf-2` (no new contracts) |
| Status | done, review closed |

## 1. Context and goal

The payment state machine (4.1) is driven by canonical gateway events (4.3) and the
completion policy (4.5), but the `created`/`pending` states have no "alarm" event:
invoice expiration depends on time only (`expires_at` is issued to the merchant in the
creation response, `CBL-000` section 5). Without a background task, an invoice with an
expired `expires_at` stays in `created`/`pending` forever: the merchant never receives
`payment.expired` (4.7), and reconciliation 9.x and reporting see eternally open
invoices.

The second class is stuck payments:

- `creating`: process 4.2 could crash between creating the record and fixing the quote
  or the invoice at the gateway; the payment stays in `creating` forever, and too-early
  gateway events for it live only until the retry pauses are exhausted (KI-payment-05);
- `detected`/`confirming` with no movement: a lost gateway event or a long confirmation
  collection; the money may be on-chain, automatic terminalization is prohibited - an
  observable status is needed (WARN, counters) plus remediation via 4.4 (after KI-01)
  and reconciliation 9.x.

The task adds a janitor: a periodic sweep that (a) expires `created`/`pending` invoices
through the `ShouldExpire` predicate (4.5), (b) deterministically closes stuck
`creating` payments with the `failed` state, (c) makes stuck `detected`/`confirming`
payments visible in logs without changing the state and without touching the money.

## 2. Boundaries

In scope: janitor (usecase), storage queries `FindExpired`/`FindCreating`/`FindStuck`,
a partial index for `creating` (migration), configuration and `main` wiring, unit/
integration/e2e tests.

Out of scope: re-checking `GetGatewayPayment` at the gateway (4.4, blocked by KI-01);
publishing `payment.events` from janitor transitions (4.7 - transitions are visible via
`payment_transitions`; 4.7 will add the outbox record); reconciliation with the
Reconciler (9.x); closing the invoice at the gateway (the invoice expires on the gateway
side by the transmitted `expires_at`, Connector 3.x); Prometheus metrics (0.7).

## 3. Functional requirements

FR-1. `usecase.Janitor` performs an immediate sweep at startup, then sweeps
(`Sweep`) on a schedule via `Run(ctx)`: ticker `PAYMENT_JANITOR_INTERVAL` (30s by
default; `0` - janitor disabled; an invalid value or less than 1s - startup
refusal), query batch size `PAYMENT_JANITOR_BATCH` (100 by default, range
1 - 1000, an invalid value - startup refusal; the batch capacity is provided by
storage: background queries use a separate ceiling of 1000, not the user list
limit of 200). The sweep returns and triggers logging by counters: `expired`,
`creating_failed`, `stuck`, `conflicts`, `errors`. The summary log is Info only
with non-zero effects; an empty sweep summary is Debug.

FR-2. Expiration: `FindExpired(now, batch)` returns `created`/`pending` with
`expires_at <= now`; for each payment the janitor re-checks
`domain.ShouldExpire(state, expiresAt, now)` on the snapshot and executes
`Transition(id, state_version, expired, reason = "janitor expired")`. The CAS on
`state_version` closes the race with the consumer/4.4/4.2: the losing conflict
(`domain.ErrStateConflict`) is skipped without an error and counted by the
`conflicts` counter; the next sweep will see the current state. The second DB
barrier (trigger matrix) makes `detected -> expired` impossible even with a bug
in the code (KI-payment-08).

FR-3. Stuck creations: `FindCreating(now - PAYMENT_CREATING_TIMEOUT, batch)`
returns `creating` payments created not later than the threshold; the janitor
closes them with `Transition(id, state_version, failed, reason = "janitor
creating timeout")`. `PAYMENT_CREATING_TIMEOUT` is 5m by default (strict
parsing, minimum 1m: a smaller value would mass-close live creations of 4.2 with
an RPC deadline of 6 s). Rationale: a normal creation (4.2) completes within
seconds; `failed` makes too-early events for this payment deterministically
stale instead of endless retries (KI-payment-05). A possible "orphaned" invoice
at the gateway without an address at the merchant is unpayable and expires on
the gateway side; reconciliation 9.x will see it as unpaid (recorded in
KI-payment-09).

FR-4. Stuck `detected`/`confirming`: `FindStuck(now - PAYMENT_STUCK_TIMEOUT,
batch)`; the janitor only logs WARN (up to 10 payments per sweep with the full
`payment_id`, the rest - by counter) and counts them in `stuck`: automatic
terminalization of a monetary state is unacceptable (money on-chain, T-01).
`PAYMENT_STUCK_TIMEOUT` is 6h by default (strict parsing, minimum 1m).
Remediation: 4.4 after KI-01, reconciliation 9.x; until then the payment is not
modified. An adjacent case (money on-chain against an already expired invoice)
is additionally singled out in consumer 4.3: a late `detected`/`confirmed`
after `expired` writes a separate WARN
`late canonical event after expiry needs manual review` (alerting by logs until
metrics 0.7, KI-payment-08).

FR-5. Sweep fault tolerance: an error of an individual payment (not a conflict)
does not interrupt the sweep - the payment is counted by the `errors` counter
with a WARN log, and the sweep continues; a query error skips the corresponding
sweep section (`errors + 1`, WARN); context cancellation (SIGTERM) interrupts
the sweep between payments; the janitor does not return errors outward -
observability via logs, after 0.7 via metrics.

FR-6. Configuration and `main` wiring: the janitor starts after migrations are
applied when `PG_DSN` is set (Kafka is not required), runs in a separate
goroutine until the context is done; the effective configuration (interval,
batch, timeouts) is logged at startup, with `PAYMENT_JANITOR_INTERVAL=0` - the
log `janitor disabled`. Strict configuration parsing: an empty value means the
default, an invalid, negative, out-of-range, or below-minimum value - startup
refusal of the service (a typo must not silently disable, speed up background
transitions, or mass-close live creations).

FR-7. All times are UTC; threshold comparisons at the boundary include equality
(`expires_at == now` expires; `created_at == now - timeout` is stuck).

## 4. Contracts

- External contracts do not change: there are no new gRPC/REST/Kafka topics or
  schemas; janitor transitions are ordinary state machine transitions observable in
  `payment_transitions` (reason `janitor expired`/`janitor creating timeout`); the
  `expired` state corresponds to the `PAYMENT_STATE_EXPIRED` enum (CF-1).
- Environment variables: `PAYMENT_JANITOR_INTERVAL` (duration, 30s by default,
  0 - disabled, minimum 1s), `PAYMENT_JANITOR_BATCH` (int, 100 by default,
  1 - 1000), `PAYMENT_CREATING_TIMEOUT` (duration, 5m by default, minimum 1m),
  `PAYMENT_STUCK_TIMEOUT` (duration, 6h by default, minimum 1m).
- Storage: `FindCreating` (new), `FindExpired`/`FindStuck` (4.1) over the partial
  indexes `payments_creating_idx`/`payments_expiry_idx`/`payments_stuck_idx`;
  background queries are clamped by a separate ceiling of 1000 (`maxBackgroundLimit`),
  not by the user list limit of 200.

## 5. Data model and migrations

Migration `00004_creating_idx.sql` (forward only):
`CREATE INDEX payments_creating_idx ON payments (created_at) WHERE state = 'creating'`
(lookup of stuck creations without scanning the whole table). Indexes for expiration
and stuck detected/confirming were created in 00001. Existing data is not rewritten.
`CREATE INDEX` without `CONCURRENTLY` takes a brief write lock on `payments`: at the
current scale this is acceptable (style of 00001); as the table grows
(KI-payment-02), a separate task with a `NO TRANSACTION` migration.

## 6. State machines and rules

Janitor transitions go strictly through the 4.1 matrix and `storage.Repository.Transition`
(CAS on `state_version`, row lock in a transaction, a `payment_transitions` row):

| Section | From | To | Condition | Reason |
| :-- | :-- | :-- | :-- | :-- |
| Expiration | created, pending | expired | `ShouldExpire` (4.5) | `janitor expired` |
| Stuck | creating | failed | age > `PAYMENT_CREATING_TIMEOUT` | `janitor creating timeout` |
| Observation | detected, confirming | - | age > `PAYMENT_STUCK_TIMEOUT` | WARN log, no transition |

The janitor does not touch terminal states; `expired` is terminal.

## 7. Edge cases

`expires_at` NULL (does not expire); `expires_at` exactly `now` (expires); the snapshot
is `pending`, by the time of the transition it is already `detected`/`completed` (CAS
conflict or the transition is forbidden by the second barrier - skip without an error);
a repeated sweep over an already expired payment (the state query will not return it);
two janitor instances (CAS makes the effect single, extra conflicts are visible by the
counter); `created_at` exactly at the timeout boundary (counted as stuck); a creation
where 4.2 managed to fix `quote_id`/`gateway_id` but did not bring it to `created`
(closed as `failed`, partial data is not erased); a large backlog (one batch per sweep,
subsequent ticks finish the remainder); an error of one transition (the sweep
continues); `stuck` payments of real networks with a legitimately long confirmation
collection (the 6h threshold covers typical networks; this is an alert threshold, not a
decision); context cancellation in the middle of a sweep (early exit); `errors` due to
an invalid state in the DB (counter, log, no transition).

## 8. Non-functional requirements

One sweep is at most three queries over partial indexes and one UPDATE per found
payment; there are no external calls; concurrency with the consumer and gRPC is via
CAS without global locks (T-PAY-02); batching bounds the sweep duration and DB load
when unwinding a backlog; there is no N+1 (transitions per batch are sequential over
the found rows); logs contain no amounts or payment details, only payment_id, state,
version; intervals and timeouts are validated at startup (FR-6); after 0.7 the
counters `expired`, `creating_failed`, `stuck`, `conflicts`, `errors` are published
as Prometheus metrics (extension point - the `metricsHandler` placeholder).

## 9. Test plan

| Level | Scenarios |
| :-- | :-- |
| Smoke | build, startup with the janitor configuration, health |
| Unit (usecase) | fake storage: expiration of created/pending with reason; skip of a non-expired snapshot (double `ShouldExpire` check); version conflict - `conflicts` without `errors`; a creation older than the timeout - `failed`; a younger creation - skip; stuck - `stuck` without transitions; a query error - the section is skipped; equality boundaries (`expires_at == now` expires, `created_at == now - timeout` is stuck) on the fixed clock `JanitorConfig.Clock`; query arguments (now, cutoff, batch); context cancellation; a transition error does not interrupt the sweep |
| Unit (domain/storage) | `ShouldExpire`: boundary `expires_at == now` (expires); the remaining pairs are covered by 4.5 |
| Integration | real PostgreSQL: expiration of a real payment (transition, journal, reason); a repeated sweep - no-op; `detected` with a past `expires_at` is not touched; a stuck `creating` (backdated `created_at`) is closed as `failed`; `FindCreating` does not return fresh and non-`creating` payments; a batch of 205 rows with batch 250 is not cut by the list limit of 200; `FindStuck` (clock moved forward) returns `detected` and `confirming`; two janitors in parallel - one effect; a late `detected` after `expired` - the consumer's separate WARN (KI-payment-08) |
| E2E local | compose stand (run instruction `test/e2e/E2E.md`, scenario E2E_Payment_Schema): the payment container with `PAYMENT_JANITOR_INTERVAL=2s` moves an expired pending to `expired` with reason `janitor expired`, a stuck `creating` (backdate 10m) - to `failed`, detected with a past `expires_at` does not change; migration version 4 |
| E2E global | not affected: transitions are local to Payment (the end-to-end expired -> notification flow will be seen by 4.7) |
| Load | 14.1: the janitor sweep is not on the hot path; a separate profile is not required |

Target unit+integration coverage: at least 80%.

## 10. Work plan and commit

Tests first (janitor unit with fake storage, integration, the janitor section in
`schema.ps1`), then `migrations/00004_creating_idx.sql`, `FindCreating` in
storage, `internal/usecase/janitor.go`, `main` wiring, AGENTS.md update.
Commit `CBL-4.6: background tasks for invoice expiration and stuck payments`;
the spec is in the same commit.

## 11. Open questions and decisions

- A late `detected` after `expired` (payment arrived after expiration) remains a
  manual investigation: the janitor only guarantees that expiration does not
  override an already detected payment (CAS + second barrier); alerts - 9.x
  (KI-payment-08).
- A stuck `creating` is closed as `failed` without contacting the gateway: an
  unpaid invoice without an address issued to the merchant carries no risk; a
  possible discrepancy "invoice exists, payment failed" is a subject for
  reconciliation 9.x (KI-payment-09).
- Automatic transition of stuck `detected`/`confirming` to `failed` is rejected:
  money is on-chain, the decision is made by reconciliation with the gateway
  (T-01, 9.x), the janitor is limited to observation.
- `created` lives until 4.2 appears (KI-01): until then tickets in `created` may
  hang until `expires_at`; this is not a janitor fault but a consequence of the
  missing invoice creation step at the gateway. Requirement for 4.2: `expires_at`
  is fixed no later than the `creating -> created` transition - otherwise the
  invoice will not expire (ShouldExpire without a deadline is always false) and
  will hang until observation appears in 9.x.
- Relationship with 4.7: janitor transitions (expired/failed) must get into
  `payment.events` through the same outbox as consumer transitions; the
  embedding point is a single record in `Repository.TransitionTx` (acceptance
  criterion of task 4.7), otherwise the merchant will not receive
  `payment.expired` (8.x).

## 12. Review (CBL-001, step 6)

Four independent passes (15.10.2026). No blockers. MAJOR findings closed:
the background query batch was cut by the list ceiling of 200 against the
declared 1000 (`maxBackgroundLimit` 1000 introduced, integration test of 205
rows with batch 250); FR-7 equality boundaries were not covered (unit tests
`expires_at == now`, `created_at == cutoff`, query arguments, the domain case
`ShouldExpire`); AGENTS.md updated in this commit (T-PAY-06, env,
KI-payment-05 reformulated, KI-payment-08 refined, KI-payment-09 added, runbook,
journal); a typo in env could speed up background transitions (interval minimums
1s, creating/stuck 1m, strict batch parsing); a late `detected` after `expired`
is now a separate WARN
`late canonical event after expiry needs manual review` (KI-payment-08).
MINOR findings closed: context cancellation does not spam errors (early exit
between sections), a fresh creation in unit tests, `confirming` in the
integration stuck scenario, two parallel janitors, `require.Len` before
indexing, the first sweep immediately at startup, the `CONCURRENTLY` caveat for
prod, a comment on 2s in compose.
Accepted without changes: no jitter for replicas (correctness is held by CAS)
and `lock_timeout` for background transactions (the batch is sequential, the
risk threshold is 14.6).

| Role | Key findings | Closure |
| :-- | :-- | :-- |
| No context | MAJOR: batch is cut to 200; FR-7 boundaries not covered; MINOR: ctx-cancel spams `errors`; silent batch env; edge case gaps; AGENTS; NIT: fake arguments, `WithClock` wording, NotEmpty assert, literal 1000, CONCURRENTLY, interval lower bound | `maxBackgroundLimit` + test 205/250; boundary unit tests and the domain case; early exit on ctx; `parseBatchEnv`/minimums; fresh creation, `confirming`, parallel janitors, `require.Len`; AGENTS in the commit; the fake records cutoff/limit with asserts; the spec is aligned; `require.Len(found, 2)`; interval minimum 1s; caveat in §5 |
| Architecture | MAJOR: batch 200 vs 1000; AGENTS without T-PAY-06/KI-09/journal; MINOR: outbox 4.7 must cover janitor transitions; `created` without `expires_at` is a dead end; no parallel test; no jitter; CONCURRENTLY; silent batch; NIT: first sweep, shutdown, compose comment | `maxBackgroundLimit`; AGENTS (T-PAY-06, KI-09, KI-05/08, journal); outbox embedding contract in `TransitionTx` - requirement for 4.7 (§11); requirement for 4.2 to fix `expires_at` (§11); parallel integration test; jitter accepted without changes (safety is CAS, conflicts are accounted for); §5 caveat; strict batch; immediate first `Sweep`; compose comment |
| With context | MAJOR: batch; boundaries; step 7 of the process (AGENTS/spec); MINOR: ctx-cancel as errors; fresh creating only in integration; only detected in the stuck test; `p-detected` with a future deadline; sequential two sweeps; NIT: commit message, parse `%w`, FindCreating docstring, .env.example, `transitions[0]` without require, goroutine shutdown | batch; boundary tests; AGENTS and done status; early exit; unit fresh creating; `confirming` added; the deadline made past; parallel sweeps; message per the template; `%w` in parseDurationEnv; docstring reformulated; `.env.example` absent in the service (source - §5 AGENTS); `require.Len`; shutdown note in T-PAY-06 |
| Security | no BLOCKER; MAJOR: env without lower bounds (speeding up transitions, mass closing creating); a late detected after expired only as a generic WARN; MINOR: batch 200; silent batch; no lock_timeout; NIT: comment, full payment_id, compose 2s, schema.ps1 (no injection), CONCURRENTLY | minimums 1s/1m/1m and strict batch; a separate WARN `late canonical event after expiry needs manual review` + observer test; `maxBackgroundLimit`; `parseBatchEnv`; lock_timeout accepted (the batch is sequential, T-PAY-06); comment; payment_id is not a secret (§8 policy); comment in compose; concatenation of GUID only; §5 caveat |
