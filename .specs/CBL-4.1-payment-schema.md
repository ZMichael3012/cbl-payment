# CBL-4.1. Payment schema, state machine and state_version

| Field | Value |
| :-- | :-- |
| Epic | E4, Payment Service |
| Repository | `cbl-payment` |
| Size | L |
| Wave and parallel group | W2-PAYMENT |
| Depends on | CF-2, 1.2 (cbl-kit/outbox), 1.3 (cbl-kit/inbox) |
| Contract version | `cbl-protos@cf-2` (`billing/payment/v1`, `billing/events/v1`) |
| Status | done, review closed |

## 1. Context and goal

Payment owns the payment state machine (`CBL-000`, section 2): the source of truth about
the invoice state, protected from out-of-order transitions and from races between events
and requests. The task creates the DB schema (payments, transition history, outbox/inbox),
the domain transition matrix and `state_version` with optimistic locking - the foundation
for 4.2 - 4.7.

## 2. Boundaries

In scope: migrations 00001 - 00003, the state machine domain, the transitions storage
layer with CAS, tests, e2e schema checks.
Out of scope: gRPC handlers CreatePayment/GetPayment (4.2), the canonical event consumer
(4.3), confirmation thresholds (4.5), background tasks (4.6), `payment.events` publication
(4.7), integration with Quote/Connector (4.2, 3.2).

## 3. Functional requirements

FR-1. The `payments` schema (migration 00001): `id` uuid PK (UUIDv7), `merchant_id`,
`idempotency_key` + `request_hash` with the uniqueness `(merchant_id, idempotency_key)`,
`state` (CHECK against the canonical states), `state_version bigint >= 1`, amounts
`expected_*` (mandatory, `> 0`), `received_*` and `fee_*` (nullable until detection,
`>= 0` when set), `quote_id`, `rate`, `pay_address`, `network`, `gateway_id`,
`provider`, `txid`, `confirmations`, `expires_at`, `created_at`, `updated_at`,
`completed_at` (CHECK: only for `completed`/`underpaid`/`overpaid`). Asset fields follow
the format `^[A-Z0-9]{2,16}$`, decimals 0 - 18.
Indexes: `(merchant_id, created_at DESC, id DESC)` for ListPayments; the partial
`(expires_at) WHERE state IN ('created','pending')` for expiration (4.6); the partial
`(updated_at) WHERE state IN ('detected','confirming')` for stuck payments (4.6).

FR-2. Transition history `payment_transitions` (append-only, trigger on UPDATE/DELETE/
TRUNCATE): `payment_id` (FK), `from_state` (empty for creation; CHECK against the state
dictionary), `to_state` (CHECK against the dictionary), `state_version`, `reason`,
`occurred_at`; the uniqueness `(payment_id, state_version)`. The payment timeline
(proto `TimelineItem`) is assembled from this table.

FR-3. Transition matrix (domain, the single source of rules):

```
creating   -> created | cancelled | failed
created    -> pending | cancelled | expired | failed
pending    -> detected | cancelled | expired | failed
detected   -> confirming | underpaid | overpaid | failed
confirming -> completed | underpaid | overpaid | failed
terminal: completed, expired, underpaid, overpaid, failed, cancelled
```

Semantics: `creating` - the record is created, the quote is locked/the gateway is called
(4.2); `created` - the quote is locked, the gateway address is expected (a CreateInvoice
timeout is resolved by a retry or by background task 4.6); `pending` - the invoice is
created at the gateway, the address is issued (the payment acceptance sequence,
`CBL-000`); `detected`/`confirming` - a payment and a confirmation set are detected
(4.3, 4.5); `underpaid`/`overpaid`, `expired`, `failed`, `cancelled` - terminal.
Backward transitions and transitions from terminal states are forbidden; the signatures
`allowed(from, to)` and `terminal(state)`.
A self-transition (updating data without changing the state, for example the growth of
`confirmations` from a repeated canonical event) is allowed only in non-terminal states,
increments `state_version` by 1 and is written to the journal with equal from/to.

FR-4. `state_version`: at creation it equals 1; every accepted transition (including a
self-transition) increments it by exactly 1. A transition is performed optimistically:
reading the row under a row lock and `UPDATE ... WHERE id = ? AND state_version = ?`; a
version conflict - `domain.ErrStateConflict` for the retry loop of the caller; a stale
event is not applied (out-of-order transitions are forbidden, input for 4.3). The
`payment_transitions` row is written in the same transaction.

FR-5. Second DB barrier: the BEFORE INSERT trigger of `payments` requires
`state='creating' AND state_version=1` (creating a payment bypassing the state machine is
impossible); the BEFORE UPDATE trigger validates the `(OLD.state, NEW.state)` pairs
against the matrix and `NEW.state_version = OLD.state_version + 1`; changes to immutable
fields (id, merchant_id, idempotency_key, request_hash, created_at, expected_*) and
self-transitions in terminal states are forbidden. The Go and SQL matrices are verified
by exhaustive enumeration of all state pairs in an integration test (drift is caught).

FR-6. Migrations 00002 (outbox) and 00003 (inbox) - reference DDL from `cbl-kit`
(`CBL-003`, section 6), as in the ledger (2.1).

FR-7. Storage: `Create` (inserting `creating` version 1 - the state and version are forced
to the initial ones, the version 1 history row in the same transaction), `Transition`
(CAS on `state_version` + history), `Get` (payment and timeline in one read-only
repeatable read transaction), `List` (state filter, cursor `(created_at, id)`,
limit 1 - 200, 50 by default), `FindExpired` (states `created`/`pending`: `creating` is
advanced by the background to `created` or `failed`, there is no `creating -> expired`
transition in the matrix), `FindStuck` (for 4.6).

## 4. Contracts

External contracts do not change (CF-1/CF-2 are frozen): the domain `State` corresponds
to `billing.payment.v1.PaymentState` (a 1:1 mapping by names, verified by a test);
`state_version` is published in `payment.events` (4.7) and in `Payment` (CF-1).

## 5. Data model and migrations

```sql
payments(id, merchant_id, idempotency_key, request_hash, state, state_version,
         expected_amount, expected_asset, expected_decimals,
         received_amount, received_asset, received_decimals,
         fee_amount, fee_asset, fee_decimals,
         quote_id, rate, pay_address, network, gateway_id, provider, txid,
         confirmations, expires_at, created_at, updated_at, completed_at)
payment_transitions(id bigserial, payment_id, from_state, to_state, state_version,
                    reason, occurred_at, UNIQUE (payment_id, state_version))
outbox, inbox - as in CBL-003 (section 6)
```

## 6. State machines

The FR-3 matrix; the states and transitions are duplicated in the spec test table (every
pair of the matrix and every forbidden class: backward, from terminal, unknown state).

## 7. Edge cases

Creation with version 1 (the state and version passed by the caller are ignored; a direct
INSERT with another state is rejected by the BEFORE INSERT trigger); concurrent
transitions from one version (exactly one wins); a stale event after a newer transition
(rejection, the version and journal do not grow); a backward transition
(`confirming -> detected`), a transition from terminal, an unknown pair (rejected by the
domain and the trigger, exhaustive enumeration in the test); a confirmations
self-transition in a non-terminal state (allowed, version +1, journal row) and in a
terminal one (forbidden); a direct SQL update bypassing CAS (rejected by the trigger);
UPDATE of immutable fields; UPDATE/DELETE/TRUNCATE of `payment_transitions`; a journal
record with an unknown state (dictionary CHECK); a duplicate
`(merchant_id, idempotency_key)` (4.2); negative/zero amounts, an empty asset, decimals
out of range; `completed_at` outside a terminal success (CHECK); `expires_at` in the past
(expiration does not apply to `creating` - it is advanced to `created`/`failed` by
background 4.6); `pay_address` before `pending` (ordered by scenario 4.2).

## 8. Non-functional requirements

Transactionality: a transition and its history are one transaction; concurrency - only
optimistic retries (no global locks, as in the ledger); the transition journal is
append-only and serves as the state audit; the GetPayment budget of 1 s (CF-1) fits into
a read by PK; ListPayments uses the merchant index.

## 9. Test plan

| Level | Scenarios |
| :-- | :-- |
| Unit (domain) | the whole matrix (every allowed pair), forbidden pairs (backward, from terminal, unknown states), terminal(), the 1:1 `PaymentState` mapping, version increment, self-transitions |
| Integration (storage) | Create with v1 history and forced `creating`; a CAS transition and increment; concurrent transitions (exactly one); a stale version (the version and journal do not grow); exhaustive enumeration of all state pairs against the trigger (mirror of the matrix); a direct UPDATE/INSERT bypassing the state machine; immutable fields; `completed_at` only in terminal success; the journal dictionary; append-only `payment_transitions`; List with cursor and filter; FindExpired/FindStuck; CHECKs of amounts/assets/scales; idempotency key uniqueness |
| E2E | compose stand (run instruction `test/e2e/E2E.md`, scenario E2E_Payment_Schema): migrations applied (version 3), the full chain `creating -> created -> pending -> detected -> confirming -> completed` with every transition recorded, a forbidden transition and a journal overwrite are rejected by the second barrier |
| Load | 14.1 |

## 10. Work plan and commit

Tests first (domain, storage integration), then migrations, domain/state.go,
storage/repository.go, the stand e2e script. Commit
`CBL-4.1: payment schema, state machine and state_version`; the spec is in the same
commit.

## 11. Open questions and review decisions

- `created` vs `creating` on a CreateInvoice timeout (4.2): the quote is locked before the
  gateway call, so a timeout leaves the payment in `created` (the address is expected);
  the wording of CBL-003 (section 5) is clarified, task 4.2 implements the behavior.
- `creating` is not included in the expiration selection: there is no
  `creating -> expired` transition in the matrix, stuck creations are advanced by the
  background (4.6) to `created` or `failed`. The decision is recorded in FR-7 and indexed
  (`payments_expiry_idx`).
- `payment_id` in the Quote quote (quotes.payment_id) and the link of the quote to the
  payment - 4.2 (LockQuote does not accept payment_id in CF-1). Reading a payment by
  `(merchant_id, idempotency_key)` for an idempotent retry - 4.2.
- The second barrier protects against DML roles; the DB owner can drop the triggers
  (KI-payment-03): the migrator/runtime roles are separated before prod (0.4/0.6).

## 12. Review (CBL-001, step 6)

Four independent passes (15.10.2026). The security BLOCKER (a direct INSERT created a
payment in any state bypassing the state machine) is closed by the BEFORE INSERT trigger,
forced `creating` in `Create` and tests; the with-context reviewer BLOCKER (the umbrella
compose built payment without the services/ context) is closed by fixing
`deploy/docker-compose.yml` and verifying the build; the MAJORs (FindExpired included
`creating` without a transition to `expired`; `completed_at` without a restriction; E2E
without the full chain; the task journal empty) are closed. MINOR/NIT are closed by tests
(exhaustive enumeration of pairs against the trigger, CHECK boundaries, the journal
dictionary, a stale version does not grow the journal), by aligning the dependencies and
the linter config v2, by transactional `Get`, and by clarifications to the spec and
CBL-002.

| Role | Key findings | Closure |
| :-- | :-- | :-- |
| No context | MAJOR: E2E without the full chain; Create accepts a foreign state; the postgres wait does not work and does not wait for migrations; Get without a consistent snapshot; the SQL matrix is not mirrored by a full test; `completed_at` not restricted; the index direction; any 23505 into ErrDuplicateKey; cursor without UUID; duplicate package comments; CHECK boundaries without tests; the plan about usecase stubs | E2E passes the chain to completed with the journal; Create forces creating/1 + BEFORE INSERT; waiting for the goose version with retries; Get in repeatable read; a test of exhaustive enumeration of 121 pairs; CHECK completed_at + test; the index id DESC; a constraint name check; UUID validation of the cursor; comments removed; boundaries extended; the plan fixed |
| Architecture | MAJOR: FindExpired/the index included creating without a transition; completed_at without control; MINOR: Create, INSERT bypass, prod roles, T-PAY-01 about the test, creating/created in the docblocks/CBL-003, the limit clamp, the Get proposal, e2e | creating is excluded from expiration; CHECK completed_at; Create forces; BEFORE INSERT; KI-payment-03; the full matrix test; CBL-003 and the comments clarified; the clamp unified; transactional Get; e2e extended |
| With context | BLOCKER: the umbrella compose payment; MAJOR: the journal is empty, CBL-003 is not clarified, the e2e chain; MINOR: the lint config v1, the T-PAY-01 render, Create, Get, NextVersion unknown state, edge cases, the stand wait, the e2e error text, the plan, CBL-002 | the umbrella compose fixed (the build verified); the journal filled; CBL-003 clarified; e2e; `.golangci.yml` v2 (gosec); the T-PAY-01 table fixed; Create/Get/NextVersion; edge cases closed by CHECK/the spec; waiting via goose with retries and the error text; the plan; CBL-002 updated |
| Security | BLOCKER: INSERT bypasses the FSM and the audit; MINOR: owner roles, journal forgery, `completed_at`/lengths, dependency version drift, gosec missing; NIT: the e2e UUID, reflection, .dockerignore | closed by the trigger + Create + tests; KI-payment-03; dictionary CHECKs of the journal; CHECK completed_at; versions aligned with the unified set; gosec enabled in the v2 config; NITs recorded (reflection - 0.7) |
