# CBL-17.10. Payment provider rules

| Field | Value |
| :-- | :-- |
| Epic | E17 (real provider integration) |
| Repository | `cbl-payment` |
| Size | L |
| Wave and parallel group | W5-PROVIDER |
| Depends on | 17.9, 17.7 (adapters and canonical evidence), 4.x (payment core), CF-4 contracts |
| Contract version | none (CF-4 fields already frozen; no `.proto` change) |
| Status | done (increment 1: payment provider rules; the quote and ledger increments of 17.10 land in their repositories) |

## 1. Context and goal

Real providers deliver evidence the payment core did not consume: the raw
provider status, the fee split, repeated deposits on one invoice (NOWPayments
`parent_payment_id`, late top-ups) and deposits in the wrong asset. This task
implements the payment-side rules of the provider integration (working plan
section 4, line 169 and decisions 9/10/18): persist and expose `raw_status` and
`checkout_url`, pass the provider fee split through `payment.events`, classify a
repeated valid deposit as an auto top-up (a derived completed payment linked by
`parent_payment_id`, decision 9) and classify a wrong-asset deposit distinctly
for reconciliation alerts (17.13). The provider window semantics for
`partially_paid` are already authoritative upstream: the connector fetch
mandate resolves the final provider status before the canonical event.

## 2. Scope

In scope: migration 00006 (`raw_status`, `checkout_url`, `gateway_fee`,
`network_fee`, `fx_spread`, `parent_payment_id`), the insert-guard exception for
derived top-up rows, the repeated-deposit top-up path in the canonical consumer,
fee split validation and persistence, wrong-asset classification reason, read
exposure of `raw_status`/`checkout_url`, tests and e2e extension.

Out of scope: BitPay under/over auto-refunds (17.12); reconciler alerting on the
new classification (17.13); the manual review workflow for top-ups without a
txid (recorded KI-payment-10); `checkout_url` population (task 4.2 CreatePayment
will write it; the column and read path land here); ledger postings of the fee
split (cbl-ledger 17.10 increment); quote audit fields (cbl-quote 17.10
increment).

## 3. Architecture sketch

### 3.1. Schema (migration 00006)

```sql
ALTER TABLE payments
    ADD COLUMN raw_status        text,
    ADD COLUMN checkout_url      text,
    ADD COLUMN gateway_fee       bigint CHECK (gateway_fee IS NULL OR gateway_fee >= 0),
    ADD COLUMN network_fee       bigint CHECK (network_fee IS NULL OR network_fee >= 0),
    ADD COLUMN fx_spread         bigint CHECK (fx_spread IS NULL OR fx_spread >= 0),
    ADD COLUMN parent_payment_id uuid REFERENCES payments (id);
-- CHECK (char_length(raw_status) <= 64), (char_length(checkout_url) <= 2048)
CREATE INDEX payments_parent_idx ON payments (parent_payment_id)
    WHERE parent_payment_id IS NOT NULL;
CREATE UNIQUE INDEX payments_topup_txid_idx ON payments (parent_payment_id, txid)
    WHERE parent_payment_id IS NOT NULL AND txid IS NOT NULL AND txid <> '';
```

The transition guard adds `parent_payment_id` to the immutable fields;
`raw_status`, `checkout_url` and the fee components stay mutable (they follow
events). The insert guard gains a derived-row exception (D4) hardened by the
review findings (B1/m2):

```sql
-- The guard runs with a pinned search_path, so the parent lookup uses the
-- actual schema of the guarded table (TG_TABLE_SCHEMA).
EXECUTE format('SELECT state FROM %I.payments WHERE id = $1', TG_TABLE_SCHEMA)
    INTO parent_state USING NEW.parent_payment_id;
-- The derived row cannot be its own parent (NEW.id <> NEW.parent_payment_id),
-- the parent must exist and be in (completed, underpaid, overpaid); then:
-- IF NEW.state = 'completed' AND NEW.state_version = 1 AND NEW.completed_at IS NOT NULL
--     THEN RETURN NEW; -- derived top-up row, the audit transition is written by the repository
```

### 3.2. Fee split rule (D5)

The canonical event carries only the split components `gateway_fee`,
`network_fee`, `fx_spread` (cf-4.1; same asset/scale as `received_amount`; a
zero component means the provider did not report it); the aggregate `fee` field
exists only in `payment.events` and is derived here. Rule:

1. `total = gateway + network + fx` (FeeTotal, aggregate 0 on the canonical
   path);
2. Components must be non-negative, match the payment asset/scale and not
   exceed `received`; the total may not exceed `received`;
3. The payment stores `fee_amount/asset/decimals = total` and the components;
   `payment.events` re-emits `fee` plus the components (CF-4 fields).

### 3.3. Repeated deposits (top-up, decisions 9/10, D1 - D4)

A canonical `confirmed` event for a payment in a terminal success state
(`completed`, `underpaid`, `overpaid`) where the event received amount is
greater than the baseline already attributed to the parent is a repeated
deposit. The baseline is `parent.received_amount + SUM(child.received_amount)`
of the existing derived children, read under a `FOR UPDATE` lock of the parent
row (B1): the amount semantics of canonical events are cumulative (the verified
provider total), so successive cumulative events (100k parent, 150k, then 180k)
credit only the new part (50k and 30k) and never re-credit an earlier delta.
The row lock serializes concurrent events of one parent (E-7):

1. `delta = event.received - baseline` (> 0 by the branch condition);
2. `txid` must be non-empty (D3): without it the deposit cannot be deduplicated,
   the consumer logs `topup deposit without txid needs manual review` and
   acknowledges (KI-payment-10); the identity uses the canonicalized txid
   (`domain.CanonicalTxID`, M1): the canonical form of a hex hash is the
   lowercased hex body with the `0x`/`0X` prefix stripped, so prefix and case
   variants of one hash fold into a single identity (base58 stays as-is, with
   the documented residual: a base58 value consisting only of hex characters
   would fold too, which is negligible for realistic hash lengths; an empty or
   prefix-only value canonicalizes to empty and goes to manual review);
3. A derived payment row is inserted (D4): new UUIDv7, same merchant, provider,
   gateway_id, network, asset/scale, `parent_payment_id` = the parent,
   `expected_amount = received_amount = delta`, state `completed` directly (the
   guard exception), `raw_status` and the canonical `txid` from the event,
   reason `topup deposit of <parent> txid <canonical txid>`;
4. Idempotency: the merchant idempotency key `topup:<parent>:<canonical txid>`
   and the unique `(parent_payment_id, txid)` index both block duplicates; a
   unique violation of exactly these two indexes is a duplicate, acknowledged
   with a WARN (any other unique violation is an error, not a masked duplicate);
5. The parent row is NOT changed (`received_amount` stays the original evidence;
   the delta lives on the child);
6. `payment.completed` is emitted for the child (the ledger credits the merchant
   through the existing rule);
7. `detected` repeats stay stale (WARN); credit happens only on `confirmed` (D1);
8. An `expired` parent stays on the KI-payment-08 manual path (late deposits
   after expiry are flagged, not credited).

### 3.4. Wrong-asset classification

A received amount whose asset/scale differs from the payment stays a DLQ-bound
invalid event, but the reason becomes a distinct `wrong asset deposit` text so
17.13 can alert on it without parsing generic invalid reasons. No state effect
and no credit (the funds remain at the provider).

## 4. Functional requirements

FR-1. Migration 00006 applies forward; the insert guard allows derived completed
rows only with `parent_payment_id`, version 1 and `completed_at`; the transition
guard forbids changing `parent_payment_id`.

FR-2. The consumer persists `raw_status` from every canonical event onto the
target transition patch, and the fee split per 3.2; inconsistent split data is
an invalid event.

FR-3. The top-up path of 3.3 creates the derived row, its audit transition
(`'' -> completed`, version 1, reason) and the `payment.completed` outbox event
in the Guard transaction; duplicates are acknowledged.

FR-4. `GetPayment`/`ListPayments` return `raw_status` and `checkout_url`.

FR-5. The wrong-asset reason is distinct (`wrong asset deposit`).

FR-6. E2E: the Kafka canonical-consumer scenario gains a top-up flow (parent
completed, second confirmed event with a larger amount and a new txid -> child
completed row with `parent_payment_id`, one additional `payment.completed`);
E2E.md records it; the migration version check moves to 6.

## 5. Contracts

No `.proto` change: `gateway_events.proto` (raw_status, fee split),
`payment_events.proto` (provider, raw_status, fee split) and
`payment.proto` (checkout_url, provider, raw_status) already carry the fields
(CF-4); the payment service now populates and exposes them.

## 6. Data model and migrations

Migration 00006 as in 3.1; `payment.events` payload gains the fee split
components from the new columns; no changes to `payment_transitions`.

## 7. State machines and rules

The transition matrix is unchanged. Derived top-up rows exist only in
`completed` with a single audit transition; a top-up on an underpaid/overpaid
parent does not change the parent state.

## 8. Edge cases

E-1. An event at or below the baseline on a terminal-success parent
(stale/replayed confirmed) -> WARN `stale canonical event skipped`, no effect.
E-2. Top-up without txid -> manual-review WARN, ack (KI-payment-10).
E-3. Duplicate top-up (same parent+txid, new event id, above the baseline) ->
unique-violation on the top-up identity -> WARN duplicate, ack; case and
prefix (`0x`/`0X`) variants of the same hex txid fold into the same identity
(M1).
E-4. `detected` repeat on a terminal parent -> stale WARN, no credit; the
following `confirmed` credits (D1).
E-5. Fee components above received, negative, or inconsistent with the
aggregate -> invalid event (retry/DLQ); a zero component is treated as absent, a
non-zero component must carry the exact payment asset and scale.
E-6. A parent in `expired`/`failed`/`cancelled` follows existing paths (late
event manual review; no top-up).
E-7. Concurrent top-up inserts (same txid) -> the parent row lock serializes the
baseline reads, one inserts, the other sees the baseline/identity and
acknowledges.
E-8. `raw_status` longer than 64 bytes or containing any character outside
printable ASCII (0x20 - 0x7E: control characters, DEL, C1 controls, Unicode
separators and non-ASCII) -> invalid event.
E-9. Successive cumulative top-ups (100k, 150k, 180k) -> children 50k and 30k
(the earlier delta is never re-credited, B1).

## 9. Non-functional requirements

- NFR-1. Coverage stays at least 80% unit+integration.
- NFR-2. No new dependencies.
- NFR-3. Money stays int64 minor units; no float; split arithmetic is int64 with
  overflow-safe comparison against received.
- NFR-4. English only, no em dash, godoc on exported entities.

## 10. Test plan

### 10.1. Unit matrix

| ID | Case | Input | Expected |
| :-- | :-- | :-- | :-- |
| P-01 | fee split validation | components sum; aggregate only; inconsistent; negative; above received; zero event | total/patches or errors (domain function) |
| P-02 | top-up decision | terminal-success parent, baseline 100k/150k, delta > 0, txid set/missing, at/below baseline, expired parent | TopUpCredit(delta) / TopUpManualReview / TopUpSkip |
| P-03 | read mapping | payment with raw_status/checkout_url/provider | gRPC fields populated |
| P-04 | txid canonicalization | hex with/without `0x` prefix mixed case, base58, spaces, empty | canonical identity string |

### 10.2. Integration matrix (PostgreSQL, Kafka)

| ID | Case | Input | Expected |
| :-- | :-- | :-- | :-- |
| I-01 | migration guards | derived insert without parent; parent change (UPDATE); version != 1; non-terminal parent; self-parent | rejected by the trigger |
| I-02 | top-up flow | completed parent, confirmed delta with txid; then a successive cumulative event | children 50k and 30k, parent untouched, audit transitions, payment.completed outbox rows |
| I-03 | duplicate top-up | second event above the baseline, same txid (and a case-variant spelling), new event id | one child, WARN, ack |
| I-04 | fee split persisted | confirmed event with components | fee columns and the payment.events payload carry the split (I-04 decode) |
| I-05 | wrong-asset reason | received asset mismatch | error text contains `wrong asset deposit` |
| I-06 | kafka e2e | canonical events over Kafka (successive + case-variant stages) | parent completed, two children 50k/30k, replay folded, three payment.completed events |

### 10.3. Smoke set

`make test`, `make test-integration`, `make test-e2e`, `make vet`.

### 10.4. E2E scenario descriptions

| Scenario | Description | Tag or stand |
| :-- | :-- | :-- |
| E2E_Payment_Schema (extended) | compose stand: migrations up to version 6, the state chain remains green; the top-up flow is exercised in the Kafka testcontainers scenario (E2E_Payment_CanonicalKafka) with a repeated deposit producing a linked child payment | `test/e2e/E2E.md` |

### 10.5. Load plan

Not applicable (state transitions only; the rate is unchanged).

### 10.6. Test-first evidence

Red run recorded before implementation (18.10.2026):
`go test ./internal/domain/...` failed with `undefined: domain.FeeTotal` and
`undefined: domain.TopUpDelta`; `go test -tags integration ./internal/usecase/`
failed to compile with `storage.Payment has no field RawStatus` (and the
GatewayFee/FxSpread/ParentPaymentID fields), i.e. the new schema and rules were
absent. Maintenance fact: `go mod tidy` was required to build the module even at
HEAD (a stale go.mod after the Go 1.26 toolchain wave; testify/cpuid patch
bumps in this commit).

Final battery after the review fixes: unit green; the full battery with
integration (`go test -tags integration -coverpkg=./internal/... ./...`) green,
total coverage 91.3%; the Kafka testcontainers flow
(`go test -tags e2e ./test/e2e/...`) green with the extended top-up stages
(successive cumulative 50k/30k, case-variant replay folded into one child); the
full compose stand scenario `E2E_Payment_Schema` PASS 26/26 (mark
`20261018t125343z`, migrations v6, the full chain walk, the
transition/journal/derived-insert barriers rejected live, janitor expiry and
stuck handling, outbox rows and relay drain, the gRPC reads; artifacts in
`test/e2e/.runs/20261018t125343z/`).

Review of the first staged round (18.10.2026, four reviewers) produced one
blocking and several major/minor findings; all were closed on the staged diff:
B1 (the delta is now measured against the parent baseline - the recorded amount
plus the existing children - under a `FOR UPDATE` row lock, with a successive
cumulative test), M1 (`domain.CanonicalTxID` folds case-variant hex replays),
the guard parent validation and self-parent rejection (m2), the narrowed 23505
mapping to the two top-up indexes, the printable `raw_status` rejection (E-8),
the strict non-zero fee component validation (E-5), the I-04 payment.events
payload assertions and the P-03 read mapping test, the E2E.md migration note,
and the full compose run replacing the smoke-only claim. The second review
round found no blocking issues and three non-blocking ones, all closed:
case-insensitive `0x`/`0X` prefix folding of the txid identity with empty-body
manual review (F1), the strict printable-ASCII `raw_status` check including C1
controls and Unicode separators (F2) and the concurrent-serialization
regression test `TestIntegrationTopUpConcurrentEventsSerialized` (F3). The
third round confirmed the closure with no new findings (journal).

## 11. Work plan and commits

Increment 1 (this commit): spec, migration 00006, storage patches and derived
insert, consumer rules, read exposure, tests, e2e extension, AGENTS journal.
