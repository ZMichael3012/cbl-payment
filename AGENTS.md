# AGENTS.md - cbl-payment

Payment state machine: invoice creation, canonical event processing, confirmation thresholds.

Mandatory global rules: `../../AGENTS.md`, `../../.specs/CBL-001-development-process.md`,
`../../.specs/CBL-002-repos-and-docker.md`, contracts `../../.specs/CBL-003-contracts.md`.
The em dash (U+2014) is forbidden, only the hyphen is used.

## 1. Boundaries

In scope: payment state machine, payment.events, background jobs for expiry and stuck payments.

Out of scope: other platform services; interaction only via `cbl-protos` contracts
(gRPC), Kafka and the perimeter REST (see `CBL-000`, section 2).

## 2. Contracts

- REST: administrative /healthz, /readyz
- gRPC: server PaymentService (GetPayment/ListPayments - 4.8; CreatePayment,
  CancelPayment - 4.2); client ConnectorService, QuoteService
- Kafka produces: payment.events (via outbox, 4.7)
- Kafka consumes: gateway.events.canonical (group `cbl-payment-canonical`)
- Storage: PostgreSQL payment

Contracts are frozen in `cbl-protos` (CF-1 gRPC, CF-2/CF-3 events; quote and
read fields - `cbl-protos@cf-3.3`/`cf-3.4`, CBL-4.8), see `CBL-004`.

## 3. Architecture

Layers: `internal/transport` -> `internal/usecase` -> `internal/domain` -> `internal/storage`.
Libraries: the standard `CBL-002` set (Gin, GORM + pgx driver, franz-go + pkg/sr, go-redis,
goose, zap). `AutoMigrate` is forbidden, goose migrations only, forward only.
Schema (4.1): `payments` (state, `state_version`, amounts, requisites),
`payment_transitions` (append-only transition journal), `outbox`, `inbox`.
Transition matrix - `internal/domain/state.go`, duplicated by the DB trigger
(`payments_transition_guard`); transitions only via `storage.Repository.Transition`
with CAS on `state_version`.
Background jobs (4.6) - `internal/usecase/janitor.go`: invoice expiry
(created/pending), closing stuck creations (creating -> failed), watching
stuck detected/confirming; selections via partial indexes, batch
`maxBackgroundLimit` 1000.
Events (4.7): every applied transition (`Repository.TransitionTx`) writes a
`payment.events` envelope to `outbox` in the same transaction (sink -
`internal/usecase/payment_events.go`); the kit relay in `main` publishes when
`KAFKA_BROKERS` is set (batch 100, interval 250 ms), without Kafka events accumulate.
Reads (4.8): `internal/usecase/payment_query.go` (PaymentQuery) and
`internal/transport/grpcserver/payment.go` (GetPayment/ListPayments); migration
00005 adds `quote_locked_until` and the fill-once rule for the quote fields
(`quote_id`, `rate`, `quote_locked_until`) in the transition trigger; Get
history is limited to the last 500 transitions; the gRPC server is assembled in
`main` with `grpcmw.UnaryServer`.
Provider rules (17.10): migration 00006 adds `raw_status`, `checkout_url`, the
fee split columns (`gateway_fee`, `network_fee`, `fx_spread`) and
`parent_payment_id` with the derived-row insert exception (parent must exist and
be terminal-success, no self-parenting) and the parent-txid unique index. The
canonical consumer persists the raw provider status, derives the fee total from
the split components (`domain.FeeTotal`, cf-4.1 canonical events carry
components only) and credits a repeated confirmed deposit above the baseline
attributed to the parent as a derived completed child (the baseline is the
recorded amount plus the existing children, read under a `FOR UPDATE` lock of
the parent row; the credited delta is `event.received - baseline`, the parent
row untouched, the identity uses the canonicalized txid
`domain.CanonicalTxID`; `internal/usecase/canonical_consumer.go` handleTopUp,
`storage.Repository.CreateDerived`, `storage.Repository.LockAndSumTopUpsTx`);
a deposit in a foreign asset keeps its distinct DLQ reason
(`wrong asset deposit`). Reads expose `raw_status`, `checkout_url` and
`provider`; `payment.events` now carries the provider, raw_status and fee split
fields (T-PAY-09/T-PAY-10).
Creation (4.2): `internal/usecase/payment_creation.go` implements
`CreatePayment`/`CancelPayment`: shape validation, the idempotent replay by
`(merchant_id, idempotency_key)` (hash over merchant, amount, pay asset,
network, pay scale and expiry; a different body conflicts), the atomic key
claim via the unique index, the quote lock
(`QuoteService.LockQuote`, TTL clamped 60..900 s) and the integer rate
conversion (`domain.ConvertRate`) filling the locked pay amount on
`creating -> created` (migration 00007 allows exactly this one expected-field
fill and keeps every other immutable field, including the 00006 parent link),
then the Connector `CreateInvoice` (`created -> pending` with the address or
the checkout page). A quote timeout leaves the payment in `creating`, an
invoice timeout in `created`; a retry resumes the missing steps and the
connector replays the same provider invoice by order id (its mapping now
stores the destination, `34d570a`); business rejections fail the payment
closed with fixed reasons. The clients live in `internal/quoteclient` and
`internal/connectorclient` (fail-closed `Unavailable` without the addresses).

## 4. Commands

```powershell
make build        # go build ./...
make test         # go test ./...
make test-integration  # integration (testcontainers: PostgreSQL)
make test-e2e     # e2e on testcontainers: canonical events Kafka -> payment state machine (no compose stand needed)
make vet          # go vet ./...
make lint         # golangci-lint in Docker (repo context; modules from GitHub)
make up           # docker compose -f deploy/docker-compose.yml --profile app up -d --build
make down         # docker compose -f deploy/docker-compose.yml down -v
```

Compose stand e2e runs follow the `test/e2e/E2E.md` instruction (scenario
E2E_Payment_Schema; artifacts in `test/e2e/.runs/<mark>/`); there are no
PowerShell run scripts.

## 5. Ports and environment

HTTP `8030`, gRPC `9030`, metrics `9230`.
Variables (applicable to the service): `HTTP_ADDR`, `GRPC_ADDR`, `METRICS_ADDR`, `PG_DSN`
(required), `MIGRATIONS_DIR` (default `./migrations`), `DB_MAX_OPEN_CONNS`
(default 20, cap 100), `DB_MAX_IDLE_CONNS` (default 10),
`KAFKA_BROKERS`, `KAFKA_CONSUMER_GROUP` (default `cbl-payment-canonical`),
`PAYMENT_CONFIRMATION_POLICY` (JSON of confirmation threshold tiers per asset, see 4.5),
`PAYMENT_UNDERPAID_TOLERANCE_BPS` and `PAYMENT_OVERPAID_TOLERANCE_BPS`
(0 - 10000, default 0, an invalid value refuses startup),
`PAYMENT_JANITOR_INTERVAL` (default 30s, 0 - janitor disabled, minimum 1s),
`PAYMENT_JANITOR_BATCH` (default 100, 1 - 1000),
`PAYMENT_CREATING_TIMEOUT` (default 5m, minimum 1m),
`PAYMENT_STUCK_TIMEOUT` (default 6h, minimum 1m; invalid janitor variable
values - startup refused, see 4.6),
`QUOTE_GRPC_ADDR`, `CONNECTOR_GRPC_ADDR` (creation clients; both required to
enable `CreatePayment`/`CancelPayment`, otherwise the handlers answer
`Unavailable` fail-closed),
`PAYMENT_ASSET_DECIMALS` (CSV `ASSET:SCALE` of the pay assets; an unknown asset
fails the creation closed; `USDT:6,BTC:8,USDC:6` on the stands; the quote cap
is 900 s and must stay within the deployed `LOCK_QUOTE_MAX_TTL`),
`SCHEMA_REGISTRY_URL`, `REDIS_ADDR`.

## 6. Development rules

- Pipeline (TDD-first, CBL-001): an SDD spec with the architecture sketch and the full test
  plan (unit and integration matrices, e2e scenario descriptions) -> contracts -> red tests
  with recorded evidence -> implementation -> 4-agent review on the staged diff (no-context,
  architecture, with-context, security - blocking) -> AGENTS.md -> final runs (unit +
  integration + local e2e) -> linters -> commit.
- Unit+integration coverage at least 80%; money scenarios only on real PostgreSQL and
  Kafka (testcontainers); local e2e in docker-compose is mandatory.
- Money only int64 in minor units plus decimals; float is forbidden.
- Godoc docstrings are mandatory in English: the first line starts with the entity name and
  ends with a period; comments explain why; errors are wrapped with `%w`; panics in business
  code are forbidden.
- Commit: `CBL-<epic>.<number>: <short message>`, the spec with the same prefix in the same commit.

## 7. SLO and metrics

SLO is specified in task 14.4. Base metrics: latency p95/p99, error rate, outbox lag,
consumer lag; dashboards after task 0.7.

## 8. Trade-offs

| ID | Decision | Pros | Cons and compensation |
| :-- | :-- | :-- | :-- |
| T-PAY-01 | Transition matrix in the domain plus two DB triggers (second barrier): BEFORE INSERT fixes creation in `creating`/version 1, BEFORE UPDATE checks state pairs, increments `state_version`, immutable fields and terminal states | out-of-order transitions and payment creation bypassing the state machine are impossible; the state audit is complete | duplication of the matrix in Go and SQL, trigger cost on every transition; the DB owner can drop the triggers | the matrix is mirrored by full enumeration of pairs in an integration test; the trigger cost is comparable to the ledger (T-LED-02); changing the rules - only via migration; prod roles - KI-payment-03 |
| T-PAY-02 | Optimistic transitions: read under a row lock and CAS on `state_version` | correct transition history, no global locks | under contention on one payment a transition gets `ErrStateConflict` and is retried | conflicts are rare (transitions from different sources); the retry with reading the fresh version - 4.3; the retry metric in 0.7 |
| T-PAY-03 | `updated_at` is maintained by the transition trigger | background jobs (4.6) see the real time of the last movement | an arbitrary `updated_at` update bypassing it is forbidden | "stuck" ones are determined by a time threshold, backdate is not needed |
| T-PAY-04 | The canonical event consumer writes transitions in the Guard transaction (atomically with inbox), CAS retries inside the transaction (up to 3) | the crash window between the effect and inbox is excluded, an event retry does not create extra transitions | the Guard transaction is longer (a read plus up to two savepoint transitions) | the difference from the ledger T-LED-08 is deliberate: there the effect is idempotent by business key, here rollback is more reliable; version conflicts after 3 attempts go to the retry topics |
| T-PAY-05 | The platform policy (4.5) takes precedence over Connector classification: underpayment/overpayment within tolerance completes as `completed`, beyond it - terminal states; thresholds are set by the operator in asset units | the business decision about money is in one place (Payment), thresholds are configurable without a release | double classification (gateway and platform), divergence with the Reconciler, USD-equivalent drift with manual configuration | reconciliation 9.x must account for tolerances (status_mismatch not for a difference within tolerance); USD feed and fee - KI-payment-06 |
| T-PAY-06 | Janitor in the service process (4.6): ticker, batched selections via partial indexes, CAS transitions; without a separate deployment and leader election | no new service and its operation; works without Kafka; disabled by an env; several replicas are safe (CAS gives a single effect) | extra conflicts and duplicate selections in replicas (jitter deliberately not introduced); the batch is sequential - a long row-lock delays the pass (the next tick finishes the remainder); context cancellation interrupts the pass between payments | observability via janitor logs (metrics - 0.7); the load risk threshold - 14.6; replica coordination is not required for correctness |
| T-PAY-07 | payment.events events via the transactional outbox (4.7): emission in `Repository.TransitionTx` (the same transaction as the transition and the inbox Guard), the kit relay in the service process (FOR UPDATE SKIP LOCKED, batch 100, 250 ms, publication inside the DB transaction - kit T-KK5) | no event loss on failures (the DB transaction is primary), Kafka is not on the critical path, at-least-once, duplicates are deduplicated by `event_id` and by `state_version` monitoring (self-transitions are published) | with several replicas the event order between transactions is not guaranteed (consumers discard stale `state_version`); without a topic/broker rows accumulate, the relay retries (WARN at startup without Kafka); GORM logging of SQL values - common question 0.7 | outbox retention - 14.2 (KI-payment-02); outbox lag metrics - 0.7; emission is verified by unit/integration/e2e (Kafka), a sink failure rolls back the transition |
| T-PAY-08 | Reads (4.8) without authN: ListPayments trusts the `merchant_id` from the body, GetPayment returns a payment by UUID; ownership is checked by the caller (7.4 takes the merchant from the token), Get history is limited to 500 transitions | the BFF gets invoices and timeline without a cross-service DB; the limited response does not depend on the payment lifetime | `merchant_id`/UUID spoofing is not caught until mTLS (0.6, KI-payment-04): port 9030 is not published outside the trusted network, prod is forbidden; self-transition confirmations bloat the journal - full history 14.2 | KI-payment-04 is extended with the gRPC surface; e2e filters and boundaries are verified; list pagination is clamped (50/200) |
| T-PAY-09 | A repeated valid deposit (a confirmed canonical event above the baseline attributed to a terminal-success payment, 17.10 decision 9) is credited as a derived completed child payment linked by `parent_payment_id`; the baseline is the recorded amount plus the sum of the existing children, read under a `FOR UPDATE` lock of the parent row, so successive cumulative events credit only the new part and concurrent events serialize; the delta only, the parent row untouched; the insert guard allows completed rows only with an existing terminal-success parent, version 1 and `completed_at` (no self-parenting); dedup by the merchant top-up key and the `(parent_payment_id, txid)` unique index over the canonicalized txid | merchant money for a repeated deposit reaches the ledger through the ordinary `payment.completed` path; the parent evidence and its name at the provider stay untouched (no reversal); a duplicate delivery or a case-variant hex replay is a no-op | the child bypasses the creating..confirming chain (single audit transition `'' -> completed`); a deposit without a txid is not credited automatically (KI-payment-10); the delta is unbounded above, so a forged canonical event (KI-payment-04) can mint an arbitrarily large child - prod remains forbidden until broker ACL/mTLS (0.5/0.6), and 14.4 should alert on unusually large top-ups; the original partial of an underpaid parent stays for 17.12/17.13 | tests cover the happy path, successive cumulative deltas (50k and 30k), duplicates, case-variant replays, missing txid and the DB barriers (unit, integration, Kafka e2e); the top-up rules are confined to the consumer and the documented guard exception |
| T-PAY-10 | Fee split read-through (17.10, decision 18): canonical events carry the components only (cf-4.1), the total is their sum (`domain.FeeTotal`: non-negative, asset/scale match, not above received), stored on the payment and re-emitted in `payment.events` together with `provider`/`raw_status` | the ledger and reporting receive the provider split without a schema change; the aggregate `fee` field of `payment.events` is derived in one place | a provider that reports no split leaves the fee at zero (the ledger credits no fee); fee policy (who pays the network fee) is a business question deferred with KI-payment-06 | the validation fails closed (retry/DLQ) on inconsistent data; the split semantics are pinned by the integration tests and the provider sandbox runs 17.16 |
| T-PAY-11 | Creation (4.2): the row is inserted in `creating` with the request amount as a placeholder (the key is claimed atomically by the unique index), the quote is locked next and the single `creating -> created` transition fills the locked pay amount and the quote fields (migration 00007 relaxes the expected-field immutability for exactly that pair, keeping every other immutable field incl. the 00006 parent link), the connector call follows (`created -> pending`); retries resume the missing steps by the request hash (amounts, assets, network, pay scale, expiry; canonical merchant id), business rejections fail the payment closed with fixed reasons, and the call budgets are fixed (quote 500 ms, connector 5 s) | CBL-003 timeout semantics hold: a quote timeout leaves `creating`, an invoice timeout `created`, and a retry completes the flow without a second provider invoice (the connector replays by order id) | during `creating`/`created` the `expected_*` fields carry the request placeholder until the quote fills them; the connector replay returns the stored destination only since `34d570a` (older mappings replay without one); a crash orphans the quote lock (money-neutral, E-2); cancelling does not notify the provider, so a late deposit needs the manual-review WARN (KI-payment-08/09) | unit C-01..C-23, storage I-01..I-03, the testcontainers e2e `E2E_Payment_CreatePayment` and the 4.2 review round pin the behaviour |

## 9. Known issues

| ID | Problem | Impact | Plan | Status |
| :-- | :-- | :-- | :-- | :-- |
| KI-payment-01 | CI and pre-commit are not configured | linters run manually | task 0.2 | open |
| KI-payment-02 | No rotation and partitioning of `payments`, `payment_transitions`, `outbox`: the tables grow indefinitely (outbox - one row per transition, 4.7) | DB and backup growth; reads by PK/indexes are not affected | retention/partitioning - task 14.2 (like KI-ledger-09); until then cleaning published rows manually is not allowed (audit) | open |
| KI-payment-03 | Runtime and migrations run under the same DSN: the schema owner can drop the second-barrier triggers (`DISABLE TRIGGER`, `session_replication_role`, `DROP TRIGGER`) | in the dev circuit the second barrier is dropped by the DB owner; formally T-PAY-01 guarantees protection only from DML roles | separate the migrator (DDL) and runtime (DML without DDL) roles before going to prod (0.4/0.6); outside the dev stand threat model | open |
| KI-payment-04 | The topics `gateway.events.canonical` and `payment.events` are not authenticated: a participant with produce rights to canonical moves a payment to detected/confirming and (since 4.5) to terminal states, and since 4.7 a confirmed payment publishes `payment.completed`, which Ledger 2.7 idempotently posts with real money (Dr clearing / Cr merchant) without checking with the gateway; forging `payment.events` directly forges the posting (cross-reference KI-ledger-02). Since 17.10 a forged confirmed event on a terminal-success payment also mints a derived child with an arbitrarily large delta (T-PAY-09). Since 4.8 gRPC reads (9030) are without authN: `ListPayments` returns payments of any `merchant_id` from the body, `GetPayment` - any payment by UUID (pay_address, txid, amounts, timeline), reflection exposes the method catalog. Since 4.2 the write surface is live too: `CreatePayment` accepts any `merchant_id` and locks quotes and provider invoices (spam), `CancelPayment` cancels any payment by UUID without an ownership check | a leak of produce access = completion of arbitrary payments and movement of money (including unbounded top-up minting); access to port 9030 = a statement of others' deposit addresses and amounts, synthetic invoice creation and arbitrary cancellations; fixing already posted ones - only reversal (10.x), not event replay | broker ACL and mTLS: produce to canonical only by Connector, to `payment.events` only by cbl-payment (0.5/0.6); gRPC mTLS and ownership check (7.4 takes the merchant from the token, Get - ownership in 7.4) before mTLS 0.6; do not publish port 9030 outside the trusted network, disable reflection in prod; prod deployment is forbidden until then; re-check before completion - 4.4 (after KI-01); investigation of a suspicious `payment.completed` or top-up - runbook | open |
| KI-payment-05 | Too-early events rely on the nominal retry pauses 5s/1m/10m: since 4.6 a stuck creation is closed as `failed` after 5 minutes, so an event for it becomes stale instead of being retried indefinitely; since 4.2 a payment legitimately waits in `created` until the retry resumes the connector step, and an event past ~11 minutes still drives a valid event to the DLQ | confirmation delayed until manual replay | parking/replay of the DLQ in the runbook; the retry-budget revision and the `created`-without-movement watch remain open (candidate 9.x/14.x) | open |
| KI-payment-06 | payment `fee` is populated from the provider split since 17.10 but the platform-side fee rules (who bears the network fee, fee on top-ups) are not defined; confirmation thresholds are configured manually without a USD equivalent (the feed from Quote 6.x is not connected) | fee under-collection on top-ups; threshold drift as the rate moves requires manual revision | fee policy in 7.x and 17.12; automatic tier recalculation - when accounting requirements appear/14.x | open |
| KI-payment-07 | Monotonicity by `gateway_event_ts` is not implemented (was delegated to 3.5/T-CONNECTOR-05): an outdated event from the retry loop can regress `confirmations` or terminalize a payment after a newer one | rare false terminal statuses/counter rollback until manual investigation | comparing event time when storing `gateway_event_ts` (4.x) or status re-check 4.4 after KI-01 | open |
| KI-payment-08 | Race "`expired` committed before a late `detected`": the janitor re-reads the state via CAS before expiry (implemented in 4.6); a late `detected`/`confirmed` after `expired` or `cancelled` is marked by the consumer with a separate WARN `late canonical event after terminal state needs manual review` and the payment keeps its terminal state - the payment requires manual investigation/alerting | money on the network may remain unnoticed without log-based alerting | the alert and remediation rule - in 9.x; the related time comparison - KI-payment-07 | open |
| KI-payment-09 | A stuck creation is closed as `failed` without contacting the gateway (4.6): if 4.2 created an invoice but did not drive the payment to `pending`, the invoice remains at the gateway without a payment; cancelling a payment (4.2) leaves the provider invoice live as well | the gateway sees an invoice, the payment is failed or cancelled; a later deposit needs the manual-review WARN (KI-payment-08) | reconciliation (17.13) classifies such invoices; provider-side invoice cancellation needs a Connector RPC (a follow-up after the task 21 seam and the 17.16 runs) | open |
| KI-payment-10 | A repeated deposit without a txid (17.10) cannot be deduplicated against a redelivery: the consumer logs `topup deposit without txid needs manual review` and acknowledges without crediting | the deposit is not credited automatically; it needs manual investigation and a possible manual top-up | 17.13 reconciliation classifies such deposits from the provider exports; the alert on the WARN line - 14.4 | open |
| KI-payment-11 | A payment interrupted in `creating` (quote timeout or crash) exposes the request pricing amount in `expected_amount`/`expected_asset` as a placeholder until the `creating -> created` transition fills the locked pay amount; the window is bounded by the retry or the janitor (5m) | until the quote lands, `GetPayment`/the merchant invoice REST would render that placeholder as the pay amount if they ignore the state | the 7.4 merchant invoice must render `pay_amount`/`pay_asset` from `created` onward (or from the 7.4 response only when the payment is past `creating`); the placeholder is documented in the CBL-4.2 spec and T-PAY-11 | open |

## 10. Task journal

| CBL | Date | Status | Spec | Commit |
| :-- | :-- | :-- | :-- | :-- |
| CBL-4.1 | 2026-10-15 | done, review closed (payments+payment_transitions schema, transition matrix in the domain and DB, state_version with CAS, e2e of the full chain, coverage 83.5%) | `.specs/CBL-4.1-payment-schema.md` | acd3c57 |
| CBL-4.3 | 2026-10-15 | done, review closed (canonical event consumer in the Guard transaction, event plan with duplicate and order protection, e2e Kafka) | `.specs/CBL-4.3-canonical-consumer.md` | 2d5f263 |
| CBL-4.5 | 2026-10-15 | done, review closed (threshold policy per asset and amount, tolerances without float, auto-completion from the consumer, e2e) | `.specs/CBL-4.5-confirmation-policy.md` | 0a9d627 |
| CBL-4.6 | 2026-10-15 | done, review closed (janitor: invoice expiry, stuck creations to failed, watching detected/confirming, batched selections on partial indexes, e2e) | `.specs/CBL-4.6-background-janitor.md` | ccb2d79 |
| CBL-4.7 | 2026-10-15 | done, review closed (payment.events via outbox: emission in TransitionTx, CF-2 envelopes, relay in main, traceparent chain, e2e Kafka) | `.specs/CBL-4.7-payment-events-outbox.md` | eee1780 |
| CBL-4.8 | 2026-10-15 | done, review closed (gRPC reads: GetPayment with quote/txid/confirmations/timeline, ListPayments with merchant, state and period; migration 00005 fill-once; timeline cap 500; contracts cf-3.3/cf-3.4) | `.specs/CBL-4.8-invoice-read-api.md` | bc903bd |
| CBL-16.6 | 2026-10-16 | done (English comments and documentation across the repository: code, tests, migrations, configs, all six specs, AGENTS.md, README; e2e runs via `test/e2e/E2E.md` with scenario `E2E_Payment_Schema`, `schema.ps1` and `make e2e` removed; validation run PASS; coverage unchanged) | `.specs/CBL-16.6-conventions.md` | this commit |
| CBL-17.10 (increment 1, payment) | 2026-10-18 | done, review closed (provider rules: migration 00006 adds raw_status, checkout_url, the fee split columns and parent_payment_id; the derived-row insert exception requires an existing terminal-success parent and rejects self-parenting, the parent-txid unique index and the parent link immutable; the canonical consumer persists the raw status and derives the fee total from the components (domain.FeeTotal); repeated confirmed deposits are credited as derived completed children against the baseline (recorded amount plus the existing children) read under a FOR UPDATE lock of the parent row - successive cumulative events credit 50k then 30k, the parent stays untouched; the identity uses the canonicalized txid (domain.CanonicalTxID) and the narrowed top-up unique-violation mapping; a deposit without a txid is a manual-review WARN (KI-payment-10); the distinct `wrong asset deposit` DLQ reason; reads expose provider/raw_status/checkout_url; payment.events carries provider/raw_status/fee split; the first review round found a blocking double-credit on successive top-ups (B1) and a case-variant txid dedup gap (M1), both fixed and re-verified; unit+integration coverage 91.3%, Kafka e2e green with the successive/case-variant stages, full compose stand E2E_Payment_Schema PASS 26/26 (mark `20261018t125343z`, migrations v6); go mod tidy maintenance: a stale go.mod was fixed (testify/cpuid patch bumps); the quote and ledger increments land in their repositories) | `.specs/CBL-17.10-payment-provider-rules.md` | this commit |
| CBL-4.2 | 2026-10-19 | done, review closed (CreatePayment/CancelPayment: the creation service with the idempotent replay by (merchant, key) and the request hash (amount, assets, network, pay scale, expiry; canonical ids), the atomic key claim, the quote lock and the integer rate conversion (domain.ConvertRate), migration 00007 fills the locked pay amount only on creating -> created (keeping every other immutable field incl. the 00006 parent link), the connector invoice call (created -> pending), fixed failure reasons on business rejections, seed-driven timeout resume (quote -> creating, invoice -> created, retry completes), cancel before detection with idempotent replay, the quoteclient/connectorclient with the closed error taxonomy (rejected/temporary/overloaded) and call budgets, new envs QUOTE_GRPC_ADDR/CONNECTOR_GRPC_ADDR/PAYMENT_ASSET_DECIMALS with fail-closed wiring; the cbl-connector increment 34d570a stores the invoice destination so the replay of a lost response returns the address; reviews: 4 reviewers + closure, 3 blocking findings (00007 guard drift vs 00006, connector replay without a destination, expiry validation before the replay lookup) and the majors closed; unit C-01..C-23, storage I-01..I-03, the testcontainers e2e E2E_Payment_CreatePayment green; unit+integration coverage 92.1%; lint 0) | `.specs/CBL-4.2-create-payment.md` | this commit |
| CBL-0.9 | 2026-10-20 | done (cbl-kit and cbl-protos are pinned remote modules fetched from the GitHub origin; no local replace directives; Docker builds from the repository root; the lint container fetches the modules via GOPRIVATE and the git insteadOf rewrite) | `../../.specs/CBL-0.9-remote-modules.md` (umbrella) | this commit |

## 11. Runbook

Filled in by the first product task of the service. Typical incidents: dependency
unavailability, error growth, processing lag; actions: check health/metrics, logs with
request_id, Kafka consumer group state and outbox lag, restart the service if needed.

State machine (4.1):

1. `ErrStateConflict` in logs: a concurrent transition on one payment; the caller repeats
   the cycle with reading the fresh version (`Get`). Systematic cases - check whether
   canonical events are duplicated (inbox 4.3).
2. Payment state and history: `SELECT state, state_version FROM payments WHERE id = '...'`;
   the full transition path - `SELECT from_state, to_state, state_version, reason, occurred_at
   FROM payment_transitions WHERE payment_id = '...' ORDER BY state_version`.
3. Editing the state directly in the DB is impossible (trigger): fixing is only a valid
   transition through the application or a new record; `payment_transitions` is append-only.

Canonical events (4.3):

1. `gateway.events.canonical.dlq`: the reason is in the `x-error` header; typical ones - an
   invalid event (payment_id/amount/asset/decimals, requisites mismatch with the expected
   ones), an unknown payment, a too-early event, exhausted version conflicts. Retry after
   fixing the cause - manual replay from the DLQ (KI-payment-05).
2. Consumer lag `cbl-payment-canonical`: check logs `canonical event applied`,
   `stale canonical event skipped`, conflicts (`conflict_retry`), DB state.
3. An event arrived before `pending` (too early): the payment stays in creating/created for
   a long time; check 4.2/4.6, after the payment is moved to pending the event is retried
   from the retry topics.
4. Topic trust boundaries - KI-payment-04: producers must be from the trusted network only
   (ACL/mTLS - 0.5/0.6); since 4.5 a forged `confirmed` can complete the payment.

Completion policy (4.5):

1. Effective thresholds and tolerances are printed at startup (`payment amount policy
   configured`); during an incident verify the configuration of exactly this run.
2. `PAYMENT_CONFIRMATION_POLICY` changes only with a service restart; after editing
   the tiers check the startup log and a test payment.
3. The payment is completed by the consumer at `confirmations >= required` and the amount
   within tolerance; divergence with the gateway classification within tolerance is normal
   (T-PAY-05) - do not count it as a reconciliation mismatch.
4. Expiry is performed by 4.6 via `ShouldExpire`; a late detected after `expired` -
   manual investigation (KI-payment-08).

Background jobs (4.6):

1. Configuration is printed at startup (`payment janitor started`): interval,
   batch, `creating_timeout`, `stuck_timeout`; with `PAYMENT_JANITOR_INTERVAL=0` -
   `payment janitor disabled`. A typo in the values blocks startup instead of
   silently degrading.
2. Janitor transition reasons in the journal: `janitor expired` (created/pending ->
   expired) and `janitor creating timeout` (creating -> failed, by default after
   5m). Fresh creations are not touched; failing a stuck creation limits
   the window of too-early events (KI-payment-05).
3. `janitor: sweep finished` with non-zero counters - investigation: `conflicts` -
   a normal race of several replicas; growth of `errors` - look at the DB/logs;
   `stuck` > 0 - WARN `janitor: payment without movement` (a payment without movement
   longer than `PAYMENT_STUCK_TIMEOUT`, money on the network): manually check the
   gateway status (4.4 after KI-01), wait for reconciliation 9.x; the janitor
   does not terminalize detected/confirming automatically.
4. Backlog (many expired/stuck): passes go in batches, the number per pass -
   `PAYMENT_JANITOR_BATCH`; with a sustained backlog increase the batch/decrease the
   interval, check why the canonical event consumer is not working.
5. `late canonical event after terminal state needs manual review` (consumer 4.3, WARN):
   a deposit against an expired or cancelled invoice - money on the network, manual
   investigation (KI-payment-08/09).

payment.events events (4.7):

1. Outbox lag: `SELECT count(*), min(created_at) FROM outbox WHERE published_at IS
   NULL`; relay logs `outbox published batch` / `outbox publish batch failed`.
   Losses are impossible: unpublished rows wait for the topic/broker and go out
   in the next cycles; duplicates are possible (at-least-once, consumers deduplicate
   by `event_id`).
2. Publications do not happen without a topic: the topic `payment.events` must exist
   (`deploy/kafka/init-topics.sh` in the global stand, 0.5); without `KAFKA_BROKERS`
   the service writes `payment events accumulate in outbox` and accumulates rows.
3. Events of one payment are verified by `state_version`/`event_type`
   (`SELECT event_id, headers->>'event_type' FROM outbox WHERE "key" = '<payment_id>'
   ORDER BY id`); stale events are normal (transitions from different sources),
   retention - KI-payment-02 (14.2).
4. A suspicious `payment.completed` (KI-payment-04): confirm the legitimacy of the
   canonical event/payment before the posting; if the posting has already happened -
   fixing is only reversal (10.x), not event repetition; the incident - with an
   investigation of produce access to the topics.

Reads (4.8):

1. `payment_not_found` (NotFound) on GetPayment: the UUID is correct but the payment
   does not exist (or has not been created yet by 4.2).
2. `invalid_query`/`invalid_period`/`invalid_cursor` (InvalidArgument):
   an empty/long `merchant_id`, an unknown `state_filter`, period boundaries
   (to not later than from, a mark outside the protobuf range), a cursor not from
   `next_cursor`.
3. An empty ListPayments list - normal for a foreign/unknown merchant
   (comparison with the expected `merchant_id` from the token is 7.4's responsibility).
4. GetPayment returns the last 500 transitions: a truncated timeline on a
   long-lived non-terminal payment is expected (full history - 14.2;
   journal growth from self-transition confirmations - KI-payment-07).
5. Direct access to 9030 outside the trusted network is forbidden (KI-payment-04):
   a statement of another merchant by spoofing `merchant_id`; mTLS/ownership - 7.4, 0.6.

Creation (4.2):

1. `CreatePayment` classes: `invalid_*` (InvalidArgument) - malformed input or an
   expired/over-horizon new request; `idempotency_key_reuse` (AlreadyExists) -
   the key is bound to a different request (amount, assets, network, pay scale or
   expiry); `quote_rejected`/`invoice_rejected` (FailedPrecondition) - the payment
   is `failed` with the fixed reason and a NEW key is required; `service_unavailable`
   (Unavailable) - retry with the same key resumes (a quote timeout leaves the
   payment in `creating`, an invoice timeout in `created`, both by design, CBL-003).
2. A payment stuck in `creating` beyond `PAYMENT_CREATING_TIMEOUT` is failed by the
   janitor (reason `janitor creating timeout`); a payment in `created` waits for a
   retry and expires by its own `expires_at`. Check
   `SELECT state, state_version, expected_amount, expected_asset, quote_id, gateway_id FROM payments WHERE id = '<id>'`
   (KI-payment-09 for an invoice left at the provider).
3. A merchant cancel (CancelPayment) transitions `creating`/`created`/`pending` to
   `cancelled` idempotently; after detection it answers `invoice_not_cancellable`.
   Cancelling does NOT cancel the provider invoice: a later deposit on the address
   is caught by the late-event WARN (KI-payment-08/09).
4. `PAYMENT_ASSET_DECIMALS` must cover every pay asset the BFF requests; an
   unknown asset is a request error, and the 900 s quote-lock cap must stay within
   the deployed `LOCK_QUOTE_MAX_TTL` (a lower quote cap surfaces as
   `quote_rejected`).

Provider rules (17.10):

1. Repeated deposits: logs `topup deposit credited` (parent, child, delta, txid);
   the delta is measured against the baseline (the recorded amount plus the
   existing children, under the parent row lock), so a later cumulative event
   credits only the part above everything already credited; a duplicate delivery
   is `topup duplicate skipped`; a deposit without a txid is
   `topup deposit without txid needs manual review` and is NOT credited
   (KI-payment-10); check the children with
   `SELECT id, state, received_amount, txid FROM payments WHERE parent_payment_id = '<parent>' ORDER BY created_at`.
2. A `wrong asset deposit` in the `x-error` of the canonical DLQ means the event
   received asset/scale does not match the payment: the funds stay at the
   provider and the case goes to reconciliation (17.13); do not replay it as a
   top-up.
3. Fee split: `SELECT fee_amount, gateway_fee, network_fee, fx_spread FROM
   payments WHERE id = '<id>'`; the total follows the components (T-PAY-10); a
   provider without a split leaves the fee at zero.
4. Migration 00006 allows a completed insert only for parented rows; a manual SQL
   repair of a payment must follow the transition matrix - the guard exception
   exists only for the derived insert path.
