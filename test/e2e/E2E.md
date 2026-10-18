# E2E cbl-payment: payment schema, state machine, janitor and read API

Run instruction for the local e2e scenario of the service. A run is performed by an
agent platform-independently: docker, a command shell, psql (inside the stand) and
grpcurl (in a container). There are no PowerShell run scripts (customer decision of
16.10.2026, `CBL-002` section 7).

Repository: `cbl-payment`. Stand: `deploy/docker-compose.yml` (postgres, kafka,
schema-registry, payment in the `app` profile with a fast janitor,
`PAYMENT_JANITOR_INTERVAL=2s`).

## 1. Scenarios

| Scenario | Checks | Contracts |
| :-- | :-- | :-- |
| `E2E_Payment_Schema` | goose migrations to version 6, the full state chain creating -> created -> pending -> detected -> confirming -> completed with a per-step transition journal, the DB barriers (forbidden backward transition, append-only transition journal, the derived top-up insert barrier: a completed row without a parent is rejected), janitor expiry and stuck-creation handling (expired / failed / detected intact), outbox rows payment.expired/payment.failed published by the relay, and the gRPC read API (GetPayment quote/txid/confirmations/timeline, ListPayments merchant/state/period filters, NotFound). The repeated-deposit top-up flow itself is exercised by the Kafka testcontainers scenario `E2E_Payment_CanonicalKafka` (successive cumulative deposits crediting against the baseline, a case-variant txid replay folded) | payment schema and state machine (CBL-4.1), janitor (CBL-4.6), outbox (CBL-4.7), read API (CBL-4.8), provider rules (CBL-17.10); gRPC PaymentService (CF-3.3/cf-3.4) |
| `E2E_Payment_CanonicalKafka` | testcontainers Kafka+PostgreSQL (`make test-e2e`): canonical events over the topic drive the state machine, duplicates are deduplicated by the inbox; since 17.10 a repeated confirmed deposit above the recorded amount (new txid) creates a linked completed top-up child, the parent stays untouched and the child `payment.completed` reaches `payment.events` | canonical consumer (CBL-4.3/4.5), provider rules (CBL-17.10); CF-2/CF-4 events |

## 2. Isolation and parallelism

- Run mark: UTC lowercase `yyyymmddthhmmssz` (uppercase is rejected by compose project
  naming rules).
- Artifact directory: `test/e2e/.runs/<mark>/` (create before the start).
- Compose project: `cbl-payment-e2e-<mark>` (the compose file declares
  `name: cbl-payment`; the `-p` flag overrides it).
- The payment container in the project is `cbl-payment-e2e-<mark>-payment-1`.
- Host ports 5432, 9092, 8081, 8030, 9030, 9230 are published; do not run two payment
  stands in parallel and stop other payment stands first.
- All commands run from the repository root.

## 3. Common run order

1. `<MARK>` = current UTC time in lowercase; create `test/e2e/.runs/<MARK>/`.
2. Stop the previous stand: `docker compose -p cbl-payment-e2e-<MARK> -f deploy/docker-compose.yml --profile app down -v`
   (output to `compose-down.log`; the first run has nothing to stop).
3. Start the stand: `docker compose -p cbl-payment-e2e-<MARK> -f deploy/docker-compose.yml --profile app up -d --build --wait`
   (`compose-up.log`); `docker compose ps` into `compose-ps.txt`.
4. Wait for migrations to version 6 (retry up to 60 s):
   `docker compose -p ... exec -T postgres psql -U cbl -d payment -v ON_ERROR_STOP=1 -t -A -c "SELECT max(version_id) FROM goose_db_version"`
   must print `6`.
5. Create the events topic (topic auto-creation is disabled):
   `docker compose -p ... exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic payment.events --partitions 1 --replication-factor 1`.
6. Run the scenario steps (section 4), appending every SQL transcript to `schema.log`
   and every OK/FAIL line to `schema-checks.txt`.
7. Capture logs: `docker compose -p ... logs --no-color > compose-logs.txt`.
8. `summary.md` by the `.additionals/summary.template.md` template; stop the stand:
   `down -v` (append to `compose-down.log`).

Utility notes:

- psql helper: `docker compose -p ... exec -T postgres psql -U cbl -d payment -v ON_ERROR_STOP=1 -t -A -c "<SQL>"`;
  output is trimmed. For expected failures run without `-v ON_ERROR_STOP=1` and check
  the exit code and the error text in stderr.
- Multi-statement SQL for janitor seeds must be sent as ONE `-c "stmt1; stmt2; ..."`
  (a single implicit transaction), so the 2 s janitor cannot interfere mid-chain.
- grpcurl helper (JSON body via stdin, the stand network of the payment container):
  `echo <JSON> | docker run -i --rm --network container:cbl-payment-e2e-<MARK>-payment-1 fullstorydev/grpcurl:v1.9.1 -plaintext -max-time 10 -d @ localhost:9030 billing.payment.v1.PaymentService/<Method>`
  (a successful call exits 0; errors print the status code to stderr).

## 4. E2E_Payment_Schema

1. Common run order, steps 1-5.
2. Seed a payment:
   `INSERT INTO payments (id, merchant_id, idempotency_key, request_hash, state, expected_amount, expected_asset, expected_decimals, network) VALUES ('<id>', 'm-e2e', 'key-e2e', 'hash-e2e', 'creating', 1000, 'USDT', 2, 'trc20')`
   plus the initial `payment_transitions` row (`'' -> creating`, version 1); the id is a
   fresh UUID. Quote fields (`quote_id` = `q-e2e`, `rate` = `78.125`,
   `quote_locked_until` = `now() + interval '15 minutes'`) are set on the
   creating -> created step (fill-once, 4.8).
3. Walk the full chain step by step (UPDATE payments SET state, state_version, extras;
   INSERT payment_transitions row per step): creating -> created (v2) -> pending (v3)
   -> detected (v4, txid `tx-e2e`, received amount 1000 USDT/2) -> confirming (v5,
   confirmations 1) -> completed (v6, confirmations 6, completed_at now()).
   Expect `state:version` = `completed:6` and exactly 6 transitions.
4. DB barrier checks (all three must be REJECTED):
   - backward transition: `UPDATE payments SET state = 'pending', state_version = 7 WHERE id = '<id>'`
     fails with `forbidden` (transition guard trigger);
   - journal rewrite: `UPDATE payment_transitions SET reason = 'rewritten' WHERE payment_id = '<id>'`
     fails with `append-only`;
   - derived top-up insert (`17.10`): `INSERT INTO payments (... state 'completed', state_version 1,
     completed_at now())` without `parent_payment_id` fails with `must start in creating`
     (insert guard).
5. Janitor seeds (three payments, each chain in a single `-c` call):
   - expired: chain to `pending` with `expires_at = now() - interval '1 minute'`;
   - stuck creation: state `creating` with `created_at`/`updated_at` 10 minutes in the
     past (backdating is allowed on INSERT);
   - detected: chain to `detected` with an already expired `expires_at` (must stay
     untouched, KI-payment-08).
   Then poll up to 60 s: the expired payment becomes `expired` with reason
   `janitor expired`; the stuck one becomes `failed` with reason
   `janitor creating timeout`; the detected one stays `detected:4`.
6. Outbox (4.7): exactly one `outbox` row with topic `payment.events` and
   `headers->>'event_type'` = `payment.expired` for the expired id, and one with
   `payment.failed` for the stuck id; then poll up to 30 s until
   `SELECT count(*) FROM outbox WHERE published_at IS NULL` = 0 (the relay published
   through the created topic).
7. gRPC reads (4.8), using the seeded completed payment and merchant `m-e2e`:
   - `GetPayment` of the completed id: `rate` = `78.125`, non-empty `lockedUntil`,
     `txid` = `tx-e2e`, `confirmations` = 6, timeline length 6;
   - `ListPayments` with `merchantId` `m-e2e` and state filter
     `PAYMENT_STATE_COMPLETED`: exactly one entry, the seeded id;
   - `ListPayments` with `createdFrom` `2100-01-01T00:00:00Z`: empty list;
   - `GetPayment` of a fresh random UUID: NotFound.
8. Every step records an OK/FAIL line in `schema-checks.txt` (check name, expected,
   actual).

## 5. Mandatory run artifacts

- `summary.md` (template `.additionals/summary.template.md`; PASS/FAIL verdict, steps,
  key values, artifact list, time);
- `compose-ps.txt`, `compose-up.log`, `compose-down.log`, `compose-logs.txt`;
- `schema.log` (all SQL and grpcurl transcripts) and `schema-checks.txt`;
- on failure: container states, the payment container log fragments (janitor, outbox
  relay, gRPC errors), the failing SQL and its raw output.

## 6. Auxiliary files (.additionals)

- `summary.template.md` - the `summary.md` template;
- `inventory.md` - the list of auxiliary files and their purpose.

The stand-side e2e tests on testcontainers (`make test-e2e`,
`test/e2e/canonical_kafka_test.go`) are a separate local check and do not use the
compose stand.

## 7. Cleanup

`docker compose -p cbl-payment-e2e-<MARK> -f deploy/docker-compose.yml --profile app down -v`
at the end; `.runs/` is not committed; a repeated run must start from a clean stand.
