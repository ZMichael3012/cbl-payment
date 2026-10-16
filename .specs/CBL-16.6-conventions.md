# CBL-16.6. cbl-payment: English comments and documentation; e2e via E2E.md

| Field | Value |
| :-- | :-- |
| Epic | E16, `Convention alignments` |
| Repository | `cbl-payment` |
| Size | M |
| Wave and parallel group | W0-CORR |
| Depends on | `CBL-16.1` (convention), `CBL-16.2` (pilot pattern) |
| Contract version | not applicable (no contract changes) |
| Status | done, review closed |

## 1. Context and goal

Two cross-cutting customer corrections (16.10.2026): (1) comments and documentation are
written in English in all files (code, tests, migrations, specs, AGENTS.md, README,
configurations; log and error messages stay unchanged as executable contracts);
(2) e2e runs are not PowerShell scripts but an agent instruction `test/e2e/E2E.md` with
run artifacts in `test/e2e/.runs/`. The convention is `CBL-002` section 7.

## 2. Scope

In scope: English translation of all comments, docstrings, migration SQL comments,
Dockerfile/compose comments, all six specs (CBL-4.1/4.3/4.5/4.6/4.7/4.8), AGENTS.md and
README.md; `test/e2e/E2E.md` (scenario `E2E_Payment_Schema` covering the whole former
script behavior: migrations v5, the full state chain with the journal, DB barriers,
janitor expiry/stuck handling, outbox publication, gRPC reads); removal of
`test/e2e/schema.ps1` and the `make e2e` target; Makefile/.gitignore updates.

Out of scope: changing behavior and runtime messages (gRPC status texts built by
`grpcmw.E` in `internal/transport/grpcserver/payment.go`, zap messages), the separate
testcontainers check (`make test-e2e`, `test/e2e/canonical_kafka_test.go`), other
repositories (16.7 - 16.16).

## 3. Requirements

FR-1. Language: no Russian comments or docstrings remain; Russian string literals remain
only as runtime messages (gRPC status texts, janitor log fragments quoted in specs as
contracts); test failure descriptions are English; godoc docstrings keep the entity-name
first line and end with a period; AGENTS.md development rules state the English
documentation rule.

FR-2. E2E: `test/e2e/E2E.md` contains preconditions, isolation (compose project per
lowercase UTC mark, fixed host ports, payment container name), the run order (stand up,
migrations to version 5, topic creation), the scenario with the full check list
(chain completed:6 with 6 transitions, forbidden backward transition, append-only
rewrite, janitor expired/failed/detected intact, outbox expired+failed rows, relay
publication, GetPayment quote/txid/confirmations/timeline, ListPayments filters,
NotFound), mandatory artifacts and cleanup; there are no PowerShell run scripts;
AGENTS.md references the instruction.

FR-3. Auxiliary files live in `test/e2e/.additionals/` (summary template, inventory);
`.runs/` is gitignored.

FR-4. Validation run: the scenario executed per `E2E.md`, artifacts in
`test/e2e/.runs/<mark>/`, `summary.md` filled with a PASS verdict.

## 4. Plan

Comment and documentation translation (parallel by file groups: cmd/domain/usecase,
storage/transport, migrations/configs/e2e/README, specs, AGENTS.md) -> E2E.md and
`.additionals` -> removal of the ps1 script and the e2e target -> validation run ->
4 reviews -> commit `CBL-16.6: English comments and docs; e2e via E2E.md`.

## 5. Reviews and runs

Verified: unit+integration tests green (no production code changes), lint 0, the e2e
scenario run per `E2E.md` with run artifacts, em-dash check clean, comment language
verified by a Cyrillic scan (remaining Cyrillic: gRPC status message literals in
`internal/transport/grpcserver/payment.go`; none are comments).
