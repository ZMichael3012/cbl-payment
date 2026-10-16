# CBL-4.8. Payment gRPC reads for the merchant invoice

| Field | Value |
| :-- | :-- |
| Epic | E4 (Payment) |
| Repository | `cbl-payment` (+ contract `cbl-protos@cf-3.3`) |
| Size | S |
| Wave | W3-BILLING (prerequisite of 7.4, FR-8 of the CBL-7.1 spec) |
| Depends on | 4.1 (schema), CF-3, tag `cbl-protos@cf-3.3` |
| Status | done, review closed |

## 1. Context and goal

The 7.1 review showed: the merchant REST invoice (`quote{rate, locked_until}`, list
filters, `pay_amount`/`pay_asset`) had no source in Payment gRPC: the `Payment`
message did not carry the rate and the lock period, `ListPayments` did not carry the
period, and the reads themselves (`GetPayment`/`ListPayments`) were not implemented
at all. 4.8 adds additive contract fields (`cbl-protos` `CBL-4.8`, tag `cf-3.3`) and
implements the reads over the 4.1 schema. `CreatePayment`/`CancelPayment` remain in
4.2 (blocked by KI-01, the gateway): 7.4 starts after 4.2, 2.10 and 4.8.

## 2. Boundaries

In scope: the gRPC server PaymentService (`GetPayment`, `ListPayments`), quote
fields, the list period, the `quote_locked_until` migration. Out of scope:
`CreatePayment`/`CancelPayment` (4.2), fixing the quote at creation (4.2 fills
`rate`/`locked_until`), re-checking the gateway status (4.4), Billing API REST
(7.4).

## 3. Contract (cf-3.3 and cf-3.4)

- `Payment` + `rate = 13`, `locked_until = 14` (cf-3.3), `txid = 15`,
  `confirmations = 16` (cf-3.4); `CreatePaymentResponse` + `rate = 6`,
  `locked_until = 7` (cf-3.3), `expected_amount = 8` (cf-3.4 - the source of
  `pay_amount`/`pay_asset` in the 201 response).
- `ListPaymentsRequest` + `created_from = 5`, `created_to = 6` (by the transaction
  `created_at`: from inclusive, to exclusive); the cursor is the opaque
  `next_cursor` value of the previous response.
- `pay_amount`/`pay_asset` of the reads are derived from `expected_amount` (`Money`
  with an asset); `fee` is not exposed in `Payment` (fee rules - KI-payment-06,
  there is no separate REST fee field in the invoice - the webhook field is fed by
  `payment.events`).

## 4. Functional requirements

FR-1. `GetPayment`: the payment by `payment_id` (UUID) with the transition history
(`timeline`: state, time, reason) and quote fields (`quote_id`, `rate`,
`locked_until`), amounts (`expected_amount`, `received_amount`), address, network,
`txid`, `expires_at`; an unknown id - `NotFound` (`payment_not_found`).

FR-2. `ListPayments`: mandatory `merchant_id` (the owner from the token in 7.4);
filters `state_filter` (UNSPECIFIED - no filter, an unknown value -
`InvalidArgument`), `created_from`/`created_to` (inclusive and exclusive
boundaries), the cursor `payment_id` of the previous page (order
`created_at DESC, id DESC`), limit 1 - 200 (default 50); the timeline is not
filled in the list.

FR-3. Storage: `ListFilter` is extended with the period; a direct `INSERT` in
tests allows backdating `created_at` (immutable for UPDATE only); an invalid
cursor - `ErrInvalidCursor` (sentinel), an unknown payment - `ErrNotFound`.

FR-4. Migration 00005: column `quote_locked_until timestamptz`; the trigger
`payments_transition_guard` is recreated with a "fill-once" rule for the quote
fields (`quote_id`, `rate`, `quote_locked_until`): while a value is NULL, a
transition may fill it; after it is filled, modification is forbidden (the
migration is forward-compatible, Down restores the previous body).

FR-5. Transport: validation of untrusted input before storage (UUID, mandatory
`merchant_id` and its length, enum value, `Timestamp` validity via `CheckValid`,
cursor length, boundary order); the server is registered in main with
`grpcmw.UnaryServer`; `CreatePayment`/`CancelPayment` - `Unimplemented` until 4.2.

FR-6. Tests: unit - validation, state mapping (11 values + unknown),
`mapPaymentError`; integration storage - quote round-trip, fill-once forbids
overwriting and allows late filling, period/cursor/merchant isolation;
integration bufconn - `GetPayment` (rate/locked_until/timeline) and `ListPayments`
(filters, pagination, boundaries), `NotFound`; e2e compose - grpcurl checks of the
reads against the real server.

## 5. Checks

| Check | Command | Result |
| :-- | :-- | :-- |
| build/vet | `go build ./...`, `go vet ./...`, `go vet -tags "integration e2e" ./...` | clean |
| unit | `go test ./internal/... -count=1` | green |
| integration | `go test -tags integration ./internal/...` | green (quote, fill-once, period, history cap, limit clamp, bufconn) |
| e2e compose | run instruction `test/e2e/E2E.md`, scenario E2E_Payment_Schema (migrations 5, grpcurl rate/locked_until/txid/confirmations/filters) | green |
| e2e Kafka | `make test-e2e` (canonical consumer) | green |
| coverage | `-coverpkg` over internal | 91.7% (was 90.x) |
| lint/em-dash | golangci-lint in Docker; `check-no-emdash.ps1` | 0 issues; clean |

## 6. Non-functional requirements

Reads use only the existing 4.1 indexes: the `payments` PK for Get and
`payments_merchant_created_idx (merchant_id, created_at DESC, id DESC)` for listing
with a period; no new indexes are required; deadline 1 s (CBL-000, section 3); the
gRPC port is not published outside the trusted network (KI-payment-04); monetary
values without float.

## 7. Edge cases

A cursor from another query is accepted (filter `(created_at, id) <`); a bare
`payment_id` never appears in the cursor - the contract describes an opaque
`next_cursor`; `created_to` equal to `created_from` - `InvalidArgument` (empty
period); a record exactly on the `to` boundary is not included; a payment without
a quote (creation did not reach `created`) is returned without
`rate`/`locked_until`; terminal states are immutable (4.1); a foreign/nonexistent
`merchant_id` - an empty list (not 404); `limit` greater than 200 is clamped; the
Get history is limited to the last 500 transitions (an unbounded timeline of a
non-terminal payment is a response growth vector, KI-payment-07/02: full history -
14.2); invalid UUIDs are normalized (`parsed.String()`) before the DB query.

## 8. Review (CBL-001, step 6)

Four independent passes (15.10.2026). BLOCKER findings closed by the additive
`cbl-protos@cf-3.4` release: `txid`/`confirmations` in `Payment` (REST invoice 7.4
without a source) and `expected_amount` in `CreatePaymentResponse` (201
completeness); the cursor description was also fixed to the opaque `next_cursor`.
MAJOR findings closed: the timeline limit in `Repository.Get` (the last 500
transitions - protection against an unbounded response for a long-lived
non-terminal payment); the read trust model fixed in T-PAY-08 and the extended
KI-payment-04 (gRPC without authN: `merchant_id` from the body, Get by UUID,
reflection; do not publish the port outside the trusted network, 7.4 takes the
merchant from the token); the transport -> usecase -> storage boundary preserved,
the layer does not let storage calls through directly; UUID normalization cuts off
`urn:uuid` forms before the Postgres cast. MINOR/NIT findings closed:
`usecase.PaymentQuery` without a dead log; `%w` in decodeCursor; the migration
Down verbatim repeats the 00001 body; the e2e container moved to a variable; tests
for an invalid/equal period, an empty list of a foreign merchant, nil mapping
paths, the timeline cap, the limit clamp, a cursor from another query; asserts of
`txid`/`confirmations`/`received_amount`/comment in integration and e2e.
Accepted deliberately: `fee` is not in the contract (KI-payment-06; there is no
REST fee field in the invoice); the REST timeline does not deduplicate
self-transitions - deduplication is determined by 7.4; `CreatePayment`/`CancelPayment`
remain `Unimplemented` until 4.2 (the 4.2 scope in CBL-004 is extended with
CancelPayment).

| Role | Key findings | Closure |
| :-- | :-- | :-- |
| No context | BLOCKER: txid is promised by FR-1 but is absent in the contract; MINOR/NIT: cursor in the contract - payment_id vs base64; soft UUID validation; no tests of edge cases (a cursor from another query, equal boundaries, the clamp, nil paths, comment/received), fragile sleep | cf-3.4 + mapping; the contract comment fixed; UUID normalization; tests added; seed via the state machine/asserts |
| Architecture | BLOCKER: no txid/confirmations and expected_amount for 201; MAJOR: the invoice field projection is not broken down, the layer boundary is violated, documents are not synchronized; MINOR: CancelPayment without an owner, the fee wording, timeline semantics, fail-closed states, AGENTS | cf-3.4; §9 of the spec (7.4 projection: Billing DB - amount/currency/meta, Payment - state/amounts/network); usecase.PaymentQuery without storage types in the transport... the filter is already usecase; CBL-003/000/004 + protos follow-up; CBL-004 4.2 + Cancel; wording; fixed in 7.4; stateFromProto fail-closed (empty -> InvalidArgument for the filter, UNSPECIFIED is not exposed outward); AGENTS |
| With context | BLOCKER: txid; MAJOR: cursor, completeness of updates; MINOR: log, %w, Down comment, e2e container; NIT: dependencies, §5/§8 | cf-3.4; fixed; docs+follow-up; removed; %w; added; extracted; filled |
| Security | MAJOR: IDOR of the read perimeter + the KI reference is wrong; timeline without a limit; MINOR: cursor oracle, rate without CHECK; NIT: reflection, port publication, UUID normalization, grpcurl image | T-PAY-08 + KI-payment-04 extended; cap 500; no oracle (opaque); CHECK rate - 4.2 (validates the rate); documented in KI; normalized; image tag as in 10.x |

## 9. Decisions and open questions

- Payment `fee` - KI-payment-06 (fee rules 7.x): there is no separate REST fee
  field in the invoice; the webhook field is fed by `payment.events` (CF-2).
- `pay_amount`/`pay_asset` are derived from `expected_amount`; the 201 response
  receives `expected_amount` in `CreatePaymentResponse` (cf-3.4) - a separate
  GetPayment call on the hot path is not needed.
- 7.4 invoice projection: `amount`/`currency`, `merchant_order_id`, `metadata`,
  `checkout_url` and the filter by `merchant_order_id` live in the Billing API DB;
  Payment is the source of state, amounts, quote, network and timeline; the list
  cursor is by Payment (`created_at, id`), the projection is joined by `payment_id`.
- KI-payment-04 (no authN on gRPC): ListPayments accepts `merchant_id` from the
  body, GetPayment - by UUID; 7.4 must take the merchant from the token; until
  mTLS (0.6) port 9030 is not published outside the trusted network; reflection is
  a dev convenience.
- REST timeline: self-transitions (confirmation updates) get into the history;
  deduplication/filtering is a 7.4 decision; full history - 14.2.
- `rate` without a CHECK in the DB: validation is the writer's duty (4.2).

## 10. Work plan and commit

Implementation of storage/usecase/transport, migration 00005, tests, AGENTS/runbook,
commit `CBL-4.8: Payment gRPC reads for the merchant invoice`; the contract commit
has already been released (`cbl-protos` `CBL-4.8`, tag `cf-3.3`).
