# CBL-4.2. CreatePayment and CancelPayment (quote lock, Connector call, timeout handling)

| Field | Value |
| :-- | :-- |
| Epic | E4 (payment service) |
| Repository | `cbl-payment` |
| Size | L |
| Wave and parallel group | W5-PROVIDER |
| Depends on | 4.1 (schema and state machine), 3.1 (Connector SPI), 6.2 (LockQuote), 17.2 (provider core), cf-4.2 |
| Contract version | `cbl-protos@cf-4.2` (consumed; no change) |
| Status | done |

## 1. Context and goal

`CreatePayment` was deferred in 4.1/4.8 (KI-01, the gateway choice) and is the
last missing money-path RPC of the payment service: the merchant invoice flow
(7.4, milestone M1) needs an invoice created from a locked quote with a real
provider invoice address or checkout URL. This task implements `CreatePayment`
and `CancelPayment` end to end: validation of untrusted input, the idempotent
replay by `(merchant_id, idempotency_key)`, the quote lock, the Connector
`CreateInvoice` call, the `creating -> created -> pending` progression and the
retry semantics of CBL-003 (a retry after a timeout resumes the flow instead of
creating a second invoice).

## 2. Scope

In scope: the `usecase` creation service, the quote and connector gRPC clients,
`storage.Repository.ByIdempotencyKey`, the gRPC handlers with validation and
error mapping, the rate-to-amount conversion, envs and compose wiring, tests
(unit, integration, e2e), docs.

Out of scope: status re-check before completion (4.4), merchant REST (7.4/7.6),
under/over refunds, cancel-side provider notification (the connector invoice
stays at the provider; the mapping and the janitor own the residual risk,
KI-payment-09), authN/authZ (0.6, KI-payment-04).

## 3. Architecture sketch

Layers: `transport` (gRPC handlers) -> `usecase.PaymentCreation` -> `domain`
(conversion and errors) -> `storage`; the quote and connector calls go through
new client adapters (`internal/quoteclient`, `internal/connectorclient`)
following the payout ledger-client pattern (insecure dial on the trusted
network, per-class budgets, error classification).

Ports (usecase side):

```go
type CreationStore interface {
    Create(ctx context.Context, payment storage.Payment) error
    ByIdempotencyKey(ctx context.Context, merchantID, key string) (storage.Payment, error)
    Payment(ctx context.Context, id string) (storage.Payment, error)
    Transition(ctx context.Context, id string, expectedVersion uint64, to domain.State, patch storage.TransitionPatch) (storage.Payment, error)
}
type QuoteLocker interface {
    LockQuote(ctx context.Context, req QuoteLockRequest) (QuoteLock, error)
}
type InvoiceCreator interface {
    CreateInvoice(ctx context.Context, req InvoiceRequest) (InvoiceResult, error)
}
```

`CreatePayment` flow (one handler call, 6 s contract budget):

1. Validate: `merchant_id` UUID; `idempotency_key` non-empty and bounded;
   `amount` positive, decimals within the kit bound, asset valid (uppercase
   `[A-Z0-9]{2,16}`); `pay_asset` valid; `network` bounded; `expires_at`
   present, in the future and within the 7-day horizon.
2. Compute the request hash (canonical merchant id, pricing amount and asset,
   pay asset, network, pay scale, expiry) and look up `(merchant_id,
   idempotency_key)`:
   - a row with the same hash resumes the flow below from its current state
     (idempotent replay returns the previous result, even after the expiry);
   - a row with a different hash is `AlreadyExists` (`idempotency_key_reuse`).
3. Insert the payment in `creating` (the unique index claims the key
   atomically; `request_hash` and `expires_at` are fixed here; the
   `expected_*` placeholder carries the requested pricing amount until the
   quote fills the real pay amount at `creating -> created`; see KI-payment-11).
   A unique violation from a concurrent retry re-reads the row and resumes.
4. Lock the quote (`LockQuote(from_asset = amount.Asset, to_asset = pay_asset,
   amount, ttl)`; TTL = the remaining time to the expiry clamped to [60, 900]
   seconds, 500 ms budget). A business rejection (invalid pair, amount below the
   configured minimum) fails the payment (`creating -> failed`, reason `quote
   rejected`) and returns `FailedPrecondition`; a temporary failure leaves the
   payment in `creating` and returns the retryable class.
5. Convert the pay amount: `expected = amount * rate` with integer math
   (floor, no float), then `creating -> created` fills `quote_id`, `rate`,
   `quote_locked_until` and the real `expected_*` (reason `quote locked`).
6. Call the Connector `CreateInvoice(payment_id, expected amount, pay_asset,
   network, expires_at)` (5 s class budget). A business rejection fails the
   payment (`created -> failed`, reason `invoice rejected`) and returns
   `FailedPrecondition`; a temporary failure or timeout leaves the payment in
   `created` and returns the retryable class: the retry resumes at this step
   and the connector replays the same provider invoice including the stored
   destination (its own order-id idempotency, `cbl-connector@34d570a`).
7. On success `created -> pending` fills `gateway_id`, `provider`,
   `pay_address`, `checkout_url` (reason `invoice created`) and the response
   carries `payment_id`, `state`, `pay_address`, `checkout_url`, `quote_id`,
   `rate`, `locked_until`, `expected_amount`, `expires_at`.

A payment already in `pending` or later (or in a terminal state) replays as
is; the handler never drives a second provider invoice.

`CancelPayment` flow: validate; load the payment (missing ->
`payment_not_found`); `cancelled` replays as is; `creating`/`created`/`pending`
transition to `cancelled` (reason `cancelled by merchant`, CAS retry loop);
every other state is `FailedPrecondition` (`invoice_not_cancellable`).

## 4. Functional requirements

FR-1. A first `CreatePayment` with a valid request creates exactly one
payment and one provider invoice: the response carries the locked quote, the
pay amount (asset and minor units) and the address or the checkout URL; the
payment ends in `pending`.

FR-2. A repeated `CreatePayment` with the same `(merchant_id,
idempotency_key)` and the same request (hash over the canonical merchant id,
amount, pricing asset, pay asset, network, pay scale and expiry) returns the
previous result without a second provider invoice, whatever the interruption
point and even after its own expiry; from `creating` the flow may lock a new
quote (the first lock did not complete, E-2) and then replays the connector step,
which returns the same provider invoice; a different request under the same key
is `AlreadyExists`.

FR-3. CBL-003 timeout semantics: a quote timeout leaves the payment in
`creating`; an invoice timeout leaves it in `created` (the quote is locked,
the address is expected); both return the retryable gRPC class and the retry
completes the still-missing steps.

FR-4. Business rejections fail closed: a quote or invoice business rejection
drives the payment to `failed` with a fixed reason and returns
`FailedPrecondition`; no retry can revive a failed creation (a new key is
required).

FR-5. `CancelPayment` cancels a payment in `creating`, `created` or `pending`
exactly once (CAS), is idempotent on `cancelled`, and is
`FailedPrecondition` (`invoice_not_cancellable`) after detection. The
idempotency key is validated for contract symmetry but cancelling is
state-idempotent by itself; the provider invoice is not cancelled (KI-payment-09)
and a later deposit is caught by the consumer manual-review WARN (KI-payment-08).

FR-6. Money conversion: `expected_pay_minor = floor(amount_minor * rate *
10^(pay_decimals - pricing_decimals))` computed with integer math only; the
rate is a positive decimal string bounded by the contract (40 chars, no
exponent); the pay amount is positive.

FR-7. The `creating` and `created` states expose the quote fields and the
request idempotency metadata; the merchant-facing `pay_amount` is the locked
pay amount from `created` onward (the 7.4 requirement; KI-payment-11).

## 5. Contracts

`PaymentService/CreatePayment` and `CancelPayment` (cf-1, CF-4 additive
fields: `rate`, `locked_until`, `expected_amount`, `checkout_url`), consumed
as documented in `CBL-003`. `QuoteService/LockQuote` and
`ConnectorService/CreateInvoice` consumed per CF-1. No contract change.

## 6. Data model and migrations

Migration 00007 replaces the transition guard: the single
`creating -> created` transition may fill `expected_amount`/`expected_asset`/
`expected_decimals` once (the locked pay amount replaces the request
placeholder); every other update keeps them immutable. The idempotent key
lookup uses the existing unique `(merchant_id, idempotency_key)` index;
`quote_id`/`rate`/`quote_locked_until` stay fill-once (migration 00005).

## 7. State machines and rules

`creating -> created -> pending` per the 4.1 matrix; failures
`creating -> failed` (quote rejection, janitor) and `created -> failed`
(invoice rejection); cancel `creating|created|pending -> cancelled`. The
canonical consumer treats `creating`/`created` as too early (`ErrEventTooEarly`)
and `pending` as ready, so the provider events created by the address flow
apply only after step 7.

## 8. Edge cases

E-1. Concurrent retries: the unique index lets one insert win; the loser
re-reads and resumes; both answer the same payment.
E-2. The quote succeeds but the process dies before the transition: the retry
re-reads `creating`, locks a new quote (the previous lock is orphaned,
money-neutral) and proceeds.
E-3. The connector succeeds but the response is lost: the retry re-calls the
connector, which replays the stored invoice by order id (17.2) and the
payment moves to `pending`.
E-4. A retry with the same key but a different amount, asset, network or
expiry: `AlreadyExists`.
E-5. For a new payment `expires_at` missing, in the past or beyond 7 days:
`InvalidArgument` before any external call; a replay skips the window check and
returns the stored payment after its own expiry (E-8).
E-6. A payment failed by the janitor (creating timeout) replays as failed.
E-7. Cancel after detection (`detected`/`confirming`/terminal): not
cancellable.
E-8. The original request has expired but the key already owns a payment: the
replay returns the stored result instead of `invalid_expires_at` (FR-2).
E-9. The pay-scale configuration changed between the insert and the retry: the
hash no longer matches and the replay fails closed as `idempotency_key_reuse`.
E-10. The connector returns both an address and a checkout page: the hosted
checkout page wins, exactly one destination reaches the merchant (the proto
invariant).
E-11. The provider answers without an address and without a checkout page: the
payment stays `created` and the retry re-asks (the connector replays the stored
destination since `34d570a`).

## 9. Non-functional requirements

- NFR-1. Coverage at least 80% (unit+integration).
- NFR-2. No float in money or rate math; int64 minor units only.
- NFR-3. No new dependencies (the gRPC clients use the existing stack).
- NFR-4. English only, no em dash, godoc on exported entities.

## 10. Test plan

### 10.1 Unit matrix

| ID | Case | Expected |
| :-- | :-- | :-- |
| C-01 | valid request | payment created, quote locked once, invoice called once, state pending, response fields |
| C-02 | replay after full success | same payment returned, quote/invoice call counts unchanged |
| C-03 | resume from `created` (invoice timeout) | only the invoice step retries, state pending |
| C-04 | resume from `creating` (quote timeout) | quote and invoice steps run, state pending |
| C-05 | same key, different body | AlreadyExists, no external calls |
| C-06 | quote business rejection | payment failed with the fixed reason, FailedPrecondition class |
| C-07 | invoice business rejection | payment failed, FailedPrecondition class |
| C-08 | quote temporary failure | state creating, temporary class |
| C-09 | invoice temporary failure | state created, temporary class |
| C-10 | concurrent insert race (`ErrDuplicateKey`) | re-read and resume, no second invoice |
| C-11 | rate conversion | floor integer math, asset/decimals of the pay asset |
| C-12 | invalid rate (zero, exponent, over-long) | error before any transition |
| C-13 | cancel from pending | cancelled once, CAS |
| C-14 | cancel from cancelled | idempotent, same result |
| C-15 | cancel after detection | FailedPrecondition, no transition |
| C-16 | cancel of a missing payment | NotFound |
| C-17 | terminal payment replay | failed/expired payments replay as is |
| C-18 | request hash stability | whitespace-equivalent retries replay |
| C-19 | uppercase/URN merchant id | canonicalized before the hash and the store; replay matches |
| C-20 | replay after the request expiry | the stored payment returns, no `invalid_expires_at` |
| C-21 | pay-scale config change between create and retry | `idempotency_key_reuse` |
| C-22 | cancel from `creating`/`created` | `cancelled` (FR-5) |
| C-23 | provider overload (ResourceExhausted) | `ErrOverloaded` classification on both clients and the 429 gRPC class |

### 10.2 Integration matrix (testcontainers PostgreSQL)

| ID | Case | Expected |
| :-- | :-- | :-- |
| I-01 | `ByIdempotencyKey` round-trip | the row of the (merchant, key) |
| I-02 | creation through the service with stub quote/connector over gRPC | state pending, quote fields filled, response |
| I-03 | cancel idempotency through the service | one transition, repeat returns cancelled |

### 10.3 E2E scenario description

`E2E_Payment_CreatePayment` (testcontainers PostgreSQL, in-process stub Quote
and Connector gRPC servers over TCP, the real clients, storage and usecase
behind the gRPC server): create happy path (pending, quote and invoice called
once); replay with the same key; connector timeout then resume; cancel and
repeat cancel; `invoice_not_cancellable` after detection. Artifacts per
E2E.md once the compose stand gains the flow (17.14 chains the real
connector).

### 10.4 Load

Deferred: the M1 acceptance and the 6 s budget are measured in 17.14/14.4.

## 11. Reviews

Red run (19.10.2026, before the implementation): `go test ./internal/domain/`
failed on `undefined: domain.ConvertRate`, `ErrPayAmount`, `ErrInvalidRate`,
`RequestHash`, `ValidAssetCode`; the usecase tests failed on
`undefined: usecase.PaymentCreation`, `NewPaymentCreation`, `Config`,
`QuoteLocker`, `InvoiceCreator` (captured in the session transcript).

Four reviewers ran on the staged diff (19.10.2026): no-context (21 findings),
architecture (10, one blocking), with-context (12, two blocking), security
(11, one blocking). Closure:

- Blocking: migration 00007 was authored from the 00005 guard body and lost
  the `parent_payment_id` immutability clause added by 00006 (three reviewers).
  Closed: 00007 was rebuilt from the 00006 body plus the single
  `creating -> created` expected fill; the Down restores the 00006 body; a new
  integration test proves a version-bumping re-parent is rejected.
- Blocking: the connector idempotent replay returned no destination, so the
  E-3 resume could never reach `pending` (with-context). Closed by the
  `cbl-connector` increment `34d570a` (migration 00009 stores
  `provider_invoices.address`/`checkout_url` and the replay returns them).
- Blocking: the expiry window was validated before the idempotency lookup, so a
  replay after its own expiry failed (two reviewers). Closed: shape validation
  always runs, the window check only for a new payment; C-20 pins it.
- Major: the C-10 duplicate-race branch was not actually exercised (the seed
  made the first lookup hit). Closed with a scripted lookup queue.
- Major: the request hash now includes the pay scale (a config change cannot
  respin a locked amount, C-21) and the merchant id is canonicalized before
  the hash, the store and the lookup (C-19; the write surface of KI-payment-04
  records that the identity is trusted until 0.6).
- Major: `failPayment` no longer adopts a stale `created` row for an invoice
  rejection (a concurrent attempt could be mid-invoice); the quote rejection
  keeps the adoption.
- Major: clients keep the caller-context class (a dead caller context returns
  `ctx.Err()`), bound every peer-provided identifier and map
  `RESOURCE_EXHAUSTED` to `ErrOverloaded` -> 429 (C-23).
- Major closed on the docs side: `AGENTS.md` (contracts line, creation
  architecture paragraph, envs, T-PAY-11, KI-payment-04 write surface,
  KI-payment-08/09 wording, KI-payment-11, journal, runbook),
  `test/e2e/E2E.md` (migration version 7, the new scenario), the umbrella
  CBL-004 status.
- Minor closed: exactly one destination reaches the merchant (checkout wins,
  E-10); network charset validation; `mapCreationError` dead branches removed
  and `context.DeadlineExceeded` mapped to a retryable class; the late
  canonical event after cancellation raises the manual-review WARN
  (KI-payment-08/09); the quote TTL cap documented against the deployed
  `LOCK_QUOTE_MAX_TTL`; test hygiene and the extended matrices.
- Declined with rationale: CBL-003's phrase "completion by a background job"
  for `creating` is superseded by retry-resume (recorded in section 13 as an
  erratum for the next system-spec revision; editing CBL-003 is a dedicated
  system task); in-process stubs in I-02 (the real gRPC clients are covered by
  the e2e); the `creating` placeholder exposure is KI-payment-11; the per-asset
  (not per-network) scale map is sufficient for the current catalog and is
  recorded in T-PAY-11.

Final runs (19.10.2026): unit green; integration green (testcontainers
PostgreSQL, including the 00007 guard proof); the full testcontainers e2e
suite green (`E2E_Payment_CanonicalKafka` and `E2E_Payment_CreatePayment`);
unit+integration coverage 92.1% (above the 91.3% baseline); `go vet` clean;
lint 0; the em dash check green.

## 12. Documentation

`AGENTS.md` (contracts, architecture, env, trade-offs, KI-payment-11,
journal, runbook), `test/e2e/E2E.md`, `deploy/docker-compose.yml` envs,
`.env.example`, `CBL-004` status.

## 13. Open questions

- The merchant REST `pay_amount` rendering for a `creating` payment
  (KI-payment-11) is a 7.4 requirement.
- Erratum for the next system-spec revision: `CBL-003` says a `creating`
  payment is completed "by a background job"; with 4.2 the completion is a
  retry-resume (`created` waits for the caller) and the janitor only fails
  stuck creations (KI-payment-05).



