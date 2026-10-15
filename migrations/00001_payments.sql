-- +goose Up
CREATE TABLE payments (
    id                uuid PRIMARY KEY,
    merchant_id       text        NOT NULL,
    idempotency_key   text        NOT NULL,
    request_hash      text        NOT NULL,
    state             text        NOT NULL CHECK (state IN (
        'creating', 'created', 'pending', 'detected', 'confirming',
        'completed', 'expired', 'underpaid', 'overpaid', 'failed', 'cancelled'
    )),
    state_version     bigint      NOT NULL DEFAULT 1 CHECK (state_version >= 1),
    expected_amount   bigint      NOT NULL CHECK (expected_amount > 0),
    expected_asset    text        NOT NULL CHECK (expected_asset ~ '^[A-Z0-9]{2,16}$'),
    expected_decimals int         NOT NULL CHECK (expected_decimals BETWEEN 0 AND 18),
    received_amount   bigint      CHECK (received_amount IS NULL OR received_amount >= 0),
    received_asset    text        CHECK (received_asset IS NULL OR received_asset ~ '^[A-Z0-9]{2,16}$'),
    received_decimals int         CHECK (received_decimals IS NULL OR received_decimals BETWEEN 0 AND 18),
    fee_amount        bigint      CHECK (fee_amount IS NULL OR fee_amount >= 0),
    fee_asset         text        CHECK (fee_asset IS NULL OR fee_asset ~ '^[A-Z0-9]{2,16}$'),
    fee_decimals      int         CHECK (fee_decimals IS NULL OR fee_decimals BETWEEN 0 AND 18),
    quote_id          text,
    rate              text,
    pay_address       text,
    network           text        NOT NULL,
    gateway_id        text,
    provider          text,
    txid              text,
    confirmations     int         NOT NULL DEFAULT 0 CHECK (confirmations >= 0),
    expires_at        timestamptz,
    created_at        timestamptz NOT NULL DEFAULT now(),
    updated_at        timestamptz NOT NULL DEFAULT now(),
    completed_at      timestamptz,
    CHECK (completed_at IS NULL OR state IN ('completed', 'underpaid', 'overpaid'))
);

CREATE UNIQUE INDEX payments_merchant_idempotency_idx
    ON payments (merchant_id, idempotency_key);
CREATE INDEX payments_merchant_created_idx
    ON payments (merchant_id, created_at DESC, id DESC);
CREATE INDEX payments_expiry_idx
    ON payments (expires_at) WHERE state IN ('created', 'pending');
CREATE INDEX payments_stuck_idx
    ON payments (updated_at) WHERE state IN ('detected', 'confirming');

CREATE TABLE payment_transitions (
    id            bigserial PRIMARY KEY,
    payment_id    uuid        NOT NULL REFERENCES payments (id),
    from_state    text        NOT NULL DEFAULT '' CHECK (from_state IN (
        '', 'creating', 'created', 'pending', 'detected', 'confirming',
        'completed', 'expired', 'underpaid', 'overpaid', 'failed', 'cancelled'
    )),
    to_state      text        NOT NULL CHECK (to_state IN (
        'creating', 'created', 'pending', 'detected', 'confirming',
        'completed', 'expired', 'underpaid', 'overpaid', 'failed', 'cancelled'
    )),
    state_version bigint      NOT NULL CHECK (state_version >= 1),
    reason        text        NOT NULL DEFAULT '',
    occurred_at   timestamptz NOT NULL DEFAULT now(),
    UNIQUE (payment_id, state_version)
);

-- +goose StatementBegin
CREATE FUNCTION payments_transition_guard() RETURNS trigger AS $$
DECLARE
    allowed boolean;
BEGIN
    IF NEW.id IS DISTINCT FROM OLD.id
        OR NEW.merchant_id IS DISTINCT FROM OLD.merchant_id
        OR NEW.idempotency_key IS DISTINCT FROM OLD.idempotency_key
        OR NEW.request_hash IS DISTINCT FROM OLD.request_hash
        OR NEW.created_at IS DISTINCT FROM OLD.created_at
        OR NEW.expected_amount IS DISTINCT FROM OLD.expected_amount
        OR NEW.expected_asset IS DISTINCT FROM OLD.expected_asset
        OR NEW.expected_decimals IS DISTINCT FROM OLD.expected_decimals THEN
        RAISE EXCEPTION 'payments: immutable fields cannot change';
    END IF;
    IF NEW.state_version <> OLD.state_version + 1 THEN
        RAISE EXCEPTION 'payments: state_version must increment by 1 (got % -> %)',
            OLD.state_version, NEW.state_version;
    END IF;
    -- Самопереход (обновление confirmations и т.п.) допустим только в нетерминальных
    -- состояниях; терминальные состояния неизменяемы.
    IF NEW.state = OLD.state THEN
        IF OLD.state IN ('completed', 'expired', 'underpaid', 'overpaid', 'failed', 'cancelled') THEN
            RAISE EXCEPTION 'payments: terminal state % is immutable', OLD.state;
        END IF;
    ELSE
        allowed := (OLD.state, NEW.state) IN (
            ('creating', 'created'), ('creating', 'cancelled'), ('creating', 'failed'),
            ('created', 'pending'), ('created', 'cancelled'), ('created', 'expired'), ('created', 'failed'),
            ('pending', 'detected'), ('pending', 'cancelled'), ('pending', 'expired'), ('pending', 'failed'),
            ('detected', 'confirming'), ('detected', 'underpaid'), ('detected', 'overpaid'), ('detected', 'failed'),
            ('confirming', 'completed'), ('confirming', 'underpaid'), ('confirming', 'overpaid'), ('confirming', 'failed')
        );
        IF NOT allowed THEN
            RAISE EXCEPTION 'payments: transition % -> % is forbidden', OLD.state, NEW.state;
        END IF;
    END IF;
    NEW.updated_at := now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql SET search_path = pg_catalog, pg_temp;
-- +goose StatementEnd

CREATE TRIGGER payments_transition_guard_trigger
    BEFORE UPDATE ON payments
    FOR EACH ROW EXECUTE FUNCTION payments_transition_guard();

-- +goose StatementBegin
CREATE FUNCTION payments_insert_guard() RETURNS trigger AS $$
BEGIN
    IF NEW.state <> 'creating' OR NEW.state_version <> 1 THEN
        RAISE EXCEPTION 'payments: new payment must start in creating with state_version 1';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql SET search_path = pg_catalog, pg_temp;
-- +goose StatementEnd

CREATE TRIGGER payments_insert_guard_trigger
    BEFORE INSERT ON payments
    FOR EACH ROW EXECUTE FUNCTION payments_insert_guard();

-- +goose StatementBegin
CREATE FUNCTION payment_transitions_append_only() RETURNS trigger AS $$
BEGIN
    RAISE EXCEPTION 'payment_transitions is append-only: % is forbidden', TG_OP;
END;
$$ LANGUAGE plpgsql SET search_path = pg_catalog, pg_temp;
-- +goose StatementEnd

CREATE TRIGGER payment_transitions_append_only_trigger
    BEFORE UPDATE OR DELETE ON payment_transitions
    FOR EACH ROW EXECUTE FUNCTION payment_transitions_append_only();

-- +goose StatementBegin
CREATE FUNCTION payment_transitions_no_truncate() RETURNS trigger AS $$
BEGIN
    RAISE EXCEPTION 'payment_transitions is append-only: TRUNCATE is forbidden';
END;
$$ LANGUAGE plpgsql SET search_path = pg_catalog, pg_temp;
-- +goose StatementEnd

CREATE TRIGGER payment_transitions_no_truncate_trigger
    BEFORE TRUNCATE ON payment_transitions
    FOR EACH STATEMENT EXECUTE FUNCTION payment_transitions_no_truncate();

-- +goose Down
DROP TRIGGER IF EXISTS payment_transitions_no_truncate_trigger ON payment_transitions;
DROP FUNCTION IF EXISTS payment_transitions_no_truncate();
DROP TRIGGER IF EXISTS payment_transitions_append_only_trigger ON payment_transitions;
DROP FUNCTION IF EXISTS payment_transitions_append_only();
DROP TRIGGER IF EXISTS payments_insert_guard_trigger ON payments;
DROP FUNCTION IF EXISTS payments_insert_guard();
DROP TRIGGER IF EXISTS payments_transition_guard_trigger ON payments;
DROP FUNCTION IF EXISTS payments_transition_guard();
DROP TABLE IF EXISTS payment_transitions;
DROP TABLE IF EXISTS payments;
