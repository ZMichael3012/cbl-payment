-- +goose Up
-- Provider rules (17.10): raw provider status, hosted checkout url, the fee
-- split passthrough and the parent link of a repeated-deposit top-up payment.
ALTER TABLE payments
    ADD COLUMN raw_status        text CHECK (raw_status IS NULL OR char_length(raw_status) <= 64),
    ADD COLUMN checkout_url      text CHECK (checkout_url IS NULL OR char_length(checkout_url) <= 2048),
    ADD COLUMN gateway_fee       bigint CHECK (gateway_fee IS NULL OR gateway_fee >= 0),
    ADD COLUMN network_fee       bigint CHECK (network_fee IS NULL OR network_fee >= 0),
    ADD COLUMN fx_spread         bigint CHECK (fx_spread IS NULL OR fx_spread >= 0),
    ADD COLUMN parent_payment_id uuid REFERENCES payments (id);

CREATE INDEX payments_parent_idx ON payments (parent_payment_id)
    WHERE parent_payment_id IS NOT NULL;
-- A repeated deposit is identified by its parent and the network transaction:
-- the unique index is the second dedup barrier behind the inbox.
CREATE UNIQUE INDEX payments_topup_txid_idx ON payments (parent_payment_id, txid)
    WHERE parent_payment_id IS NOT NULL AND txid IS NOT NULL AND txid <> '';

-- +goose StatementBegin
CREATE OR REPLACE FUNCTION payments_transition_guard() RETURNS trigger AS $$
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
        OR NEW.expected_decimals IS DISTINCT FROM OLD.expected_decimals
        OR NEW.parent_payment_id IS DISTINCT FROM OLD.parent_payment_id
        OR (OLD.quote_id IS NOT NULL AND NEW.quote_id IS DISTINCT FROM OLD.quote_id)
        OR (OLD.rate IS NOT NULL AND NEW.rate IS DISTINCT FROM OLD.rate)
        OR (OLD.quote_locked_until IS NOT NULL AND NEW.quote_locked_until IS DISTINCT FROM OLD.quote_locked_until) THEN
        RAISE EXCEPTION 'payments: immutable fields cannot change';
    END IF;
    IF NEW.state_version <> OLD.state_version + 1 THEN
        RAISE EXCEPTION 'payments: state_version must increment by 1 (got % -> %)',
            OLD.state_version, NEW.state_version;
    END IF;
    -- A self-transition (confirmations update and so on) is allowed only in
    -- non-terminal states; terminal states are immutable.
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

-- +goose StatementBegin
CREATE OR REPLACE FUNCTION payments_insert_guard() RETURNS trigger AS $$
DECLARE
    parent_state text;
BEGIN
    -- A derived top-up row is created directly in completed (17.10): the
    -- parent must be an existing terminal-success payment, the initial
    -- version and the completion time are mandatory.
    IF NEW.parent_payment_id IS NOT NULL THEN
        IF NEW.id = NEW.parent_payment_id THEN
            RAISE EXCEPTION 'payments: a derived payment cannot be its own parent';
        END IF;
        -- The guard runs with a pinned search_path, so the parent lookup uses
        -- the actual schema of the guarded table (TG_TABLE_SCHEMA).
        EXECUTE format('SELECT state FROM %I.payments WHERE id = $1', TG_TABLE_SCHEMA)
            INTO parent_state USING NEW.parent_payment_id;
        IF parent_state IS NULL THEN
            RAISE EXCEPTION 'payments: a derived payment must reference an existing parent';
        END IF;
        IF parent_state NOT IN ('completed', 'underpaid', 'overpaid') THEN
            RAISE EXCEPTION 'payments: a derived payment requires a terminal-success parent';
        END IF;
        IF NEW.state = 'completed' AND NEW.state_version = 1 AND NEW.completed_at IS NOT NULL THEN
            RETURN NEW;
        END IF;
        RAISE EXCEPTION 'payments: a derived payment must start completed at state_version 1 with completed_at';
    END IF;
    IF NEW.state <> 'creating' OR NEW.state_version <> 1 THEN
        RAISE EXCEPTION 'payments: new payment must start in creating with state_version 1';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql SET search_path = pg_catalog, pg_temp;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
CREATE OR REPLACE FUNCTION payments_insert_guard() RETURNS trigger AS $$
BEGIN
    IF NEW.state <> 'creating' OR NEW.state_version <> 1 THEN
        RAISE EXCEPTION 'payments: new payment must start in creating with state_version 1';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql SET search_path = pg_catalog, pg_temp;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE OR REPLACE FUNCTION payments_transition_guard() RETURNS trigger AS $$
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
        OR NEW.expected_decimals IS DISTINCT FROM OLD.expected_decimals
        OR (OLD.quote_id IS NOT NULL AND NEW.quote_id IS DISTINCT FROM OLD.quote_id)
        OR (OLD.rate IS NOT NULL AND NEW.rate IS DISTINCT FROM OLD.rate)
        OR (OLD.quote_locked_until IS NOT NULL AND NEW.quote_locked_until IS DISTINCT FROM OLD.quote_locked_until) THEN
        RAISE EXCEPTION 'payments: immutable fields cannot change';
    END IF;
    IF NEW.state_version <> OLD.state_version + 1 THEN
        RAISE EXCEPTION 'payments: state_version must increment by 1 (got % -> %)',
            OLD.state_version, NEW.state_version;
    END IF;
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

DROP INDEX IF EXISTS payments_topup_txid_idx;
DROP INDEX IF EXISTS payments_parent_idx;
ALTER TABLE payments
    DROP COLUMN parent_payment_id,
    DROP COLUMN fx_spread,
    DROP COLUMN network_fee,
    DROP COLUMN gateway_fee,
    DROP COLUMN checkout_url,
    DROP COLUMN raw_status;
