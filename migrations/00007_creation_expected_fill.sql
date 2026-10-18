-- +goose Up
-- 4.2: the pay amount is known only after the quote lock, while the creating
-- row must carry a positive expected amount from the first insert (the guard
-- keeps the expected fields immutable otherwise). The single
-- creating -> created transition may fill the locked pay amount once; every
-- other update keeps every immutable field, including the parent link added by
-- 00006, unchanged.
-- +goose StatementBegin
CREATE OR REPLACE FUNCTION payments_transition_guard() RETURNS trigger AS $$
DECLARE
    allowed boolean;
    fills_expected boolean;
BEGIN
    fills_expected := OLD.state = 'creating' AND NEW.state = 'created';
    IF NEW.id IS DISTINCT FROM OLD.id
        OR NEW.merchant_id IS DISTINCT FROM OLD.merchant_id
        OR NEW.idempotency_key IS DISTINCT FROM OLD.idempotency_key
        OR NEW.request_hash IS DISTINCT FROM OLD.request_hash
        OR NEW.created_at IS DISTINCT FROM OLD.created_at
        OR (NOT fills_expected AND (
            NEW.expected_amount IS DISTINCT FROM OLD.expected_amount
            OR NEW.expected_asset IS DISTINCT FROM OLD.expected_asset
            OR NEW.expected_decimals IS DISTINCT FROM OLD.expected_decimals))
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

-- +goose Down
-- Restore the 00006 body (with the parent link immutability, without the
-- expected fill).
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
