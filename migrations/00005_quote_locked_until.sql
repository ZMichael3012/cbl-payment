-- +goose Up
-- Поля котировки счета мерчанта (4.8): срок фиксации курса. rate и quote_id
-- уже есть с 4.1; здесь добавляется locked_until, а правила неизменяемости
-- распространяются на все поля котировки в режиме «fill-once»: пока значение
-- NULL, переход может его заполнить (создание/фиксация котировки 4.2), после
-- заполнения изменение запрещено.
ALTER TABLE payments ADD COLUMN quote_locked_until timestamptz;

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

-- +goose Down
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

ALTER TABLE payments DROP COLUMN quote_locked_until;
