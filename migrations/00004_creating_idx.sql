-- +goose Up
CREATE INDEX payments_creating_idx
    ON payments (created_at) WHERE state = 'creating';

-- +goose Down
DROP INDEX IF EXISTS payments_creating_idx;
