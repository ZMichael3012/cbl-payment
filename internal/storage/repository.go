package storage

import (
	"context"
	"database/sql"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"github.com/cbl/cbl-payment/internal/domain"
)

// Storage errors.
var (
	// ErrNotFound reports a missing payment.
	ErrNotFound = errors.New("payment: payment not found")
	// ErrDuplicateKey reports a taken merchant idempotency key.
	ErrDuplicateKey = errors.New("payment: idempotency key already used")
	// ErrInvalidCursor reports an invalid payment list cursor.
	ErrInvalidCursor = errors.New("payment: invalid cursor")
)

// Pagination bounds of the payment list and the transition history: Get returns
// the last maxTimelineTransitions journal entries, which protects against an
// unbounded response for a long-lived non-terminal payment (self-transition
// confirmations).
const (
	defaultListLimit       = 50
	maxListLimit           = 200
	maxTimelineTransitions = 500
)

// Background batch bounds (janitor 4.6): a separate, higher ceiling than the
// user-facing list so that backlog draining proceeds in batches without
// degrading interactive requests.
const maxBackgroundLimit = 1000

// Payment describes a payments table row.
type Payment struct {
	// ID holds the payment UUIDv7.
	ID string `gorm:"column:id;type:uuid;primaryKey"`
	// MerchantID holds the merchant owning the payment.
	MerchantID string `gorm:"column:merchant_id"`
	// IdempotencyKey holds the creation idempotency key.
	IdempotencyKey string `gorm:"column:idempotency_key"`
	// RequestHash holds the hash of the creation request body.
	RequestHash string `gorm:"column:request_hash"`
	// State holds the current state machine state.
	State string `gorm:"column:state"`
	// StateVersion holds the monotonic state version.
	StateVersion int64 `gorm:"column:state_version"`
	// ExpectedAmount holds the expected amount in minor units.
	ExpectedAmount int64 `gorm:"column:expected_amount"`
	// ExpectedAsset holds the asset of the expected amount.
	ExpectedAsset string `gorm:"column:expected_asset"`
	// ExpectedDecimals holds the scale of the expected amount.
	ExpectedDecimals int `gorm:"column:expected_decimals"`
	// ReceivedAmount holds the received amount in minor units.
	ReceivedAmount *int64 `gorm:"column:received_amount"`
	// ReceivedAsset holds the asset of the received amount.
	ReceivedAsset *string `gorm:"column:received_asset"`
	// ReceivedDecimals holds the scale of the received amount.
	ReceivedDecimals *int `gorm:"column:received_decimals"`
	// FeeAmount holds the fee in minor units.
	FeeAmount *int64 `gorm:"column:fee_amount"`
	// FeeAsset holds the fee asset.
	FeeAsset *string `gorm:"column:fee_asset"`
	// FeeDecimals holds the fee scale.
	FeeDecimals *int `gorm:"column:fee_decimals"`
	// QuoteID holds the locked rate quote.
	QuoteID *string `gorm:"column:quote_id"`
	// Rate holds the locked rate as a decimal string.
	Rate *string `gorm:"column:rate"`
	// QuoteLockedUntil holds the quote lock deadline (filled once, 4.8).
	QuoteLockedUntil *time.Time `gorm:"column:quote_locked_until"`
	// PayAddress holds the payment address at the gateway.
	PayAddress *string `gorm:"column:pay_address"`
	// Network holds the payment network.
	Network string `gorm:"column:network"`
	// GatewayID holds the payment identifier at the gateway.
	GatewayID *string `gorm:"column:gateway_id"`
	// Provider holds the gateway provider name.
	Provider *string `gorm:"column:provider"`
	// RawStatus holds the provider status string untouched (17.10).
	RawStatus *string `gorm:"column:raw_status"`
	// CheckoutURL holds the hosted payment page of hosted providers (17.10).
	CheckoutURL *string `gorm:"column:checkout_url"`
	// GatewayFee holds the gateway fee component in minor units (17.10).
	GatewayFee *int64 `gorm:"column:gateway_fee"`
	// NetworkFee holds the network fee component in minor units (17.10).
	NetworkFee *int64 `gorm:"column:network_fee"`
	// FxSpread holds the fx spread component in minor units (17.10).
	FxSpread *int64 `gorm:"column:fx_spread"`
	// ParentPaymentID holds the parent of a repeated-deposit top-up (17.10).
	ParentPaymentID *string `gorm:"column:parent_payment_id"`
	// TxID holds the network transaction hash.
	TxID *string `gorm:"column:txid"`
	// Confirmations holds the number of network confirmations.
	Confirmations int `gorm:"column:confirmations"`
	// ExpiresAt holds the invoice expiration time.
	ExpiresAt *time.Time `gorm:"column:expires_at"`
	// CreatedAt holds the creation time.
	CreatedAt time.Time `gorm:"column:created_at"`
	// UpdatedAt holds the time of the last transition.
	UpdatedAt time.Time `gorm:"column:updated_at"`
	// CompletedAt holds the time of terminal success.
	CompletedAt *time.Time `gorm:"column:completed_at"`
}

// TableName returns the payments table name.
func (Payment) TableName() string { return "payments" }

// Transition describes a payment_transitions journal row.
type Transition struct {
	// ID holds the sequential journal entry number.
	ID int64 `gorm:"column:id;primaryKey"`
	// PaymentID holds the payment of the transition.
	PaymentID string `gorm:"column:payment_id"`
	// FromState holds the state before the transition (empty for creation).
	FromState string `gorm:"column:from_state"`
	// ToState holds the state after the transition.
	ToState string `gorm:"column:to_state"`
	// StateVersion holds the state version after the transition.
	StateVersion int64 `gorm:"column:state_version"`
	// Reason holds the transition reason (event, request, background).
	Reason string `gorm:"column:reason"`
	// OccurredAt holds the transition time.
	OccurredAt time.Time `gorm:"column:occurred_at"`
}

// TableName returns the payment_transitions table name.
func (Transition) TableName() string { return "payment_transitions" }

// TransitionPatch describes the payment fields changed by a transition;
// nil fields are not changed.
type TransitionPatch struct {
	// ReceivedAmount holds the received amount.
	ReceivedAmount *int64
	// ReceivedAsset holds the asset of the received amount.
	ReceivedAsset *string
	// ReceivedDecimals holds the scale of the received amount.
	ReceivedDecimals *int
	// FeeAmount holds the fee.
	FeeAmount *int64
	// FeeAsset holds the fee asset.
	FeeAsset *string
	// FeeDecimals holds the fee scale.
	FeeDecimals *int
	// QuoteID holds the rate quote.
	QuoteID *string
	// Rate holds the locked rate.
	Rate *string
	// QuoteLockedUntil holds the quote lock deadline (fill-once, 4.8).
	QuoteLockedUntil *time.Time
	// ExpectedAmount holds the locked pay amount in minor units (filled once
	// by the creating -> created transition, 4.2).
	ExpectedAmount *int64
	// ExpectedAsset holds the locked pay asset (4.2).
	ExpectedAsset *string
	// ExpectedDecimals holds the locked pay asset scale (4.2).
	ExpectedDecimals *int
	// PayAddress holds the payment address.
	PayAddress *string
	// GatewayID holds the payment identifier at the gateway.
	GatewayID *string
	// Provider holds the gateway provider.
	Provider *string
	// RawStatus holds the provider raw status (17.10).
	RawStatus *string
	// CheckoutURL holds the hosted checkout url (17.10).
	CheckoutURL *string
	// GatewayFee holds the gateway fee component (17.10).
	GatewayFee *int64
	// NetworkFee holds the network fee component (17.10).
	NetworkFee *int64
	// FxSpread holds the fx spread component (17.10).
	FxSpread *int64
	// TxID holds the network transaction hash.
	TxID *string
	// Network holds the payment network from the canonical event.
	Network *string
	// Confirmations holds the number of confirmations.
	Confirmations *int
	// ExpiresAt holds the invoice expiration time.
	ExpiresAt *time.Time
	// CompletedAt holds the time of terminal success.
	CompletedAt *time.Time
	// Reason holds the transition reason for the journal.
	Reason string
}

// ListFilter describes the ListPayments filter.
type ListFilter struct {
	// MerchantID holds the required owner of the payments.
	MerchantID string
	// State holds the optional state filter.
	State *domain.State
	// CreatedFrom holds the inclusive lower bound of the creation period (4.8).
	CreatedFrom *time.Time
	// CreatedTo holds the exclusive upper bound of the creation period (4.8).
	CreatedTo *time.Time
	// Limit holds the page size (1 - 200, default 50).
	Limit int
	// Cursor holds the previous page cursor.
	Cursor string
}

// Repository reads and modifies payments in PostgreSQL.
type Repository struct {
	db        *gorm.DB
	eventSink TransitionEventSink
}

// Option configures a Repository.
type Option func(*Repository)

// NewRepository creates a Repository.
func NewRepository(db *gorm.DB, opts ...Option) *Repository {
	repo := &Repository{db: db}
	for _, opt := range opts {
		opt(repo)
	}
	return repo
}

// Create creates a payment in the creating state and writes the version 1
// transition in a single transaction. The state and version are forced to the
// initial values: the state machine is the only creation point (the second
// barrier is the BEFORE INSERT trigger). A taken idempotency key returns
// ErrDuplicateKey.
func (r *Repository) Create(ctx context.Context, payment Payment) error {
	now := time.Now().UTC()
	payment.State = string(domain.StateCreating)
	payment.StateVersion = 1
	payment.CreatedAt = now
	payment.UpdatedAt = now
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if createErr := tx.Create(&payment).Error; createErr != nil {
			if isUniqueViolation(createErr) {
				return fmt.Errorf("%w: merchant %s key %s", ErrDuplicateKey, payment.MerchantID, payment.IdempotencyKey)
			}
			return createErr
		}
		history := Transition{
			PaymentID:    payment.ID,
			FromState:    "",
			ToState:      payment.State,
			StateVersion: 1,
			Reason:       "created",
			OccurredAt:   now,
		}
		return tx.Create(&history).Error
	})
	if err != nil {
		return fmt.Errorf("storage: create payment: %w", err)
	}
	return nil
}

// Transition performs an optimistic state transition: it checks the domain
// matrix, updates the payment with the state_version = expectedVersion condition
// and writes the journal row. A version conflict is domain.ErrStateConflict.
func (r *Repository) Transition(ctx context.Context, id string, expectedVersion uint64, to domain.State, patch TransitionPatch) (Payment, error) {
	return r.TransitionTx(ctx, nil, id, expectedVersion, to, patch)
}

// TransitionTx performs the transition within the given transaction (tx=nil
// means its own): the canonical event consumer writes the transition atomically
// with the Guard inbox record.
func (r *Repository) TransitionTx(ctx context.Context, tx *gorm.DB, id string, expectedVersion uint64, to domain.State, patch TransitionPatch) (Payment, error) {
	db := tx
	if db == nil {
		db = r.db
	}
	var updated Payment
	err := db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var current Payment
		findErr := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("id = ?", id).First(&current).Error
		if errors.Is(findErr, gorm.ErrRecordNotFound) {
			return fmt.Errorf("%w: %s", ErrNotFound, id)
		}
		if findErr != nil {
			return findErr
		}
		if uint64(current.StateVersion) != expectedVersion { //nolint:gosec // state_version is constrained by CHECK (>= 1)
			return fmt.Errorf("%w: payment %s version %d, expected %d",
				domain.ErrStateConflict, id, current.StateVersion, expectedVersion)
		}
		from, parseErr := domain.ParseState(current.State)
		if parseErr != nil {
			return parseErr
		}
		newVersion, nextErr := domain.NextVersion(from, to, uint64(current.StateVersion)) //nolint:gosec // state_version is constrained by CHECK
		if nextErr != nil {
			return nextErr
		}
		updates := patch.updates()
		updates["state"] = string(to)
		updates["state_version"] = int64(newVersion) //nolint:gosec // grows by 1 from an int64 database value
		res := tx.Model(&Payment{}).
			Where("id = ? AND state_version = ?", id, expectedVersion).
			Updates(updates)
		if res.Error != nil {
			return res.Error
		}
		if res.RowsAffected == 0 {
			return fmt.Errorf("%w: payment %s lost race at version %d", domain.ErrStateConflict, id, expectedVersion)
		}
		history := Transition{
			PaymentID:    id,
			FromState:    current.State,
			ToState:      string(to),
			StateVersion: int64(newVersion), //nolint:gosec // grows by 1 from an int64 database value
			Reason:       patch.Reason,
			OccurredAt:   time.Now().UTC(),
		}
		if createErr := tx.Create(&history).Error; createErr != nil {
			return createErr
		}
		if loadErr := tx.Where("id = ?", id).First(&updated).Error; loadErr != nil {
			return loadErr
		}
		return r.emitTransitionEvent(ctx, tx, TransitionEvent{
			Payment:    updated,
			From:       from,
			To:         to,
			OccurredAt: history.OccurredAt,
		})
	})
	if err != nil {
		return Payment{}, fmt.Errorf("storage: transition payment %s: %w", id, err)
	}
	return updated, nil
}

// CreateDerived inserts a derived completed payment (a repeated-deposit top-up
// of a terminal parent, 17.10): the row starts directly in completed (the
// insert guard allows it only for parented rows), the audit transition is
// version 1 and the payment.completed event goes to the outbox in the same
// transaction. A taken top-up identity (merchant idempotency key or the parent
// and txid) returns ErrDuplicateKey.
func (r *Repository) CreateDerived(ctx context.Context, tx *gorm.DB, payment Payment, reason string) (Payment, error) {
	db := tx
	if db == nil {
		db = r.db
	}
	now := time.Now().UTC()
	payment.State = string(domain.StateCompleted)
	payment.StateVersion = 1
	if payment.CompletedAt == nil {
		payment.CompletedAt = &now
	}
	payment.CreatedAt = now
	payment.UpdatedAt = now
	err := db.WithContext(ctx).Transaction(func(inner *gorm.DB) error {
		if createErr := inner.Create(&payment).Error; createErr != nil {
			if isTopUpUniqueViolation(createErr) {
				return fmt.Errorf("%w: derived payment for parent %v", ErrDuplicateKey, payment.ParentPaymentID)
			}
			return createErr
		}
		history := Transition{
			PaymentID:    payment.ID,
			FromState:    "",
			ToState:      payment.State,
			StateVersion: 1,
			Reason:       reason,
			OccurredAt:   now,
		}
		if createErr := inner.Create(&history).Error; createErr != nil {
			return createErr
		}
		return r.emitTransitionEvent(ctx, inner, TransitionEvent{
			Payment:    payment,
			From:       "",
			To:         domain.StateCompleted,
			OccurredAt: now,
		})
	})
	if err != nil {
		return Payment{}, fmt.Errorf("storage: create derived payment: %w", err)
	}
	return payment, nil
}

// CreateDerivedTx implements the canonical consumer storage port (17.10).
func (r *Repository) CreateDerivedTx(ctx context.Context, tx *gorm.DB, payment Payment, reason string) (Payment, error) {
	return r.CreateDerived(ctx, tx, payment, reason)
}

// LockAndSumTopUpsTx locks the parent payment row and returns the baseline
// already attributed to it (17.10, B1): the recorded received amount plus the
// sum of the derived children. The row lock serializes concurrent canonical
// events of one parent, so successive top-ups credit only the new delta.
// A parent without a recorded amount or a sum overflowing int64 is an error.
func (r *Repository) LockAndSumTopUpsTx(ctx context.Context, tx *gorm.DB, parentID string) (int64, error) {
	db := tx
	if db == nil {
		db = r.db
	}
	var row struct {
		ReceivedAmount *int64
	}
	if err := db.WithContext(ctx).
		Raw("SELECT received_amount FROM payments WHERE id = ? FOR UPDATE", parentID).
		Scan(&row).Error; err != nil {
		return 0, fmt.Errorf("storage: lock top-up parent %s: %w", parentID, err)
	}
	if row.ReceivedAmount == nil {
		return 0, fmt.Errorf("storage: top-up parent %s has no recorded amount", parentID)
	}
	var sum int64
	if err := db.WithContext(ctx).
		Raw("SELECT COALESCE(SUM(received_amount), 0)::bigint FROM payments WHERE parent_payment_id = ? AND state = ?",
			parentID, string(domain.StateCompleted)).
		Scan(&sum).Error; err != nil {
		return 0, fmt.Errorf("storage: sum top-ups of %s: %w", parentID, err)
	}
	return *row.ReceivedAmount + sum, nil
}

// isTopUpUniqueViolation reports that the database error is a taken top-up
// identity (17.10): the merchant idempotency key or the parent and txid index.
// Any other unique violation is a bug and is not masked as a duplicate.
func isTopUpUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "23505" {
		return false
	}
	return pgErr.ConstraintName == "payments_merchant_idempotency_idx" ||
		pgErr.ConstraintName == "payments_topup_txid_idx"
}

// Get returns a payment and its transition history in ascending version order;
// the history is limited to the last maxTimelineTransitions entries (protection
// from an unbounded response, the full history is task 14.2). Both queries run
// in a single read-only repeatable read transaction: a concurrent transition
// must not return a timeline newer than the payment itself.
func (r *Repository) Get(ctx context.Context, id string) (Payment, []Transition, error) {
	var payment Payment
	var history []Transition
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if findErr := tx.Where("id = ?", id).First(&payment).Error; findErr != nil {
			return findErr
		}
		if historyErr := tx.Where("payment_id = ?", id).
			Order("state_version DESC").
			Limit(maxTimelineTransitions).
			Find(&history).Error; historyErr != nil {
			return historyErr
		}
		return nil
	}, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return Payment{}, nil, fmt.Errorf("%w: %s", ErrNotFound, id)
	}
	if err != nil {
		return Payment{}, nil, fmt.Errorf("storage: get payment %s: %w", id, err)
	}
	// The journal was read from newest to oldest (LIMIT of the latest entries);
	// it is returned in ascending order.
	for left, right := 0, len(history)-1; left < right; left, right = left+1, right-1 {
		history[left], history[right] = history[right], history[left]
	}
	return payment, history, nil
}

// Payment returns the payment without the transition history.
func (r *Repository) Payment(ctx context.Context, id string) (Payment, error) {
	return r.PaymentTx(ctx, nil, id)
}

// ByIdempotencyKey returns the payment of the (merchant, key) pair; the unique
// index guarantees at most one row. ErrNotFound when the key is unused (4.2).
func (r *Repository) ByIdempotencyKey(ctx context.Context, merchantID, key string) (Payment, error) {
	var payment Payment
	err := r.db.WithContext(ctx).
		Where("merchant_id = ? AND idempotency_key = ?", merchantID, key).
		Take(&payment).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return Payment{}, fmt.Errorf("%w: merchant %s key %s", ErrNotFound, merchantID, key)
	}
	if err != nil {
		return Payment{}, fmt.Errorf("storage: read payment by idempotency key: %w", err)
	}
	return payment, nil
}

// PaymentTx returns the payment without the history within the given transaction
// (tx=nil means its own): the consumer reads the state in the Guard transaction.
func (r *Repository) PaymentTx(ctx context.Context, tx *gorm.DB, id string) (Payment, error) {
	db := tx
	if db == nil {
		db = r.db
	}
	var payment Payment
	err := db.WithContext(ctx).Where("id = ?", id).First(&payment).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return Payment{}, fmt.Errorf("%w: %s", ErrNotFound, id)
	}
	if err != nil {
		return Payment{}, fmt.Errorf("storage: get payment %s: %w", id, err)
	}
	return payment, nil
}

// List returns a page of merchant payments and the next page cursor.
func (r *Repository) List(ctx context.Context, filter ListFilter) ([]Payment, string, error) {
	if strings.TrimSpace(filter.MerchantID) == "" {
		return nil, "", errors.New("storage: merchant_id is required")
	}
	limit := filter.Limit
	if limit <= 0 {
		limit = defaultListLimit
	}
	if limit > maxListLimit {
		limit = maxListLimit
	}
	query := r.db.WithContext(ctx).Where("merchant_id = ?", filter.MerchantID)
	if filter.State != nil {
		query = query.Where("state = ?", string(*filter.State))
	}
	if filter.CreatedFrom != nil {
		query = query.Where("created_at >= ?", filter.CreatedFrom.UTC())
	}
	if filter.CreatedTo != nil {
		query = query.Where("created_at < ?", filter.CreatedTo.UTC())
	}
	if strings.TrimSpace(filter.Cursor) != "" {
		createdAt, id, err := decodeCursor(filter.Cursor)
		if err != nil {
			return nil, "", err
		}
		query = query.Where("(created_at, id) < (?, ?)", createdAt, id)
	}
	var payments []Payment
	err := query.
		Order("created_at DESC, id DESC").
		Limit(limit + 1).
		Find(&payments).Error
	if err != nil {
		return nil, "", fmt.Errorf("storage: list payments: %w", err)
	}
	nextCursor := ""
	if len(payments) > limit {
		payments = payments[:limit]
		last := payments[len(payments)-1]
		nextCursor = encodeCursor(last.CreatedAt, last.ID)
	}
	return payments, nextCursor, nil
}

// FindExpired returns unpaid invoices past their expiration for task 4.6:
// the creating state is not included, stuck creations are brought by the
// background to created or failed (the matrix has no creating -> expired
// transition).
func (r *Repository) FindExpired(ctx context.Context, before time.Time, limit int) ([]Payment, error) {
	return r.findByFilter(ctx,
		"state IN ? AND expires_at IS NOT NULL AND expires_at <= ?",
		[]any{[]string{string(domain.StateCreated), string(domain.StatePending)}, before.UTC()},
		"expires_at ASC", limit)
}

// FindStuck returns payments in detected/confirming without movement for task 4.6.
func (r *Repository) FindStuck(ctx context.Context, olderThan time.Time, limit int) ([]Payment, error) {
	return r.findByFilter(ctx,
		"state IN ? AND updated_at <= ?",
		[]any{[]string{string(domain.StateDetected), string(domain.StateConfirming)}, olderThan.UTC()},
		"updated_at ASC", limit)
}

// FindCreating returns stuck creations for task 4.6: payments in creating that
// were created at or before the threshold; a normal creation (4.2) finishes in
// seconds, the threshold is set by the caller (janitor).
func (r *Repository) FindCreating(ctx context.Context, olderThan time.Time, limit int) ([]Payment, error) {
	return r.findByFilter(ctx,
		"state = ? AND created_at <= ?",
		[]any{string(domain.StateCreating), olderThan.UTC()},
		"created_at ASC", limit)
}

// findByFilter selects payments by condition with the background batch limit
// (the maxBackgroundLimit ceiling).
func (r *Repository) findByFilter(ctx context.Context, where string, args []any, order string, limit int) ([]Payment, error) {
	if limit <= 0 {
		limit = defaultListLimit
	}
	if limit > maxBackgroundLimit {
		limit = maxBackgroundLimit
	}
	var payments []Payment
	err := r.db.WithContext(ctx).
		Where(where, args...).
		Order(order).
		Limit(limit).
		Find(&payments).Error
	if err != nil {
		return nil, fmt.Errorf("storage: find payments: %w", err)
	}
	return payments, nil
}

// updates builds the change map from the non-nil patch fields.
func (p TransitionPatch) updates() map[string]any {
	updates := make(map[string]any, 16)
	if p.ReceivedAmount != nil {
		updates["received_amount"] = *p.ReceivedAmount
	}
	if p.ReceivedAsset != nil {
		updates["received_asset"] = *p.ReceivedAsset
	}
	if p.ReceivedDecimals != nil {
		updates["received_decimals"] = *p.ReceivedDecimals
	}
	if p.FeeAmount != nil {
		updates["fee_amount"] = *p.FeeAmount
	}
	if p.FeeAsset != nil {
		updates["fee_asset"] = *p.FeeAsset
	}
	if p.FeeDecimals != nil {
		updates["fee_decimals"] = *p.FeeDecimals
	}
	if p.QuoteID != nil {
		updates["quote_id"] = *p.QuoteID
	}
	if p.Rate != nil {
		updates["rate"] = *p.Rate
	}
	if p.QuoteLockedUntil != nil {
		updates["quote_locked_until"] = *p.QuoteLockedUntil
	}
	if p.ExpectedAmount != nil {
		updates["expected_amount"] = *p.ExpectedAmount
	}
	if p.ExpectedAsset != nil {
		updates["expected_asset"] = *p.ExpectedAsset
	}
	if p.ExpectedDecimals != nil {
		updates["expected_decimals"] = *p.ExpectedDecimals
	}
	if p.PayAddress != nil {
		updates["pay_address"] = *p.PayAddress
	}
	if p.GatewayID != nil {
		updates["gateway_id"] = *p.GatewayID
	}
	if p.Provider != nil {
		updates["provider"] = *p.Provider
	}
	if p.RawStatus != nil {
		updates["raw_status"] = *p.RawStatus
	}
	if p.CheckoutURL != nil {
		updates["checkout_url"] = *p.CheckoutURL
	}
	if p.GatewayFee != nil {
		updates["gateway_fee"] = *p.GatewayFee
	}
	if p.NetworkFee != nil {
		updates["network_fee"] = *p.NetworkFee
	}
	if p.FxSpread != nil {
		updates["fx_spread"] = *p.FxSpread
	}
	if p.TxID != nil {
		updates["txid"] = *p.TxID
	}
	if p.Network != nil {
		updates["network"] = *p.Network
	}
	if p.Confirmations != nil {
		updates["confirmations"] = *p.Confirmations
	}
	if p.ExpiresAt != nil {
		updates["expires_at"] = *p.ExpiresAt
	}
	if p.CompletedAt != nil {
		updates["completed_at"] = *p.CompletedAt
	}
	return updates
}

// encodeCursor builds an opaque page cursor from the time and the identifier.
func encodeCursor(createdAt time.Time, id string) string {
	raw := createdAt.UTC().Format(time.RFC3339Nano) + "|" + id
	return base64.RawURLEncoding.EncodeToString([]byte(raw))
}

// decodeCursor parses a page cursor and validates its format.
func decodeCursor(cursor string) (time.Time, string, error) {
	raw, err := base64.RawURLEncoding.DecodeString(cursor)
	if err != nil {
		return time.Time{}, "", fmt.Errorf("%w: %w", ErrInvalidCursor, err)
	}
	createdAtRaw, id, found := strings.Cut(string(raw), "|")
	if !found || id == "" {
		return time.Time{}, "", ErrInvalidCursor
	}
	createdAt, err := time.Parse(time.RFC3339Nano, createdAtRaw)
	if err != nil {
		return time.Time{}, "", fmt.Errorf("%w: %w", ErrInvalidCursor, err)
	}
	parsed, err := uuid.Parse(id)
	if err != nil {
		return time.Time{}, "", ErrInvalidCursor
	}
	return createdAt.UTC(), parsed.String(), nil
}

// isUniqueViolation reports that the database error is a taken merchant
// idempotency key (other uniqueness violations are not masked).
func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505" &&
		pgErr.ConstraintName == "payments_merchant_idempotency_idx"
}
