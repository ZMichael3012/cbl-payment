package usecase

import (
	"context"
	"time"

	"github.com/cbl/cbl-payment/internal/domain"
	"github.com/cbl/cbl-payment/internal/storage"
)

// ListFilter describes the merchant payment list filter (4.8): the owner is
// required, state and creation period are optional.
type ListFilter struct {
	// MerchantID defines the required owner of the payments.
	MerchantID string
	// State defines the optional state filter.
	State *domain.State
	// CreatedFrom defines the inclusive lower bound of the creation period.
	CreatedFrom *time.Time
	// CreatedTo defines the exclusive upper bound of the creation period.
	CreatedTo *time.Time
	// Limit defines the page size (0 means the server default).
	Limit int
	// Cursor defines the cursor of the previous page.
	Cursor string
}

// PaymentQuery provides payment reads for the BFF (7.4): Get with transition
// history and List with merchant, state and period filters (4.8).
type PaymentQuery struct {
	repo *storage.Repository
}

// NewPaymentQuery creates a PaymentQuery.
func NewPaymentQuery(repo *storage.Repository) *PaymentQuery {
	return &PaymentQuery{repo: repo}
}

// Get returns the payment and its transition history.
func (q *PaymentQuery) Get(ctx context.Context, paymentID string) (storage.Payment, []storage.Transition, error) {
	return q.repo.Get(ctx, paymentID)
}

// List returns a page of merchant payments and the continuation cursor.
func (q *PaymentQuery) List(ctx context.Context, filter ListFilter) ([]storage.Payment, string, error) {
	return q.repo.List(ctx, storage.ListFilter{
		MerchantID:  filter.MerchantID,
		State:       filter.State,
		CreatedFrom: filter.CreatedFrom,
		CreatedTo:   filter.CreatedTo,
		Limit:       filter.Limit,
		Cursor:      filter.Cursor,
	})
}
