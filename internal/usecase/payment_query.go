package usecase

import (
	"context"
	"time"

	"github.com/cbl/cbl-payment/internal/domain"
	"github.com/cbl/cbl-payment/internal/storage"
)

// ListFilter описывает фильтр списка платежей мерчанта (4.8): владелец
// обязателен, состояние и период создания необязательны.
type ListFilter struct {
	// MerchantID задает обязательного владельца платежей.
	MerchantID string
	// State задает необязательный фильтр состояния.
	State *domain.State
	// CreatedFrom задает нижнюю границу периода создания включительно.
	CreatedFrom *time.Time
	// CreatedTo задает верхнюю границу периода создания исключительно.
	CreatedTo *time.Time
	// Limit задает размер страницы (0 - серверный дефолт).
	Limit int
	// Cursor задает курсор предыдущей страницы.
	Cursor string
}

// PaymentQuery предоставляет чтения платежей для BFF (7.4): Get с историей
// переходов и List с фильтрами мерчанта, состояния и периода (4.8).
type PaymentQuery struct {
	repo *storage.Repository
}

// NewPaymentQuery создает PaymentQuery.
func NewPaymentQuery(repo *storage.Repository) *PaymentQuery {
	return &PaymentQuery{repo: repo}
}

// Get возвращает платеж и его историю переходов.
func (q *PaymentQuery) Get(ctx context.Context, paymentID string) (storage.Payment, []storage.Transition, error) {
	return q.repo.Get(ctx, paymentID)
}

// List возвращает страницу платежей мерчанта и курсор продолжения.
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
