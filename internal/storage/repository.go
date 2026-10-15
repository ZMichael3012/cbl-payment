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

// Ошибки storage.
var (
	// ErrNotFound сообщает об отсутствии платежа.
	ErrNotFound = errors.New("payment: payment not found")
	// ErrDuplicateKey сообщает о занятом ключе идемпотентности мерчанта.
	ErrDuplicateKey = errors.New("payment: idempotency key already used")
	// ErrInvalidCursor сообщает о некорректном курсоре списка платежей.
	ErrInvalidCursor = errors.New("payment: invalid cursor")
)

// Границы пагинации списка платежей и истории переходов: Get возвращает
// последние maxTimelineTransitions записей журнала - защита от неограниченного
// ответа при долгой жизни нетерминального платежа (self-transition confirmations).
const (
	defaultListLimit       = 50
	maxListLimit           = 200
	maxTimelineTransitions = 500
)

// Границы пакетов фоновых выборок (janitor 4.6): отдельный, более высокий
// потолок, чем у пользовательского списка, чтобы разгребание backlog шло
// пачками без деградации интерактивных запросов.
const maxBackgroundLimit = 1000

// Payment описывает строку таблицы payments.
type Payment struct {
	// ID задает UUIDv7 платежа.
	ID string `gorm:"column:id;type:uuid;primaryKey"`
	// MerchantID задает мерчанта-владельца платежа.
	MerchantID string `gorm:"column:merchant_id"`
	// IdempotencyKey задает ключ идемпотентности создания.
	IdempotencyKey string `gorm:"column:idempotency_key"`
	// RequestHash задает хэш тела запроса создания.
	RequestHash string `gorm:"column:request_hash"`
	// State задает текущее состояние автомата.
	State string `gorm:"column:state"`
	// StateVersion задает монотонную версию состояния.
	StateVersion int64 `gorm:"column:state_version"`
	// ExpectedAmount задает ожидаемую сумму в минимальных единицах.
	ExpectedAmount int64 `gorm:"column:expected_amount"`
	// ExpectedAsset задает актив ожидаемой суммы.
	ExpectedAsset string `gorm:"column:expected_asset"`
	// ExpectedDecimals задает шкалу ожидаемой суммы.
	ExpectedDecimals int `gorm:"column:expected_decimals"`
	// ReceivedAmount задает полученную сумму в минимальных единицах.
	ReceivedAmount *int64 `gorm:"column:received_amount"`
	// ReceivedAsset задает актив полученной суммы.
	ReceivedAsset *string `gorm:"column:received_asset"`
	// ReceivedDecimals задает шкалу полученной суммы.
	ReceivedDecimals *int `gorm:"column:received_decimals"`
	// FeeAmount задает комиссию в минимальных единицах.
	FeeAmount *int64 `gorm:"column:fee_amount"`
	// FeeAsset задает актив комиссии.
	FeeAsset *string `gorm:"column:fee_asset"`
	// FeeDecimals задает шкалу комиссии.
	FeeDecimals *int `gorm:"column:fee_decimals"`
	// QuoteID задает зафиксированную котировку курса.
	QuoteID *string `gorm:"column:quote_id"`
	// Rate задает зафиксированный курс десятичной строкой.
	Rate *string `gorm:"column:rate"`
	// QuoteLockedUntil задает срок фиксации котировки (заполняется один раз, 4.8).
	QuoteLockedUntil *time.Time `gorm:"column:quote_locked_until"`
	// PayAddress задает адрес оплаты в шлюзе.
	PayAddress *string `gorm:"column:pay_address"`
	// Network задает сеть оплаты.
	Network string `gorm:"column:network"`
	// GatewayID задает идентификатор платежа в шлюзе.
	GatewayID *string `gorm:"column:gateway_id"`
	// Provider задает имя провайдера шлюза.
	Provider *string `gorm:"column:provider"`
	// TxID задает хэш транзакции сети.
	TxID *string `gorm:"column:txid"`
	// Confirmations задает число подтверждений сети.
	Confirmations int `gorm:"column:confirmations"`
	// ExpiresAt задает срок действия счета.
	ExpiresAt *time.Time `gorm:"column:expires_at"`
	// CreatedAt задает время создания.
	CreatedAt time.Time `gorm:"column:created_at"`
	// UpdatedAt задает время последнего перехода.
	UpdatedAt time.Time `gorm:"column:updated_at"`
	// CompletedAt задает время терминального успеха.
	CompletedAt *time.Time `gorm:"column:completed_at"`
}

// TableName задает имя таблицы payments.
func (Payment) TableName() string { return "payments" }

// Transition описывает строку журнала переходов payment_transitions.
type Transition struct {
	// ID задает порядковый номер записи журнала.
	ID int64 `gorm:"column:id;primaryKey"`
	// PaymentID задает платеж перехода.
	PaymentID string `gorm:"column:payment_id"`
	// FromState задает состояние до перехода (пусто для создания).
	FromState string `gorm:"column:from_state"`
	// ToState задает состояние после перехода.
	ToState string `gorm:"column:to_state"`
	// StateVersion задает версию состояния после перехода.
	StateVersion int64 `gorm:"column:state_version"`
	// Reason задает причину перехода (событие, запрос, фон).
	Reason string `gorm:"column:reason"`
	// OccurredAt задает время перехода.
	OccurredAt time.Time `gorm:"column:occurred_at"`
}

// TableName задает имя таблицы payment_transitions.
func (Transition) TableName() string { return "payment_transitions" }

// TransitionPatch описывает изменяемые при переходе поля платежа;
// nil-поля не изменяются.
type TransitionPatch struct {
	// ReceivedAmount задает полученную сумму.
	ReceivedAmount *int64
	// ReceivedAsset задает актив полученной суммы.
	ReceivedAsset *string
	// ReceivedDecimals задает шкалу полученной суммы.
	ReceivedDecimals *int
	// FeeAmount задает комиссию.
	FeeAmount *int64
	// FeeAsset задает актив комиссии.
	FeeAsset *string
	// FeeDecimals задает шкалу комиссии.
	FeeDecimals *int
	// QuoteID задает котировку курса.
	QuoteID *string
	// Rate задает зафиксированный курс.
	Rate *string
	// QuoteLockedUntil задает срок фиксации котировки (fill-once, 4.8).
	QuoteLockedUntil *time.Time
	// PayAddress задает адрес оплаты.
	PayAddress *string
	// GatewayID задает идентификатор платежа в шлюзе.
	GatewayID *string
	// Provider задает провайдера шлюза.
	Provider *string
	// TxID задает хэш транзакции сети.
	TxID *string
	// Network задает сеть оплаты из канонического события.
	Network *string
	// Confirmations задает число подтверждений.
	Confirmations *int
	// ExpiresAt задает срок действия счета.
	ExpiresAt *time.Time
	// CompletedAt задает время терминального успеха.
	CompletedAt *time.Time
	// Reason задает причину перехода для журнала.
	Reason string
}

// ListFilter описывает фильтр ListPayments.
type ListFilter struct {
	// MerchantID задает обязательного владельца платежей.
	MerchantID string
	// State задает необязательный фильтр состояния.
	State *domain.State
	// CreatedFrom задает нижнюю границу периода создания включительно (4.8).
	CreatedFrom *time.Time
	// CreatedTo задает верхнюю границу периода создания исключительно (4.8).
	CreatedTo *time.Time
	// Limit задает размер страницы (1 - 200, по умолчанию 50).
	Limit int
	// Cursor задает курсор предыдущей страницы.
	Cursor string
}

// Repository читает и изменяет платежи в PostgreSQL.
type Repository struct {
	db        *gorm.DB
	eventSink TransitionEventSink
}

// Option настраивает Repository.
type Option func(*Repository)

// NewRepository создает Repository.
func NewRepository(db *gorm.DB, opts ...Option) *Repository {
	repo := &Repository{db: db}
	for _, opt := range opts {
		opt(repo)
	}
	return repo
}

// Create создает платеж в состоянии creating и записывает переход версии 1
// одной транзакцией. Состояние и версия принудительно приводятся к начальным:
// единственная точка создания - автомат (второй барьер - BEFORE INSERT триггер).
// Занятый ключ идемпотентности возвращает ErrDuplicateKey.
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

// Transition выполняет оптимистичный переход состояния: проверяет матрицу
// домена, обновляет платеж условием state_version = expectedVersion и пишет
// строку журнала. Конфликт версии - domain.ErrStateConflict.
func (r *Repository) Transition(ctx context.Context, id string, expectedVersion uint64, to domain.State, patch TransitionPatch) (Payment, error) {
	return r.TransitionTx(ctx, nil, id, expectedVersion, to, patch)
}

// TransitionTx выполняет переход в переданной транзакции (tx=nil - своя):
// consumer канонических событий пишет переход атомарно с inbox-записью Guard.
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
		if uint64(current.StateVersion) != expectedVersion { //nolint:gosec // state_version ограничен CHECK (>= 1)
			return fmt.Errorf("%w: payment %s version %d, expected %d",
				domain.ErrStateConflict, id, current.StateVersion, expectedVersion)
		}
		from, parseErr := domain.ParseState(current.State)
		if parseErr != nil {
			return parseErr
		}
		newVersion, nextErr := domain.NextVersion(from, to, uint64(current.StateVersion)) //nolint:gosec // state_version ограничен CHECK
		if nextErr != nil {
			return nextErr
		}
		updates := patch.updates()
		updates["state"] = string(to)
		updates["state_version"] = int64(newVersion) //nolint:gosec // рост на 1 от int64-значения БД
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
			StateVersion: int64(newVersion), //nolint:gosec // рост на 1 от int64-значения БД
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

// Get возвращает платеж и его историю переходов по возрастанию версии; история
// ограничена последними maxTimelineTransitions записями (защита от
// неограниченного ответа, полная история - 14.2). Обе выборки выполняются в
// одной read-only транзакции repeatable read: конкурентный переход не должен
// вернуть timeline новее самого платежа.
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
	// Журнал читался от новых к старым (LIMIT последних); наружу - по возрастанию.
	for left, right := 0, len(history)-1; left < right; left, right = left+1, right-1 {
		history[left], history[right] = history[right], history[left]
	}
	return payment, history, nil
}

// Payment возвращает платеж без истории переходов.
func (r *Repository) Payment(ctx context.Context, id string) (Payment, error) {
	return r.PaymentTx(ctx, nil, id)
}

// PaymentTx возвращает платеж без истории в переданной транзакции
// (tx=nil - своя): consumer читает состояние в транзакции Guard.
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

// List возвращает страницу платежей мерчанта и курсор следующей страницы.
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

// FindExpired возвращает счета с истекшим сроком без оплаты для 4.6:
// состояние creating не включается, зависшие создания доводятся фоном
// до created либо failed (перехода creating -> expired в матрице нет).
func (r *Repository) FindExpired(ctx context.Context, before time.Time, limit int) ([]Payment, error) {
	return r.findByFilter(ctx,
		"state IN ? AND expires_at IS NOT NULL AND expires_at <= ?",
		[]any{[]string{string(domain.StateCreated), string(domain.StatePending)}, before.UTC()},
		"expires_at ASC", limit)
}

// FindStuck возвращает платежи в detected/confirming без движения для 4.6.
func (r *Repository) FindStuck(ctx context.Context, olderThan time.Time, limit int) ([]Payment, error) {
	return r.findByFilter(ctx,
		"state IN ? AND updated_at <= ?",
		[]any{[]string{string(domain.StateDetected), string(domain.StateConfirming)}, olderThan.UTC()},
		"updated_at ASC", limit)
}

// FindCreating возвращает зависшие создания для 4.6: платежи в creating,
// созданные не позже порога; штатное создание (4.2) укладывается в секунды,
// порог задает вызывающий (janitor).
func (r *Repository) FindCreating(ctx context.Context, olderThan time.Time, limit int) ([]Payment, error) {
	return r.findByFilter(ctx,
		"state = ? AND created_at <= ?",
		[]any{string(domain.StateCreating), olderThan.UTC()},
		"created_at ASC", limit)
}

// findByFilter выполняет выборку платежей по условию с ограничением пакета
// фоновых задач (потолок maxBackgroundLimit).
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

// updates собирает карту изменений из непустых полей патча.
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
	if p.PayAddress != nil {
		updates["pay_address"] = *p.PayAddress
	}
	if p.GatewayID != nil {
		updates["gateway_id"] = *p.GatewayID
	}
	if p.Provider != nil {
		updates["provider"] = *p.Provider
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

// encodeCursor собирает непрозрачный курсор страницы из времени и идентификатора.
func encodeCursor(createdAt time.Time, id string) string {
	raw := createdAt.UTC().Format(time.RFC3339Nano) + "|" + id
	return base64.RawURLEncoding.EncodeToString([]byte(raw))
}

// decodeCursor разбирает курсор страницы и валидирует его формат.
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

// isUniqueViolation сообщает, что ошибка БД - занятый ключ идемпотентности
// мерчанта (другие нарушения уникальности не маскируются).
func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505" &&
		pgErr.ConstraintName == "payments_merchant_idempotency_idx"
}
