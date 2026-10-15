package usecase

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.uber.org/zap"

	"github.com/cbl/cbl-payment/internal/domain"
	"github.com/cbl/cbl-payment/internal/storage"
)

// Значения конфигурации janitor-а по умолчанию.
const (
	// DefaultJanitorInterval задает период проходов без настройки.
	DefaultJanitorInterval = 30 * time.Second
	// DefaultJanitorBatch задает размер пакета выборок без настройки.
	DefaultJanitorBatch = 100
	// DefaultCreatingTimeout задает порог зависшего создания без настройки.
	DefaultCreatingTimeout = 5 * time.Minute
	// DefaultStuckTimeout задает порог наблюдения за detected/confirming без настройки.
	DefaultStuckTimeout = 6 * time.Hour
	// maxJanitorBatch ограничивает пакет выборок janitor-а.
	maxJanitorBatch = 1000
	// maxStuckDetails ограничивает число платежей, детализируемых в WARN за проход.
	maxStuckDetails = 10
)

// ErrInvalidJanitorConfig сообщает о некорректной конфигурации janitor-а.
var ErrInvalidJanitorConfig = errors.New("usecase: invalid janitor config")

// JanitorStore описывает операции платежей, нужные janitor-у: выборки кандидатов
// и оптимистичные переходы (CAS по state_version закрывает гонки с consumer 4.3,
// задачами 4.2/4.4 и другими экземплярами janitor-а).
type JanitorStore interface {
	// FindExpired возвращает счета created/pending со сроком не позже before.
	FindExpired(ctx context.Context, before time.Time, limit int) ([]storage.Payment, error)
	// FindCreating возвращает платежи в creating, созданные не позже olderThan.
	FindCreating(ctx context.Context, olderThan time.Time, limit int) ([]storage.Payment, error)
	// FindStuck возвращает detected/confirming без движения с updated_at не позже olderThan.
	FindStuck(ctx context.Context, olderThan time.Time, limit int) ([]storage.Payment, error)
	// Transition выполняет оптимистичный переход состояния.
	Transition(ctx context.Context, id string, expectedVersion uint64, to domain.State, patch storage.TransitionPatch) (storage.Payment, error)
}

// JanitorConfig задает параметры фоновых задач; нулевые значения заменяются
// значениями по умолчанию, отрицательные (и Batch больше потолка) отклоняются.
type JanitorConfig struct {
	// Interval задает период проходов.
	Interval time.Duration
	// Batch задает размер пакета выборок (1 - 1000).
	Batch int
	// CreatingAfter задает порог зависшего создания.
	CreatingAfter time.Duration
	// StuckAfter задает порог наблюдения за detected/confirming.
	StuckAfter time.Duration
	// Clock задает источник времени (nil - time.Now); тестовый шов.
	Clock func() time.Time
}

// JanitorResult собирает счетчики одного прохода.
type JanitorResult struct {
	// Expired задает число истекших счетов.
	Expired int
	// CreatingFailed задает число закрытых зависших созданий.
	CreatingFailed int
	// Stuck задает число замеченных зависших платежей.
	Stuck int
	// Conflicts задает число переходов, проигравших CAS-гонку.
	Conflicts int
	// Errors задает число ошибок выборок и переходов.
	Errors int
}

// Janitor выполняет фоновые задачи платежа: истекает счета created/pending по
// предикату политики (4.5), закрывает зависшие создания состоянием failed и
// делает зависшие detected/confirming видимыми в логах, не трогая деньги.
type Janitor struct {
	store         JanitorStore
	log           *zap.Logger
	interval      time.Duration
	batch         int
	creatingAfter time.Duration
	stuckAfter    time.Duration
	clock         func() time.Time
}

// NewJanitor создает Janitor; пустые значения конфигурации заменяются
// значениями по умолчанию, некорректные возвращают ErrInvalidJanitorConfig.
func NewJanitor(store JanitorStore, log *zap.Logger, cfg JanitorConfig) (*Janitor, error) {
	if store == nil {
		return nil, fmt.Errorf("%w: store is nil", ErrInvalidJanitorConfig)
	}
	if cfg.Interval < 0 || cfg.CreatingAfter < 0 || cfg.StuckAfter < 0 {
		return nil, fmt.Errorf("%w: negative duration", ErrInvalidJanitorConfig)
	}
	if cfg.Batch < 0 || cfg.Batch > maxJanitorBatch {
		return nil, fmt.Errorf("%w: batch %d", ErrInvalidJanitorConfig, cfg.Batch)
	}
	if log == nil {
		log = zap.NewNop()
	}
	janitor := &Janitor{
		store:         store,
		log:           log,
		interval:      cfg.Interval,
		batch:         cfg.Batch,
		creatingAfter: cfg.CreatingAfter,
		stuckAfter:    cfg.StuckAfter,
		clock:         cfg.Clock,
	}
	if janitor.interval == 0 {
		janitor.interval = DefaultJanitorInterval
	}
	if janitor.batch == 0 {
		janitor.batch = DefaultJanitorBatch
	}
	if janitor.creatingAfter == 0 {
		janitor.creatingAfter = DefaultCreatingTimeout
	}
	if janitor.stuckAfter == 0 {
		janitor.stuckAfter = DefaultStuckTimeout
	}
	if janitor.clock == nil {
		janitor.clock = time.Now
	}
	return janitor, nil
}

// Run выполняет немедленный проход, затем повторяет их по тикеру до отмены
// контекста (SIGTERM): истечение не откладывается на первый интервал.
func (j *Janitor) Run(ctx context.Context) {
	if ctx.Err() != nil {
		return
	}
	j.Sweep(ctx)
	ticker := time.NewTicker(j.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			j.Sweep(ctx)
		}
	}
}

// Sweep выполняет один проход: истечение счетов, закрытие зависших созданий,
// наблюдение за зависшими платежами. Ошибки отдельных платежей и выборок не
// прерывают проход и попадают в счетчики результата; отмена контекста
// завершает проход без ошибок.
func (j *Janitor) Sweep(ctx context.Context) JanitorResult {
	var result JanitorResult
	if ctx.Err() != nil {
		return result
	}
	now := j.clock().UTC()
	j.expireDue(ctx, now, &result)
	if ctx.Err() != nil {
		return result
	}
	j.failStuckCreating(ctx, now, &result)
	if ctx.Err() != nil {
		return result
	}
	j.reportStuck(ctx, now, &result)
	j.logResult(result)
	return result
}

// expireDue истекает счета created/pending: выборка кандидатов, повторная
// проверка предиката политики на снимке и CAS-переход в expired.
func (j *Janitor) expireDue(ctx context.Context, now time.Time, result *JanitorResult) {
	payments, err := j.store.FindExpired(ctx, now, j.batch)
	if err != nil {
		result.Errors++
		j.log.Warn("janitor: find expired payments failed", zap.Error(err))
		return
	}
	for _, payment := range payments {
		if ctx.Err() != nil {
			return
		}
		state, parseErr := domain.ParseState(payment.State)
		if parseErr != nil {
			result.Errors++
			j.log.Warn("janitor: skip payment with invalid state",
				zap.String("payment_id", payment.ID),
				zap.String("state", payment.State),
				zap.Error(parseErr))
			continue
		}
		// Повторная проверка предиката на снимке: сама выборка уже гарантирует
		// состояние и срок, но переход выполняется по данным, прочитанным
		// вызывающим кодом, и не должен доверять только SQL-условию.
		if !domain.ShouldExpire(state, payment.ExpiresAt, now) {
			continue
		}
		j.transition(ctx, payment, domain.StateExpired, "janitor expired", result, &result.Expired)
	}
}

// failStuckCreating закрывает создания, зависшие дольше порога: failed делает
// too-early события для платежа детерминированно устаревшими (KI-payment-05).
func (j *Janitor) failStuckCreating(ctx context.Context, now time.Time, result *JanitorResult) {
	cutoff := now.Add(-j.creatingAfter)
	payments, err := j.store.FindCreating(ctx, cutoff, j.batch)
	if err != nil {
		result.Errors++
		j.log.Warn("janitor: find stuck creating payments failed", zap.Error(err))
		return
	}
	for _, payment := range payments {
		if ctx.Err() != nil {
			return
		}
		if payment.CreatedAt.After(cutoff) {
			continue
		}
		j.transition(ctx, payment, domain.StateFailed, "janitor creating timeout", result, &result.CreatingFailed)
	}
}

// reportStuck делает зависшие detected/confirming видимыми: состояние и деньги
// не трогаются, ремедиация - 4.4 (после KI-01) и сверка 9.x.
func (j *Janitor) reportStuck(ctx context.Context, now time.Time, result *JanitorResult) {
	cutoff := now.Add(-j.stuckAfter)
	payments, err := j.store.FindStuck(ctx, cutoff, j.batch)
	if err != nil {
		result.Errors++
		j.log.Warn("janitor: find stuck payments failed", zap.Error(err))
		return
	}
	result.Stuck += len(payments)
	for i, payment := range payments {
		if ctx.Err() != nil {
			return
		}
		if i >= maxStuckDetails {
			break
		}
		j.log.Warn("janitor: payment without movement",
			zap.String("payment_id", payment.ID),
			zap.String("state", payment.State),
			zap.Int64("state_version", payment.StateVersion),
			zap.Time("updated_at", payment.UpdatedAt))
	}
	if len(payments) > maxStuckDetails {
		j.log.Warn("janitor: more stuck payments not detailed",
			zap.Int("count", len(payments)-maxStuckDetails))
	}
}

// transition выполняет CAS-переход janitor-а с учетом конфликта версии и ошибок.
func (j *Janitor) transition(ctx context.Context, payment storage.Payment, to domain.State, reason string, result *JanitorResult, applied *int) {
	version := uint64(payment.StateVersion) //nolint:gosec // state_version ограничен CHECK (>= 1)
	updated, err := j.store.Transition(ctx, payment.ID, version, to, storage.TransitionPatch{Reason: reason})
	if errors.Is(err, domain.ErrStateConflict) {
		result.Conflicts++
		j.log.Debug("janitor: transition lost version race",
			zap.String("payment_id", payment.ID),
			zap.String("to", string(to)))
		return
	}
	if err != nil {
		result.Errors++
		j.log.Warn("janitor: transition failed",
			zap.String("payment_id", payment.ID),
			zap.String("to", string(to)),
			zap.Error(err))
		return
	}
	*applied++
	j.log.Info("janitor: payment transitioned",
		zap.String("payment_id", payment.ID),
		zap.String("from", payment.State),
		zap.String("to", string(to)),
		zap.Int64("state_version", updated.StateVersion),
		zap.String("reason", reason))
}

// logResult пишет сводку прохода: Info при эффектах, Debug при пустом проходе.
func (j *Janitor) logResult(result JanitorResult) {
	if result == (JanitorResult{}) {
		j.log.Debug("janitor: sweep finished, nothing to do")
		return
	}
	j.log.Info("janitor: sweep finished",
		zap.Int("expired", result.Expired),
		zap.Int("creating_failed", result.CreatingFailed),
		zap.Int("stuck", result.Stuck),
		zap.Int("conflicts", result.Conflicts),
		zap.Int("errors", result.Errors))
}
