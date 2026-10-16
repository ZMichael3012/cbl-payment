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

// Default janitor configuration values.
const (
	// DefaultJanitorInterval defines the sweep period without configuration.
	DefaultJanitorInterval = 30 * time.Second
	// DefaultJanitorBatch defines the selection batch size without configuration.
	DefaultJanitorBatch = 100
	// DefaultCreatingTimeout defines the stuck creation threshold without configuration.
	DefaultCreatingTimeout = 5 * time.Minute
	// DefaultStuckTimeout defines the detected/confirming observation threshold without configuration.
	DefaultStuckTimeout = 6 * time.Hour
	// maxJanitorBatch bounds the janitor selection batch.
	maxJanitorBatch = 1000
	// maxStuckDetails bounds the number of payments detailed in WARN per sweep.
	maxStuckDetails = 10
)

// ErrInvalidJanitorConfig reports an invalid janitor configuration.
var ErrInvalidJanitorConfig = errors.New("usecase: invalid janitor config")

// JanitorStore describes the payment operations the janitor needs: candidate
// selections and optimistic transitions (CAS on state_version closes races with
// the 4.3 consumer, tasks 4.2/4.4 and other janitor instances).
type JanitorStore interface {
	// FindExpired returns created/pending invoices with a deadline not later than before.
	FindExpired(ctx context.Context, before time.Time, limit int) ([]storage.Payment, error)
	// FindCreating returns payments in creating created not later than olderThan.
	FindCreating(ctx context.Context, olderThan time.Time, limit int) ([]storage.Payment, error)
	// FindStuck returns detected/confirming payments without movement with updated_at not later than olderThan.
	FindStuck(ctx context.Context, olderThan time.Time, limit int) ([]storage.Payment, error)
	// Transition performs an optimistic state transition.
	Transition(ctx context.Context, id string, expectedVersion uint64, to domain.State, patch storage.TransitionPatch) (storage.Payment, error)
}

// JanitorConfig defines the background task parameters; zero values are replaced
// by defaults, negative ones (and Batch above the ceiling) are rejected.
type JanitorConfig struct {
	// Interval defines the sweep period.
	Interval time.Duration
	// Batch defines the selection batch size (1 to 1000).
	Batch int
	// CreatingAfter defines the stuck creation threshold.
	CreatingAfter time.Duration
	// StuckAfter defines the detected/confirming observation threshold.
	StuckAfter time.Duration
	// Clock defines the time source (nil means time.Now); a test seam.
	Clock func() time.Time
}

// JanitorResult collects the counters of one sweep.
type JanitorResult struct {
	// Expired defines the number of expired invoices.
	Expired int
	// CreatingFailed defines the number of closed stuck creations.
	CreatingFailed int
	// Stuck defines the number of observed stuck payments.
	Stuck int
	// Conflicts defines the number of transitions that lost the CAS race.
	Conflicts int
	// Errors defines the number of selection and transition errors.
	Errors int
}

// Janitor performs the payment background tasks: it expires created/pending
// invoices by the policy predicate (4.5), closes stuck creations with the failed
// state and makes stuck detected/confirming payments visible in logs without
// touching money.
type Janitor struct {
	store         JanitorStore
	log           *zap.Logger
	interval      time.Duration
	batch         int
	creatingAfter time.Duration
	stuckAfter    time.Duration
	clock         func() time.Time
}

// NewJanitor creates a Janitor; empty configuration values are replaced by
// defaults, invalid ones return ErrInvalidJanitorConfig.
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

// Run performs an immediate sweep, then repeats sweeps on a ticker until the
// context is cancelled (SIGTERM): expiry is not deferred to the first interval.
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

// Sweep performs one sweep: invoice expiry, closing of stuck creations and
// observation of stuck payments. Errors of individual payments and selections
// do not interrupt the sweep and go into the result counters; context
// cancellation finishes the sweep without errors.
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

// expireDue expires created/pending invoices: candidate selection, re-check of
// the policy predicate on the snapshot and a CAS transition to expired.
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
		// Re-check of the predicate on the snapshot: the selection itself already
		// guarantees state and deadline, but the transition runs on data read by
		// the caller and must not trust the SQL condition alone.
		if !domain.ShouldExpire(state, payment.ExpiresAt, now) {
			continue
		}
		j.transition(ctx, payment, domain.StateExpired, "janitor expired", result, &result.Expired)
	}
}

// failStuckCreating closes creations stuck longer than the threshold: failed
// makes too-early events for the payment deterministically stale (KI-payment-05).
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

// reportStuck makes stuck detected/confirming payments visible: state and money
// are not touched, remediation is 4.4 (after KI-01) and reconciliation 9.x.
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

// transition performs the janitor CAS transition, handling version conflict and errors.
func (j *Janitor) transition(ctx context.Context, payment storage.Payment, to domain.State, reason string, result *JanitorResult, applied *int) {
	version := uint64(payment.StateVersion) //nolint:gosec // state_version is bounded by CHECK (>= 1)
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

// logResult writes the sweep summary: Info on effects, Debug on an empty sweep.
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
