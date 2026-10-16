//go:build integration

package usecase_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
	"gorm.io/gorm"

	eventsv1 "github.com/cbl/cbl-protos/gen/go/billing/events/v1"

	"github.com/cbl/cbl-kit/outbox"

	"github.com/cbl/cbl-payment/internal/domain"
	"github.com/cbl/cbl-payment/internal/storage"
	"github.com/cbl/cbl-payment/internal/usecase"
)

// failingSink refuses to write the event: transition atomicity check.
type failingSink struct{ err error }

// AppendTransitionEvent always returns an error.
func (f failingSink) AppendTransitionEvent(context.Context, *gorm.DB, storage.TransitionEvent) error {
	return f.err
}

// sinkRepository assembles a repository with the real payment.events sink.
func sinkRepository(db *gorm.DB) *storage.Repository {
	return storage.NewRepository(db, storage.WithEventSink(usecase.NewPaymentEventSink(zap.NewNop())))
}

// readOutbox returns the outbox rows in insertion order.
func readOutbox(t *testing.T, db *gorm.DB) []outbox.Record {
	t.Helper()
	var records []outbox.Record
	require.NoError(t, db.Order("id").Find(&records).Error)
	return records
}

// decodeEvent decodes an outbox row into a PaymentEvent.
func decodeEvent(t *testing.T, record outbox.Record) *eventsv1.PaymentEvent {
	t.Helper()
	payload := &eventsv1.PaymentEvent{}
	require.NoError(t, proto.Unmarshal(record.Payload, payload))
	return payload
}

func TestIntegrationTransitionEmitsPaymentEvent(t *testing.T) {
	db := setupConsumerDB(t)
	repo := sinkRepository(db)
	ctx := context.Background()

	payment := newJanitorIntegrationPayment(t, repo, "events-1")
	assert.Empty(t, readOutbox(t, db), "creation does not emit an event: creating is outside the CF-2 contract")

	traceparent := "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
	traced := storage.WithTraceparent(ctx, traceparent)
	payment, err := repo.Transition(traced, payment.ID, 1, domain.StateCreated, storage.TransitionPatch{})
	require.NoError(t, err)

	records := readOutbox(t, db)
	require.Len(t, records, 1)
	record := records[0]
	assert.Equal(t, usecase.TopicPaymentEvents, record.Topic)
	assert.Equal(t, payment.ID, record.Key)
	assert.Equal(t, "payment.created", record.Headers[outbox.HeaderEventType])
	assert.Equal(t, usecase.ProducerPayment, record.Headers[outbox.HeaderProducer])
	assert.Equal(t, usecase.SchemaVersion, record.Headers[outbox.HeaderSchemaVersion])
	assert.Equal(t, traceparent, record.Headers[outbox.HeaderTraceparent])

	payload := decodeEvent(t, record)
	assert.Equal(t, eventsv1.PaymentEventType_PAYMENT_EVENT_TYPE_CREATED, payload.GetType())
	assert.Equal(t, payment.ID, payload.GetPaymentId())
	assert.Equal(t, uint64(payment.StateVersion), payload.GetStateVersion())
	assert.Equal(t, record.Headers[outbox.HeaderOccurredAt],
		payload.GetOccurredAt().AsTime().UTC().Format(time.RFC3339Nano),
		"the occurred_at header mirrors the body")

	// The transition to pending has no event type: no rows are added.
	payment, err = repo.Transition(ctx, payment.ID, uint64(payment.StateVersion), domain.StatePending, storage.TransitionPatch{})
	require.NoError(t, err)
	records = readOutbox(t, db)
	require.Len(t, records, 1, "the transition to pending has no event")

	// An invalid traceparent does not go into the envelope.
	badCtx := storage.WithTraceparent(ctx, "not-a-traceparent")
	payment, err = repo.Transition(badCtx, payment.ID, uint64(payment.StateVersion), domain.StateDetected, storage.TransitionPatch{})
	require.NoError(t, err)
	records = readOutbox(t, db)
	require.Len(t, records, 2)
	assert.Equal(t, "payment.detected", records[1].Headers[outbox.HeaderEventType])
	assert.NotContains(t, records[1].Headers, outbox.HeaderTraceparent)
}

func TestIntegrationTransitionConflictEmitsNothing(t *testing.T) {
	db := setupConsumerDB(t)
	repo := sinkRepository(db)
	ctx := context.Background()

	payment := newJanitorIntegrationPayment(t, repo, "events-2")
	payment, err := repo.Transition(ctx, payment.ID, 1, domain.StateCreated, storage.TransitionPatch{})
	require.NoError(t, err)
	require.Len(t, readOutbox(t, db), 1)

	_, err = repo.Transition(ctx, payment.ID, 1, domain.StatePending, storage.TransitionPatch{})
	require.ErrorIs(t, err, domain.ErrStateConflict)
	assert.Len(t, readOutbox(t, db), 1, "a version conflict leaves no event")

	_, history, err := repo.Get(ctx, payment.ID)
	require.NoError(t, err)
	assert.Len(t, history, 2)
	assert.Equal(t, string(domain.StateCreated), history[1].ToState)
}

func TestIntegrationSinkFailureRollsBackTransition(t *testing.T) {
	db := setupConsumerDB(t)
	repo := sinkRepository(db)
	ctx := context.Background()

	payment := newJanitorIntegrationPayment(t, repo, "events-3")
	payment, err := repo.Transition(ctx, payment.ID, 1, domain.StateCreated, storage.TransitionPatch{})
	require.NoError(t, err)

	failing := storage.NewRepository(db, storage.WithEventSink(failingSink{err: errors.New("outbox down")}))
	_, err = failing.Transition(ctx, payment.ID, uint64(payment.StateVersion), domain.StatePending, storage.TransitionPatch{})
	require.Error(t, err)

	unchanged, history, err := repo.Get(ctx, payment.ID)
	require.NoError(t, err)
	assert.Equal(t, string(domain.StateCreated), unchanged.State, "a sink failure rolls back the transition")
	assert.Equal(t, payment.StateVersion, unchanged.StateVersion)
	assert.Len(t, history, 2, "the transition journal is rolled back as well")
	assert.Len(t, readOutbox(t, db), 1)
}

func TestIntegrationConsumerChainEmitsOrderedEvents(t *testing.T) {
	db := setupConsumerDB(t)
	repo := sinkRepository(db)
	consumer := usecase.NewCanonicalConsumer(repo, zap.NewNop())
	ctx := context.Background()

	pendingPayment(t, repo)

	require.NoError(t, consumer.Handle(ctx, nil, canonicalMessage(t, detectedEvent())))
	confirmed := detectedEvent()
	confirmed.Type = eventsv1.CanonicalPaymentEventType_CANONICAL_PAYMENT_EVENT_TYPE_CONFIRMED
	confirmed.Confirmations = 4
	require.NoError(t, consumer.Handle(ctx, nil, canonicalMessage(t, confirmed)))
	confirmed.Confirmations = 3
	require.NoError(t, consumer.Handle(ctx, nil, canonicalMessage(t, confirmed)))

	records := readOutbox(t, db)
	require.Len(t, records, 4, "created, detected, confirming and the confirming self-transition")
	wantTypes := []string{"payment.created", "payment.detected", "payment.confirming", "payment.confirming"}
	wantVersions := []uint64{2, 4, 5, 6}
	for i, record := range records {
		assert.Equal(t, wantTypes[i], record.Headers[outbox.HeaderEventType])
		payload := decodeEvent(t, record)
		assert.Equal(t, wantVersions[i], payload.GetStateVersion())
		if i > 0 {
			assert.Less(t, records[i-1].ID, record.ID, "publication order by outbox id")
		}
	}
}

func TestIntegrationJanitorCreatingTimeoutEmitsFailed(t *testing.T) {
	db := setupConsumerDB(t)
	repo := sinkRepository(db)
	ctx := context.Background()

	stuckID, err := uuid.NewV7()
	require.NoError(t, err)
	require.NoError(t, db.Exec(`INSERT INTO payments
		(id, merchant_id, idempotency_key, request_hash, state, state_version,
		 expected_amount, expected_asset, expected_decimals, network, created_at, updated_at)
		VALUES (?, 'm-janitor', 'events-stuck-1', 'hash', 'creating', 1, 100000, 'USDT', 2, 'trc20',
		        now() - interval '10 minutes', now() - interval '10 minutes')`, stuckID.String()).Error)

	result := janitorAt(t, repo, time.Now().UTC()).Sweep(ctx)
	require.Equal(t, 1, result.CreatingFailed)

	records := readOutbox(t, db)
	require.Len(t, records, 1)
	assert.Equal(t, "payment.failed", records[0].Headers[outbox.HeaderEventType])
	payload := decodeEvent(t, records[0])
	assert.Equal(t, eventsv1.PaymentEventType_PAYMENT_EVENT_TYPE_FAILED, payload.GetType())
	assert.Equal(t, stuckID.String(), payload.GetPaymentId())
	assert.Equal(t, uint64(2), payload.GetStateVersion())
}

func TestIntegrationJanitorExpiryEmitsEvent(t *testing.T) {
	db := setupConsumerDB(t)
	repo := sinkRepository(db)
	ctx := context.Background()

	past := time.Now().UTC().Add(-time.Minute)
	payment := newJanitorIntegrationPayment(t, repo, "events-4")
	payment, err := repo.Transition(ctx, payment.ID, 1, domain.StateCreated, storage.TransitionPatch{ExpiresAt: &past})
	require.NoError(t, err)

	janitor := janitorAt(t, repo, time.Now().UTC())
	result := janitor.Sweep(ctx)
	require.Equal(t, 1, result.Expired)

	records := readOutbox(t, db)
	require.Len(t, records, 2)
	assert.Equal(t, "payment.created", records[0].Headers[outbox.HeaderEventType])
	assert.Equal(t, "payment.expired", records[1].Headers[outbox.HeaderEventType])
	payload := decodeEvent(t, records[1])
	assert.Equal(t, eventsv1.PaymentEventType_PAYMENT_EVENT_TYPE_EXPIRED, payload.GetType())
	assert.Equal(t, payment.ID, payload.GetPaymentId())
}
