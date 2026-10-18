//go:build integration

package usecase_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
	"gorm.io/gorm"

	commonv1 "github.com/cbl/cbl-protos/gen/go/billing/common/v1"
	eventsv1 "github.com/cbl/cbl-protos/gen/go/billing/events/v1"

	"github.com/cbl/cbl-kit/inbox"

	"github.com/cbl/cbl-payment/internal/domain"
	"github.com/cbl/cbl-payment/internal/storage"
	"github.com/cbl/cbl-payment/internal/usecase"
)

// completedParent drives a fresh payment to completed with the received amount.
func completedParent(t *testing.T, repo *storage.Repository, received int64) storage.Payment {
	t.Helper()
	ctx := context.Background()
	payment := pendingPayment(t, repo)
	patch := storage.TransitionPatch{ReceivedAmount: &received}
	payment, err := repo.Transition(ctx, payment.ID, uint64(payment.StateVersion), domain.StateDetected, patch)
	require.NoError(t, err)
	payment, err = repo.Transition(ctx, payment.ID, uint64(payment.StateVersion), domain.StateConfirming, storage.TransitionPatch{})
	require.NoError(t, err)
	now := time.Now().UTC()
	payment, err = repo.Transition(ctx, payment.ID, uint64(payment.StateVersion), domain.StateCompleted, storage.TransitionPatch{CompletedAt: &now})
	require.NoError(t, err)
	return payment
}

// topUpEvent builds a repeated-deposit confirmed event above the parent amount.
func topUpEvent(amount int64, txid string) *eventsv1.CanonicalPaymentEvent {
	event := detectedEvent()
	event.Type = eventsv1.CanonicalPaymentEventType_CANONICAL_PAYMENT_EVENT_TYPE_CONFIRMED
	event.ReceivedAmount = &commonv1.Money{MinorUnits: amount, Asset: "USDT", Decimals: 2}
	event.Txid = txid
	event.RawStatus = "finished"
	event.Provider = "nowpayments"
	return event
}

func TestIntegrationTopUpCreatesLinkedChild(t *testing.T) {
	db := setupConsumerDB(t)
	repo := sinkRepository(db)
	core, observed := observer.New(zap.WarnLevel)
	consumer := usecase.NewCanonicalConsumer(repo, zap.New(core))
	ctx := context.Background()
	parent := completedParent(t, repo, 100_000)

	require.NoError(t, consumer.Handle(ctx, nil, canonicalMessage(t, topUpEvent(150_000, "tx-topup-1"))))

	var child storage.Payment
	require.NoError(t, db.Where("parent_payment_id = ?", parent.ID).First(&child).Error)
	assert.Equal(t, string(domain.StateCompleted), child.State)
	assert.EqualValues(t, 1, child.StateVersion)
	assert.Equal(t, int64(50_000), child.ExpectedAmount)
	require.NotNil(t, child.ReceivedAmount)
	assert.Equal(t, int64(50_000), *child.ReceivedAmount)
	require.NotNil(t, child.Provider)
	assert.Equal(t, "nowpayments", *child.Provider)
	require.NotNil(t, child.RawStatus)
	assert.Equal(t, "finished", *child.RawStatus)
	require.NotNil(t, child.CompletedAt)
	assert.Equal(t, "trc20", child.Network)
	require.NotNil(t, child.GatewayID)
	assert.Equal(t, "g-1", *child.GatewayID)
	require.NotNil(t, child.TxID)
	assert.Equal(t, "tx-topup-1", *child.TxID)

	var transitions []storage.Transition
	require.NoError(t, db.Where("payment_id = ?", child.ID).Order("state_version").Find(&transitions).Error)
	require.Len(t, transitions, 1)
	assert.Equal(t, "", transitions[0].FromState)
	assert.Equal(t, string(domain.StateCompleted), transitions[0].ToState)
	assert.Contains(t, transitions[0].Reason, "topup deposit")

	// The parent stays untouched: the delta lives on the child.
	reloaded, _, err := repo.Get(ctx, parent.ID)
	require.NoError(t, err)
	require.NotNil(t, reloaded.ReceivedAmount)
	assert.Equal(t, int64(100_000), *reloaded.ReceivedAmount)
	assert.Equal(t, parent.StateVersion, reloaded.StateVersion)

	// The child completed event reached the outbox.
	found := false
	for _, record := range readOutbox(t, db) {
		payload := decodeEvent(t, record)
		if payload.GetPaymentId() == child.ID {
			found = true
			assert.Equal(t, eventsv1.PaymentEventType_PAYMENT_EVENT_TYPE_COMPLETED, payload.GetType())
		}
	}
	assert.True(t, found, "the child payment.completed event must be in the outbox")

	// A duplicate delivery (new event id, same txid) above the credited
	// baseline hits the identity barrier: one child, acknowledged with the
	// duplicate warning.
	require.NoError(t, consumer.Handle(ctx, nil, canonicalMessage(t, topUpEvent(180_000, "tx-topup-1"))))
	var count int64
	require.NoError(t, db.Model(&storage.Payment{}).Where("parent_payment_id = ?", parent.ID).Count(&count).Error)
	assert.EqualValues(t, 1, count, "the duplicate txid must not create a second child")

	warned := false
	for _, entry := range observed.All() {
		if entry.Message == "topup duplicate skipped" {
			warned = true
		}
	}
	assert.True(t, warned, "the duplicate is acknowledged with the topup duplicate skipped warning")
}

func TestIntegrationTopUpSuccessiveCumulative(t *testing.T) {
	db := setupConsumerDB(t)
	repo := sinkRepository(db)
	consumer := usecase.NewCanonicalConsumer(repo, zap.NewNop())
	ctx := context.Background()
	parent := completedParent(t, repo, 100_000)

	require.NoError(t, consumer.Handle(ctx, nil, canonicalMessage(t, topUpEvent(150_000, "tx-succ-1"))))
	require.NoError(t, consumer.Handle(ctx, nil, canonicalMessage(t, topUpEvent(180_000, "tx-succ-2"))))

	var children []storage.Payment
	require.NoError(t, db.Where("parent_payment_id = ?", parent.ID).Find(&children).Error)
	require.Len(t, children, 2)
	deltas := map[int64]bool{}
	for _, child := range children {
		require.NotNil(t, child.ReceivedAmount)
		deltas[*child.ReceivedAmount] = true
	}
	assert.True(t, deltas[50_000] && deltas[30_000],
		"successive cumulative events credit 50k and 30k, got %v", deltas)
}

func TestIntegrationTopUpCaseVariantTxidDeduplicated(t *testing.T) {
	db := setupConsumerDB(t)
	repo := sinkRepository(db)
	consumer := usecase.NewCanonicalConsumer(repo, zap.NewNop())
	ctx := context.Background()
	parent := completedParent(t, repo, 100_000)

	require.NoError(t, consumer.Handle(ctx, nil, canonicalMessage(t, topUpEvent(150_000, "0xAbCdEf01"))))
	require.NoError(t, consumer.Handle(ctx, nil, canonicalMessage(t, topUpEvent(180_000, "0xabcdef01"))))

	var count int64
	require.NoError(t, db.Model(&storage.Payment{}).Where("parent_payment_id = ?", parent.ID).Count(&count).Error)
	assert.EqualValues(t, 1, count, "a case-variant replay of the same hex txid is one identity")

	var child storage.Payment
	require.NoError(t, db.Where("parent_payment_id = ?", parent.ID).First(&child).Error)
	require.NotNil(t, child.TxID)
	assert.Equal(t, "abcdef01", *child.TxID, "the stored txid is canonicalized")
}

func TestIntegrationTopUpConcurrentEventsSerialized(t *testing.T) {
	db := setupConsumerDB(t)
	repo := sinkRepository(db)
	consumer := usecase.NewCanonicalConsumer(repo, zap.NewNop())
	ctx := context.Background()
	parent := completedParent(t, repo, 100_000)

	guard := inbox.NewGuard()
	events := []inbox.Message{
		canonicalMessage(t, topUpEvent(150_000, "tx-conc-1")),
		canonicalMessage(t, topUpEvent(180_000, "tx-conc-2")),
	}
	var wg sync.WaitGroup
	errs := make(chan error, len(events))
	for i := range events {
		msg := events[i]
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := guard.Process(ctx, db, usecase.ConsumerGroupCanonical, uuid.NewString(), func(tx *gorm.DB) error {
				return consumer.Handle(ctx, tx, msg)
			})
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}

	var children []storage.Payment
	require.NoError(t, db.Where("parent_payment_id = ?", parent.ID).Find(&children).Error)
	// The processing order is free: the smaller event first yields children
	// 50k and 30k, the larger first makes the smaller one stale and yields one
	// 80k child. The invariant is the total credited = max(event) - parent; a
	// missing lock would credit 50k + 80k = 130k.
	require.NotEmpty(t, children)
	require.LessOrEqual(t, len(children), 2)
	var total int64
	for _, child := range children {
		require.NotNil(t, child.ReceivedAmount)
		total += *child.ReceivedAmount
	}
	assert.EqualValues(t, 80_000, total,
		"concurrent cumulative events credit only max(event) - parent (the parent row lock serializes the baseline)")
}

func TestIntegrationTopUpWithoutTxidIsAcknowledged(t *testing.T) {
	db := setupConsumerDB(t)
	repo := sinkRepository(db)
	consumer := usecase.NewCanonicalConsumer(repo, zap.NewNop())
	ctx := context.Background()
	parent := completedParent(t, repo, 100_000)

	require.NoError(t, consumer.Handle(ctx, nil, canonicalMessage(t, topUpEvent(150_000, ""))))
	var count int64
	require.NoError(t, db.Model(&storage.Payment{}).Where("parent_payment_id = ?", parent.ID).Count(&count).Error)
	assert.Zero(t, count, "a deposit without a txid cannot be deduplicated and stays for manual review")
}

func TestIntegrationRepeatedBelowParentIsStale(t *testing.T) {
	db := setupConsumerDB(t)
	repo := sinkRepository(db)
	consumer := usecase.NewCanonicalConsumer(repo, zap.NewNop())
	ctx := context.Background()
	parent := completedParent(t, repo, 100_000)

	require.NoError(t, consumer.Handle(ctx, nil, canonicalMessage(t, topUpEvent(100_000, "tx-same"))))
	var count int64
	require.NoError(t, db.Model(&storage.Payment{}).Where("parent_payment_id = ?", parent.ID).Count(&count).Error)
	assert.Zero(t, count)
}

func TestIntegrationFeeSplitPersisted(t *testing.T) {
	db := setupConsumerDB(t)
	repo := sinkRepository(db)
	consumer := usecase.NewCanonicalConsumer(repo, zap.NewNop())
	ctx := context.Background()
	payment := pendingPayment(t, repo)

	event := detectedEvent()
	event.Type = eventsv1.CanonicalPaymentEventType_CANONICAL_PAYMENT_EVENT_TYPE_CONFIRMED
	event.RawStatus = "Settled"
	event.GatewayFee = &commonv1.Money{MinorUnits: 100, Asset: "USDT", Decimals: 2}
	event.NetworkFee = &commonv1.Money{MinorUnits: 200, Asset: "USDT", Decimals: 2}
	event.FxSpread = &commonv1.Money{MinorUnits: 300, Asset: "USDT", Decimals: 2}
	require.NoError(t, consumer.Handle(ctx, nil, canonicalMessage(t, event)))

	stored, _, err := repo.Get(ctx, payment.ID)
	require.NoError(t, err)
	require.NotNil(t, stored.FeeAmount)
	assert.Equal(t, int64(600), *stored.FeeAmount)
	require.NotNil(t, stored.GatewayFee)
	assert.Equal(t, int64(100), *stored.GatewayFee)
	require.NotNil(t, stored.NetworkFee)
	assert.Equal(t, int64(200), *stored.NetworkFee)
	require.NotNil(t, stored.FxSpread)
	assert.Equal(t, int64(300), *stored.FxSpread)
	require.NotNil(t, stored.RawStatus)
	assert.Equal(t, "Settled", *stored.RawStatus)

	// The split reaches the payment.events payload (I-04): the aggregate fee is
	// the sum of the components, the provider identity travels along. The data
	// patch lands on the last transition of the canonical event plan.
	found := false
	for _, record := range readOutbox(t, db) {
		payload := decodeEvent(t, record)
		if payload.GetPaymentId() != payment.ID || payload.GetFee() == nil {
			continue
		}
		found = true
		assert.Equal(t, int64(600), payload.GetFee().GetMinorUnits())
		assert.Equal(t, int64(100), payload.GetGatewayFee().GetMinorUnits())
		assert.Equal(t, int64(200), payload.GetNetworkFee().GetMinorUnits())
		assert.Equal(t, int64(300), payload.GetFxSpread().GetMinorUnits())
		assert.Equal(t, "Settled", payload.GetRawStatus())
		assert.Equal(t, "mock", payload.GetProvider())
	}
	assert.True(t, found, "the split must reach the payment.events payload")
}

func TestIntegrationFeeSplitRejectsForeignScale(t *testing.T) {
	db := setupConsumerDB(t)
	repo := sinkRepository(db)
	consumer := usecase.NewCanonicalConsumer(repo, zap.NewNop())
	ctx := context.Background()
	pendingPayment(t, repo)

	event := detectedEvent()
	event.Type = eventsv1.CanonicalPaymentEventType_CANONICAL_PAYMENT_EVENT_TYPE_CONFIRMED
	event.NetworkFee = &commonv1.Money{MinorUnits: 50, Asset: "", Decimals: 0}
	err := consumer.Handle(ctx, nil, canonicalMessage(t, event))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "network_fee asset does not match the payment")
}

func TestIntegrationWrongAssetReason(t *testing.T) {
	db := setupConsumerDB(t)
	repo := sinkRepository(db)
	consumer := usecase.NewCanonicalConsumer(repo, zap.NewNop())
	ctx := context.Background()
	pendingPayment(t, repo)

	event := detectedEvent()
	event.ReceivedAmount = &commonv1.Money{MinorUnits: 100_000, Asset: "BTC", Decimals: 8}
	err := consumer.Handle(ctx, nil, canonicalMessage(t, event))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "wrong asset deposit")
}

func TestIntegrationDerivedInsertGuard(t *testing.T) {
	db := setupConsumerDB(t)
	repo := storage.NewRepository(db)
	parent := completedParent(t, repo, 100_000)
	now := time.Now().UTC()

	bad := storage.Payment{
		ID: uuid.NewString(), MerchantID: "m-guard", IdempotencyKey: "topup-guard-1",
		RequestHash: "h-guard", State: string(domain.StateCompleted), StateVersion: 1,
		ExpectedAmount: 1, ExpectedAsset: "USDT", ExpectedDecimals: 2, Network: "trc20",
		CompletedAt: &now,
	}
	require.Error(t, db.Create(&bad).Error, "a completed row without a parent must be rejected by the insert guard")

	bad.ParentPaymentID = &parent.ID
	require.NoError(t, db.Create(&bad).Error, "a derived completed row with a parent is allowed")

	bad2 := storage.Payment{
		ID: uuid.NewString(), MerchantID: "m-guard", IdempotencyKey: "topup-guard-2",
		RequestHash: "h-guard", State: string(domain.StateCompleted), StateVersion: 2,
		ExpectedAmount: 1, ExpectedAsset: "USDT", ExpectedDecimals: 2, Network: "trc20",
		CompletedAt: &now, ParentPaymentID: &parent.ID,
	}
	require.Error(t, db.Create(&bad2).Error, "the derived row must start at state_version 1")

	// The parent must exist and be in a terminal-success state: a fresh pending
	// payment (its own identity, not the fixed canonical one) is rejected.
	ctx := context.Background()
	nonTerminal := storage.Payment{
		ID: uuid.NewString(), MerchantID: "m-guard", IdempotencyKey: "guard-parent-" + uuid.NewString(),
		RequestHash: "h-guard", ExpectedAmount: 1, ExpectedAsset: "USDT", ExpectedDecimals: 2, Network: "trc20",
	}
	require.NoError(t, repo.Create(ctx, nonTerminal))
	nonTerminal, err := repo.Transition(ctx, nonTerminal.ID, 1, domain.StateCreated, storage.TransitionPatch{})
	require.NoError(t, err)
	nonTerminal, err = repo.Transition(ctx, nonTerminal.ID, uint64(nonTerminal.StateVersion), domain.StatePending, storage.TransitionPatch{})
	require.NoError(t, err)

	bad3 := storage.Payment{
		ID: uuid.NewString(), MerchantID: "m-guard", IdempotencyKey: "topup-guard-3",
		RequestHash: "h-guard", State: string(domain.StateCompleted), StateVersion: 1,
		ExpectedAmount: 1, ExpectedAsset: "USDT", ExpectedDecimals: 2, Network: "trc20",
		CompletedAt: &now, ParentPaymentID: &nonTerminal.ID,
	}
	require.Error(t, db.Create(&bad3).Error, "the parent must be in a terminal-success state")

	// A derived row cannot reference itself.
	selfID := uuid.NewString()
	self := storage.Payment{
		ID: selfID, MerchantID: "m-guard", IdempotencyKey: "topup-guard-4",
		RequestHash: "h-guard", State: string(domain.StateCompleted), StateVersion: 1,
		ExpectedAmount: 1, ExpectedAsset: "USDT", ExpectedDecimals: 2, Network: "trc20",
		CompletedAt: &now, ParentPaymentID: &selfID,
	}
	require.Error(t, db.Create(&self).Error, "a derived payment cannot be its own parent")

	// The parent link is immutable after creation.
	require.Error(t, db.Model(&storage.Payment{}).Where("id = ?", bad.ID).
		Update("parent_payment_id", nonTerminal.ID).Error, "the parent link cannot change")
}
