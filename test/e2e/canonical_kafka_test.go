//go:build e2e

// Package e2e contains the end-to-end scenario of the payment canonical event
// consumer: gateway.events.canonical events from Kafka drive the payment state
// machine in PostgreSQL; duplicates are not re-applied, stale events are no-ops.
// Stand: real Kafka and PostgreSQL.
package e2e

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/kafka"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	commonv1 "github.com/ZMichael3012/cbl-protos/gen/go/billing/common/v1"
	eventsv1 "github.com/ZMichael3012/cbl-protos/gen/go/billing/events/v1"

	"github.com/ZMichael3012/cbl-kit/inbox"
	"github.com/ZMichael3012/cbl-kit/inbox/kafkaconsumer"
	"github.com/ZMichael3012/cbl-kit/outbox"
	"github.com/ZMichael3012/cbl-kit/outbox/kafkapub"
	"github.com/ZMichael3012/cbl-kit/storagex"

	"github.com/cbl/cbl-payment/internal/domain"
	"github.com/cbl/cbl-payment/internal/storage"
	"github.com/cbl/cbl-payment/internal/usecase"
)

const canonicalE2EPaymentID = "01a13d53-6bf5-707f-a944-c0d7358654f0"

func setupE2EPostgres(t *testing.T) *gorm.DB {
	t.Helper()
	ctx := context.Background()
	container, err := tcpostgres.Run(ctx,
		"postgres:16-alpine",
		tcpostgres.WithDatabase("payment_e2e"),
		tcpostgres.WithUsername("cbl"),
		tcpostgres.WithPassword("cbl"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(90*time.Second),
		),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = container.Terminate(context.Background()) })

	dsn, err := container.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	require.NoError(t, err)
	require.NoError(t, storagex.Migrate(ctx, db, filepath.Join("..", "..", "migrations")))
	return db
}

func setupE2EKafka(t *testing.T) []string {
	t.Helper()
	ctx := context.Background()
	container, err := kafka.Run(ctx,
		"confluentinc/cp-kafka:7.6.1",
		kafka.WithClusterID("cbl-payment-e2e"),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = container.Terminate(context.Background()) })

	brokers, err := container.Brokers(ctx)
	require.NoError(t, err)

	adminClient, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	require.NoError(t, err)
	defer adminClient.Close()
	admin := kadm.NewClient(adminClient)
	topic := usecase.TopicGatewayEventsCanonical
	topics := []string{
		topic, topic + inbox.RetrySuffix5s, topic + inbox.RetrySuffix1m,
		topic + inbox.RetrySuffix10m, topic + inbox.DLQSuffix,
		usecase.TopicPaymentEvents,
	}
	for _, name := range topics {
		_, err := admin.CreateTopic(ctx, 1, 1, nil, name)
		require.NoError(t, err, name)
	}
	return brokers
}

// canonicalRecord builds a Kafka record of a canonical event.
func canonicalRecord(t *testing.T, eventType eventsv1.CanonicalPaymentEventType, confirmations uint32) *kgo.Record {
	t.Helper()
	return canonicalRecordWith(t, eventType, confirmations, 100_000, "tx-e2e")
}

// canonicalRecordWith builds a canonical record with an explicit amount and txid.
func canonicalRecordWith(t *testing.T, eventType eventsv1.CanonicalPaymentEventType, confirmations uint32, amount int64, txid string) *kgo.Record {
	t.Helper()
	event := &eventsv1.CanonicalPaymentEvent{
		PaymentId:      canonicalE2EPaymentID,
		GatewayId:      "g-e2e",
		Provider:       "mock",
		Type:           eventType,
		ReceivedAmount: &commonv1.Money{MinorUnits: amount, Asset: "USDT", Decimals: 2},
		Confirmations:  confirmations,
		Txid:           txid,
		Network:        "trc20",
	}
	body, err := proto.Marshal(event)
	require.NoError(t, err)
	return &kgo.Record{
		Topic: usecase.TopicGatewayEventsCanonical,
		Key:   []byte(canonicalE2EPaymentID),
		Value: body,
		Headers: []kgo.RecordHeader{
			{Key: outbox.HeaderEventID, Value: []byte(uuid.Must(uuid.NewV7()).String())},
			{Key: outbox.HeaderEventType, Value: []byte("gateway.payment")},
			{Key: outbox.HeaderProducer, Value: []byte("cbl-connector")},
		},
	}
}

// waitForState waits for the payment state or fails on context timeout.
func waitForState(t *testing.T, ctx context.Context, repo *storage.Repository, want domain.State) storage.Payment {
	t.Helper()
	for {
		payment, _, err := repo.Get(ctx, canonicalE2EPaymentID)
		require.NoError(t, err)
		if payment.State == string(want) {
			return payment
		}
		select {
		case <-ctx.Done():
			require.FailNow(t, "payment state not reached", string(want), payment.State)
		default:
			time.Sleep(200 * time.Millisecond)
		}
	}
}

func TestCanonicalConsumerKafkaFlow(t *testing.T) {
	db := setupE2EPostgres(t)
	brokers := setupE2EKafka(t)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	repo := storage.NewRepository(db, storage.WithEventSink(usecase.NewPaymentEventSink(zap.NewNop())))
	payment := storage.Payment{
		ID:               canonicalE2EPaymentID,
		MerchantID:       "m-e2e",
		IdempotencyKey:   "e2e-1",
		RequestHash:      "hash-e2e",
		ExpectedAmount:   100_000,
		ExpectedAsset:    "USDT",
		ExpectedDecimals: 2,
		Network:          "trc20",
	}
	require.NoError(t, repo.Create(ctx, payment))
	payment, err := repo.Transition(ctx, payment.ID, 1, domain.StateCreated, storage.TransitionPatch{})
	require.NoError(t, err)
	payment, err = repo.Transition(ctx, payment.ID, uint64(payment.StateVersion), domain.StatePending, storage.TransitionPatch{})
	require.NoError(t, err)

	publisher, err := kafkapub.New(brokers)
	require.NoError(t, err)
	defer publisher.Close()

	relay := outbox.NewRelay(db, publisher, zap.NewNop(), outbox.WithInterval(50*time.Millisecond))
	go func() { _ = relay.Run(ctx) }()

	policy, err := domain.ParseAmountPolicy(`{"USDT":[{"below_minor":1000000,"confirmations":1}]}`, 0, 0)
	require.NoError(t, err)
	consumer := usecase.NewCanonicalConsumer(repo, zap.NewNop(), usecase.WithAmountPolicy(policy))
	processor := inbox.NewProcessor(usecase.ConsumerGroupCanonical, db, inbox.NewGuard(), consumer, publisher,
		inbox.NewRouter(usecase.TopicGatewayEventsCanonical, 3), zap.NewNop())
	topic := usecase.TopicGatewayEventsCanonical
	topics := []string{
		topic, topic + inbox.RetrySuffix5s, topic + inbox.RetrySuffix1m, topic + inbox.RetrySuffix10m,
	}
	runner, err := kafkaconsumer.New(brokers, usecase.ConsumerGroupCanonical, topics, processor, zap.NewNop())
	require.NoError(t, err)
	defer runner.Close()
	go func() { _ = runner.Run(ctx) }()

	producer, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	require.NoError(t, err)
	defer producer.Close()

	detected := canonicalRecord(t, eventsv1.CanonicalPaymentEventType_CANONICAL_PAYMENT_EVENT_TYPE_DETECTED, 1)
	require.NoError(t, producer.ProduceSync(ctx, detected).FirstErr())
	detectedPayment := waitForState(t, ctx, repo, domain.StateDetected)
	require.NotNil(t, detectedPayment.TxID)
	assert.Equal(t, "tx-e2e", *detectedPayment.TxID)

	// Duplicate of the same record: the inbox does not re-apply the event. The
	// check is deterministic by partition order: confirmed arrives after the
	// duplicate, so an extra self-transition from the duplicate would increment
	// the version twice.
	require.NoError(t, producer.ProduceSync(ctx, detected).FirstErr())
	confirmed := canonicalRecord(t, eventsv1.CanonicalPaymentEventType_CANONICAL_PAYMENT_EVENT_TYPE_CONFIRMED, 3)
	require.NoError(t, producer.ProduceSync(ctx, confirmed).FirstErr())
	completedPayment := waitForState(t, ctx, repo, domain.StateCompleted)
	assert.Equal(t, 3, completedPayment.Confirmations)
	assert.Equal(t, detectedPayment.StateVersion+2, completedPayment.StateVersion,
		"the duplicate created no extra transition; the confirmation completed the payment per policy")

	// Stale detected after a terminal state is a no-op.
	stale := canonicalRecord(t, eventsv1.CanonicalPaymentEventType_CANONICAL_PAYMENT_EVENT_TYPE_DETECTED, 1)
	require.NoError(t, producer.ProduceSync(ctx, stale).FirstErr())
	waitForInboxCount(t, ctx, db, 3)
	after, history, err := repo.Get(ctx, canonicalE2EPaymentID)
	require.NoError(t, err)
	assert.Equal(t, string(domain.StateCompleted), after.State)
	assert.Equal(t, completedPayment.StateVersion, after.StateVersion, "the stale event does not change the state")
	assert.Len(t, history, 6, fmt.Sprintf("history: %d", len(history)))

	// 4.7: the transitions produced exactly four events (created, detected,
	// confirming, completed); the duplicate and the stale event had no effect.
	var outboxCount int64
	require.NoError(t, db.Table("outbox").Count(&outboxCount).Error)
	require.Equal(t, int64(4), outboxCount)

	consumed := consumePaymentEvents(t, ctx, brokers, 4)
	byType := make(map[string]kgo.Record, len(consumed))
	for _, record := range consumed {
		byType[string(recordHeader(record, outbox.HeaderEventType))] = record
	}
	wantVersions := map[string]uint64{
		"payment.created": 2, "payment.detected": 4, "payment.confirming": 5, "payment.completed": 6,
	}
	require.Len(t, byType, 4, "exactly four event types: the duplicate and the stale event produced none")
	for eventType, record := range byType {
		assert.Equal(t, usecase.ProducerPayment, string(recordHeader(record, outbox.HeaderProducer)))
		assert.Equal(t, usecase.SchemaVersion, string(recordHeader(record, outbox.HeaderSchemaVersion)))
		assert.Equal(t, canonicalE2EPaymentID, string(record.Key))

		var payload eventsv1.PaymentEvent
		require.NoError(t, proto.Unmarshal(record.Value, &payload))
		assert.Equal(t, wantVersions[eventType], payload.GetStateVersion())
		assert.Equal(t, canonicalE2EPaymentID, payload.GetPaymentId())
		assert.Equal(t, "m-e2e", payload.GetMerchantId())
		assert.Equal(t, string(recordHeader(record, outbox.HeaderOccurredAt)),
			payload.GetOccurredAt().AsTime().UTC().Format(time.RFC3339Nano),
			"the occurred_at header mirrors the payload")
	}
	var completed eventsv1.PaymentEvent
	require.NoError(t, proto.Unmarshal(byType["payment.completed"].Value, &completed))
	assert.Equal(t, int64(100_000), completed.GetReceivedAmount().GetMinorUnits())
	assert.Equal(t, "tx-e2e", completed.GetTxid())

	// 17.10: a repeated confirmed deposit above the baseline attributed to the
	// parent creates a linked completed top-up child and one more completed
	// event; the hex txid is canonicalized for the identity.
	topUp := canonicalRecordWith(t, eventsv1.CanonicalPaymentEventType_CANONICAL_PAYMENT_EVENT_TYPE_CONFIRMED, 3, 150_000, "0xAbCdEf01")
	require.NoError(t, producer.ProduceSync(ctx, topUp).FirstErr())
	waitForInboxCount(t, ctx, db, 4)

	var child storage.Payment
	require.Eventually(t, func() bool {
		return db.Where("parent_payment_id = ?", canonicalE2EPaymentID).First(&child).Error == nil
	}, 30*time.Second, 200*time.Millisecond, "the top-up child must appear")
	assert.Equal(t, string(domain.StateCompleted), child.State)
	require.NotNil(t, child.ReceivedAmount)
	assert.Equal(t, int64(50_000), *child.ReceivedAmount)
	require.NotNil(t, child.TxID)
	assert.Equal(t, "abcdef01", *child.TxID, "the hex txid is lowercased and unprefixed for the identity")

	// A case-variant replay of the same txid above the baseline is a duplicate:
	// no second child appears (B1/M1).
	replay := canonicalRecordWith(t, eventsv1.CanonicalPaymentEventType_CANONICAL_PAYMENT_EVENT_TYPE_CONFIRMED, 3, 180_000, "0xabcdef01")
	require.NoError(t, producer.ProduceSync(ctx, replay).FirstErr())
	waitForInboxCount(t, ctx, db, 5)
	var childCount int64
	require.NoError(t, db.Model(&storage.Payment{}).Where("parent_payment_id = ?", canonicalE2EPaymentID).Count(&childCount).Error)
	assert.EqualValues(t, 1, childCount, "a case-variant replay must not create a second child")

	// A successive cumulative deposit credits only the part above the credited
	// baseline (B1): the second child carries the 30k delta.
	second := canonicalRecordWith(t, eventsv1.CanonicalPaymentEventType_CANONICAL_PAYMENT_EVENT_TYPE_CONFIRMED, 3, 180_000, "0xDeadBeef02")
	require.NoError(t, producer.ProduceSync(ctx, second).FirstErr())
	waitForInboxCount(t, ctx, db, 6)
	require.Eventually(t, func() bool {
		var count int64
		return db.Model(&storage.Payment{}).Where("parent_payment_id = ?", canonicalE2EPaymentID).Count(&count).Error == nil && count == 2
	}, 30*time.Second, 200*time.Millisecond, "the successive top-up child must appear")
	var secondChild storage.Payment
	require.NoError(t, db.Where("parent_payment_id = ? AND txid = ?", canonicalE2EPaymentID, "deadbeef02").First(&secondChild).Error)
	require.NotNil(t, secondChild.ReceivedAmount)
	assert.Equal(t, int64(30_000), *secondChild.ReceivedAmount)

	require.NoError(t, db.Table("outbox").Count(&outboxCount).Error)
	assert.Equal(t, int64(6), outboxCount, "each child completion extends the outbox")

	topUpEvents := consumePaymentEvents(t, ctx, brokers, 6)
	childIDs := map[string]bool{child.ID: false, secondChild.ID: false}
	for i := range topUpEvents {
		id := string(topUpEvents[i].Key)
		if _, ok := childIDs[id]; !ok {
			continue
		}
		var payload eventsv1.PaymentEvent
		require.NoError(t, proto.Unmarshal(topUpEvents[i].Value, &payload))
		assert.Equal(t, eventsv1.PaymentEventType_PAYMENT_EVENT_TYPE_COMPLETED, payload.GetType())
		childIDs[id] = true
	}
	assert.True(t, childIDs[child.ID] && childIDs[secondChild.ID], "both top-up children must reach payment.events")
}

// recordHeader returns the value of a Kafka record header.
func recordHeader(record kgo.Record, key string) []byte {
	for _, header := range record.Headers {
		if header.Key == key {
			return header.Value
		}
	}
	return nil
}

// consumePaymentEvents reads payment.events from the start of the topic until want records.
func consumePaymentEvents(t *testing.T, ctx context.Context, brokers []string, want int) []kgo.Record {
	t.Helper()
	consumer, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumeTopics(usecase.TopicPaymentEvents),
		kgo.ConsumerGroup("e2e-payment-events-"+uuid.Must(uuid.NewV7()).String()),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	require.NoError(t, err)
	defer consumer.Close()

	var records []kgo.Record
	for len(records) < want {
		fetches := consumer.PollFetches(ctx)
		if err := fetches.Err(); err != nil {
			require.FailNow(t, "poll payment.events failed", err.Error(), fmt.Sprintf("got %d of %d", len(records), want))
		}
		fetches.EachRecord(func(record *kgo.Record) {
			records = append(records, *record)
		})
	}
	return records
}

// waitForInboxCount waits for the expected number of inbox rows: a marker that
// the consumer fetched and processed the events (sleep does not verify processing).
func waitForInboxCount(t *testing.T, ctx context.Context, db *gorm.DB, want int64) {
	t.Helper()
	for {
		var count int64
		require.NoError(t, db.Table("inbox").Count(&count).Error)
		if count >= want {
			return
		}
		select {
		case <-ctx.Done():
			require.FailNow(t, "inbox rows not reached", fmt.Sprintf("want=%d got=%d", want, count))
		default:
			time.Sleep(100 * time.Millisecond)
		}
	}
}
