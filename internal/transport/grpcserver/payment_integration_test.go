//go:build integration

package grpcserver_test

import (
	"context"
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/timestamppb"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"

	paymentv1 "github.com/cbl/cbl-protos/gen/go/billing/payment/v1"

	"github.com/cbl/cbl-kit/grpcmw"
	"github.com/cbl/cbl-kit/storagex"

	"github.com/cbl/cbl-payment/internal/domain"
	"github.com/cbl/cbl-payment/internal/storage"
	"github.com/cbl/cbl-payment/internal/transport/grpcserver"
	"github.com/cbl/cbl-payment/internal/usecase"
)

// setupPaymentServer starts a test PaymentService on bufconn over a real
// PostgreSQL (testcontainers).
func setupPaymentServer(t *testing.T) (paymentv1.PaymentServiceClient, *gorm.DB) {
	t.Helper()
	ctx := context.Background()
	container, err := tcpostgres.Run(ctx,
		"postgres:16-alpine",
		tcpostgres.WithDatabase("payment"),
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
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	require.NoError(t, err)
	require.NoError(t, storagex.Migrate(ctx, db, filepath.Join("..", "..", "..", "migrations")))

	repo := storage.NewRepository(db)
	query := usecase.NewPaymentQuery(repo)

	listener := bufconn.Listen(1 << 20)
	server := grpc.NewServer(grpc.UnaryInterceptor(grpcmw.UnaryServer(zap.NewNop())))
	paymentv1.RegisterPaymentServiceServer(server, grpcserver.NewPayment(query, nil))
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return listener.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	return paymentv1.NewPaymentServiceClient(conn), db
}

// seedGrpcPayment creates a payment through the state machine (the creation
// journal is written) and locks the quote with the creating -> created
// transition.
func seedGrpcPayment(t *testing.T, db *gorm.DB, merchantID, key string, rate string, lockedUntil time.Time) storage.Payment {
	t.Helper()
	ctx := context.Background()
	repo := storage.NewRepository(db)
	id := uuid.Must(uuid.NewV7())
	payment := storage.Payment{
		ID:               id.String(),
		MerchantID:       merchantID,
		IdempotencyKey:   key,
		RequestHash:      "hash-" + key,
		ExpectedAmount:   100_000,
		ExpectedAsset:    "USDT",
		ExpectedDecimals: 2,
		Network:          "trc20",
	}
	require.NoError(t, repo.Create(ctx, payment))
	created, err := repo.Payment(ctx, payment.ID)
	require.NoError(t, err)
	quoteID := uuid.Must(uuid.NewV7()).String()
	expiresAt := lockedUntil.Add(15 * time.Minute)
	updated, err := repo.Transition(ctx, created.ID, uint64(created.StateVersion), domain.StateCreated, storage.TransitionPatch{
		QuoteID:          &quoteID,
		Rate:             &rate,
		QuoteLockedUntil: &lockedUntil,
		ExpiresAt:        &expiresAt,
		PayAddress:       strPtr("TJRabPrwbZy45sbavfcjinPJC18kjpRTv8"),
		Reason:           "quote fixed",
	})
	require.NoError(t, err)
	return updated
}

// strPtr returns a pointer to the string for transition patches.
func strPtr(value string) *string { return &value }

func TestIntegrationGrpcGetPayment(t *testing.T) {
	client, db := setupPaymentServer(t)
	ctx := context.Background()

	lockedUntil := time.Now().UTC().Add(15 * time.Minute).Truncate(time.Microsecond)
	seed := seedGrpcPayment(t, db, "m-1", "grpc-get-1", "78.125", lockedUntil)

	response, err := client.GetPayment(ctx, &paymentv1.GetPaymentRequest{PaymentId: seed.ID})
	require.NoError(t, err)
	payment := response.GetPayment()
	require.NotNil(t, payment)
	assert.Equal(t, seed.ID, payment.GetPaymentId())
	assert.Equal(t, "m-1", payment.GetMerchantId())
	assert.Equal(t, paymentv1.PaymentState_PAYMENT_STATE_CREATED, payment.GetState())
	assert.Equal(t, "78.125", payment.GetRate())
	require.NotNil(t, payment.GetLockedUntil())
	assert.True(t, payment.GetLockedUntil().AsTime().Equal(lockedUntil))
	require.NotNil(t, seed.QuoteID)
	assert.Equal(t, *seed.QuoteID, payment.GetQuoteId())
	assert.Equal(t, int64(100_000), payment.GetExpectedAmount().GetMinorUnits())
	assert.Equal(t, "USDT", payment.GetExpectedAmount().GetAsset())
	assert.Equal(t, uint32(2), payment.GetExpectedAmount().GetDecimals())
	assert.Equal(t, "TJRabPrwbZy45sbavfcjinPJC18kjpRTv8", payment.GetPayAddress())
	assert.Equal(t, uint64(2), payment.GetStateVersion())
	require.Len(t, payment.GetTimeline(), 2, "creation and quote locking")
	assert.Equal(t, paymentv1.PaymentState_PAYMENT_STATE_CREATING, payment.GetTimeline()[0].GetState())
	assert.Equal(t, paymentv1.PaymentState_PAYMENT_STATE_CREATED, payment.GetTimeline()[1].GetState())
	assert.Equal(t, "quote fixed", payment.GetTimeline()[1].GetComment(), "comment is the transition reason")

	// Network data comes from the consumer transitions (txid, confirmations,
	// amount).
	repo := storage.NewRepository(db)
	_, err = repo.Transition(ctx, seed.ID, 2, domain.StatePending, storage.TransitionPatch{})
	require.NoError(t, err)
	txid := "tx-abc"
	received := int64(99_000)
	receivedAsset := "USDT"
	receivedDecimals := 2
	confirmations := 6
	_, err = repo.Transition(ctx, seed.ID, 3, domain.StateDetected, storage.TransitionPatch{
		TxID:             &txid,
		ReceivedAmount:   &received,
		ReceivedAsset:    &receivedAsset,
		ReceivedDecimals: &receivedDecimals,
		Confirmations:    &confirmations,
	})
	require.NoError(t, err)

	response, err = client.GetPayment(ctx, &paymentv1.GetPaymentRequest{PaymentId: seed.ID})
	require.NoError(t, err)
	payment = response.GetPayment()
	assert.Equal(t, "tx-abc", payment.GetTxid())
	assert.Equal(t, uint32(6), payment.GetConfirmations())
	assert.Equal(t, int64(99_000), payment.GetReceivedAmount().GetMinorUnits())
	assert.Equal(t, "USDT", payment.GetReceivedAmount().GetAsset())
	require.Len(t, payment.GetTimeline(), 4)

	_, err = client.GetPayment(ctx, &paymentv1.GetPaymentRequest{PaymentId: uuid.Must(uuid.NewV7()).String()})
	require.Equal(t, codes.NotFound, status.Code(err))
}

// TestIntegrationGrpcGetPaymentWithoutQuote checks the nil mapping paths:
// a creation that has not reached quote locking is returned without
// rate/locked_until.
func TestIntegrationGrpcGetPaymentWithoutQuote(t *testing.T) {
	client, db := setupPaymentServer(t)
	ctx := context.Background()

	repo := storage.NewRepository(db)
	id := uuid.Must(uuid.NewV7())
	require.NoError(t, repo.Create(ctx, storage.Payment{
		ID:               id.String(),
		MerchantID:       "m-plain",
		IdempotencyKey:   "grpc-plain-1",
		RequestHash:      "hash-plain",
		ExpectedAmount:   50_000,
		ExpectedAsset:    "USDT",
		ExpectedDecimals: 2,
		Network:          "trc20",
	}))

	response, err := client.GetPayment(ctx, &paymentv1.GetPaymentRequest{PaymentId: id.String()})
	require.NoError(t, err)
	payment := response.GetPayment()
	assert.Equal(t, paymentv1.PaymentState_PAYMENT_STATE_CREATING, payment.GetState())
	assert.Empty(t, payment.GetRate())
	assert.Nil(t, payment.GetLockedUntil())
	assert.Empty(t, payment.GetQuoteId())
	assert.Empty(t, payment.GetTxid())
	assert.Zero(t, payment.GetConfirmations())
	assert.Nil(t, payment.GetReceivedAmount())
	require.Len(t, payment.GetTimeline(), 1, "creation only")
}

func TestIntegrationGrpcListPayments(t *testing.T) {
	client, db := setupPaymentServer(t)
	ctx := context.Background()

	// Spread the creations in time so that the period and order are
	// deterministic.
	first := seedGrpcPayment(t, db, "m-list", "grpc-list-0", "1.1", time.Now().UTC().Add(10*time.Minute))
	time.Sleep(10 * time.Millisecond)
	second := seedGrpcPayment(t, db, "m-list", "grpc-list-1", "2.2", time.Now().UTC().Add(20*time.Minute))
	time.Sleep(10 * time.Millisecond)
	third := seedGrpcPayment(t, db, "m-list", "grpc-list-2", "3.3", time.Now().UTC().Add(30*time.Minute))
	seedGrpcPayment(t, db, "m-other", "grpc-list-other", "4.4", time.Now().UTC().Add(10*time.Minute))

	from := second.CreatedAt
	page, err := client.ListPayments(ctx, &paymentv1.ListPaymentsRequest{
		MerchantId:  "m-list",
		CreatedFrom: timestamppb.New(from),
	})
	require.NoError(t, err)
	require.Len(t, page.GetPayments(), 2, "merchant filter and period from inclusive")
	assert.Equal(t, third.ID, page.GetPayments()[0].GetPaymentId(), "created_at DESC order")
	assert.Equal(t, second.ID, page.GetPayments()[1].GetPaymentId())
	assert.Empty(t, page.GetPayments()[0].GetTimeline(), "the list carries no timeline")

	paged, err := client.ListPayments(ctx, &paymentv1.ListPaymentsRequest{
		MerchantId:  "m-list",
		CreatedFrom: timestamppb.New(from),
		Limit:       1,
	})
	require.NoError(t, err)
	require.Len(t, paged.GetPayments(), 1)
	require.NotEmpty(t, paged.GetNextCursor())

	next, err := client.ListPayments(ctx, &paymentv1.ListPaymentsRequest{
		MerchantId:  "m-list",
		CreatedFrom: timestamppb.New(from),
		Limit:       1,
		Cursor:      paged.GetNextCursor(),
	})
	require.NoError(t, err)
	require.Len(t, next.GetPayments(), 1)
	assert.Equal(t, second.ID, next.GetPayments()[0].GetPaymentId())
	assert.Empty(t, next.GetNextCursor())

	toPage, err := client.ListPayments(ctx, &paymentv1.ListPaymentsRequest{
		MerchantId: "m-list",
		CreatedTo:  timestamppb.New(third.CreatedAt),
	})
	require.NoError(t, err)
	require.Len(t, toPage.GetPayments(), 2, "to exclusive: the third payment is not included")
	assert.Equal(t, second.ID, toPage.GetPayments()[0].GetPaymentId())

	empty, err := client.ListPayments(ctx, &paymentv1.ListPaymentsRequest{
		MerchantId: "m-list",
		CreatedTo:  timestamppb.New(first.CreatedAt),
	})
	require.NoError(t, err)
	assert.Empty(t, empty.GetPayments(), "an entry exactly on the to boundary is not included")

	statePage, err := client.ListPayments(ctx, &paymentv1.ListPaymentsRequest{
		MerchantId:  "m-list",
		StateFilter: paymentv1.PaymentState_PAYMENT_STATE_CREATED,
	})
	require.NoError(t, err)
	require.Len(t, statePage.GetPayments(), 3)

	unknown, err := client.ListPayments(ctx, &paymentv1.ListPaymentsRequest{MerchantId: "m-none"})
	require.NoError(t, err)
	assert.Empty(t, unknown.GetPayments(), "an unknown merchant gives an empty list, not a 404")

	_, err = client.ListPayments(ctx, &paymentv1.ListPaymentsRequest{MerchantId: "m-list", Cursor: "junk"})
	require.Equal(t, codes.InvalidArgument, status.Code(err))
}
