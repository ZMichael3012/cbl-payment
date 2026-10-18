//go:build integration

package grpcserver_test

import (
	"context"
	"net"
	"path/filepath"
	"testing"
	"time"

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

	commonv1 "github.com/cbl/cbl-protos/gen/go/billing/common/v1"
	paymentv1 "github.com/cbl/cbl-protos/gen/go/billing/payment/v1"

	"github.com/cbl/cbl-kit/grpcmw"
	"github.com/cbl/cbl-kit/storagex"

	"github.com/cbl/cbl-payment/internal/domain"
	"github.com/cbl/cbl-payment/internal/storage"
	"github.com/cbl/cbl-payment/internal/transport/grpcserver"
	"github.com/cbl/cbl-payment/internal/usecase"
)

type stubQuoteLocker struct {
	calls int
	lock  usecase.QuoteLock
}

func (s *stubQuoteLocker) LockQuote(context.Context, usecase.QuoteLockRequest) (usecase.QuoteLock, error) {
	s.calls++
	return s.lock, nil
}

type stubInvoiceCreator struct {
	calls int
	err   error
}

func (s *stubInvoiceCreator) CreateInvoice(context.Context, usecase.InvoiceRequest) (usecase.InvoiceResult, error) {
	s.calls++
	if s.err != nil {
		return usecase.InvoiceResult{}, s.err
	}
	return usecase.InvoiceResult{GatewayInvoiceID: "gw-1", Provider: "coingate", CheckoutURL: "https://checkout.example/1"}, nil
}

func setupCreationServer(t *testing.T) (paymentv1.PaymentServiceClient, *gorm.DB, *stubQuoteLocker, *stubInvoiceCreator) {
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
	quote := &stubQuoteLocker{lock: usecase.QuoteLock{
		QuoteID:     "q-1",
		Rate:        "1.5",
		LockedUntil: time.Now().UTC().Add(5 * time.Minute),
	}}
	invoices := &stubInvoiceCreator{}
	creation, err := usecase.NewPaymentCreation(repo, quote, invoices, usecase.CreationConfig{
		AssetScales: map[string]uint32{"USDT": 6},
	})
	require.NoError(t, err)

	listener := bufconn.Listen(1 << 20)
	server := grpc.NewServer(grpc.UnaryInterceptor(grpcmw.UnaryServer(zap.NewNop())))
	paymentv1.RegisterPaymentServiceServer(server, grpcserver.NewPayment(usecase.NewPaymentQuery(repo), creation))
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
	return paymentv1.NewPaymentServiceClient(conn), db, quote, invoices
}

func TestIntegrationCreatePaymentThroughTransport(t *testing.T) {
	client, db, quote, invoices := setupCreationServer(t)
	ctx := context.Background()

	response, err := client.CreatePayment(ctx, &paymentv1.CreatePaymentRequest{
		IdempotencyKey: "key-1",
		MerchantId:     "01a14f70-0000-7000-8000-0000000000aa",
		Amount:         &commonv1.Money{MinorUnits: 10_000_000, Asset: "USDT", Decimals: 6},
		PayAsset:       "USDT",
		Network:        "usdt_trc20",
		ExpiresAt:      timestamppb.New(time.Now().UTC().Add(time.Hour)),
	})
	if err != nil {
		t.Fatalf("create payment: %v", err)
	}
	require.Equal(t, paymentv1.PaymentState_PAYMENT_STATE_PENDING, response.GetState())
	require.Equal(t, int64(15_000_000), response.GetExpectedAmount().GetMinorUnits())
	require.Equal(t, "https://checkout.example/1", response.GetCheckoutUrl())
	require.Equal(t, "q-1", response.GetQuoteId())
	require.NotEmpty(t, response.GetPayAddress()+response.GetCheckoutUrl())
	require.Equal(t, 1, quote.calls)
	require.Equal(t, 1, invoices.calls)

	repo := storage.NewRepository(db)
	stored, err := repo.ByIdempotencyKey(ctx, "01a14f70-0000-7000-8000-0000000000aa", "key-1")
	require.NoError(t, err)
	require.Equal(t, string(domain.StatePending), stored.State)

	// Cancel by a repeated key: the first transition wins, the second replays.
	cancelled, err := client.CancelPayment(ctx, &paymentv1.CancelPaymentRequest{PaymentId: response.GetPaymentId(), IdempotencyKey: "cancel-1"})
	require.NoError(t, err)
	require.Equal(t, paymentv1.PaymentState_PAYMENT_STATE_CANCELLED, cancelled.GetPayment().GetState())
	again, err := client.CancelPayment(ctx, &paymentv1.CancelPaymentRequest{PaymentId: response.GetPaymentId(), IdempotencyKey: "cancel-2"})
	require.NoError(t, err)
	require.Equal(t, cancelled.GetPayment().GetStateVersion(), again.GetPayment().GetStateVersion())
	require.Equal(t, 1, quote.calls, "cancel never touches the quote")
}

func TestIntegrationCreatePaymentKeyConflict(t *testing.T) {
	client, _, _, _ := setupCreationServer(t)
	ctx := context.Background()
	first := &paymentv1.CreatePaymentRequest{
		IdempotencyKey: "key-1",
		MerchantId:     "01a14f70-0000-7000-8000-0000000000aa",
		Amount:         &commonv1.Money{MinorUnits: 10_000_000, Asset: "USDT", Decimals: 6},
		PayAsset:       "USDT",
		Network:        "usdt_trc20",
		ExpiresAt:      timestamppb.New(time.Now().UTC().Add(time.Hour)),
	}
	_, err := client.CreatePayment(ctx, first)
	require.NoError(t, err)

	second := &paymentv1.CreatePaymentRequest{
		IdempotencyKey: "key-1",
		MerchantId:     "01a14f70-0000-7000-8000-0000000000aa",
		Amount:         &commonv1.Money{MinorUnits: 11_000_000, Asset: "USDT", Decimals: 6},
		PayAsset:       "USDT",
		Network:        "usdt_trc20",
		ExpiresAt:      timestamppb.New(first.GetExpiresAt().AsTime()),
	}
	_, err = client.CreatePayment(ctx, second)
	require.Error(t, err)
	require.Equal(t, codes.AlreadyExists, status.Code(err))
}
