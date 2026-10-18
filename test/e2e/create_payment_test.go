//go:build e2e

package e2e

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/cbl/cbl-protos/gen/go/billing/common/v1"
	connectorv1 "github.com/cbl/cbl-protos/gen/go/billing/connector/v1"
	paymentv1 "github.com/cbl/cbl-protos/gen/go/billing/payment/v1"
	quotev1 "github.com/cbl/cbl-protos/gen/go/billing/quote/v1"

	"github.com/cbl/cbl-kit/grpcmw"

	"github.com/cbl/cbl-payment/internal/connectorclient"
	"github.com/cbl/cbl-payment/internal/domain"
	"github.com/cbl/cbl-payment/internal/quoteclient"
	"github.com/cbl/cbl-payment/internal/storage"
	"github.com/cbl/cbl-payment/internal/transport/grpcserver"
	"github.com/cbl/cbl-payment/internal/usecase"
)

// e2eQuoteStub answers LockQuote with a fixed rate.
type e2eQuoteStub struct {
	quotev1.UnimplementedQuoteServiceServer
	calls int
}

func (s *e2eQuoteStub) LockQuote(_ context.Context, req *quotev1.LockQuoteRequest) (*quotev1.LockQuoteResponse, error) {
	s.calls++
	return &quotev1.LockQuoteResponse{
		QuoteId:     "q-e2e-1",
		Rate:        "1.5",
		LockedUntil: timestamppb.New(time.Now().UTC().Add(5 * time.Minute)),
	}, nil
}

// e2eConnectorStub answers CreateInvoice; failCount makes the first calls fail
// with Unavailable to exercise the resume path.
type e2eConnectorStub struct {
	connectorv1.UnimplementedConnectorServiceServer
	calls     int
	failCount int
}

func (s *e2eConnectorStub) CreateInvoice(context.Context, *connectorv1.CreateInvoiceRequest) (*connectorv1.CreateInvoiceResponse, error) {
	s.calls++
	if s.calls <= s.failCount {
		return nil, status.Error(codes.Unavailable, "connector down")
	}
	return &connectorv1.CreateInvoiceResponse{
		GatewayInvoiceId: "gw-e2e-1",
		Provider:         "coingate",
		Address:          "TE2eAddress",
	}, nil
}

// startE2EStub serves one stub service on a local TCP listener.
func startE2EStub(t *testing.T, register func(*grpc.Server)) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	server := grpc.NewServer()
	register(server)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	return listener.Addr().String()
}

func TestE2ECreatePaymentContour(t *testing.T) {
	db := setupE2EPostgres(t)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	log := zap.NewNop()

	quoteStub := &e2eQuoteStub{}
	connectorStub := &e2eConnectorStub{}
	quoteAddr := startE2EStub(t, func(server *grpc.Server) { quotev1.RegisterQuoteServiceServer(server, quoteStub) })
	connectorAddr := startE2EStub(t, func(server *grpc.Server) { connectorv1.RegisterConnectorServiceServer(server, connectorStub) })

	quoteClient, err := quoteclient.Dial(quoteAddr)
	require.NoError(t, err)
	defer func() { _ = quoteClient.Close() }()
	connectorClient, err := connectorclient.Dial(connectorAddr)
	require.NoError(t, err)
	defer func() { _ = connectorClient.Close() }()

	repo := storage.NewRepository(db, storage.WithEventSink(usecase.NewPaymentEventSink(log)))
	creation, err := usecase.NewPaymentCreation(repo, quoteClient, connectorClient, usecase.CreationConfig{
		AssetScales: map[string]uint32{"USDT": 6},
		Log:         log,
	})
	require.NoError(t, err)

	// The production server maps the platform error kinds through the kit
	// middleware; the e2e uses the same interceptor so the wire codes match.
	paymentListener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	paymentServer := grpc.NewServer(grpc.UnaryInterceptor(grpcmw.UnaryServer(log)))
	paymentv1.RegisterPaymentServiceServer(paymentServer, grpcserver.NewPayment(usecase.NewPaymentQuery(repo), creation))
	go func() { _ = paymentServer.Serve(paymentListener) }()
	t.Cleanup(paymentServer.Stop)
	paymentAddr := paymentListener.Addr().String()
	conn, err := grpc.NewClient(paymentAddr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithUnaryInterceptor(grpcmw.UnaryClient(log)))
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()
	client := paymentv1.NewPaymentServiceClient(conn)

	merchantID := "01a14f70-0000-7000-8000-0000000000aa"
	expires := timestamppb.New(time.Now().UTC().Add(time.Hour))
	createReq := func(key string, minor int64) *paymentv1.CreatePaymentRequest {
		return &paymentv1.CreatePaymentRequest{
			IdempotencyKey: key,
			MerchantId:     merchantID,
			Amount:         &commonv1.Money{MinorUnits: minor, Asset: "USDT", Decimals: 6},
			PayAsset:       "USDT",
			Network:        "usdt_trc20",
			ExpiresAt:      expires,
		}
	}

	// Happy path: the quote is locked once, the invoice created once, the
	// response carries the locked pay amount and the address.
	response, err := client.CreatePayment(ctx, createReq("key-1", 10_000_000))
	require.NoError(t, err)
	assert.Equal(t, paymentv1.PaymentState_PAYMENT_STATE_PENDING, response.GetState())
	assert.Equal(t, int64(15_000_000), response.GetExpectedAmount().GetMinorUnits())
	assert.Equal(t, "TE2eAddress", response.GetPayAddress())
	assert.Equal(t, "q-e2e-1", response.GetQuoteId())
	assert.Equal(t, 1, quoteStub.calls)
	assert.Equal(t, 1, connectorStub.calls)

	// The replay returns the previous result without new external calls.
	replay, err := client.CreatePayment(ctx, createReq("key-1", 10_000_000))
	require.NoError(t, err)
	assert.Equal(t, response.GetPaymentId(), replay.GetPaymentId())
	assert.Equal(t, 1, quoteStub.calls)
	assert.Equal(t, 1, connectorStub.calls)

	// A different body under the same key is a conflict.
	_, err = client.CreatePayment(ctx, createReq("key-1", 11_000_000))
	require.Equal(t, codes.AlreadyExists, status.Code(err))

	// Connector timeout: the payment stays created; the retry resumes and
	// settles without locking a second quote (the connector replays the same
	// provider invoice by order id in production).
	connectorStub.failCount = connectorStub.calls + 1
	_, err = client.CreatePayment(ctx, createReq("key-2", 10_000_000))
	require.Equal(t, codes.Unavailable, status.Code(err))
	stored, err := repo.ByIdempotencyKey(ctx, merchantID, "key-2")
	require.NoError(t, err)
	assert.Equal(t, string(domain.StateCreated), stored.State)

	resumed, err := client.CreatePayment(ctx, createReq("key-2", 10_000_000))
	require.NoError(t, err)
	assert.Equal(t, paymentv1.PaymentState_PAYMENT_STATE_PENDING, resumed.GetState())
	assert.Equal(t, 2, quoteStub.calls, "the quote is not locked twice across the resume")

	// Cancel: pending -> cancelled once, the repeat replays the same version.
	cancelled, err := client.CancelPayment(ctx, &paymentv1.CancelPaymentRequest{PaymentId: response.GetPaymentId(), IdempotencyKey: "cancel-1"})
	require.NoError(t, err)
	assert.Equal(t, paymentv1.PaymentState_PAYMENT_STATE_CANCELLED, cancelled.GetPayment().GetState())
	again, err := client.CancelPayment(ctx, &paymentv1.CancelPaymentRequest{PaymentId: response.GetPaymentId(), IdempotencyKey: "cancel-2"})
	require.NoError(t, err)
	assert.Equal(t, cancelled.GetPayment().GetStateVersion(), again.GetPayment().GetStateVersion())

	// After detection the cancel is a precondition failure.
	detected, err := repo.Payment(ctx, resumed.GetPaymentId())
	require.NoError(t, err)
	_, err = repo.Transition(ctx, detected.ID, uint64(detected.StateVersion), domain.StateDetected, storage.TransitionPatch{Reason: "e2e detection"})
	require.NoError(t, err)
	_, err = client.CancelPayment(ctx, &paymentv1.CancelPaymentRequest{PaymentId: resumed.GetPaymentId(), IdempotencyKey: "cancel-3"})
	require.Equal(t, codes.FailedPrecondition, status.Code(err))

	// Creation without the wiring answers Unavailable fail-closed.
	bareListener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	bareServer := grpc.NewServer(grpc.UnaryInterceptor(grpcmw.UnaryServer(log)))
	paymentv1.RegisterPaymentServiceServer(bareServer, grpcserver.NewPayment(usecase.NewPaymentQuery(repo), nil))
	go func() { _ = bareServer.Serve(bareListener) }()
	t.Cleanup(bareServer.Stop)
	bareConn, err := grpc.NewClient(bareListener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer func() { _ = bareConn.Close() }()
	_, err = paymentv1.NewPaymentServiceClient(bareConn).CreatePayment(ctx, createReq("key-3", 10_000_000))
	require.Equal(t, codes.Unavailable, status.Code(err))
}
