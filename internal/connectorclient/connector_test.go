package connectorclient_test

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	connectorv1 "github.com/cbl/cbl-protos/gen/go/billing/connector/v1"

	"github.com/cbl/cbl-kit/money"

	"github.com/cbl/cbl-payment/internal/connectorclient"
	"github.com/cbl/cbl-payment/internal/domain"
	"github.com/cbl/cbl-payment/internal/usecase"
)

type stubConnector struct {
	connectorv1.UnimplementedConnectorServiceServer
	err  error
	noID bool
	last *connectorv1.CreateInvoiceRequest
}

func (s *stubConnector) CreateInvoice(_ context.Context, req *connectorv1.CreateInvoiceRequest) (*connectorv1.CreateInvoiceResponse, error) {
	s.last = req
	if s.err != nil {
		return nil, s.err
	}
	if s.noID {
		return &connectorv1.CreateInvoiceResponse{}, nil
	}
	return &connectorv1.CreateInvoiceResponse{
		GatewayInvoiceId: "gw-1",
		Provider:         "coingate",
		CheckoutUrl:      "https://checkout.example/1",
	}, nil
}

func startConnectorStub(t *testing.T, stub *stubConnector) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	server := grpc.NewServer()
	connectorv1.RegisterConnectorServiceServer(server, stub)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	return listener.Addr().String()
}

func TestCreateInvoiceRoundTrip(t *testing.T) {
	t.Parallel()
	stub := &stubConnector{}
	client, err := connectorclient.Dial(startConnectorStub(t, stub))
	require.NoError(t, err)
	defer func() { require.NoError(t, client.Close()) }()

	expires := time.Date(2026, 10, 18, 13, 0, 0, 0, time.UTC)
	result, err := client.CreateInvoice(context.Background(), usecase.InvoiceRequest{
		PaymentID: "01a14f70-0000-7000-8000-000000000001",
		Amount:    money.MustParse("15", "USDT", 6),
		Network:   "usdt_trc20",
		ExpiresAt: expires,
	})
	require.NoError(t, err)
	assert.Equal(t, "gw-1", result.GatewayInvoiceID)
	assert.Equal(t, "coingate", result.Provider)
	assert.Equal(t, "https://checkout.example/1", result.CheckoutURL)

	require.NotNil(t, stub.last)
	assert.Equal(t, "01a14f70-0000-7000-8000-000000000001", stub.last.GetIdempotencyKey(), "the payment id is the connector idempotency key")
	assert.Equal(t, int64(15_000_000), stub.last.GetAmount().GetMinorUnits())
	assert.Equal(t, expires, stub.last.GetExpiresAt().AsTime().UTC())
}

func TestCreateInvoiceClassification(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		code codes.Code
		want error
	}{
		{"failed precondition", codes.FailedPrecondition, domain.ErrInvoiceRejected},
		{"invalid argument", codes.InvalidArgument, domain.ErrInvoiceRejected},
		{"unavailable", codes.Unavailable, domain.ErrDependencyTemporary},
		{"deadline", codes.DeadlineExceeded, domain.ErrDependencyTemporary},
		{"not found", codes.NotFound, domain.ErrDependencyTemporary},
		{"resource exhausted", codes.ResourceExhausted, domain.ErrOverloaded},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			stub := &stubConnector{err: status.Error(tc.code, "boom")}
			client, err := connectorclient.Dial(startConnectorStub(t, stub))
			require.NoError(t, err)
			defer func() { require.NoError(t, client.Close()) }()

			_, err = client.CreateInvoice(context.Background(), usecase.InvoiceRequest{
				PaymentID: "01a14f70-0000-7000-8000-000000000001",
				Amount:    money.MustParse("15", "USDT", 6),
				Network:   "usdt_trc20",
				ExpiresAt: time.Now().Add(time.Hour),
			})
			require.ErrorIs(t, err, tc.want, tc.name)
		})
	}
}

func TestCreateInvoiceWithoutID(t *testing.T) {
	t.Parallel()
	stub := &stubConnector{noID: true}
	client, err := connectorclient.Dial(startConnectorStub(t, stub))
	require.NoError(t, err)
	defer func() { require.NoError(t, client.Close()) }()

	_, err = client.CreateInvoice(context.Background(), usecase.InvoiceRequest{
		PaymentID: "01a14f70-0000-7000-8000-000000000001",
		Amount:    money.MustParse("15", "USDT", 6),
		Network:   "usdt_trc20",
		ExpiresAt: time.Now().Add(time.Hour),
	})
	require.ErrorIs(t, err, domain.ErrDependencyTemporary)
}

func TestCreateInvoiceCanceledContext(t *testing.T) {
	t.Parallel()
	stub := &stubConnector{}
	client, err := connectorclient.Dial(startConnectorStub(t, stub))
	require.NoError(t, err)
	defer func() { require.NoError(t, client.Close()) }()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = client.CreateInvoice(ctx, usecase.InvoiceRequest{
		PaymentID: "01a14f70-0000-7000-8000-000000000001",
		Amount:    money.MustParse("15", "USDT", 6),
		Network:   "usdt_trc20",
		ExpiresAt: time.Now().Add(time.Hour),
	})
	require.ErrorIs(t, err, context.Canceled, "a dead caller context stays the caller's error")
}
