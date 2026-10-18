package quoteclient_test

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
	"google.golang.org/protobuf/types/known/timestamppb"

	quotev1 "github.com/cbl/cbl-protos/gen/go/billing/quote/v1"

	"github.com/cbl/cbl-kit/money"

	"github.com/cbl/cbl-payment/internal/domain"
	"github.com/cbl/cbl-payment/internal/quoteclient"
	"github.com/cbl/cbl-payment/internal/usecase"
)

type stubQuote struct {
	quotev1.UnimplementedQuoteServiceServer
	err        error
	incomplete bool
	last       *quotev1.LockQuoteRequest
}

func (s *stubQuote) LockQuote(_ context.Context, req *quotev1.LockQuoteRequest) (*quotev1.LockQuoteResponse, error) {
	s.last = req
	if s.err != nil {
		return nil, s.err
	}
	if s.incomplete {
		return &quotev1.LockQuoteResponse{QuoteId: "q-1"}, nil
	}
	return &quotev1.LockQuoteResponse{
		QuoteId:     "q-1",
		Rate:        "2.5",
		LockedUntil: timestamppb.New(time.Date(2026, 10, 18, 12, 5, 0, 0, time.UTC)),
	}, nil
}

func startQuoteStub(t *testing.T, stub *stubQuote) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	server := grpc.NewServer()
	quotev1.RegisterQuoteServiceServer(server, stub)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	return listener.Addr().String()
}

func TestLockQuoteRoundTrip(t *testing.T) {
	t.Parallel()
	stub := &stubQuote{}
	client, err := quoteclient.Dial(startQuoteStub(t, stub))
	require.NoError(t, err)
	defer func() { require.NoError(t, client.Close()) }()

	lock, err := client.LockQuote(context.Background(), usecase.QuoteLockRequest{
		FromAsset:  "USDT",
		ToAsset:    "BTC",
		Amount:     money.MustParse("10", "USDT", 6),
		TTLSeconds: 300,
	})
	require.NoError(t, err)
	assert.Equal(t, "q-1", lock.QuoteID)
	assert.Equal(t, "2.5", lock.Rate)
	assert.Equal(t, time.Date(2026, 10, 18, 12, 5, 0, 0, time.UTC), lock.LockedUntil)
	require.NotNil(t, stub.last)
	assert.Equal(t, "USDT", stub.last.GetFromAsset())
	assert.Equal(t, "BTC", stub.last.GetToAsset())
	assert.Equal(t, uint32(300), stub.last.GetTtlSeconds())
	assert.Equal(t, int64(10_000_000), stub.last.GetAmount().GetMinorUnits())
}

func TestLockQuoteClassification(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		code codes.Code
		want error
	}{
		{"invalid argument", codes.InvalidArgument, domain.ErrQuoteRejected},
		{"failed precondition", codes.FailedPrecondition, domain.ErrQuoteRejected},
		{"unavailable", codes.Unavailable, domain.ErrDependencyTemporary},
		{"deadline", codes.DeadlineExceeded, domain.ErrDependencyTemporary},
		{"internal", codes.Internal, domain.ErrDependencyTemporary},
		{"resource exhausted", codes.ResourceExhausted, domain.ErrOverloaded},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			stub := &stubQuote{err: status.Error(tc.code, "boom")}
			client, err := quoteclient.Dial(startQuoteStub(t, stub))
			require.NoError(t, err)
			defer func() { require.NoError(t, client.Close()) }()

			_, err = client.LockQuote(context.Background(), usecase.QuoteLockRequest{
				FromAsset: "USDT", ToAsset: "BTC", Amount: money.MustParse("10", "USDT", 6),
			})
			require.ErrorIs(t, err, tc.want, tc.name)
		})
	}
}

func TestLockQuoteIncompleteAnswer(t *testing.T) {
	t.Parallel()
	stub := &stubQuote{incomplete: true}
	client, err := quoteclient.Dial(startQuoteStub(t, stub))
	require.NoError(t, err)
	defer func() { require.NoError(t, client.Close()) }()

	_, err = client.LockQuote(context.Background(), usecase.QuoteLockRequest{
		FromAsset: "USDT", ToAsset: "BTC", Amount: money.MustParse("10", "USDT", 6),
	})
	require.ErrorIs(t, err, domain.ErrDependencyTemporary)
}

func TestLockQuoteCanceledContext(t *testing.T) {
	t.Parallel()
	stub := &stubQuote{}
	client, err := quoteclient.Dial(startQuoteStub(t, stub))
	require.NoError(t, err)
	defer func() { require.NoError(t, client.Close()) }()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = client.LockQuote(ctx, usecase.QuoteLockRequest{
		FromAsset: "USDT", ToAsset: "BTC", Amount: money.MustParse("10", "USDT", 6),
	})
	require.ErrorIs(t, err, context.Canceled, "a dead caller context stays the caller's error")
}
