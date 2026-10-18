// Package quoteclient adapts QuoteService/LockQuote for the payment creation
// flow (4.2).
package quoteclient

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	commonv1 "github.com/cbl/cbl-protos/gen/go/billing/common/v1"
	quotev1 "github.com/cbl/cbl-protos/gen/go/billing/quote/v1"

	"github.com/cbl/cbl-payment/internal/domain"
	"github.com/cbl/cbl-payment/internal/usecase"
)

// lockBudget bounds one LockQuote call; the quote service answers within
// 250 ms and the CreatePayment contract budget is 6 s (CBL-003).
const (
	lockBudget = 500 * time.Millisecond
	// maxQuoteIDLen bounds the peer-provided quote identifier.
	maxQuoteIDLen = 128
)

// Client calls QuoteService over the trusted network; mTLS arrives with the
// platform task 0.6.
type Client struct {
	conn *grpc.ClientConn
	svc  quotev1.QuoteServiceClient
}

// Dial connects the quote client.
func Dial(target string) (*Client, error) {
	conn, err := grpc.NewClient(target, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("payment: dial quote %q: %w", target, err)
	}
	return &Client{conn: conn, svc: quotev1.NewQuoteServiceClient(conn)}, nil
}

// Close closes the connection.
func (c *Client) Close() error { return c.conn.Close() }

// LockQuote locks the pair rate; a business rejection maps to
// domain.ErrQuoteRejected, a temporary failure to domain.ErrDependencyTemporary
// and a dead caller context stays the caller's error.
func (c *Client) LockQuote(ctx context.Context, req usecase.QuoteLockRequest) (usecase.QuoteLock, error) {
	callCtx, cancel := context.WithTimeout(ctx, lockBudget)
	defer cancel()
	response, err := c.svc.LockQuote(callCtx, &quotev1.LockQuoteRequest{
		FromAsset:  req.FromAsset,
		ToAsset:    req.ToAsset,
		Amount:     &commonv1.Money{MinorUnits: req.Amount.MinorUnits, Asset: req.Amount.Asset, Decimals: req.Amount.Decimals},
		TtlSeconds: req.TTLSeconds,
	})
	if err != nil {
		if ctx.Err() != nil {
			return usecase.QuoteLock{}, ctx.Err()
		}
		return usecase.QuoteLock{}, classify(err)
	}
	lock := usecase.QuoteLock{
		QuoteID: response.GetQuoteId(),
		Rate:    response.GetRate(),
	}
	if response.GetLockedUntil() != nil {
		lock.LockedUntil = response.GetLockedUntil().AsTime().UTC()
	}
	if lock.QuoteID == "" || len(lock.QuoteID) > maxQuoteIDLen || lock.Rate == "" || lock.LockedUntil.IsZero() {
		return usecase.QuoteLock{}, fmt.Errorf("%w: quote service returned an incomplete lock", domain.ErrDependencyTemporary)
	}
	return lock, nil
}

// classify maps the gRPC status of a LockQuote failure to the payment error
// classes.
func classify(err error) error {
	code := status.Code(err)
	switch code {
	case codes.InvalidArgument, codes.FailedPrecondition, codes.AlreadyExists, codes.NotFound:
		return fmt.Errorf("%w: quote lock rejected (%s)", domain.ErrQuoteRejected, code)
	case codes.ResourceExhausted:
		return fmt.Errorf("%w: quote service %s", domain.ErrOverloaded, code)
	default:
		return fmt.Errorf("%w: quote service %s", domain.ErrDependencyTemporary, code)
	}
}
