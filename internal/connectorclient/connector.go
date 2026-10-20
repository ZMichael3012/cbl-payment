// Package connectorclient adapts ConnectorService/CreateInvoice for the
// payment creation flow (4.2).
package connectorclient

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/ZMichael3012/cbl-protos/gen/go/billing/common/v1"
	connectorv1 "github.com/ZMichael3012/cbl-protos/gen/go/billing/connector/v1"

	"github.com/cbl/cbl-payment/internal/domain"
	"github.com/cbl/cbl-payment/internal/usecase"
)

// invoiceBudget bounds one CreateInvoice call (the connector owns provider
// retries within its own budgets; CBL-003 names 5 s for the RPC).
const (
	invoiceBudget = 5 * time.Second
	// maxGatewayIDLen bounds the peer-provided invoice identifier.
	maxGatewayIDLen = 128
	// maxProviderLen bounds the peer-provided provider id.
	maxProviderLen = 64
	// maxAddressLen bounds the peer-provided deposit address.
	maxAddressLen = 256
	// maxCheckoutLen bounds the peer-provided checkout page.
	maxCheckoutLen = 2048
)

// Client calls ConnectorService over the trusted network; mTLS arrives with
// the platform task 0.6.
type Client struct {
	conn *grpc.ClientConn
	svc  connectorv1.ConnectorServiceClient
}

// Dial connects the connector client.
func Dial(target string) (*Client, error) {
	conn, err := grpc.NewClient(target, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("payment: dial connector %q: %w", target, err)
	}
	return &Client{conn: conn, svc: connectorv1.NewConnectorServiceClient(conn)}, nil
}

// Close closes the connection.
func (c *Client) Close() error { return c.conn.Close() }

// CreateInvoice creates the provider invoice; the payment id doubles as the
// connector idempotency key and the order id, so every retry replays the same
// provider invoice. A business rejection maps to domain.ErrInvoiceRejected, a
// temporary failure to domain.ErrDependencyTemporary.
func (c *Client) CreateInvoice(ctx context.Context, req usecase.InvoiceRequest) (usecase.InvoiceResult, error) {
	callCtx, cancel := context.WithTimeout(ctx, invoiceBudget)
	defer cancel()
	response, err := c.svc.CreateInvoice(callCtx, &connectorv1.CreateInvoiceRequest{
		IdempotencyKey: req.PaymentID,
		PaymentId:      req.PaymentID,
		Amount:         &commonv1.Money{MinorUnits: req.Amount.MinorUnits, Asset: req.Amount.Asset, Decimals: req.Amount.Decimals},
		Asset:          req.Amount.Asset,
		Network:        req.Network,
		ExpiresAt:      timestamppb.New(req.ExpiresAt.UTC()),
	})
	if err != nil {
		if ctx.Err() != nil {
			return usecase.InvoiceResult{}, ctx.Err()
		}
		return usecase.InvoiceResult{}, classify(err)
	}
	result := usecase.InvoiceResult{
		GatewayInvoiceID: response.GetGatewayInvoiceId(),
		Provider:         response.GetProvider(),
		Address:          response.GetAddress(),
		CheckoutURL:      response.GetCheckoutUrl(),
	}
	if result.GatewayInvoiceID == "" || len(result.GatewayInvoiceID) > maxGatewayIDLen {
		return usecase.InvoiceResult{}, fmt.Errorf("%w: connector returned no usable invoice id", domain.ErrDependencyTemporary)
	}
	if result.Provider == "" || len(result.Provider) > maxProviderLen ||
		len(result.Address) > maxAddressLen || len(result.CheckoutURL) > maxCheckoutLen {
		return usecase.InvoiceResult{}, fmt.Errorf("%w: connector returned out-of-bound invoice fields", domain.ErrDependencyTemporary)
	}
	return result, nil
}

// classify maps the gRPC status of a CreateInvoice failure to the payment
// error classes.
func classify(err error) error {
	code := status.Code(err)
	switch code {
	case codes.FailedPrecondition, codes.InvalidArgument, codes.AlreadyExists:
		return fmt.Errorf("%w: connector invoice rejected (%s)", domain.ErrInvoiceRejected, code)
	case codes.ResourceExhausted:
		return fmt.Errorf("%w: connector %s", domain.ErrOverloaded, code)
	default:
		return fmt.Errorf("%w: connector %s", domain.ErrDependencyTemporary, code)
	}
}
