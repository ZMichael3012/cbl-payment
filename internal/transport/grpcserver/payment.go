// Package grpcserver contains the gRPC transport layer of the payment service:
// validation of untrusted input and mapping of domain errors to gRPC statuses.
package grpcserver

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/google/uuid"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/cbl/cbl-protos/gen/go/billing/common/v1"
	paymentv1 "github.com/cbl/cbl-protos/gen/go/billing/payment/v1"

	"github.com/cbl/cbl-kit/grpcmw"

	"github.com/cbl/cbl-payment/internal/domain"
	"github.com/cbl/cbl-payment/internal/storage"
	"github.com/cbl/cbl-payment/internal/usecase"
)

// Untrusted transport input limits.
const (
	maxMerchantIDLen = 256
	maxCursorLen     = 256
)

// Payment implements the PaymentService gRPC service. CreatePayment and
// CancelPayment will appear in 4.2 (blocked by KI-01, the gateway); the reads
// are implemented in 4.8.
type Payment struct {
	paymentv1.UnimplementedPaymentServiceServer
	query *usecase.PaymentQuery
}

// NewPayment creates the PaymentService server.
func NewPayment(query *usecase.PaymentQuery) *Payment {
	return &Payment{query: query}
}

// GetPayment returns a payment with the transition history and the locked
// quote (rate, locked_until).
func (s *Payment) GetPayment(ctx context.Context, req *paymentv1.GetPaymentRequest) (*paymentv1.GetPaymentResponse, error) {
	paymentID := strings.TrimSpace(req.GetPaymentId())
	parsed, err := uuid.Parse(paymentID)
	if err != nil {
		return nil, grpcmw.E(grpcmw.KindInvalidArgument, "invalid_payment_id", "payment_id не UUID", nil)
	}
	payment, history, err := s.query.Get(ctx, parsed.String())
	if err != nil {
		return nil, mapPaymentError(err)
	}
	return &paymentv1.GetPaymentResponse{Payment: paymentToProto(payment, history)}, nil
}

// ListPayments returns a page of merchant payments (4.8): filters by owner,
// state and creation period (from inclusive, to exclusive).
func (s *Payment) ListPayments(ctx context.Context, req *paymentv1.ListPaymentsRequest) (*paymentv1.ListPaymentsResponse, error) {
	merchantID := strings.TrimSpace(req.GetMerchantId())
	if merchantID == "" {
		return nil, grpcmw.E(grpcmw.KindInvalidArgument, "missing_merchant_id", "merchant_id обязателен", nil)
	}
	if len(merchantID) > maxMerchantIDLen {
		return nil, grpcmw.E(grpcmw.KindInvalidArgument, "merchant_id_too_long", "merchant_id длиннее предела", nil)
	}
	state, err := stateFilter(req.GetStateFilter())
	if err != nil {
		return nil, err
	}
	if len(req.GetCursor()) > maxCursorLen {
		return nil, grpcmw.E(grpcmw.KindInvalidArgument, "cursor_too_long", "cursor длиннее предела", nil)
	}
	createdFrom, err := optionalTimestamp(req.GetCreatedFrom())
	if err != nil {
		return nil, err
	}
	createdTo, err := optionalTimestamp(req.GetCreatedTo())
	if err != nil {
		return nil, err
	}
	if createdFrom != nil && createdTo != nil && !createdTo.After(*createdFrom) {
		return nil, grpcmw.E(grpcmw.KindInvalidArgument, "invalid_period", "created_to должен быть позже created_from", nil)
	}
	payments, nextCursor, err := s.query.List(ctx, usecase.ListFilter{
		MerchantID:  merchantID,
		State:       state,
		CreatedFrom: createdFrom,
		CreatedTo:   createdTo,
		Limit:       int(req.GetLimit()),
		Cursor:      req.GetCursor(),
	})
	if err != nil {
		return nil, mapPaymentError(err)
	}
	response := &paymentv1.ListPaymentsResponse{NextCursor: nextCursor}
	for _, payment := range payments {
		response.Payments = append(response.Payments, paymentToProto(payment, nil))
	}
	return response, nil
}

// paymentToProto assembles the contract payment; the timeline is filled only by Get.
func paymentToProto(payment storage.Payment, history []storage.Transition) *paymentv1.Payment {
	result := &paymentv1.Payment{
		PaymentId:      payment.ID,
		MerchantId:     payment.MerchantID,
		State:          stateToProto(domain.State(payment.State)),
		ExpectedAmount: money(payment.ExpectedAmount, payment.ExpectedAsset, payment.ExpectedDecimals),
		Network:        payment.Network,
		StateVersion:   uint64(payment.StateVersion), //nolint:gosec // CHECK >= 1
		CreatedAt:      timestamppb.New(payment.CreatedAt.UTC()),
	}
	if payment.PayAddress != nil {
		result.PayAddress = *payment.PayAddress
	}
	if payment.QuoteID != nil {
		result.QuoteId = *payment.QuoteID
	}
	if payment.Rate != nil {
		result.Rate = *payment.Rate
	}
	if payment.QuoteLockedUntil != nil {
		result.LockedUntil = timestamppb.New(payment.QuoteLockedUntil.UTC())
	}
	if payment.ExpiresAt != nil {
		result.ExpiresAt = timestamppb.New(payment.ExpiresAt.UTC())
	}
	if payment.TxID != nil {
		result.Txid = *payment.TxID
	}
	if payment.Confirmations > 0 {
		result.Confirmations = uint32(payment.Confirmations) //nolint:gosec // CHECK 0 - 1000000
	}
	if payment.ReceivedAmount != nil && payment.ReceivedAsset != nil && payment.ReceivedDecimals != nil {
		result.ReceivedAmount = money(*payment.ReceivedAmount, *payment.ReceivedAsset, *payment.ReceivedDecimals)
	}
	// Fee is not exposed in Payment v1: the fee rules are undefined
	// (KI-payment-06); there is no separate REST fee field in the invoice - the
	// webhook field is fed by payment.events (CF-2).
	for _, item := range history {
		result.Timeline = append(result.Timeline, &paymentv1.TimelineItem{
			State:      stateToProto(domain.State(item.ToState)),
			OccurredAt: timestamppb.New(item.OccurredAt.UTC()),
			Comment:    item.Reason,
		})
	}
	return result
}

// money assembles the contract amount with the asset and the scale.
func money(units int64, asset string, decimals int) *commonv1.Money {
	scale := decimals
	if scale < 0 {
		scale = 0
	}
	return &commonv1.Money{MinorUnits: units, Asset: asset, Decimals: uint32(scale)} //nolint:gosec // CHECK 0 - 18
}

// stateToProto converts a domain state to the contract one.
func stateToProto(state domain.State) paymentv1.PaymentState {
	switch state {
	case domain.StateCreating:
		return paymentv1.PaymentState_PAYMENT_STATE_CREATING
	case domain.StateCreated:
		return paymentv1.PaymentState_PAYMENT_STATE_CREATED
	case domain.StatePending:
		return paymentv1.PaymentState_PAYMENT_STATE_PENDING
	case domain.StateDetected:
		return paymentv1.PaymentState_PAYMENT_STATE_DETECTED
	case domain.StateConfirming:
		return paymentv1.PaymentState_PAYMENT_STATE_CONFIRMING
	case domain.StateCompleted:
		return paymentv1.PaymentState_PAYMENT_STATE_COMPLETED
	case domain.StateExpired:
		return paymentv1.PaymentState_PAYMENT_STATE_EXPIRED
	case domain.StateUnderpaid:
		return paymentv1.PaymentState_PAYMENT_STATE_UNDERPAID
	case domain.StateOverpaid:
		return paymentv1.PaymentState_PAYMENT_STATE_OVERPAID
	case domain.StateFailed:
		return paymentv1.PaymentState_PAYMENT_STATE_FAILED
	case domain.StateCancelled:
		return paymentv1.PaymentState_PAYMENT_STATE_CANCELLED
	default:
		return paymentv1.PaymentState_PAYMENT_STATE_UNSPECIFIED
	}
}

// stateFilter converts the optional contract state filter: UNSPECIFIED means
// no filter, an unknown value is rejected.
func stateFilter(state paymentv1.PaymentState) (*domain.State, error) {
	if state == paymentv1.PaymentState_PAYMENT_STATE_UNSPECIFIED {
		return nil, nil
	}
	mapped := stateFromProto(state)
	if mapped == "" {
		return nil, grpcmw.E(grpcmw.KindInvalidArgument, "unknown_state_filter", "state_filter не распознан", nil)
	}
	return &mapped, nil
}

// stateFromProto converts a contract state to the domain one (empty for
// unknown values).
func stateFromProto(state paymentv1.PaymentState) domain.State {
	switch state {
	case paymentv1.PaymentState_PAYMENT_STATE_CREATING:
		return domain.StateCreating
	case paymentv1.PaymentState_PAYMENT_STATE_CREATED:
		return domain.StateCreated
	case paymentv1.PaymentState_PAYMENT_STATE_PENDING:
		return domain.StatePending
	case paymentv1.PaymentState_PAYMENT_STATE_DETECTED:
		return domain.StateDetected
	case paymentv1.PaymentState_PAYMENT_STATE_CONFIRMING:
		return domain.StateConfirming
	case paymentv1.PaymentState_PAYMENT_STATE_COMPLETED:
		return domain.StateCompleted
	case paymentv1.PaymentState_PAYMENT_STATE_EXPIRED:
		return domain.StateExpired
	case paymentv1.PaymentState_PAYMENT_STATE_UNDERPAID:
		return domain.StateUnderpaid
	case paymentv1.PaymentState_PAYMENT_STATE_OVERPAID:
		return domain.StateOverpaid
	case paymentv1.PaymentState_PAYMENT_STATE_FAILED:
		return domain.StateFailed
	case paymentv1.PaymentState_PAYMENT_STATE_CANCELLED:
		return domain.StateCancelled
	default:
		return ""
	}
}

// optionalTimestamp converts the optional request time bound; an invalid
// timestamp is rejected with the single invalid_period code.
func optionalTimestamp(ts *timestamppb.Timestamp) (*time.Time, error) {
	if ts == nil {
		return nil, nil
	}
	if err := ts.CheckValid(); err != nil {
		return nil, grpcmw.E(grpcmw.KindInvalidArgument, "invalid_period", "границы периода некорректны", nil)
	}
	value := ts.AsTime().UTC()
	return &value, nil
}

// mapPaymentError converts storage errors to platform errors.
func mapPaymentError(err error) error {
	switch {
	case errors.Is(err, storage.ErrNotFound):
		return grpcmw.E(grpcmw.KindNotFound, "payment_not_found", "платеж не найден", nil)
	case errors.Is(err, storage.ErrInvalidCursor):
		return grpcmw.E(grpcmw.KindInvalidArgument, "invalid_cursor", "курсор списка некорректен", nil)
	default:
		return err
	}
}
