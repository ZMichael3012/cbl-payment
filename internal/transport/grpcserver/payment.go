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
	kitmoney "github.com/cbl/cbl-kit/money"

	"github.com/cbl/cbl-payment/internal/domain"
	"github.com/cbl/cbl-payment/internal/storage"
	"github.com/cbl/cbl-payment/internal/usecase"
)

// Untrusted transport input limits.
const (
	maxMerchantIDLen = 256
	maxCursorLen     = 256
	// maxIdempotencyKeyLen bounds the creation and cancel keys (4.2).
	maxIdempotencyKeyLen = 255
)

// Payment implements the PaymentService gRPC service: reads (4.8) and the
// creation flow (4.2).
type Payment struct {
	paymentv1.UnimplementedPaymentServiceServer
	query    *usecase.PaymentQuery
	creation *usecase.PaymentCreation
}

// NewPayment creates the PaymentService server; creation may be nil when the
// quote and connector clients are not wired (CreatePayment answers
// Unavailable fail-closed).
func NewPayment(query *usecase.PaymentQuery, creation *usecase.PaymentCreation) *Payment {
	return &Payment{query: query, creation: creation}
}

// CreatePayment creates the payment of the idempotent request: the quote is
// locked, the connector invoice is created and the response carries the pay
// amount, the address or the checkout page (4.2).
func (s *Payment) CreatePayment(ctx context.Context, req *paymentv1.CreatePaymentRequest) (*paymentv1.CreatePaymentResponse, error) {
	if s.creation == nil {
		return nil, grpcmw.E(grpcmw.KindUnavailable, "service_unavailable", "создание платежей не подключено", nil)
	}
	merchantID := strings.TrimSpace(req.GetMerchantId())
	if _, err := uuid.Parse(merchantID); err != nil {
		return nil, grpcmw.E(grpcmw.KindInvalidArgument, "invalid_merchant_id", "merchant_id не UUID", nil)
	}
	if len(merchantID) > maxMerchantIDLen {
		return nil, grpcmw.E(grpcmw.KindInvalidArgument, "invalid_merchant_id", "merchant_id длиннее предела", nil)
	}
	key := strings.TrimSpace(req.GetIdempotencyKey())
	if key == "" || len(key) > maxIdempotencyKeyLen {
		return nil, grpcmw.E(grpcmw.KindInvalidArgument, "invalid_idempotency_key", "idempotency_key обязателен и ограничен", nil)
	}
	amount := req.GetAmount()
	if amount == nil || amount.GetMinorUnits() <= 0 {
		return nil, grpcmw.E(grpcmw.KindInvalidArgument, "invalid_amount", "amount должен быть положительным", nil)
	}
	if amount.GetDecimals() > kitmoney.MaxDecimals || !domain.ValidAssetCode(amount.GetAsset()) {
		return nil, grpcmw.E(grpcmw.KindInvalidArgument, "invalid_amount", "amount вне границ", nil)
	}
	if !domain.ValidAssetCode(req.GetPayAsset()) {
		return nil, grpcmw.E(grpcmw.KindInvalidArgument, "invalid_pay_asset", "pay_asset вне границ", nil)
	}
	network := strings.TrimSpace(req.GetNetwork())
	if !domain.ValidNetwork(network) {
		return nil, grpcmw.E(grpcmw.KindInvalidArgument, "invalid_network", "network обязателен и ограничен", nil)
	}
	if req.GetExpiresAt() == nil {
		return nil, grpcmw.E(grpcmw.KindInvalidArgument, "invalid_expires_at", "expires_at обязателен", nil)
	}
	expiresAt := req.GetExpiresAt().AsTime().UTC()
	// The expiry window (future, horizon) is enforced by the creation service
	// for a new payment only: a replay of the original request must return the
	// stored result even after its own expiry (FR-2).

	payment, err := s.creation.Create(ctx, usecase.CreatePaymentRequest{
		MerchantID:     merchantID,
		IdempotencyKey: key,
		Amount:         kitmoney.Money{MinorUnits: amount.GetMinorUnits(), Asset: amount.GetAsset(), Decimals: amount.GetDecimals()},
		PayAsset:       req.GetPayAsset(),
		Network:        network,
		ExpiresAt:      expiresAt,
	})
	if err != nil {
		return nil, mapCreationError(err)
	}
	response := &paymentv1.CreatePaymentResponse{
		PaymentId: payment.ID,
		State:     stateToProto(domain.State(payment.State)),
	}
	if payment.QuoteID != nil {
		response.QuoteId = *payment.QuoteID
	}
	if payment.Rate != nil {
		response.Rate = *payment.Rate
	}
	if payment.QuoteLockedUntil != nil {
		response.LockedUntil = timestamppb.New(payment.QuoteLockedUntil.UTC())
	}
	response.ExpectedAmount = money(payment.ExpectedAmount, payment.ExpectedAsset, payment.ExpectedDecimals)
	if payment.PayAddress != nil {
		response.PayAddress = *payment.PayAddress
	}
	if payment.CheckoutURL != nil {
		response.CheckoutUrl = *payment.CheckoutURL
	}
	if payment.ExpiresAt != nil {
		response.ExpiresAt = timestamppb.New(payment.ExpiresAt.UTC())
	}
	return response, nil
}

// CancelPayment cancels a payment before detection; a repeated cancel replays
// the previous result (4.2).
func (s *Payment) CancelPayment(ctx context.Context, req *paymentv1.CancelPaymentRequest) (*paymentv1.CancelPaymentResponse, error) {
	if s.creation == nil {
		return nil, grpcmw.E(grpcmw.KindUnavailable, "service_unavailable", "создание платежей не подключено", nil)
	}
	paymentID := strings.TrimSpace(req.GetPaymentId())
	if _, err := uuid.Parse(paymentID); err != nil {
		return nil, grpcmw.E(grpcmw.KindInvalidArgument, "invalid_payment_id", "payment_id не UUID", nil)
	}
	key := strings.TrimSpace(req.GetIdempotencyKey())
	if key == "" || len(key) > maxIdempotencyKeyLen {
		return nil, grpcmw.E(grpcmw.KindInvalidArgument, "invalid_idempotency_key", "idempotency_key обязателен и ограничен", nil)
	}
	payment, err := s.creation.Cancel(ctx, paymentID, key)
	if err != nil {
		return nil, mapCreationError(err)
	}
	return &paymentv1.CancelPaymentResponse{Payment: paymentToProto(payment, nil)}, nil
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
	if payment.Provider != nil {
		result.Provider = *payment.Provider
	}
	if payment.RawStatus != nil {
		result.RawStatus = *payment.RawStatus
	}
	if payment.CheckoutURL != nil {
		result.CheckoutUrl = *payment.CheckoutURL
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
		return grpcmw.E(grpcmw.KindInvalidArgument, "invalid_cursor", "неверный курсор страницы", nil)
	default:
		return err
	}
}

// mapCreationError converts the creation flow errors to platform errors (4.2):
// business rejections are preconditions, dependency failures are retryable and
// malformed input is invalid.
func mapCreationError(err error) error {
	switch {
	case errors.Is(err, storage.ErrNotFound):
		return grpcmw.E(grpcmw.KindNotFound, "payment_not_found", "платеж не найден", nil)
	case errors.Is(err, domain.ErrIdempotencyConflict):
		return grpcmw.E(grpcmw.KindAlreadyExists, "idempotency_key_reuse", "ключ идемпотентности переиспользован с другим запросом", nil)
	case errors.Is(err, domain.ErrQuoteRejected):
		return grpcmw.E(grpcmw.KindFailedPrecondition, "quote_rejected", "курс недоступен для запрошенной пары или суммы", nil)
	case errors.Is(err, domain.ErrInvoiceRejected):
		return grpcmw.E(grpcmw.KindFailedPrecondition, "invoice_rejected", "провайдер отклонил создание инвойса", nil)
	case errors.Is(err, domain.ErrInvoiceNotCancellable):
		return grpcmw.E(grpcmw.KindFailedPrecondition, "invoice_not_cancellable", "платеж уже нельзя отменить", nil)
	case errors.Is(err, domain.ErrPayAmount):
		return grpcmw.E(grpcmw.KindFailedPrecondition, "pay_amount_invalid", "сконвертированная сумма недопустима", nil)
	case errors.Is(err, domain.ErrInvalidCreation), errors.Is(err, domain.ErrInvalidExpiry),
		errors.Is(err, domain.ErrUnknownAssetScale):
		return grpcmw.E(grpcmw.KindInvalidArgument, "invalid_creation_request", "запрос создания платежа некорректен", nil)
	case errors.Is(err, domain.ErrOverloaded):
		return grpcmw.E(grpcmw.KindResourceExhausted, "service_unavailable", "сервис перегружен, повторите позже", nil)
	case errors.Is(err, context.DeadlineExceeded):
		return grpcmw.E(grpcmw.KindUnavailable, "service_unavailable", "сервис временно недоступен", nil)
	case errors.Is(err, domain.ErrInvalidRate), errors.Is(err, domain.ErrStateConflict),
		errors.Is(err, domain.ErrDependencyTemporary):
		return grpcmw.E(grpcmw.KindUnavailable, "service_unavailable", "сервис временно недоступен", nil)
	default:
		return err
	}
}
