// Creation service of the payment state machine (4.2): idempotent
// CreatePayment with the quote lock and the Connector invoice call, and
// CancelPayment before detection.
package usecase

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"

	"github.com/ZMichael3012/cbl-kit/money"

	"github.com/cbl/cbl-payment/internal/domain"
	"github.com/cbl/cbl-payment/internal/storage"
)

// Creation bounds.
const (
	// maxCreationKeyLen bounds the idempotency key.
	maxCreationKeyLen = 255
	// defaultMaxExpiry bounds the invoice horizon when the config omits it.
	defaultMaxExpiry = 7 * 24 * time.Hour
	// creationAttempts bounds the CAS retry loop of one handler call.
	creationAttempts = 3
	// quoteMinTTL is the shortest quote lock requested.
	quoteMinTTL = 60
	// quoteMaxTTL is the longest quote lock requested (the quote service cap).
	quoteMaxTTL = 900
)

// CreationStore persists the creation flow on the payments table.
type CreationStore interface {
	// Create inserts the payment in creating; a taken key is storage.ErrDuplicateKey.
	Create(ctx context.Context, payment storage.Payment) error
	// ByIdempotencyKey returns the payment of the (merchant, key) pair.
	ByIdempotencyKey(ctx context.Context, merchantID, key string) (storage.Payment, error)
	// Payment reads one payment.
	Payment(ctx context.Context, id string) (storage.Payment, error)
	// Transition applies a CAS transition.
	Transition(ctx context.Context, id string, expectedVersion uint64, to domain.State, patch storage.TransitionPatch) (storage.Payment, error)
}

// QuoteLockRequest asks the quote service to lock a pair rate.
type QuoteLockRequest struct {
	// FromAsset is the amount asset side of the pair.
	FromAsset string
	// ToAsset is the pay asset side of the pair.
	ToAsset string
	// Amount is the amount being locked (in FromAsset).
	Amount money.Money
	// TTLSeconds bounds the lock lifetime.
	TTLSeconds uint32
}

// QuoteLock is the locked rate.
type QuoteLock struct {
	// QuoteID is the audit identifier of the lock.
	QuoteID string
	// Rate is the decimal rate (ToAsset per one FromAsset).
	Rate string
	// LockedUntil is the lock deadline.
	LockedUntil time.Time
}

// QuoteLocker locks pair rates; the client classifies business rejections as
// domain.ErrQuoteRejected and temporary failures as domain.ErrDependencyTemporary.
type QuoteLocker interface {
	// LockQuote locks the rate of the request pair.
	LockQuote(ctx context.Context, req QuoteLockRequest) (QuoteLock, error)
}

// InvoiceRequest asks the connector to create the provider invoice.
type InvoiceRequest struct {
	// PaymentID is the platform payment (the connector order id).
	PaymentID string
	// Amount is the locked pay amount.
	Amount money.Money
	// Network is the payment network.
	Network string
	// ExpiresAt bounds the invoice.
	ExpiresAt time.Time
}

// InvoiceResult is the created provider invoice.
type InvoiceResult struct {
	// GatewayInvoiceID is the provider invoice identifier.
	GatewayInvoiceID string
	// Provider is the provider id.
	Provider string
	// Address is the deposit address (address flow).
	Address string
	// CheckoutURL is the hosted payment page (hosted flow).
	CheckoutURL string
}

// InvoiceCreator creates provider invoices; the client classifies business
// rejections as domain.ErrInvoiceRejected and temporary failures as
// domain.ErrDependencyTemporary.
type InvoiceCreator interface {
	// CreateInvoice creates the provider invoice for the payment.
	CreateInvoice(ctx context.Context, req InvoiceRequest) (InvoiceResult, error)
}

// CreatePaymentRequest is a validated creation request.
type CreatePaymentRequest struct {
	// MerchantID owns the payment.
	MerchantID string
	// IdempotencyKey replays one creation.
	IdempotencyKey string
	// Amount is the requested (pricing) amount.
	Amount money.Money
	// PayAsset is the asset the customer pays.
	PayAsset string
	// Network is the payment network.
	Network string
	// ExpiresAt bounds the invoice.
	ExpiresAt time.Time
}

// CreationConfig tunes the creation service.
type CreationConfig struct {
	// AssetScales maps pay assets to their scale; an unknown asset fails closed.
	AssetScales map[string]uint32
	// MaxExpiry bounds the invoice horizon (default 7 days).
	MaxExpiry time.Duration
	// Now overrides the clock in tests.
	Now func() time.Time
	// NewID generates payment identifiers; the default is UUIDv7.
	NewID func() string
	// Log sets the logger.
	Log *zap.Logger
}

// PaymentCreation implements CreatePayment and CancelPayment (4.2).
type PaymentCreation struct {
	store       CreationStore
	quote       QuoteLocker
	invoices    InvoiceCreator
	assetScales map[string]uint32
	maxExpiry   time.Duration
	now         func() time.Time
	newID       func() string
	log         *zap.Logger
}

// NewPaymentCreation validates the dependencies and builds the service.
func NewPaymentCreation(store CreationStore, quote QuoteLocker, invoices InvoiceCreator, cfg CreationConfig) (*PaymentCreation, error) {
	if store == nil || quote == nil || invoices == nil {
		return nil, errors.New("payment: creation dependencies are required")
	}
	maxExpiry := cfg.MaxExpiry
	if maxExpiry == 0 {
		maxExpiry = defaultMaxExpiry
	}
	if maxExpiry < 0 {
		return nil, errors.New("payment: max expiry must be positive")
	}
	now := cfg.Now
	if now == nil {
		now = time.Now
	}
	newID := cfg.NewID
	if newID == nil {
		newID = func() string { return uuid.Must(uuid.NewV7()).String() }
	}
	log := cfg.Log
	if log == nil {
		log = zap.NewNop()
	}
	scales := make(map[string]uint32, len(cfg.AssetScales))
	for asset, scale := range cfg.AssetScales {
		if normalized := strings.ToUpper(strings.TrimSpace(asset)); normalized != "" {
			scales[normalized] = scale
		}
	}
	return &PaymentCreation{
		store:       store,
		quote:       quote,
		invoices:    invoices,
		assetScales: scales,
		maxExpiry:   maxExpiry,
		now:         now,
		newID:       newID,
		log:         log,
	}, nil
}

// Create creates (or resumes) the payment of the request, locking the quote
// and creating exactly one provider invoice even across retries. Shape
// validation runs first; the expiry window is enforced only for a new
// payment, so a replay of the original request still returns the stored
// payment after its expiry (FR-2).
func (c *PaymentCreation) Create(ctx context.Context, req CreatePaymentRequest) (storage.Payment, error) {
	if err := c.validateShape(req); err != nil {
		return storage.Payment{}, err
	}
	merchantID, err := uuid.Parse(strings.TrimSpace(req.MerchantID))
	if err != nil {
		return storage.Payment{}, fmt.Errorf("%w: merchant id", domain.ErrInvalidCreation)
	}
	canonicalMerchant := merchantID.String()
	payAsset := strings.ToUpper(strings.TrimSpace(req.PayAsset))
	scale, ok := c.assetScales[payAsset]
	if !ok {
		return storage.Payment{}, fmt.Errorf("%w: %s", domain.ErrUnknownAssetScale, payAsset)
	}
	hash := domain.RequestHash(canonicalMerchant, req.Amount, payAsset, req.Network, req.ExpiresAt, scale)

	existing, err := c.store.ByIdempotencyKey(ctx, canonicalMerchant, strings.TrimSpace(req.IdempotencyKey))
	switch {
	case err == nil:
		return c.resume(ctx, existing, req, payAsset, scale, hash)
	case !errors.Is(err, storage.ErrNotFound):
		return storage.Payment{}, err
	}

	if err := c.validateWindow(req, c.now().UTC()); err != nil {
		return storage.Payment{}, err
	}
	expires := req.ExpiresAt.UTC()
	payment := storage.Payment{
		ID:               c.newID(),
		MerchantID:       canonicalMerchant,
		IdempotencyKey:   strings.TrimSpace(req.IdempotencyKey),
		RequestHash:      hash,
		ExpectedAmount:   req.Amount.MinorUnits,
		ExpectedAsset:    strings.ToUpper(strings.TrimSpace(req.Amount.Asset)),
		ExpectedDecimals: int(req.Amount.Decimals),
		Network:          strings.ToLower(strings.TrimSpace(req.Network)),
		ExpiresAt:        &expires,
	}
	if err := c.store.Create(ctx, payment); err != nil {
		if errors.Is(err, storage.ErrDuplicateKey) {
			concurrent, rerr := c.store.ByIdempotencyKey(ctx, payment.MerchantID, payment.IdempotencyKey)
			if rerr != nil {
				return storage.Payment{}, rerr
			}
			return c.resume(ctx, concurrent, req, payAsset, scale, hash)
		}
		return storage.Payment{}, err
	}
	stored, err := c.store.Payment(ctx, payment.ID)
	if err != nil {
		return storage.Payment{}, err
	}
	return c.resume(ctx, stored, req, payAsset, scale, hash)
}

// resume continues a non-terminal creation from its persisted state: creating
// locks the quote, created creates the invoice; later or terminal states are
// returned as the idempotent replay.
func (c *PaymentCreation) resume(ctx context.Context, payment storage.Payment, req CreatePaymentRequest, payAsset string, scale uint32, hash string) (storage.Payment, error) {
	if payment.RequestHash != hash {
		return storage.Payment{}, fmt.Errorf("%w: payment %s", domain.ErrIdempotencyConflict, payment.ID)
	}
	current := payment
	for attempt := 0; attempt < creationAttempts; attempt++ {
		switch domain.State(current.State) {
		case domain.StateCreating:
			updated, err := c.lockQuote(ctx, current, req, payAsset, scale)
			if err != nil {
				return storage.Payment{}, err
			}
			current = updated
		case domain.StateCreated:
			return c.createInvoice(ctx, current)
		default:
			return current, nil
		}
	}
	return storage.Payment{}, fmt.Errorf("%w: creation attempts exhausted", domain.ErrStateConflict)
}

// lockQuote locks the rate and moves creating -> created, filling the locked
// pay amount (the immutable expected fields are filled by this one transition).
func (c *PaymentCreation) lockQuote(ctx context.Context, payment storage.Payment, req CreatePaymentRequest, payAsset string, scale uint32) (storage.Payment, error) {
	now := c.now().UTC()
	lock, err := c.quote.LockQuote(ctx, QuoteLockRequest{
		FromAsset:  strings.ToUpper(strings.TrimSpace(req.Amount.Asset)),
		ToAsset:    payAsset,
		Amount:     req.Amount,
		TTLSeconds: quoteTTL(payment.ExpiresAt, now),
	})
	if err != nil {
		if errors.Is(err, domain.ErrQuoteRejected) {
			c.failPayment(ctx, payment, "quote rejected", true)
		}
		return storage.Payment{}, err
	}
	expectedMinor, err := domain.ConvertRate(req.Amount.MinorUnits, lock.Rate, req.Amount.Decimals, scale)
	if err != nil {
		return storage.Payment{}, err
	}
	quoteID, rate := lock.QuoteID, lock.Rate
	lockedUntil := lock.LockedUntil.UTC()
	expectedAsset, expectedDecimals := payAsset, int(scale)
	updated, err := c.store.Transition(ctx, payment.ID, uint64(payment.StateVersion), domain.StateCreated, storage.TransitionPatch{ //nolint:gosec // state_version is constrained by CHECK (>= 1)
		QuoteID:          &quoteID,
		Rate:             &rate,
		QuoteLockedUntil: &lockedUntil,
		ExpectedAmount:   &expectedMinor,
		ExpectedAsset:    &expectedAsset,
		ExpectedDecimals: &expectedDecimals,
		Reason:           "quote locked",
	})
	if errors.Is(err, domain.ErrStateConflict) {
		// A concurrent attempt advanced the payment: continue from the fresh row.
		fresh, rerr := c.store.Payment(ctx, payment.ID)
		if rerr != nil {
			return storage.Payment{}, rerr
		}
		return fresh, nil
	}
	if err != nil {
		return storage.Payment{}, err
	}
	return updated, nil
}

// createInvoice creates the provider invoice and moves created -> pending with
// the address or the checkout page.
func (c *PaymentCreation) createInvoice(ctx context.Context, payment storage.Payment) (storage.Payment, error) {
	amount := money.Money{
		MinorUnits: payment.ExpectedAmount,
		Asset:      payment.ExpectedAsset,
		Decimals:   uint32(payment.ExpectedDecimals), //nolint:gosec // decimals are constrained by CHECK (0..18)
	}
	expires := c.now().UTC()
	if payment.ExpiresAt != nil {
		expires = payment.ExpiresAt.UTC()
	}
	result, err := c.invoices.CreateInvoice(ctx, InvoiceRequest{
		PaymentID: payment.ID,
		Amount:    amount,
		Network:   payment.Network,
		ExpiresAt: expires,
	})
	if err != nil {
		if errors.Is(err, domain.ErrInvoiceRejected) {
			// No adoption on conflict: a concurrent attempt may already be
			// creating the invoice, and failing that row would orphan it
			// (the expiry or the operator owns the residual).
			c.failPayment(ctx, payment, "invoice rejected", false)
		}
		return storage.Payment{}, err
	}
	if result.GatewayInvoiceID == "" || (result.Address == "" && result.CheckoutURL == "") {
		// A provider answer without an invoice id or a destination cannot
		// settle; the payment stays in created and the retry re-asks.
		return storage.Payment{}, fmt.Errorf("%w: provider returned no invoice destination", domain.ErrDependencyTemporary)
	}
	patch := storage.TransitionPatch{
		GatewayID: &result.GatewayInvoiceID,
		Provider:  &result.Provider,
		Reason:    "invoice created",
	}
	if result.Address != "" && result.CheckoutURL == "" {
		patch.PayAddress = &result.Address
	}
	if result.CheckoutURL != "" {
		// Exactly one destination reaches the merchant (the proto invariant);
		// a hosted checkout page wins over an address.
		patch.CheckoutURL = &result.CheckoutURL
	}
	updated, err := c.store.Transition(ctx, payment.ID, uint64(payment.StateVersion), domain.StatePending, patch) //nolint:gosec // state_version is constrained by CHECK (>= 1)
	if errors.Is(err, domain.ErrStateConflict) {
		fresh, rerr := c.store.Payment(ctx, payment.ID)
		if rerr != nil {
			return storage.Payment{}, rerr
		}
		if domain.State(fresh.State) == domain.StatePending {
			return fresh, nil
		}
		return storage.Payment{}, err
	}
	if err != nil {
		return storage.Payment{}, err
	}
	return updated, nil
}

// Cancel cancels a payment before detection; a repeated cancel replays the
// previous result. The idempotency key is accepted for contract symmetry but
// is not needed: cancelling is state-idempotent by itself.
func (c *PaymentCreation) Cancel(ctx context.Context, paymentID, idempotencyKey string) (storage.Payment, error) {
	parsed, err := uuid.Parse(strings.TrimSpace(paymentID))
	if err != nil {
		return storage.Payment{}, fmt.Errorf("%w: payment id", domain.ErrInvalidCreation)
	}
	canonicalID := parsed.String()
	if key := strings.TrimSpace(idempotencyKey); key == "" || len(key) > maxCreationKeyLen {
		return storage.Payment{}, fmt.Errorf("%w: idempotency key", domain.ErrInvalidCreation)
	}
	for attempt := 0; attempt < creationAttempts; attempt++ {
		payment, err := c.store.Payment(ctx, canonicalID)
		if err != nil {
			return storage.Payment{}, err
		}
		switch domain.State(payment.State) {
		case domain.StateCancelled:
			return payment, nil
		case domain.StateCreating, domain.StateCreated, domain.StatePending:
			updated, err := c.store.Transition(ctx, payment.ID, uint64(payment.StateVersion), domain.StateCancelled, storage.TransitionPatch{ //nolint:gosec // state_version is constrained by CHECK (>= 1)
				Reason: "cancelled by merchant",
			})
			if errors.Is(err, domain.ErrStateConflict) {
				continue
			}
			return updated, err
		default:
			return storage.Payment{}, fmt.Errorf("%w: state %s", domain.ErrInvoiceNotCancellable, payment.State)
		}
	}
	return storage.Payment{}, fmt.Errorf("%w: cancel attempts exhausted", domain.ErrStateConflict)
}

// failPayment closes a rejected creation: the quote rejection adopts the
// fresh row across CAS conflicts (no invoice exists yet); the invoice
// rejection never adopts (a concurrent attempt may be mid-invoice). A
// non-conflict failure is logged and the janitor owns the stuck creation.
func (c *PaymentCreation) failPayment(ctx context.Context, payment storage.Payment, reason string, adopt bool) {
	current := payment
	for attempt := 0; attempt < creationAttempts; attempt++ {
		_, err := c.store.Transition(ctx, current.ID, uint64(current.StateVersion), domain.StateFailed, storage.TransitionPatch{Reason: reason}) //nolint:gosec // state_version is constrained by CHECK (>= 1)
		if err == nil {
			return
		}
		if !errors.Is(err, domain.ErrStateConflict) {
			c.log.Warn("creation failure transition failed",
				zap.String("payment_id", current.ID),
				zap.String("reason", reason),
				zap.Error(err),
			)
			return
		}
		if !adopt {
			return
		}
		fresh, rerr := c.store.Payment(ctx, current.ID)
		if rerr != nil {
			return
		}
		if domain.State(fresh.State) != domain.StateCreating && domain.State(fresh.State) != domain.StateCreated {
			return
		}
		current = fresh
	}
}

// validateShape checks the creation invariants that hold for every request,
// new or replay.
func (c *PaymentCreation) validateShape(req CreatePaymentRequest) error {
	if _, err := uuid.Parse(strings.TrimSpace(req.MerchantID)); err != nil {
		return fmt.Errorf("%w: merchant id", domain.ErrInvalidCreation)
	}
	if key := strings.TrimSpace(req.IdempotencyKey); key == "" || len(key) > maxCreationKeyLen {
		return fmt.Errorf("%w: idempotency key", domain.ErrInvalidCreation)
	}
	if req.Amount.MinorUnits <= 0 || req.Amount.Decimals > money.MaxDecimals || !domain.ValidAssetCode(req.Amount.Asset) {
		return fmt.Errorf("%w: amount", domain.ErrInvalidCreation)
	}
	if !domain.ValidAssetCode(req.PayAsset) {
		return fmt.Errorf("%w: pay asset", domain.ErrInvalidCreation)
	}
	if !domain.ValidNetwork(req.Network) {
		return fmt.Errorf("%w: network", domain.ErrInvalidCreation)
	}
	if req.ExpiresAt.IsZero() {
		return fmt.Errorf("%w: expires_at", domain.ErrInvalidExpiry)
	}
	return nil
}

// validateWindow checks the expiry horizon of a new payment; a replay skips it
// so the stored result survives its own expiry (FR-2).
func (c *PaymentCreation) validateWindow(req CreatePaymentRequest, now time.Time) error {
	if !req.ExpiresAt.After(now) || req.ExpiresAt.After(now.Add(c.maxExpiry)) {
		return fmt.Errorf("%w: expires_at", domain.ErrInvalidExpiry)
	}
	return nil
}

// quoteTTL derives the lock TTL from the invoice expiry within the service
// bounds.
func quoteTTL(expiresAt *time.Time, now time.Time) uint32 {
	if expiresAt == nil {
		return quoteMaxTTL
	}
	seconds := int(expiresAt.Sub(now) / time.Second)
	if seconds < quoteMinTTL {
		return quoteMinTTL
	}
	if seconds > quoteMaxTTL {
		return quoteMaxTTL
	}
	return uint32(seconds)
}
