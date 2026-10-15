package domain_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cbl/cbl-payment/internal/domain"
)

// eventCycles задает ожидаемые планы FR-2: состояния -> событие -> результат.
var eventCycles = map[domain.State]map[domain.CanonicalEventType][]domain.State{
	domain.StateCreating: nil,
	domain.StateCreated:  nil,
	domain.StatePending: {
		domain.CanonicalDetected:  {domain.StateDetected},
		domain.CanonicalConfirmed: {domain.StateDetected, domain.StateConfirming},
		domain.CanonicalFailed:    {domain.StateFailed},
		domain.CanonicalExpired:   {domain.StateExpired},
		domain.CanonicalUnderpaid: {domain.StateDetected, domain.StateUnderpaid},
		domain.CanonicalOverpaid:  {domain.StateDetected, domain.StateOverpaid},
	},
	domain.StateDetected: {
		domain.CanonicalDetected:  {domain.StateDetected},
		domain.CanonicalConfirmed: {domain.StateConfirming},
		domain.CanonicalFailed:    {domain.StateFailed},
		domain.CanonicalExpired:   nil,
		domain.CanonicalUnderpaid: {domain.StateUnderpaid},
		domain.CanonicalOverpaid:  {domain.StateOverpaid},
	},
	domain.StateConfirming: {
		domain.CanonicalDetected:  nil,
		domain.CanonicalConfirmed: {domain.StateConfirming},
		domain.CanonicalFailed:    {domain.StateFailed},
		domain.CanonicalExpired:   nil,
		domain.CanonicalUnderpaid: {domain.StateUnderpaid},
		domain.CanonicalOverpaid:  {domain.StateOverpaid},
	},
}

var allEventTypes = []domain.CanonicalEventType{
	domain.CanonicalDetected,
	domain.CanonicalConfirmed,
	domain.CanonicalFailed,
	domain.CanonicalExpired,
	domain.CanonicalUnderpaid,
	domain.CanonicalOverpaid,
}

func TestPlanEvent(t *testing.T) {
	t.Parallel()

	for _, current := range allStates {
		for _, event := range allEventTypes {
			plan, err := domain.PlanEvent(current, event)
			if current == domain.StateCreating || current == domain.StateCreated {
				require.ErrorIsf(t, err, domain.ErrEventTooEarly, "%s + %s", current, event)
				continue
			}
			if domain.Terminal(current) {
				require.NoErrorf(t, err, "%s + %s", current, event)
				assert.Emptyf(t, plan, "%s + %s: терминальное состояние - no-op", current, event)
				continue
			}
			require.NoErrorf(t, err, "%s + %s", current, event)
			assert.Equalf(t, eventCycles[current][event], plan, "%s + %s", current, event)
		}
	}
}

func TestPlanEventValidChains(t *testing.T) {
	t.Parallel()

	// Каждая построенная цепочка обязана проходить матрицу переходов 4.1.
	for _, current := range allStates {
		for _, event := range allEventTypes {
			plan, err := domain.PlanEvent(current, event)
			if err != nil {
				continue
			}
			from := current
			version := uint64(1)
			for _, target := range plan {
				next, err := domain.NextVersion(from, target, version)
				require.NoErrorf(t, err, "%s: %s -> %s", event, from, target)
				version = next
				from = target
			}
		}
	}
}

func TestPlanEventUnknownType(t *testing.T) {
	t.Parallel()

	_, err := domain.PlanEvent(domain.StatePending, "mystery")
	require.ErrorIs(t, err, domain.ErrUnknownEvent)

	_, err = domain.PlanEvent(domain.StateCompleted, "mystery")
	require.ErrorIs(t, err, domain.ErrUnknownEvent, "неизвестный тип отвергается и для терминальных состояний")
}
