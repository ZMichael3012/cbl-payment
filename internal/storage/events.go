package storage

import (
	"context"
	"regexp"
	"time"

	"gorm.io/gorm"

	"github.com/cbl/cbl-payment/internal/domain"
)

// TransitionEvent описывает примененный переход состояния для публикации в
// payment.events (4.7): снимок платежа после перехода и служебные поля журнала.
type TransitionEvent struct {
	// Payment задает снимок платежа после перехода (включая новую версию).
	Payment Payment
	// From задает состояние до перехода.
	From domain.State
	// To задает состояние после перехода.
	To domain.State
	// OccurredAt задает время перехода (строка журнала).
	OccurredAt time.Time
	// Traceparent задает W3C trace context для конверта (если известен).
	Traceparent string
}

// TransitionEventSink записывает событие payment.events в outbox в транзакции
// перехода: событие и бизнес-изменение коммитятся или откатываются вместе.
type TransitionEventSink interface {
	// AppendTransitionEvent добавляет конверт события в переданной транзакции.
	AppendTransitionEvent(ctx context.Context, tx *gorm.DB, event TransitionEvent) error
}

// WithEventSink подключает запись payment.events; без sink события не пишутся.
func WithEventSink(sink TransitionEventSink) Option {
	return func(r *Repository) { r.eventSink = sink }
}

// emitTransitionEvent передает событие sink-у, если он настроен; явно заданный
// traceparent не затирается значением из контекста.
func (r *Repository) emitTransitionEvent(ctx context.Context, tx *gorm.DB, event TransitionEvent) error {
	if r.eventSink == nil {
		return nil
	}
	if event.Traceparent == "" {
		event.Traceparent = traceparentFromContext(ctx)
	}
	return r.eventSink.AppendTransitionEvent(ctx, tx, event)
}

// traceparentKey задает ключ контекста для W3C traceparent.
type traceparentKey struct{}

// traceparentPattern задает формат W3C trace context; произвольная строка из
// заголовка Kafka не должна попадать в конверты и логи downstream.
var traceparentPattern = regexp.MustCompile(`^[0-9a-f]{2}-[0-9a-f]{32}-[0-9a-f]{16}-[0-9a-f]{2}$`)

// WithTraceparent сохраняет traceparent в контексте: consumer кладет заголовок
// канонического события, и событие платежа наследует трассировку источника.
// Невалидный по формату W3C заголовок игнорируется.
func WithTraceparent(ctx context.Context, traceparent string) context.Context {
	if !traceparentPattern.MatchString(traceparent) {
		return ctx
	}
	return context.WithValue(ctx, traceparentKey{}, traceparent)
}

// traceparentFromContext извлекает traceparent из контекста.
func traceparentFromContext(ctx context.Context) string {
	value, _ := ctx.Value(traceparentKey{}).(string)
	return value
}
