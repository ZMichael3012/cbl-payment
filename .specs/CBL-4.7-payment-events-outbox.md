# CBL-4.7. Публикация payment.events через outbox

| Поле | Значение |
| :-- | :-- |
| Эпик | E4, Payment Service |
| Репозиторий | `cbl-payment` |
| Размер | S |
| Волна и параллельная группа | W2-PAYMENT |
| Зависит от | 4.1, 4.3, 4.5, 4.6 (все источники переходов) |
| Версия контрактов | `cbl-protos@cf-2` (`billing/events/v1/PaymentEvent`, topic `payment.events`) |
| Статус | done, ревью закрыто |

## 1. Контекст и цель

`payment.events` - контракт жизненного цикла платежа для Ledger (2.7), Notification
(8.x) и ClickHouse (11.x): `CBL-003` раздел 6 фиксирует топик, ключ `payment_id`,
типы `payment.<state>`, поля `merchant_id`, `expected_amount`, `received_amount`,
`fee`, `quote_id`, `rate`, `txid`, `state_version`. Схема `PaymentEvent` заморожена
CF-2 (`proto/billing/events/v1/payment_events.proto`).

До 4.7 payment не публикует ничего: переходы автомата (consumer 4.3, политика 4.5,
janitor 4.6) существуют только в `payments`/`payment_transitions`, а Money-контур
Ledger ждет `payment.completed` (KI-ledger-02: путь записи уже открыт через Kafka).
Задача реализует продюсер по модели transactional outbox (`CBL-000` раздел 4):
событие пишется в таблицу `outbox` (00002) в одной транзакции с переходом, relay
kit публикует его в Kafka at-least-once через `FOR UPDATE SKIP LOCKED`.

## 2. Границы

In scope: событийный sink на единой точке переходов `Repository.TransitionTx`,
сборка `PaymentEvent`/конверта, relay в `main`, traceparent-контекст consumer-а,
тесты unit/integration/e2e (Kafka), проверка outbox в compose-сценарии.

Out of scope: потребление `payment.events` (Ledger 2.7 уже реализован, Notification
8.x, ClickHouse 11.x); topics-init глобального стенда (0.5); метрики relay (0.7);
изменение схем CF-2 (заморожена; недостающие типы - вопрос CF-3).

## 3. Функциональные требования

FR-1. Единая точка публикации: `Repository.TransitionTx` после успешного UPDATE
платежа и вставки строки `payment_transitions` вызывает настроенный sink в той же
транзакции. Outbox-строка и переход коммитятся или откатываются вместе: сбой
sink откатывает переход, конфликт версии (`ErrStateConflict`) и пропущенные
(stale/too-early) события не оставляют outbox-строк. `Create` (состояние
`creating`) событий не порождает: `creating` не входит в контракт CF-2.

FR-2. Тип события по целевому состоянию перехода:

| Целевое состояние | event_type | PaymentEventType |
| :-- | :-- | :-- |
| created | payment.created | CREATED |
| detected | payment.detected | DETECTED |
| confirming | payment.confirming | CONFIRMING |
| completed | payment.completed | COMPLETED |
| expired | payment.expired | EXPIRED |
| underpaid | payment.underpaid | UNDERPAID |
| overpaid | payment.overpaid | OVERPAID |
| failed | payment.failed | FAILED |
| pending, cancelled, creating | - (нет типа в CF-2) | - |

Самопереходы (`from == to`: повторное `detected`, набор подтверждений в
`confirming`) событие пишут: state_version растет, событие фиксирует новое
состояние/данные (решение и альтернатива - раздел 11).

FR-3. Тело `billing.events.v1.PaymentEvent` (CF-2): `payment_id`,
`merchant_id`, `type`, `expected_amount` (Money: `minor_units`/`asset`/`decimals`
из `expected_*`), `received_amount` (при заданной полученной сумме), `fee`
(только при полной тройке `fee_amount`/`fee_asset`/`fee_decimals`; частичная
тройка опускается, так как не образует Money), `quote_id`, `rate`, `txid`
(пустые значения - пустые строки), `state_version` (версия после перехода),
`occurred_at` (время строки журнала, авторитетно в теле).

FR-4. Конверт: `topic = payment.events`, `key = payment_id`, заголовки
`event_id` (UUIDv7, генерирует kit), `event_type = payment.<state>`,
`occurred_at` (RFC 3339Nano UTC, зеркалит тело), `producer = cbl-payment`,
`schema_version = v1`, `traceparent` - при наличии в контексте. Traceparent
кладёт consumer 4.3 из заголовков канонического события; janitor и прочие
фоновые переходы идут без него. Валидация W3C-формата - как в леджере
(`storage.WithTraceparent`): невалидный заголовок не попадает в конверт.

FR-5. Relay: в `main` при заданных `KAFKA_BROKERS` рядом с consumer-ом
запускается kit relay (`outbox.NewRelay`) на существующем publisher-е
(at-least-once, батч 100, интервал 250 мс - значения kit); без Kafka переходы
работают, события накапливаются в `outbox` с WARN-логом. Публикация - after-commit
относительно перехода (relay читает только закоммиченные строки).

FR-6. Наблюдаемость: Debug-лог постановки события (`payment event queued in
outbox`, event_id/тип/payment_id/state_version, без сумм и реквизитов); relay
пишет свои логи kit (`outbox published batch`, `outbox publish batch failed`);
при пустом `KAFKA_BROKERS` старт пишет WARN о накоплении событий в outbox.

FR-7. Порядок и дубли: события одного платежа одного состояния несут монотонную
`state_version`; at-least-once допускает дубли (потребители дедуплицируют по
`event_id` inbox-ом и отбрасывают устаревшие версии - `CBL-003`); порядок
публикации строк - по `id` outbox, порядок между транзакциями не гарантируется.

## 4. Контракты

- Kafka: topic `payment.events` (продюсер), ключ `payment_id`; заголовки конверта
  (`event_id`, `event_type`, `occurred_at`, `producer`, `schema_version`,
  `traceparent`) - kit `outbox`; тело - `billing.events.v1.PaymentEvent`
  (`cbl-protos@cf-2`). Retry/DLQ-топики продюсер не использует (они у
  потребителей).
- Хранилище: `outbox` (миграция 00002, DDL из kit), модель - `kit/outbox.Record`;
  новых миграций нет.

## 5. Модель данных и миграций

Таблица `outbox` создана в 4.1 (00002). Изменения схемы не требуются.
`payment_transitions` остается единственным журналом переходов; связь события и
перехода - через `state_version`/`occurred_at` в теле.

## 6. Конечные автоматы и правила

Публикация не меняет матрицу: событие - следствие уже разрешенного перехода.
Событийные типы соответствуют целевым состояниям (FR-2); `cancelled` и `pending`
контракт CF-2 не покрывает (замороженная схема; расширение - вопрос CF-3 и
владельца контракта).

## 7. Edge cases

Переход в `pending`/`cancelled` (события нет); самопереход (событие есть, версия
выросла); чейн из нескольких переходов одной транзакцией (confirmed-first:
`detected` + `confirming` + `completed` - столько же outbox-строк, публикуются по
порядку id); конфликт версии после частичного применения цепочки (outbox-строки
откаченных переходов исчезают вместе с ними); сбой sink (переход откатывается,
ошибка возвращается вызывающему - для consumer это retry-контур, для janitor -
`errors`); `received_amount` NULL (поле опущено); частичная тройка `fee`
(опущена); `decimals` в пределах CHECK 0 - 18 (приведение к uint32 безопасно);
`state_version` из CHECK >= 1 (приведение к uint64 безопасно); пустые
`quote_id`/`rate`/`txid`; отсутствие Kafka (накопление, WARN при старте);
отсутствие топика `payment.events` (relay повторяет публикацию, строки не
теряются); очень длинные строки реквизитов (в лог не пишутся вовсе).

## 8. Нефункциональные требования

Публикация не в горячем пути ответа: outbox-запись - INSERT в транзакцию
перехода (T-PAY-07); relay - отдельная горутина; пакетность kit (100) и частичный
индекс `outbox_pending_idx` (00002); задержка события от перехода до Kafka -
единицы сотен миллисекунд при живом relay (интервал 250 мс); нагрузка relay
учтена бенчмарком леджера (2.9); память события не зависит от тела платежа
(все поля - скаляры).

## 9. План тестов

| Уровень | Сценарии |
| :-- | :-- |
| Smoke | сборка, запуск main без Kafka (WARN, переходы работают) |
| Unit (usecase) | сборка конверта для каждого типа FR-2; пропуск `pending`/`cancelled`/`creating`; самопереход пишет событие; частичная тройка fee опущена; received NULL опущен; traceparent в заголовках; occurred_at в теле и заголовке; producer/schema_version/ключ/топик; state_version |
| Unit (storage) | валидация traceparent-контекста (формат W3C) |
| Integration | реальный PostgreSQL: переход пишет outbox-строку (тело разбирается в PaymentEvent, заголовки); конфликт версии - без строки; цепочка consumer-а (detected, confirming) - две строки в порядке версий; janitor expired - строка `payment.expired`; сбой sink откатывает переход и журнал |
| E2E | Kafka + PostgreSQL (testcontainers): канонические события проводят платеж до completed, relay публикует `payment.events`, внешний потребитель видит `payment.detected`/`payment.confirming`/`payment.completed` с совпадающими state_version и occurred_at; дубликат записи не порождает новых событий |
| E2E локальный | compose (`test/e2e/schema.ps1`): после janitor-событий в `outbox` есть строки `payment.expired`/`payment.failed`, топик создается скриптом, relay помечает их `published_at` |
| Load | 14.1: режим relay учтен; отдельный профиль не требуется |

Целевое покрытие unit+integration: не ниже 80%.

## 10. План работ и коммит

Тесты вперед (usecase unit, integration-эмиссия, e2e Kafka-потребитель
`payment.events`, секция outbox в `schema.ps1`), затем `storage/events.go`
(sink-интерфейс, traceparent), эмиссия в `TransitionTx`, `usecase/payment_events.go`,
обвязка consumer-а и `main` (relay), обновление AGENTS.md. Коммит
`CBL-4.7: публикация payment.events через outbox`; спека в том же коммите.

## 11. Открытые вопросы и решения

- Самопереходы публикуются: событие фиксирует новую `state_version` и обновленные
  данные (полученная сумма при повторном detected, факт продолжения набора
  подтверждений). Альтернатива (публиковать только смену состояния) отклонена:
  скрывает корректировки данных от ClickHouse/Notification; потребители обязаны
  дедуплицировать по `event_id` и отбрасывать устаревшие `state_version`.
- `cancelled` не имеет типа в CF-2 (заморожено до 4.7): событие не публикуется;
  если Notification понадобится уведомление об отмене, тип добавляется владельцем
  контракта (CF-3) отдельной задачей.
- Тип для `pending` в CF-2 отсутствует осознанно: `payment.created` покрывает
  факт готовности счета; адрес мерчант получает синхронно от 4.2 (после KI-01).
- Ретеншен outbox и партиционирование - KI-payment-02 (14.2), как у леджера
  (KI-ledger-09).
- Перепроверка `completed` (4.4) после KI-01 добавит события без изменения схемы.
- `BuildPaymentEventEnvelope` экспортирован для unit-тестов и повторного
  использования (4.4); traceparent в него приходит уже валидированным
  `storage.WithTraceparent` (повторная валидация в сборщике не выполняется -
  единственная точка входа недоверенного заголовка - storage).
- Экспорт `event_type` строкой (`payment.<state>`) и enum собираются из одной
  функции маппинга; при добавлении состояния правится одна таблица.
- GORM-логирование сервиса (значения в SQL при ошибках) не меняется в 4.7:
  стиль общий для платформы, параметризованный логгер - вместе с 0.7.

## 12. Ревью (CBL-001, шаг 6)

Четыре независимых прохода (15.10.2026). Блокеров нет. MAJOR закрыты:
AGENTS.md обновлен в этом коммите (T-PAY-07, журнал, runbook outbox,
KI-payment-04 переформулирован: с 4.7 подделка канонического события доходит до
реальных проводок Ledger, ACL нужен и на `payment.events`, prod запрещен до
0.5/0.6); при пустом `KAFKA_BROKERS` добавлен WARN о накоплении событий;
добавлены unit-тесты storage (валидация traceparent, включая injection и
сверхдлинный заголовок); e2e-потребитель сопоставляет события по типу, не по
позиции в батче; интеграционно проверены `payment.failed` janitor-а и реальный
невалидный traceparent; `emitTransitionEvent` не затирает явно заданный
traceparent; traceparent в логах consumer-а усечен `shortID` (защита от
раздутого недоверенного заголовка); ассерты порядка по id исправлены;
outbox-проверки в `schema.ps1` привязаны к `key` платежа.
MINOR/NIT приняты без изменений: экспорт сборщика конверта (unit-тесты, повторное
использование 4.4), строгость W3C-regex как в леджере (консистентность),
отсутствие join relay-горутины при shutdown (потери исключены, возможен
дубль - at-least-once), GORM-логирование (см. §11).

| Роль | Ключевые замечания | Закрытие |
| :-- | :-- | :-- |
| Без контекста | MAJOR: батчевый порядок у потребителя в e2e; затирание `TransitionEvent.Traceparent`; неполные сценарии failed-события/сбоя sink; MINOR: комментарий traceparent, WARN без Kafka, текст лога, границы decimals, недостижимый fallback; NIT: nil-асимметрия, останов relay, хрупкость политики | сопоставление по типу события; `if event.Traceparent == ""`; тест `payment.failed`, проверка отсутствия строк при сбое; исправлено; WARN добавлен; спека выровнена по тексту; тест decimals 0/18; fallback оставлен осознанно; подтверждено; relay гасится ctx; тест валиден (комментарий) |
| Архитектуры | MAJOR: AGENTS.md не обновлен; MINOR: правило stale state_version вне CBL-003, WARN без Kafka, затирание traceparent, порядок при репликах, отсутствие топика - в runbook; NIT: двойной маппинг state->type, сигнатура сборщика, строгость regex, неиспользуемые поля, хрупкие count в schema.ps1 | AGENTS (T-PAY-07, журнал, KI); CBL-003 дополнен правилом; WARN; исправлено; T-PAY-07; runbook; единая функция маппинга; сигнатура оставлена; консистентность с леджером; From/Traceparent - для 4.4; привязка к key |
| С контекстом | MAJOR: шаг 7 (AGENTS, KI-02/14.2, журнал, runbook), WARN FR-5, unit-тесты storage; MINOR: затирание traceparent, текст лога, невалидный traceparent в integration, e2e без проверки лишних; NIT: self-сравнение id, экспорт сборщика, default-ветка, пустой Rate | AGENTS обновлен; WARN; `internal/storage/events_test.go`; исправлено; спека/код выровнены; тест добавлен; проверка по типам + count outbox; исправлено; решение в §11; покрыто через skip-тест; проверено |
| Безопасности | BLOCKER нет; MAJOR: KI-payment-04 - реальный денежный канал, обновить KI/runbook/ACL на payment.events; MINOR: сырой traceparent в логах, экспорт сборщика без валидации, нестрогий regex, тихий режим без sink, WARN relay, GORM-логи; NIT: интерполяция (безопасна), join relay | KI-payment-04 обновлен (+KI-ledger-02, runbook разбора `payment.completed`); `shortID` в логах; §11 (точка валидации - storage); консистентность; осознанно (тесты без sink); WARN добавлен; §11 + 0.7; подтверждено; принято (at-least-once) |
