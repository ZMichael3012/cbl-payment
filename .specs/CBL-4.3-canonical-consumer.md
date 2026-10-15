# CBL-4.3. Consumer канонических событий: дубли и порядок

| Поле | Значение |
| :-- | :-- |
| Эпик | E4, Payment Service |
| Репозиторий | `cbl-payment` |
| Размер | M |
| Волна и параллельная группа | W2-PAYMENT |
| Зависит от | 4.1, 3.5 |
| Версия контрактов | `cbl-protos@cf-2` (`billing/events/v1`, `CanonicalPaymentEvent`) |
| Статус | done, ревью закрыто |

## 1. Контекст и цель

Connector (3.5) публикует канонические события шлюза в `gateway.events.canonical`.
Payment обязан перевести автомат (4.1) в состояние, соответствующее событию:
`detected`, `confirmed`, `failed`, `expired`, `underpaid`, `overpaid`. Kafka дает
at-least-once и порядок только внутри партиции: дубли и приход событий не по порядку
(подтверждение раньше обнаружения, повтор после терминального состояния) обязаны быть
безопасными.

## 2. Границы

In scope: consumer-группа `cbl-payment-canonical`, маппинг канонических событий в
цепочки переходов, применение с CAS и повторами, защита от дублей и устаревших
событий, обвязка main.
Out of scope: пороги подтверждений и `completed` по сумме (4.5), перепроверка статуса
через `GetGatewayPayment` (4.4), публикация `payment.events` (4.7), нормализация
вебхуков (3.5).

## 3. Функциональные требования

FR-1. Подписка на `gateway.events.canonical` (группа `cbl-payment-canonical`) через
kit `inbox.Processor` + `kafkaconsumer`: retry-топики 5s/1m/10m и DLQ, забрасывание
через продюсера kit (как в 2.7). Обработка идемпотентна; бизнес-эффект - только
переходы автомата.

FR-2. План события (домен): функция `PlanEvent(current, event)` возвращает цепочку
переходов, признак «устаревшее» (пустая цепочка, подтверждение без эффекта) или
`ErrEventTooEarly` (событие опередило создание счета - повтор через retry-топики):

| Текущее | detected | confirmed | failed | expired | underpaid/overpaid |
| :-- | :-- | :-- | :-- | :-- | :-- |
| creating/created | too early | too early | too early | too early | too early |
| pending | detected | detected, confirming | failed | expired | detected, terminal |
| detected | detected (self) | confirming | failed | stale | terminal |
| confirming | stale | confirming (self) | failed | stale | terminal |
| терминальные | stale | stale | stale | stale | stale |

Терминальные: `completed`, `expired`, `underpaid`, `overpaid`, `failed`, `cancelled`.

FR-3. Применение: `PaymentTx` (снимок) -> `PlanEvent` -> последовательные
`TransitionTx` с CAS; полевой патч события (полученная сумма, `txid`, `gateway_id`,
`provider`, `network`, `confirmations`) применяется на целевом (последнем) шаге
цепочки; `confirmations` пишутся только для `detected`/`confirmed`. Переходы и
inbox-запись коммитятся в одной транзакции Guard (`CBL-003`, раздел 6): сбой
обработки откатывает эффект вместе со строкой inbox. `ErrStateConflict` (гонка с
4.2/4.4/фоновыми задачами) - повтор: перечитать, заново построить план (до 3
попыток), затем вернуть ошибку в retry-топики.

FR-4. Дубли: `event_id` из конверта защищен inbox-таблицей (kit Processor);
повторная доставка того же события не создает второго эффекта. Устаревшее событие
(FR-2) подтверждается (ack) с WARN-логом: версия и журнал не меняются.

FR-5. Ошибки входа: невалидное событие (битый protobuf, невалидный `payment_id`,
отрицательная сумма, шкала вне 0 - 18, пустой или слишком длинный идентификатор,
`confirmations` больше 1 000 000, отсутствие полученной суммы для
`detected`/`confirmed`/`underpaid`/`overpaid`, несовпадение актива или шкалы с
ожидаемыми реквизитами платежа) - `errInvalidCanonicalEvent` -> retry-топики и DLQ;
актив нормализуется в верхний регистр; `UNSPECIFIED`/неизвестный enum - WARN и ack
без эффекта; платеж не найден - ошибка (повтор): событие не может появиться раньше
транзакции создания (гонка с 4.2 закрывается ретраями). Недоверенные идентификаторы
в ошибках усекаются (не раздувают DLQ-конверт).

FR-6. main: при заданном `KAFKA_BROKERS` поднимается consumer (группа из
`KAFKA_CONSUMER_GROUP` или `cbl-payment-canonical`); без брокеров - предупреждение,
HTTP/gRPC работают.

## 4. Контракты

Вход - `billing.events.v1.CanonicalPaymentEvent` (CF-2): `payment_id`, `gateway_id`,
`provider`, `type`, `received_amount`, `confirmations`, `txid`, `network`,
`gateway_event_ts`. Исходящих контрактов нет.

## 5. Модель данных и миграции

Новых таблиц нет: используются `payments`/`payment_transitions` (4.1) и inbox (00003).

## 6. Конечные автоматы

Цепочки FR-2 применяются строго через матрицу 4.1; событие не может перевести платеж
в состояние, недостижимое по матрице.

## 7. Edge cases

Подтверждение раньше обнаружения (`pending -> detected -> confirming` одной командой);
повтор `detected`/`confirmed` с новым `event_id` (self-переход, версия +1, журнал;
дубли того же `event_id` отсекает inbox); `detected` после `completed` (stale, WARN,
ack); `failed` после терминального (stale); `expired` после обнаружения (stale);
`underpaid` из `pending` (цепочка из двух переходов); событие для
`creating`/`created` (too early, ретраи); событие неизвестного платежа (ретраи,
затем DLQ); конфликт версии из-за параллельного перехода (повтор плана, до 3);
невалидный protobuf/сумма/актив (DLQ); актив в нижнем регистре (нормализуется);
несовпадение актива/шкалы с ожидаемыми (DLQ); отсутствие суммы для денежных типов
(DLQ); `confirmations` больше предела (DLQ); сверхдлинные идентификаторы (DLQ,
в ошибках усекаются); неизвестный тип enum (ack); сбой обработки после первого
перехода цепочки (откат всего вместе с inbox); терминальное состояние + неизвестный
тип (ErrUnknownEvent в домене).

## 8. Нефункциональные требования

Обработка без блокировок: чтение снимка + CAS-переходы (T-PAY-02); число записей к
БД на событие - 1 чтение + 1 - 2 перехода; повторные конфликты ограничены 3
попытками с последующей передачей в retry-механику; переходы и inbox-строка
атомарны (транзакция Guard); сырое событие не логируется целиком (только payment_id,
тип, версии); traceparent из заголовков конверта попадает в логи consumer-а
(сквозная трассировка - 0.7).

## 9. План тестов

| Уровень | Сценарии |
| :-- | :-- |
| Unit (домен) | полная таблица FR-2: каждая пара (состояние, событие) - цепочка/stale/too early; неизвестный тип |
| Unit (usecase) | фейковое хранилище: применение цепочки и патча, stale ack, too-early ошибка, повтор после конфликта версии, исчерпание попыток, невалидные события |
| Integration | реальный PostgreSQL: handler применяет detected и сохраняет поля; confirmed-first цепочка; дубликат не меняет состояние и журнал; stale после завершения |
| E2E | Kafka + PostgreSQL: канонические detected + confirmed из Kafka переводят платеж в `confirming`; дубликат записи не растит версию; устаревшее detected после терминального - no-op |
| Load | 14.1 |

## 10. План работ и коммит

Тесты вперед (домен, usecase, integration, e2e), затем domain/canonical.go,
usecase/canonical_consumer.go, обвязка main. Коммит
`CBL-4.3: consumer канонических событий с защитой от дублей и порядка`;
спека в том же коммите.

## 11. Открытые вопросы

- Порог подтверждений (4.5) заменит self-переход `confirming` на `completed` по
  сумме; 4.3 сохраняет `confirmations` (предел 1 000 000 - защита от подделки).
- Перепроверка `GetGatewayPayment` перед `completed` - 4.4 (вызов Connector 3.2,
  заблокирован KI-01): 4.3 не вызывает Connector.
- Границы доверия Kafka: события не подписаны, дедупликация по `event_id` из
  заголовка; участник с produce-правами может двигать состояния до `confirming`
  и терминальных, но не `completed` (4.4/4.5); ACL/mTLS - 0.5/0.6 (KI-payment-04).
- Too-early событие опирается на номинальные retry-паузы 5s/1m/10m: если платеж
  долго остается в `creating`/`created` (таймаут CreateInvoice, 4.6), валидное
  событие уйдет в DLQ и потребует ручного replay (KI-payment-05, runbook).
- Отложенная обработка retry-топиков (отдельные паузы вместо мгновенного чтения
  тем же консьюмером) - общая тема kit (как в 2.7), вне 4.3.

## 12. Ревью (CBL-001, шаг 6)

Четыре независимых прохода (15.10.2026). Блокеров нет. MAJOR всех ролей
(атомарность inbox и переходов) закрыт переходом на транзакцию Guard: consumer и
репозиторий получили tx-aware методы (`PaymentTx`/`TransitionTx`), добавлен
integration-тест отката; MAJOR недоверенного входа закрыты нормализацией актива,
сверкой с ожидаемыми реквизитами платежа, требованием суммы для денежных типов,
пределом `confirmations` и усечением идентификаторов в ошибках; MAJOR границ
доверия Kafka зафиксирован в KI-payment-04; MAJOR poison-цикла (раздувание
x-error) закрыт усечением; MINOR/NIT закрыты тестами (lowercase, mismatch,
missing received, huge confirmations, все типы enum, confirmed-first,
дубликаты через Guard, атомарность), целью `make test-e2e`, пулом БД, WARN для
stale и обновлением документации.

| Роль | Ключевые замечания | Закрытие |
| :-- | :-- | :-- |
| Без контекста | MAJOR: tx Guard игнорируется (неатомарно); confirmations для underpaid/overpaid теряются; stale логируется Info; traceparent не реализован; отсутствие суммы принимается; lowercase asset падает на DB CHECK; недостающие integration-тесты; e2e вне Makefile; FR-3 против кода; network вне FR-3; мертвый код; sleep-и в e2e | tx-aware методы + атомарный тест; confirmations только для detected/confirmed (спека); Warn; traceparent в логах; сумма обязательна для денежных типов; нормализация + сверка; тесты добавлены; `make test-e2e`; FR-3 уточнен; network в FR-3; мертвый код убран; e2e через inbox-маркеры |
| Архитектуры | MAJOR: атомарность; lowercase asset; traceparent; порядок по gateway_event_ts не реализован (T-CONNECTOR-05); AGENTS не обновлен; MINOR: патч на последнем шаге vs FR-3, Info, integration-план, e2e вне make, too-early бюджет; NIT: неизвестный тип в терминальном, лишний timeline в Get, пул БД | атомарность + T-PAY-04; нормализация; traceparent; монотонность по времени события делегирована 4.5 (спека §11), T-CONNECTOR-05 согласован; AGENTS обновлен; FR-3 приведен к коду; Warn; тесты; make; KI-payment-05; unknown-first в PlanEvent; `PaymentTx` без истории; пул настроен |
| С контекстом | MAJOR: lowercase asset (последний барьер); документация шага 7; MINOR: AGENTS содержимое, WARN, FR-3/код/network/confirmations, integration confirmed-first, network без differ-теста, покрытие canonicalType, traceparent; NIT: мертвый код, дубли helpers, sleep, узкий интерфейс, fmt.Sprintf | нормализация + mismatch-тест; Makefile/AGENTS; журнал/T-PAY-04/KI/runbook; Warn; спека синхронизирована; тесты добавлены (network erc20, chain, duplicates); таблица типов; traceparent; убрано; helpers оставлены (разные пакеты, NIT); inbox-маркеры; `PaymentTx`; `fmt.Sprintf` убран |
| Безопасности | MAJOR: атомарность; MAJOR: сумма/актив/network не сверяются с ожидаемыми; MAJOR: границы доверия Kafka; MAJOR: poison-цикл размера x-error; MINOR: confirmations без предела, длины строк, self-переходы при новом event_id, граничные тесты, номинальные ретраи | атомарность; сверка asset/decimals с expected (network патчится осознанно); KI-payment-04; усечение идентификаторов (краткое payment_id, лимиты 256); предел confirmations 1e6; лимиты полей; self-переходы задокументированы (4.5 читает сумму перед терминалом); тесты границ; KI-payment-05 |
