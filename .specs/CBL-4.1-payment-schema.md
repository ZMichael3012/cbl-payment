# CBL-4.1. Схема payment, автомат состояний и state_version

| Поле | Значение |
| :-- | :-- |
| Эпик | E4, Payment Service |
| Репозиторий | `cbl-payment` |
| Размер | L |
| Волна и параллельная группа | W2-PAYMENT |
| Зависит от | CF-2, 1.2 (cbl-kit/outbox), 1.3 (cbl-kit/inbox) |
| Версия контрактов | `cbl-protos@cf-2` (`billing/payment/v1`, `billing/events/v1`) |
| Статус | done, ревью закрыто |

## 1. Контекст и цель

Payment владеет конечным автоматом платежа (`CBL-000`, раздел 2): источник истины о
состоянии счета, защищенный от переходов не по порядку и от гонок между событиями и
запросами. Задача создает схему БД (payments, история переходов, outbox/inbox),
доменную матрицу переходов и `state_version` с оптимистической блокировкой - фундамент
для 4.2 - 4.7.

## 2. Границы

In scope: миграции 00001 - 00003, домен конечного автомата, storage-слой переходов с
CAS, тесты, e2e проверки схемы.
Out of scope: gRPC-обработчики CreatePayment/GetPayment (4.2), consumer канонических
событий (4.3), пороги подтверждений (4.5), фоновые задачи (4.6), публикация
`payment.events` (4.7), интеграция с Quote/Connector (4.2, 3.2).

## 3. Функциональные требования

FR-1. Схема `payments` (миграция 00001): `id` uuid PK (UUIDv7), `merchant_id`,
`idempotency_key` + `request_hash` с уникальностью `(merchant_id, idempotency_key)`,
`state` (CHECK по каноническим состояниям), `state_version bigint >= 1`, суммы
`expected_*` (обязательные, `> 0`), `received_*` и `fee_*` (nullable до обнаружения,
`>= 0` при задании), `quote_id`, `rate`, `pay_address`, `network`, `gateway_id`,
`provider`, `txid`, `confirmations`, `expires_at`, `created_at`, `updated_at`,
`completed_at` (CHECK: только для `completed`/`underpaid`/`overpaid`). Asset-поля по
формату `^[A-Z0-9]{2,16}$`, decimals 0 - 18.
Индексы: `(merchant_id, created_at DESC, id DESC)` для ListPayments; частичный
`(expires_at) WHERE state IN ('created','pending')` для истечения (4.6); частичный
`(updated_at) WHERE state IN ('detected','confirming')` для зависших (4.6).

FR-2. История переходов `payment_transitions` (append-only, триггер UPDATE/DELETE/
TRUNCATE): `payment_id` (FK), `from_state` (пусто для создания; CHECK по словарю
состояний), `to_state` (CHECK по словарю), `state_version`, `reason`, `occurred_at`;
уникальность `(payment_id, state_version)`. Timeline платежа (proto `TimelineItem`)
собирается из этой таблицы.

FR-3. Матрица переходов (домен, единственный источник правил):

```
creating   -> created | cancelled | failed
created    -> pending | cancelled | expired | failed
pending    -> detected | cancelled | expired | failed
detected   -> confirming | underpaid | overpaid | failed
confirming -> completed | underpaid | overpaid | failed
terminal: completed, expired, underpaid, overpaid, failed, cancelled
```

Семантика: `creating` - запись создана, котировка фиксируется/шлюз вызывается (4.2);
`created` - котировка зафиксирована, адрес шлюза ожидается (таймаут CreateInvoice
доводится повтором или фоновой задачей 4.6); `pending` - счет создан в шлюзе, выдан
адрес (последовательность приема платежа, `CBL-000`); `detected`/`confirming` -
обнаружена оплата и набор подтверждений (4.3, 4.5); `underpaid`/`overpaid`,
`expired`, `failed`, `cancelled` - терминальные. Переходы назад и из терминальных
состояний запрещены; подписи `allowed(from, to)` и `terminal(state)`.
Самопереход (обновление данных без смены состояния, например рост `confirmations`
из повторного канонического события) допустим только в нетерминальных состояниях,
увеличивает `state_version` на 1 и пишется в журнал с одинаковыми from/to.

FR-4. `state_version`: при создании равна 1, каждый принятый переход (включая
самопереход) увеличивает ровно на 1. Переход выполняется оптимистично: чтение строки под
row-локом и `UPDATE ... WHERE id = ? AND state_version = ?`; конфликт версии -
`domain.ErrStateConflict` для повторного цикла вызывающего, устаревшее событие не
применяется (запрет переходов не по порядку, вход для 4.3). В той же транзакции
пишется строка `payment_transitions`.

FR-5. Второй барьер БД: BEFORE INSERT триггер `payments` требует
`state='creating' AND state_version=1` (создание в обход автомата невозможно);
BEFORE UPDATE триггер проверяет пары `(OLD.state, NEW.state)` по матрице и
`NEW.state_version = OLD.state_version + 1`; запрещены изменения неизменяемых полей
(id, merchant_id, idempotency_key, request_hash, created_at, expected_*) и
самопереходы в терминальных состояниях. Матрица Go и SQL проверяются полным
перебором всех пар состояний в integration-тесте (дрейф ловится).

FR-6. Миграции 00002 (outbox) и 00003 (inbox) - reference-DDL из `cbl-kit`
(`CBL-003`, раздел 6), как в леджере (2.1).

FR-7. Storage: `Create` (вставка `creating` версии 1 - состояние и версия
принудительно приводятся к начальным, строка истории версии 1 в одной транзакции),
`Transition` (CAS по `state_version` + история), `Get` (платеж и timeline в одной
read-only транзакции repeatable read), `List` (фильтр состояния, курсор
`(created_at, id)`, limit 1 - 200, по умолчанию 50), `FindExpired` (состояния
`created`/`pending`: `creating` доводится фоном до `created` или `failed`, перехода
`creating -> expired` в матрице нет), `FindStuck` (для 4.6).

## 4. Контракты

Внешние контракты не меняются (CF-1/CF-2 заморожены): домен `State` соответствует
`billing.payment.v1.PaymentState` (маппинг 1:1 по именам, проверяется тестом);
`state_version` публикуется в `payment.events` (4.7) и в `Payment` (CF-1).

## 5. Модель данных и миграции

```sql
payments(id, merchant_id, idempotency_key, request_hash, state, state_version,
         expected_amount, expected_asset, expected_decimals,
         received_amount, received_asset, received_decimals,
         fee_amount, fee_asset, fee_decimals,
         quote_id, rate, pay_address, network, gateway_id, provider, txid,
         confirmations, expires_at, created_at, updated_at, completed_at)
payment_transitions(id bigserial, payment_id, from_state, to_state, state_version,
                    reason, occurred_at, UNIQUE (payment_id, state_version))
outbox, inbox - как в CBL-003 (раздел 6)
```

## 6. Конечные автоматы

Матрица FR-3; состояния и переходы продублированы в spec-таблице тестов (каждая пара
матрицы и каждый запрещенный класс: назад, из терминального, неизвестное состояние).

## 7. Edge cases

Создание с версией 1 (переданные вызывающим состояние и версия игнорируются;
прямой INSERT с другим состоянием отклоняет BEFORE INSERT триггер); конкурентные
переходы из одной версии (ровно один выигрывает); устаревшее событие после более
нового перехода (отказ, версия и журнал не растут); переход назад
(`confirming -> detected`), переход из терминального, неизвестная пара (отклоняют
домен и триггер, полный перебор в тесте); самопереход confirmations в нетерминальном
состоянии (разрешен, версия +1, строка журнала) и в терминальном (запрещен); прямое
SQL-обновление в обход CAS (отклоняет триггер); UPDATE неизменяемых полей;
UPDATE/DELETE/TRUNCATE `payment_transitions`; запись в журнал с неизвестным
состоянием (словарный CHECK); дубликат `(merchant_id, idempotency_key)` (4.2);
отрицательные/нулевые суммы, пустой актив, decimals вне диапазона;
`completed_at` вне терминального успеха (CHECK); `expires_at` в прошлом (истечение
не применяется к `creating` - доводится до `created`/`failed` фоном 4.6);
`pay_address` до `pending` (упорядочивается сценарием 4.2).

## 8. Нефункциональные требования

Транзакционность: переход и его история - одна транзакция; конкурентность - только
оптимистичные повторы (без глобальных локов, как в леджере); журнал переходов
append-only и является аудитом состояний; бюджет GetPayment 1 с (CF-1) укладывается в
чтение по PK; ListPayments использует индекс мерчанта.

## 9. План тестов

| Уровень | Сценарии |
| :-- | :-- |
| Unit (домен) | вся матрица (каждая разрешенная пара), запрещенные пары (назад, из терминального, неизвестные состояния), terminal(), маппинг `PaymentState` 1:1, инкремент версии, самопереходы |
| Integration (storage) | Create с историей v1 и принудительным `creating`; CAS-переход и инкремент; конкурентные переходы (ровно один); устаревшая версия (версия и журнал не растут); полный перебор всех пар состояний против триггера (зеркало матрицы); прямой UPDATE/INSERT в обход автомата; неизменяемые поля; `completed_at` только в терминальном успехе; словарь журнала; append-only `payment_transitions`; List с курсором и фильтром; FindExpired/FindStuck; CHECK-и сумм/активов/шкал; уникальность ключа идемпотентности |
| E2E | compose: миграции применены (версия 3), полная цепочка `creating -> created -> pending -> detected -> confirming -> completed` с записью каждого перехода, запрещенный переход и перезапись журнала отклоняются вторым барьером |
| Load | 14.1 |

## 10. План работ и коммит

Тесты вперед (домен, storage integration), затем миграции, domain/state.go,
storage/repository.go, e2e-скрипт стенда. Коммит
`CBL-4.1: схема платежа, автомат состояний и state_version`; спека в том же коммите.

## 11. Открытые вопросы и решения ревью

- `created` против `creating` при таймауте CreateInvoice (4.2): котировка фиксируется
  до вызова шлюза, поэтому таймаут оставляет платеж в `created` (адрес ожидается);
  формулировка CBL-003 (раздел 5) уточнена, задача 4.2 реализует поведение.
- `creating` не входит в выборку истечения: перехода `creating -> expired` в матрице
  нет, зависшие создания доводятся фоном (4.6) до `created` или `failed`. Решение
  зафиксировано в FR-7 и проиндексировано (`payments_expiry_idx`).
- `payment_id` в котировке Quote (quotes.payment_id) и связь котировки с платежом -
  4.2 (LockQuote не принимает payment_id в CF-1). Чтение платежа по
  `(merchant_id, idempotency_key)` для идемпотентного повтора - 4.2.
- Второй барьер защищает от DML-ролей; владелец БД может снять триггеры
  (KI-payment-03): роли migrator/runtime разделяются при выходе в prod (0.4/0.6).

## 12. Ревью (CBL-001, шаг 6)

Четыре независимых прохода (15.10.2026). BLOCKER безопасности (прямой INSERT создавал
платеж в любом состоянии в обход автомата) закрыт BEFORE INSERT триггером,
принудительным `creating` в `Create` и тестами; BLOCKER ревьюера с контекстом
(зонтичный compose собирал payment без контекста services/) закрыт правкой
`deploy/docker-compose.yml` и проверкой сборки; MAJOR (FindExpired включал
`creating` без перехода в `expired`; `completed_at` без ограничения; E2E без полной
цепочки; журнал задач пуст) закрыты. MINOR/NIT закрыты тестами (полный перебор пар
против триггера, границы CHECK, словарь журнала, устаревшая версия не растит журнал),
выравниванием зависимостей и линтер-конфига v2, транзакционным `Get`, уточнениями
спеки и CBL-002.

| Роль | Ключевые замечания | Закрытие |
| :-- | :-- | :-- |
| Без контекста | MAJOR: E2E без полной цепочки; Create принимает чужое состояние; ожидание postgres не работает и не ждет миграции; Get без согласованного снимка; SQL-матрица не зеркалится полным тестом; `completed_at` не ограничен; направление индекса; любой 23505 в ErrDuplicateKey; курсор без UUID; дубли package-комментариев; границы CHECK без тестов; план про usecase-заготовки | E2E проходит цепочку до completed с журналом; Create принудительно creating/1 + BEFORE INSERT; ожидание версии goose с ретраями; Get в repeatable read; тест полного перебора 121 пары; CHECK completed_at + тест; индекс id DESC; проверка имени constraint; UUID-валидация курсора; комментарии убраны; границы расширены; план исправлен |
| Архитектуры | MAJOR: FindExpired/индекс включали creating без перехода; completed_at без контроля; MINOR: Create, INSERT в обход, роли prod, T-PAY-01 про тест, creating/created в докблоках/CBL-003, кламп limit, propose Get, e2e | creating исключен из истечения; CHECK completed_at; Create форсирует; BEFORE INSERT; KI-payment-03; полный матричный тест; CBL-003 и комментарии уточнены; кламп унифицирован; транзакционный Get; e2e расширен |
| С контекстом | BLOCKER: зонтичный compose payment; MAJOR: журнал пуст, CBL-003 не уточнен, e2e цепочка; MINOR: lint-конфиг v1, T-PAY-01 рендер, Create, Get, NextVersion неизвестное состояние, edge cases, ожидание стенда, текст ошибок e2e, план, CBL-002 | зонтичный compose исправлен (сборка проверена); журнал заполнен; CBL-003 уточнен; e2e; `.golangci.yml` v2 (gosec); таблица T-PAY-01 исправлена; Create/Get/NextVersion; edge cases закрыты CHECK/спекой; ожидание через goose с ретраями и текстом ошибки; план; CBL-002 обновлен |
| Безопасности | BLOCKER: INSERT обходит FSM и аудит; MINOR: роли владельца, подделка журнала, `completed_at`/длины, дрейф версий зависимостей, gosec отсутствует; NIT: e2e UUID, reflection, .dockerignore | закрыт триггером + Create + тестами; KI-payment-03; словарные CHECK журнала; CHECK completed_at; версии выровнены с единым набором; gosec включен в v2-конфиг; NIT зафиксированы (reflection - 0.7) |
