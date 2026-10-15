# CBL-4.8. gRPC-чтения Payment для счета мерчанта

| Поле | Значение |
| :-- | :-- |
| Эпик | E4 (Payment) |
| Репозиторий | `cbl-payment` (+ контракт `cbl-protos@cf-3.3`) |
| Размер | S |
| Волна | W3-BILLING (предпосылка 7.4, FR-8 спеки CBL-7.1) |
| Зависит от | 4.1 (схема), CF-3, тег `cbl-protos@cf-3.3` |
| Статус | done, ревью закрыто |

## 1. Контекст и цель

Ревью 7.1 показало: REST-инвойс мерчанта (`quote{rate, locked_until}`, фильтры
списка, `pay_amount`/`pay_asset`) не имел источника в Payment gRPC: сообщение
`Payment` не несло курс и срок фиксации, `ListPayments` - период, а сами
чтения (`GetPayment`/`ListPayments`) не были реализованы вовсе. 4.8 добавляет
аддитивные поля контракта (`cbl-protos` `CBL-4.8`, тег `cf-3.3`) и реализует
чтения поверх схемы 4.1. `CreatePayment`/`CancelPayment` остаются в 4.2
(блокировка KI-01, шлюз): 7.4 стартует после 4.2, 2.10 и 4.8.

## 2. Границы

In scope: gRPC-сервер PaymentService (`GetPayment`, `ListPayments`), поля
котировки, период списка, миграция `quote_locked_until`. Out of scope:
`CreatePayment`/`CancelPayment` (4.2), фиксация котировки при создании (4.2
заполняет `rate`/`locked_until`), перепроверка статуса шлюза (4.4), REST
Billing API (7.4).

## 3. Контракт (cf-3.3 и cf-3.4)

- `Payment` + `rate = 13`, `locked_until = 14` (cf-3.3), `txid = 15`,
  `confirmations = 16` (cf-3.4); `CreatePaymentResponse` + `rate = 6`,
  `locked_until = 7` (cf-3.3), `expected_amount = 8` (cf-3.4 - источник
  `pay_amount`/`pay_asset` ответа 201).
- `ListPaymentsRequest` + `created_from = 5`, `created_to = 6` (по `created_at`
  транзакции: from включительно, to исключительно); курсор - непрозрачное
  значение `next_cursor` предыдущего ответа.
- `pay_amount`/`pay_asset` чтений выводятся из `expected_amount` (`Money` с
  активом); `fee` в `Payment` не выносится (правила комиссии - KI-payment-06,
  отдельного REST-поля fee в инвойсе нет - webhook-поле питается
  `payment.events`).

## 4. Функциональные требования

FR-1. `GetPayment`: платеж по `payment_id` (UUID) с историей переходов
(`timeline`: состояние, время, причина) и полями котировки (`quote_id`, `rate`,
`locked_until`), суммами (`expected_amount`, `received_amount`), адресом, сетью,
`txid`, `expires_at`; неизвестный id - `NotFound` (`payment_not_found`).

FR-2. `ListPayments`: обязательный `merchant_id` (владелец из токена в 7.4);
фильтры `state_filter` (UNSPECIFIED - нет фильтра, неизвестное значение -
`InvalidArgument`), `created_from`/`created_to` (границы включительно и
исключительно), курсор `payment_id` предыдущей страницы (порядок
`created_at DESC, id DESC`), limit 1 - 200 (дефолт 50); timeline в списке не
заполняется.

FR-3. Хранилище: `ListFilter` дополняется периодом; прямой `INSERT` в тестах
допускает backdate `created_at` (immutable только для UPDATE); невалидный
курсор - `ErrInvalidCursor` (sentinel), неизвестный платеж - `ErrNotFound`.

FR-4. Миграция 00005: колонка `quote_locked_until timestamptz`; триггер
`payments_transition_guard` пересоздается с правилом «fill-once» для полей
котировки (`quote_id`, `rate`, `quote_locked_until`): пока значение NULL,
переход может его заполнить; после заполнения изменение запрещено (миграция
вперед-совместима, Down восстанавливает прежнее тело).

FR-5. Транспорт: валидация недоверенного ввода до хранилища (UUID, обязательный
`merchant_id` и его длина, значение enum, валидность `Timestamp` через
`CheckValid`, длина курсора, порядок границ); сервер регистрируется в main с
`grpcmw.UnaryServer`; `CreatePayment`/`CancelPayment` - `Unimplemented` до 4.2.

FR-6. Тесты: unit - валидация, маппинг состояний (11 значений + неизвестные),
`mapPaymentError`; integration storage - round-trip котировки, fill-once запрет
перезаписи и разрешение позднего заполнения, период/курсор/изоляция мерчантов;
integration bufconn - `GetPayment` (rate/locked_until/timeline) и `ListPayments`
(фильтры, пагинация, границы), `NotFound`; e2e compose - grpcurl-проверки
чтений на реальном сервере.

## 5. Проверки

| Проверка | Команда | Результат |
| :-- | :-- | :-- |
| build/vet | `go build ./...`, `go vet ./...`, `go vet -tags "integration e2e" ./...` | clean |
| unit | `go test ./internal/... -count=1` | green |
| integration | `go test -tags integration ./internal/...` | green (котировка, fill-once, период, кап истории, кламп лимита, bufconn) |
| e2e compose | `make e2e` (schema.ps1, миграции 5, grpcurl rate/locked_until/txid/confirmations/фильтры) | green |
| e2e Kafka | `make test-e2e` (canonical consumer) | green |
| покрытие | `-coverpkg` по internal | 91.7% (было 90.x) |
| lint/эм-деш | golangci-lint в Docker; `check-no-emdash.ps1` | 0 issues; clean |

## 6. Нефункциональные требования

Чтения только по существующим индексам 4.1: PK `payments` для Get и
`payments_merchant_created_idx (merchant_id, created_at DESC, id DESC)` для
листинга с периодом; новых индексов не требуется; дедлайн 1 с (CBL-000,
раздел 3); gRPC-порт не публикуется вне доверенной сети (KI-payment-04);
денежные значения без float.

## 7. Edge cases

Курсор из другой выборки принимается (фильтр `(created_at, id) <`); голого
`payment_id` в курсоре не бывает - контракт описывает непрозрачный
`next_cursor`; `created_to` равен `created_from` - `InvalidArgument` (пустой
период); запись ровно на границе `to` не входит; платеж без котировки
(создание не дошло до `created`) отдается без `rate`/`locked_until`;
терминальные состояния неизменяемы (4.1); чужой/несуществующий `merchant_id` -
пустой список (не 404); `limit` больше 200 клампится; история Get ограничена
последними 500 переходами (неограниченный timeline нетерминального платежа -
вектор роста ответа, KI-payment-07/02: полная история - 14.2); невалидные UUID
нормализуются (`parsed.String()`) до запроса в БД.

## 8. Ревью (CBL-001, шаг 6)

Четыре независимых прохода (15.10.2026). BLOCKER закрыты аддитивным релизом
`cbl-protos@cf-3.4`: `txid`/`confirmations` в `Payment` (REST-инвойс 7.4 без
источника) и `expected_amount` в `CreatePaymentResponse` (полнота 201);
заодно исправлено описание курсора на непрозрачный `next_cursor`. MAJOR
закрыты: предел timeline в `Repository.Get` (последние 500 переходов - защита
от неограниченного ответа при долгоживущем нетерминальном платеже); модель
доверия чтений зафиксирована в T-PAY-08 и расширенном KI-payment-04 (gRPC
без authN: `merchant_id` из тела, Get по UUID, reflection; порт не публиковать
вне доверенной сети, 7.4 берет мерчанта из токена); граница transport ->
usecase -> storage сохранена, слой не пропускает вызовы хранилища напрямую;
нормализация UUID отсекает `urn:uuid`-формы до Postgres-каста. MINOR/NIT
закрыты: `usecase.PaymentQuery` без мертвого log; `%w` в decodeCursor; Down
миграции дословно повторяет тело 00001; контейнер e2e вынесен в переменную;
тесты невалидного/равного периода, пустого списка чужого мерчанта, nil-путей
маппинга, timeline-капа, клампа лимита, курсора чужой выборки; ассерты
`txid`/`confirmations`/`received_amount`/comment в integration и e2e.
Принято осознанно: `fee` не в контракте (KI-payment-06; REST-поля fee в
инвойсе нет); timeline REST не дедуплицирует самопереходы - дедупликацию
определяет 7.4; `CreatePayment`/`CancelPayment` остаются `Unimplemented`
до 4.2 (скоуп 4.2 в CBL-004 расширен на CancelPayment).

| Роль | Ключевые замечания | Закрытие |
| :-- | :-- | :-- |
| Без контекста | BLOCKER: txid обещан FR-1, но отсутствует в контракте; MINOR/NIT: курсор в контракте - payment_id vs base64; мягкая UUID-валидация; нет тестов edge cases (курсор чужой выборки, равные границы, кламп, nil-пути, comment/received), хрупкий sleep | cf-3.4 + маппинг; комментарий контракта исправлен; нормализация UUID; тесты добавлены; seed через автомат/ассерты |
| Архитектуры | BLOCKER: нет txid/confirmations и expected_amount для 201; MAJOR: проекция invoice-полей не разобрана, граница слоев нарушена, документы не синхронизированы; MINOR: CancelPayment без владельца, формулировка fee, timeline-семантика, fail-closed состояния, AGENTS | cf-3.4; §9 спеки (проекция 7.4: Billing DB - amount/currency/meta, Payment - состояние/суммы/сеть); usecase.PaymentQuery без storage-типов в транспорте... фильтр уже usecase; CBL-003/000/004 + protos follow-up; CBL-004 4.2 + Cancel; формулировки; зафиксировано 7.4; stateFromProto fail-closed (пусто -> InvalidArgument для фильтра, UNSPECIFIED наружу не отдается); AGENTS |
| С контекстом | BLOCKER: txid; MAJOR: курсор, полнота обновлений; MINOR: log, %w, Down-комментарий, контейнер e2e; NIT: зависимости, §5/§8 | cf-3.4; исправлено; docs+follow-up; убрано; %w; добавлено; вынесено; заполнено |
| Безопасности | MAJOR: IDOR read-периметра + KI-ссылка неверна; timeline без предела; MINOR: курсор-оракул, rate без CHECK; NIT: reflection, публикация порта, UUID-нормализация, образ grpcurl | T-PAY-08 + KI-payment-04 расширены; кап 500; оракула нет (opaque); CHECK rate - 4.2 (валидирует курс); задокументировано в KI; нормализовано; тег образа как в 10.x |

## 9. Решения и открытые вопросы

- `fee` платежа - KI-payment-06 (правила комиссии 7.x): отдельного REST-поля
  fee в инвойсе нет; webhook-поле питается `payment.events` (CF-2).
- `pay_amount`/`pay_asset` выводятся из `expected_amount`; ответ 201 получает
  `expected_amount` в `CreatePaymentResponse` (cf-3.4) - отдельный вызов
  GetPayment на горячем пути не нужен.
- Проекция инвойса 7.4: `amount`/`currency`, `merchant_order_id`, `metadata`,
  `checkout_url` и фильтр по `merchant_order_id` живут в БД Billing API;
  Payment - источник состояния, сумм, котировки, сети и timeline; курсор
  списка - по Payment (`created_at, id`), проекция джойнится по `payment_id`.
- KI-payment-04 (нет authN на gRPC): ListPayments принимает `merchant_id` из
  тела, GetPayment - по UUID; 7.4 обязан брать мерчанта из токена; до mTLS
  (0.6) порт 9030 не публикуется вне доверенной сети; reflection - dev-удобство.
- Timeline REST: самопереходы (обновление confirmations) попадают в историю;
  дедупликация/фильтрация - решение 7.4; полная история - 14.2.
- `rate` без CHECK в БД: валидация - обязанность писателя (4.2).

## 10. План работ и коммит

Реализация storage/usecase/transport, миграция 00005, тесты, AGENTS/runbook,
коммит `CBL-4.8: gRPC-чтения Payment для счета мерчанта`; контрактный коммит уже
выпущен (`cbl-protos` `CBL-4.8`, тег `cf-3.3`).
