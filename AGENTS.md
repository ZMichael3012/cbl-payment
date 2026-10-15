# AGENTS.md - cbl-payment

Автомат состояний платежа: создание счета, обработка канонических событий, пороги подтверждений.

Обязательные глобальные правила: `../../AGENTS.md`, `../../.specs/CBL-001-development-process.md`,
`../../.specs/CBL-002-repos-and-docker.md`, контракты `../../.specs/CBL-003-contracts.md`.
Длинное тире (U+2014) запрещено, используется только дефис.

## 1. Границы

In scope: конечный автомат платежа, payment.events, фоновые задачи истечения и зависших платежей.

Out of scope: другие сервисы платформы; взаимодействие только через контракты `cbl-protos`
(gRPC), Kafka и REST периметра (см. `CBL-000`, раздел 2).

## 2. Контракты

- REST: административный /healthz, /readyz
- gRPC: сервер PaymentService; клиент ConnectorService, QuoteService
- Kafka продюсирует: payment.events
- Kafka потребляет: gateway.events.canonical (группа `cbl-payment-canonical`)
- Хранилище: PostgreSQL payment

Контракты заморожены в `cbl-protos` (CF-1 для gRPC, CF-2/CF-3 для событий), см. `CBL-004`.

## 3. Архитектура

Слои: `internal/transport` -> `internal/usecase` -> `internal/domain` -> `internal/storage`.
Библиотеки: стандартный набор `CBL-002` (Gin, GORM + pgx-драйвер, franz-go + pkg/sr, go-redis,
goose, zap). `AutoMigrate` запрещен, миграции только goose, только вперед.
Схема (4.1): `payments` (состояние, `state_version`, суммы, реквизиты),
`payment_transitions` (append-only журнал переходов), `outbox`, `inbox`.
Матрица переходов - `internal/domain/state.go`, дублируется триггером БД
(`payments_transition_guard`); переходы только через `storage.Repository.Transition`
с CAS по `state_version`.

## 4. Команды

```powershell
make build        # go build ./...
make test         # go test ./...
make test-integration  # integration (testcontainers: PostgreSQL)
make e2e          # compose-стенд: миграции, второй барьер БД, append-only журнал
make test-e2e     # e2e на testcontainers: canonical-события Kafka -> автомат платежа
make vet          # go vet ./...
make lint         # golangci-lint в Docker (контекст services/)
make up           # docker compose -f deploy/docker-compose.yml --profile app up -d --build
make down         # docker compose -f deploy/docker-compose.yml down -v
```

## 5. Порты и окружение

HTTP `8030`, gRPC `9030`, метрики `9230`.
Переменные (применимые к сервису): `HTTP_ADDR`, `GRPC_ADDR`, `METRICS_ADDR`, `PG_DSN`
(обязателен), `MIGRATIONS_DIR` (по умолчанию `./migrations`), `DB_MAX_OPEN_CONNS`
(по умолчанию 20, потолок 100), `DB_MAX_IDLE_CONNS` (по умолчанию 10),
`KAFKA_BROKERS`, `KAFKA_CONSUMER_GROUP` (по умолчанию `cbl-payment-canonical`),
`SCHEMA_REGISTRY_URL`, `REDIS_ADDR`.

## 6. Правила разработки

- Конвейер: SDD-спека в `.specs/` -> архитектура -> контракты -> тесты вперед -> код ->
  ревью 4 агентами (без контекста, архитектура, контекст, безопасность - блокирующий) ->
  AGENTS.md -> финальные прогоны -> линтеры -> коммит.
- Покрытие unit+integration не ниже 80%; денежные сценарии только на реальных PostgreSQL и
  Kafka (testcontainers); локальный e2e в docker-compose обязателен.
- Деньги только int64 в минимальных единицах плюс decimals; float запрещен.
- Docstring на русском в стиле godoc (первая строка с имени сущности, точка в конце);
  комментарии объясняют почему; ошибки через `%w`; панки в бизнес-коде запрещены.
- Коммит: `CBL-<эпик>.<номер>: <краткое сообщение>`, спека с тем же префиксом в том же коммите.

## 7. SLO и метрики

SLO уточняется в задаче 14.4. Базовые метрики: latency p95/p99, доля ошибок, outbox lag,
consumer lag; дашборды после задачи 0.7.

## 8. Trade-offs

| ID | Решение | Плюсы | Минусы и компенсация |
| :-- | :-- | :-- | :-- |
| T-PAY-01 | Матрица переходов в домене плюс два триггера БД (второй барьер): BEFORE INSERT фиксирует создание в `creating`/версии 1, BEFORE UPDATE проверяет пары состояний, инкремент `state_version`, неизменяемые поля и терминальные состояния | переходы не по порядку и создание платежа в обход автомата невозможны; аудит состояний полный | дублирование матрицы в Go и SQL, расходы триггеров на каждый переход; владелец БД может снять триггеры | матрица зеркалируется полным перебором пар в integration-тесте; цена триггера сравнима с леджером (T-LED-02); изменение правил - только миграцией; роли prod - KI-payment-03 |
| T-PAY-02 | Оптимистичные переходы: чтение под row-локом и CAS по `state_version` | корректная история переходов, нет глобальных локов | при конкуренции на платеже один из переходов получает `ErrStateConflict` и повторяется | конфликты редки (переходы разных источников); повтор с чтением свежей версии - 4.3; метрика повторов в 0.7 |
| T-PAY-03 | `updated_at` ведет триггер переходов | фоновые задачи (4.6) видят реальное время последнего движения | произвольное обновление `updated_at` в обход запрещено | «зависшие» определяются порогом времени, backdate не нужен |
| T-PAY-04 | Consumer канонических событий пишет переходы в транзакции Guard (атомарно с inbox), CAS-повторы внутри транзакции (до 3) | crash-окно между эффектом и inbox исключено, повтор события не создает лишних переходов | транзакция Guard длиннее (чтение + до двух savepoint-переходов) | отличие от леджера T-LED-08 осознанно: там эффект идемпотентен бизнес-ключом, здесь откат надежнее; конфликты версии после 3 попыток уходят в retry-топики |

## 9. Known issues

| ID | Проблема | Влияние | План | Статус |
| :-- | :-- | :-- | :-- | :-- |
| KI-payment-01 | CI и pre-commit не настроены | линтеры запускаются вручную | задача 0.2 | open |
| KI-payment-02 | Нет ротации и партиционирования `payments`, `payment_transitions`, `outbox`: таблицы растут бессрочно | рост БД и бэкапов; чтения по PK/индексам не страдают | ретеншен/партиционирование вместе с общим планом хранения (волна W4; задача-владелец пока не заведена) | open |
| KI-payment-03 | Рантайм и миграции работают под одним DSN: владелец схемы может снять триггеры второго барьера (`DISABLE TRIGGER`, `session_replication_role`, `DROP TRIGGER`) | в dev-контуре второй барьер снимается владельцем БД; формально T-PAY-01 гарантирует защиту только от DML-ролей | разделить роли migrator (DDL) и runtime (DML без DDL) при выходе в prod (0.4/0.6); вне модели угроз dev-стенда | open |
| KI-payment-04 | Топик `gateway.events.canonical` не аутентифицирован для продюсеров: участник с produce-правами переводит платеж в detected/confirming/терминальные состояния (до `completed` не дотягивается - его ставят 4.4/4.5) | при утечке produce-доступа состояния платежей искажаются, исправление - Reconciler 9.x | broker ACL и mTLS для продюсеров Connector - 0.5/0.6; до этого топик не публиковать за пределы доверенной сети | open |
| KI-payment-05 | Too-early события опираются на номинальные retry-паузы 5s/1m/10m: платеж в creating/created дольше ~11 минут уведет валидное событие в DLQ | задержка подтверждения до ручного replay | parking/replay DLQ в runbook; пересмотр бюджетов ретраев вместе с 4.6 (фоновое доведение creating) | open |

## 10. Журнал задач

| CBL | Дата | Статус | Спека | Коммит |
| :-- | :-- | :-- | :-- | :-- |
| CBL-4.1 | 2026-10-15 | done, ревью закрыто (схема payments+payment_transitions, матрица переходов в домене и БД, state_version с CAS, e2e полной цепочки, покрытие 83.5%) | `.specs/CBL-4.1-payment-schema.md` | acd3c57 |
| CBL-4.3 | 2026-10-15 | done, ревью закрыто (consumer канонических событий в транзакции Guard, план событий с защитой от дублей и порядка, e2e Kafka) | `.specs/CBL-4.3-canonical-consumer.md` | этот коммит |

## 11. Runbook

Заполняется первой продуктовой задачей сервиса. Типовые аварии: недоступность зависимости,
рост ошибок, лаг обработки; действия: проверить health/metrics, логи с request_id, состояние
Kafka consumer group и outbox lag, при необходимости перезапустить сервис.

Автомат состояний (4.1):

1. `ErrStateConflict` в логах: конкурентный переход на одном платеже; вызывающий повторяет
   цикл с чтением свежей версии (`Get`). Систематика - проверить, не дублируются ли
   канонические события (inbox 4.3).
2. Состояние платежа и история: `SELECT state, state_version FROM payments WHERE id = '...'`;
   полный путь переходов - `SELECT from_state, to_state, state_version, reason, occurred_at
   FROM payment_transitions WHERE payment_id = '...' ORDER BY state_version`.
3. Правка состояния напрямую в БД невозможна (триггер): исправление - только допустимый
   переход через приложение или новая запись; `payment_transitions` append-only.

Канонические события (4.3):

1. `gateway.events.canonical.dlq`: причина в заголовке `x-error`; типовые - невалидное
   событие (payment_id/сумма/актив/decimals, несовпадение реквизитов с ожидаемыми),
   неизвестный платеж, too-early событие, исчерпанные конфликты версии. Повтор после
   исправления причины - ручной replay из DLQ (KI-payment-05).
2. Лаг consumer `cbl-payment-canonical`: проверить логи `canonical event applied`,
   `stale canonical event skipped`, конфликты (`conflict_retry`), состояние БД.
3. Событие пришло до `pending` (too early): платеж долго в creating/created; проверить
   4.2/4.6, после перевода платежа в pending событие ретраится из retry-топиков.
4. Границы доверия топика - KI-payment-04: продюсеры должны быть только из доверенной
   сети (ACL/mTLS - 0.5/0.6).
