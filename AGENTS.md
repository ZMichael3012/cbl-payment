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
- Kafka продюсирует: payment.events (через outbox, 4.7)
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
Фоновые задачи (4.6) - `internal/usecase/janitor.go`: истечение счетов
(created/pending), закрытие зависших созданий (creating -> failed), наблюдение за
зависшими detected/confirming; выборки - по частичным индексам, пакет
`maxBackgroundLimit` 1000.
События (4.7): каждый примененный переход (`Repository.TransitionTx`) пишет
конверт `payment.events` в `outbox` в той же транзакции (sink -
`internal/usecase/payment_events.go`); relay kit в `main` публикует при заданном
`KAFKA_BROKERS` (batch 100, интервал 250 мс), без Kafka события накапливаются.

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
`PAYMENT_CONFIRMATION_POLICY` (JSON ярусов порогов по активу, см. 4.5),
`PAYMENT_UNDERPAID_TOLERANCE_BPS` и `PAYMENT_OVERPAID_TOLERANCE_BPS`
(0 - 10000, по умолчанию 0, невалидное значение - отказ старта),
`PAYMENT_JANITOR_INTERVAL` (по умолчанию 30s, 0 - janitor выключен, минимум 1s),
`PAYMENT_JANITOR_BATCH` (по умолчанию 100, 1 - 1000),
`PAYMENT_CREATING_TIMEOUT` (по умолчанию 5m, минимум 1m),
`PAYMENT_STUCK_TIMEOUT` (по умолчанию 6h, минимум 1m; невалидные значения
janitor-переменных - отказ старта, см. 4.6),
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
| T-PAY-05 | Политика платформы (4.5) главнее классификации Connector: недоплата/переплата в пределах толеранса завершается `completed`, сверх - терминальные состояния; пороги задаются оператором в единицах актива | бизнес-решение о деньгах в одном месте (Payment), конфигурируемые пороги без релиза | двойная классификация (шлюз и платформа), расхождение с Reconciler, дрейф USD-эквивалента при ручной настройке | сверка 9.x обязана учитывать толерансы (status_mismatch не для разницы в пределах толеранса); USD-feed и fee - KI-payment-06 |
| T-PAY-06 | Janitor в процессе сервиса (4.6): тикер, пакетные выборки по частичным индексам, CAS-переходы; без отдельного deployment-а и leader election | нет нового сервиса и его эксплуатации; работает без Kafka; выключается env-ом; несколько реплик безопасны (CAS дает один эффект) | лишние конфликты и дублирование выборок у реплик (jitter не введен осознанно); пакет последователен - долгий row-lock задерживает проход (следующий тик доводит остаток); отмена контекста обрывает проход между платежами | наблюдаемость через логи janitor (метрики - 0.7); порог риска по нагрузке - 14.6; координация реплик не требуется для корректности |
| T-PAY-07 | События payment.events через transactional outbox (4.7): эмиссия в `Repository.TransitionTx` (та же транзакция, что переход и inbox Guard), relay kit в процессе сервиса (FOR UPDATE SKIP LOCKED, batch 100, 250 мс, публикация внутри транзакции БД - kit T-KK5) | нет потери событий при сбоях (БД-транзакция первична), Kafka не в критическом пути, at-least-once, дубли дедуплицируются `event_id` и мониторингом `state_version` (самопереходы публикуются) | при нескольких репликах порядок событий между транзакциями не гарантирован (потребители отбрасывают устаревшие `state_version`); нет топика/брокера - строки копятся, relay повторяет (WARN при старте без Kafka); GORM-логирование значений SQL - общий вопрос 0.7 | ретеншен outbox - 14.2 (KI-payment-02); метрики outbox lag - 0.7; эмиссия проверена unit/integration/e2e (Kafka), сбой sink откатывает переход |

## 9. Known issues

| ID | Проблема | Влияние | План | Статус |
| :-- | :-- | :-- | :-- | :-- |
| KI-payment-01 | CI и pre-commit не настроены | линтеры запускаются вручную | задача 0.2 | open |
| KI-payment-02 | Нет ротации и партиционирования `payments`, `payment_transitions`, `outbox`: таблицы растут бессрочно (outbox - строка на каждый переход, 4.7) | рост БД и бэкапов; чтения по PK/индексам не страдают | ретеншен/партиционирование - задача 14.2 (как KI-ledger-09); до этого чистить published-строки вручную не разрешается (аудит) | open |
| KI-payment-03 | Рантайм и миграции работают под одним DSN: владелец схемы может снять триггеры второго барьера (`DISABLE TRIGGER`, `session_replication_role`, `DROP TRIGGER`) | в dev-контуре второй барьер снимается владельцем БД; формально T-PAY-01 гарантирует защиту только от DML-ролей | разделить роли migrator (DDL) и runtime (DML без DDL) при выходе в prod (0.4/0.6); вне модели угроз dev-стенда | open |
| KI-payment-04 | Топики `gateway.events.canonical` и `payment.events` не аутентифицированы: участник с produce-правами в canonical переводит платеж в detected/confirming и (с 4.5) в терминальные, а с 4.7 подтвержденный платеж публикует `payment.completed`, который Ledger 2.7 идемпотентно проводит реальными деньгами (Dr clearing / Cr merchant) без сверки со шлюзом; подделка `payment.events` напрямую подделывает проводку (кросс-ссылка KI-ledger-02) | утечка produce-доступа = завершение произвольных платежей и движение денег; исправление уже проведенных - только сторно (10.x), не replay события | broker ACL и mTLS: produce в canonical только Connector, в `payment.events` только cbl-payment (0.5/0.6); prod-выкладка запрещена до этого; перепроверка перед завершением - 4.4 (после KI-01); разбор подозрительного `payment.completed` - runbook | open |
| KI-payment-05 | Too-early события опираются на номинальные retry-паузы 5s/1m/10m: с 4.6 зависшее создание закрывается `failed` через 5 минут, поэтому событие для него становится stale, а не бесконечным повтором; остаточный риск - платеж в `created` дольше ~11 минут (до реализации 4.2/4.4) уводит валидное событие в DLQ | задержка подтверждения до ручного replay | parking/replay DLQ в runbook; пересмотр бюджетов ретраев вместе с 4.2; наблюдение за `created` без движения - кандидат в 9.x | open |
| KI-payment-06 | `fee` платежа NULL (трактуется как 0 - правила комиссии не определены, кандидат Billing API 7.x); пороги подтверждений настраиваются вручную без USD-эквивалента (feed из Quote 6.x не подключен) | недосбор комиссии; дрейф порогов при движении курса требует ручного пересмотра | правила комиссии - в 7.x; автоматический пересчет ярусов - при появлении требований бухгалтерии/14.x | open |
| KI-payment-07 | Монотонность по `gateway_event_ts` не реализована (делегировалась 3.5/T-CONNECTOR-05): устаревшее событие из retry-контура может регрессировать `confirmations` или терминализировать платеж после более нового | редкие ложные терминальные статусы/откат счетчика до ручного разбора | сравнение времени события при хранении `gateway_event_ts` (4.x) либо перепроверка статуса 4.4 после KI-01 | open |
| KI-payment-08 | Гонка "expired закоммичен раньше позднего detected": janitor перечитывает состояние через CAS перед истечением (реализовано 4.6), поздний `detected`/`confirmed` после `expired` consumer помечает отдельным WARN `late canonical event after expiry needs manual review` и оставляет платеж в expired - оплата требует ручного разбора/алерта | деньги в сети могут остаться незамеченными без алерта по логам | правило алерта и ремедиации - в 9.x; связанное сравнение времени - KI-payment-07 | open |
| KI-payment-09 | Зависшее создание закрывается `failed` без обращения к шлюзу (4.6): если 4.2 успел создать invoice, но не довел платеж до `pending`, invoice остается в шлюзе без платежа | шлюз видит invoice, платеж в failed; мерчант не получал адрес, оплата невозможна | сверка 9.x классифицирует такие invoice (неоплаченный invoice без платежа); при реализации 4.2 рассмотреть отмену invoice (KI-01) | open |

## 10. Журнал задач

| CBL | Дата | Статус | Спека | Коммит |
| :-- | :-- | :-- | :-- | :-- |
| CBL-4.1 | 2026-10-15 | done, ревью закрыто (схема payments+payment_transitions, матрица переходов в домене и БД, state_version с CAS, e2e полной цепочки, покрытие 83.5%) | `.specs/CBL-4.1-payment-schema.md` | acd3c57 |
| CBL-4.3 | 2026-10-15 | done, ревью закрыто (consumer канонических событий в транзакции Guard, план событий с защитой от дублей и порядка, e2e Kafka) | `.specs/CBL-4.3-canonical-consumer.md` | 2d5f263 |
| CBL-4.5 | 2026-10-15 | done, ревью закрыто (политика порогов по активу и сумме, толерансы без float, авто-завершение из consumer-а, e2e) | `.specs/CBL-4.5-confirmation-policy.md` | 0a9d627 |
| CBL-4.6 | 2026-10-15 | done, ревью закрыто (janitor: истечение счетов, зависшие создания в failed, наблюдение за detected/confirming, пакетные выборки на частичных индексах, e2e) | `.specs/CBL-4.6-background-janitor.md` | ccb2d79 |
| CBL-4.7 | 2026-10-15 | done, ревью закрыто (payment.events через outbox: эмиссия в TransitionTx, конверты CF-2, relay в main, traceparent-цепочка, e2e Kafka) | `.specs/CBL-4.7-payment-events-outbox.md` | этот коммит |

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
   сети (ACL/mTLS - 0.5/0.6); с 4.5 подделанное `confirmed` может завершить платеж.

Политика завершения (4.5):

1. Эффективные пороги и толерансы печатаются при старте (`payment amount policy
   configured`); при инциденте сверить конфигурацию именно этого запуска.
2. `PAYMENT_CONFIRMATION_POLICY` меняется только с рестартом сервиса; после правки
   ярусов проверить лог старта и тестовый платеж.
3. Платеж завершается consumer-ом при `confirmations >= required` и сумме в
   толерансе; расхождение с классификацией шлюза в пределах толеранса штатно
   (T-PAY-05) - не считать расхождением сверки.
4. Истечение выполняет 4.6 через `ShouldExpire`; поздний detected после `expired` -
   ручной разбор (KI-payment-08).

Фоновые задачи (4.6):

1. Конфигурация печатается при старте (`payment janitor started`): интервал,
   пакет, `creating_timeout`, `stuck_timeout`; при `PAYMENT_JANITOR_INTERVAL=0` -
   `payment janitor disabled`. Опечатка в значениях блокирует старт, а не
   деградирует молча.
2. Причины переходов janitor-а в журнале: `janitor expired` (created/pending ->
   expired) и `janitor creating timeout` (creating -> failed, по умолчанию через
   5m). Свежие создания не трогаются; failure зависшего создания ограничивает
   окно too-early событий (KI-payment-05).
3. `janitor: sweep finished` с ненулевыми счетчиками - разбор: `conflicts` -
   нормальная гонка нескольких реплик; рост `errors` - смотреть БД/логи;
   `stuck` > 0 - WARN `janitor: payment without movement` (платеж без движения
   дольше `PAYMENT_STUCK_TIMEOUT`, деньги в сети): вручную посмотреть статус
   шлюза (4.4 после KI-01), дождаться сверки 9.x; автоматически janitor
   detected/confirming не терминализирует.
4. Backlog (много истекших/зависших): проходы идут пакетами, число за проход -
   `PAYMENT_JANITOR_BATCH`; при устойчивом backlog увеличить batch/уменьшить
   интервал, проверить, почему не работает consumer канонических событий.
5. `late canonical event after expiry needs manual review` (consumer 4.3, WARN):
   оплата по уже истекшему счету - деньги в сети, ручной разбор (KI-payment-08).

События payment.events (4.7):

1. Outbox lag: `SELECT count(*), min(created_at) FROM outbox WHERE published_at IS
   NULL`; логи relay `outbox published batch` / `outbox publish batch failed`.
   Потери невозможны: неопубликованные строки ждут топик/брокера и уходят
   следующими циклами; возможны дубли (at-least-once, потребители дедуплицируют
   по `event_id`).
2. Публикация не идут без топика: топик `payment.events` должен существовать
   (`deploy/kafka/init-topics.sh` в глобальном стенде, 0.5); без `KAFKA_BROKERS`
   сервис пишет `payment events accumulate in outbox` и копит строки.
3. События одного платежа проверяются по `state_version`/`event_type`
   (`SELECT event_id, headers->>'event_type' FROM outbox WHERE "key" = '<payment_id>'
   ORDER BY id`); устаревшие события - норма (переходы разных источников),
   ретеншен - KI-payment-02 (14.2).
4. Подозрительный `payment.completed` (KI-payment-04): подтвердить легитимность
   канонического события/платежа до проводки; если проводка уже прошла -
   исправление только сторно (10.x), не повтор события; инцидент - с разбором
   produce-доступа к топикам.
