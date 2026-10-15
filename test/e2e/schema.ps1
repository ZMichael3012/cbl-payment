# E2E CBL-4.1/4.6/4.7/4.8: схема платежа, автомат состояний, фоновые задачи и
# gRPC-чтения на compose-стенде payment.
# Запуск из каталога сервиса: powershell -NoProfile -File test/e2e/schema.ps1
# Сценарий: compose up -> миграции (версия 5) -> полная цепочка состояний
# creating -> created -> pending -> detected -> confirming -> completed,
# запрещенные переходы и append-only журнал отклоняются вторым барьером БД,
# janitor истекает просроченный счет и закрывает зависшее создание,
# переходы janitor-а уходят в outbox payment.events и публикуются relay,
# gRPC GetPayment/ListPayments отдают котировку (rate, locked_until), timeline
# и фильтры мерчанта, состояния и периода (4.8).
$ErrorActionPreference = "Stop"

$RepoRoot = Resolve-Path (Join-Path $PSScriptRoot "..\..")
$ComposeFile = Join-Path $RepoRoot "deploy\docker-compose.yml"
$PaymentContainer = "cbl-payment-payment-1"

function Invoke-Psql([string]$Sql) {
    $output = docker compose -f $ComposeFile exec -T postgres psql -U cbl -d payment -v ON_ERROR_STOP=1 -t -A -c $Sql
    if ($LASTEXITCODE -ne 0) { throw "psql failed: $($output -join "`n")" }
    return ($output -join "`n").Trim()
}

function Assert-PsqlFails([string]$Sql, [string]$What, [string]$ExpectedText) {
    $previous = $ErrorActionPreference
    $ErrorActionPreference = "Continue"
    try {
        $output = docker compose -f $ComposeFile exec -T postgres psql -U cbl -d payment -v ON_ERROR_STOP=1 -t -A -c $Sql 2>&1
        $code = $LASTEXITCODE
    } finally {
        $ErrorActionPreference = $previous
    }
    if ($code -eq 0) { throw "$What was not rejected: $($output -join "`n")" }
    if (($output -join "`n") -notmatch $ExpectedText) { throw "$What rejected with unexpected error: $($output -join "`n")" }
}

# Invoke-PaymentGrpcurl вызывает метод PaymentService на compose-контейнере
# (сеть контейнера, plaintext); возвращает вывод и код выхода.
function Invoke-PaymentGrpcurl([string]$Method, [string]$Json) {
    $previous = $ErrorActionPreference
    $ErrorActionPreference = "Continue"
    try {
        $output = $Json | docker run -i --rm --network "container:$PaymentContainer" fullstorydev/grpcurl:v1.9.1 -plaintext -max-time 10 -d '@' localhost:9030 "billing.payment.v1.PaymentService/$Method" 2>&1
        $code = $LASTEXITCODE
    } finally {
        $ErrorActionPreference = $previous
    }
    return @{ Output = ($output -join "`n"); Code = $code }
}

Push-Location $RepoRoot
try {
    # Чистый стенд: остатки прошлого запуска приводят к конфликту имен.
    docker compose -f $ComposeFile --profile app down -v 2>$null | Out-Null
    docker compose -f $ComposeFile --profile app up -d --build
    if ($LASTEXITCODE -ne 0) { throw "docker compose up failed" }

    $migrated = $false
    foreach ($attempt in 1..30) {
        try {
            $version = Invoke-Psql "SELECT max(version_id) FROM goose_db_version"
            if ($version -eq "5") { $migrated = $true; break }
        } catch {
            Start-Sleep -Seconds 2
        }
    }
    if (-not $migrated) { throw "payment migrations are not applied (version $version)" }

    # Топик payment.events для relay: в compose авто-создание топиков выключено.
    docker compose -f $ComposeFile exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic payment.events --partitions 1 --replication-factor 1 | Out-Null
    if ($LASTEXITCODE -ne 0) { throw "payment.events topic creation failed" }

    $paymentId = [guid]::NewGuid().ToString()
    Invoke-Psql "INSERT INTO payments (id, merchant_id, idempotency_key, request_hash, state, expected_amount, expected_asset, expected_decimals, network) VALUES ('$paymentId', 'm-e2e', 'key-e2e', 'hash-e2e', 'creating', 1000, 'USDT', 2, 'trc20')" | Out-Null
    Invoke-Psql "INSERT INTO payment_transitions (payment_id, from_state, to_state, state_version, reason) VALUES ('$paymentId', '', 'creating', 1, 'created')" | Out-Null

    # Полная цепочка автомата; каждая версия получает строку журнала.
    # Котировка (4.8) фиксируется на шаге creating -> created (fill-once).
    $chain = @(
        @{ From = "creating"; To = "created"; Version = 2; Extra = ", quote_id = 'q-e2e', rate = '78.125', quote_locked_until = now() + interval '15 minutes'" },
        @{ From = "created"; To = "pending"; Version = 3; Extra = "" },
        @{ From = "pending"; To = "detected"; Version = 4; Extra = ", txid = 'tx-e2e', received_amount = 1000, received_asset = 'USDT', received_decimals = 2" },
        @{ From = "detected"; To = "confirming"; Version = 5; Extra = ", confirmations = 1" },
        @{ From = "confirming"; To = "completed"; Version = 6; Extra = ", confirmations = 6, completed_at = now()" }
    )
    foreach ($step in $chain) {
        Invoke-Psql "UPDATE payments SET state = '$($step.To)', state_version = $($step.Version)$($step.Extra) WHERE id = '$paymentId'" | Out-Null
        Invoke-Psql "INSERT INTO payment_transitions (payment_id, from_state, to_state, state_version, reason) VALUES ('$paymentId', '$($step.From)', '$($step.To)', $($step.Version), 'e2e')" | Out-Null
    }

    $state = Invoke-Psql "SELECT state || ':' || state_version FROM payments WHERE id = '$paymentId'"
    if ($state -ne "completed:6") { throw "unexpected state after chain: $state" }
    $history = Invoke-Psql "SELECT count(*) FROM payment_transitions WHERE payment_id = '$paymentId'"
    if ($history -ne "6") { throw "unexpected history length: $history" }

    # Переход назад из терминального состояния отклоняет триггер (второй барьер).
    Assert-PsqlFails "UPDATE payments SET state = 'pending', state_version = 7 WHERE id = '$paymentId'" "backward transition" "forbidden"

    # Журнал переходов append-only.
    Assert-PsqlFails "UPDATE payment_transitions SET reason = 'rewritten' WHERE payment_id = '$paymentId'" "transition rewrite" "append-only"

    # Janitor (4.6): просроченный счет истекает, зависшее создание закрывается failed.
    # Цепочки строятся одним вызовом psql (одна неявная транзакция): janitor с
    # интервалом 2s не должен вмешаться между шагами.
    $expiredId = [guid]::NewGuid().ToString()
    $expiredSql = @"
INSERT INTO payments (id, merchant_id, idempotency_key, request_hash, state, expected_amount, expected_asset, expected_decimals, network, expires_at) VALUES ('$expiredId', 'm-e2e', 'key-exp', 'hash-exp', 'creating', 1000, 'USDT', 2, 'trc20', now() - interval '1 minute');
INSERT INTO payment_transitions (payment_id, from_state, to_state, state_version, reason) VALUES ('$expiredId', '', 'creating', 1, 'created');
UPDATE payments SET state = 'created', state_version = 2 WHERE id = '$expiredId';
INSERT INTO payment_transitions (payment_id, from_state, to_state, state_version, reason) VALUES ('$expiredId', 'creating', 'created', 2, 'e2e');
UPDATE payments SET state = 'pending', state_version = 3 WHERE id = '$expiredId';
INSERT INTO payment_transitions (payment_id, from_state, to_state, state_version, reason) VALUES ('$expiredId', 'created', 'pending', 3, 'e2e');
"@
    Invoke-Psql $expiredSql | Out-Null

    # Зависшее создание: created_at в прошлом разрешен на INSERT (immutable только для UPDATE).
    $stuckCreateId = [guid]::NewGuid().ToString()
    $stuckCreateSql = @"
INSERT INTO payments (id, merchant_id, idempotency_key, request_hash, state, expected_amount, expected_asset, expected_decimals, network, created_at, updated_at) VALUES ('$stuckCreateId', 'm-e2e', 'key-stuck', 'hash-stuck', 'creating', 1000, 'USDT', 2, 'trc20', now() - interval '10 minutes', now() - interval '10 minutes');
INSERT INTO payment_transitions (payment_id, from_state, to_state, state_version, reason) VALUES ('$stuckCreateId', '', 'creating', 1, 'created');
"@
    Invoke-Psql $stuckCreateSql | Out-Null

    # Обнаруженная оплата с истекшим expires_at не истекает (KI-payment-08).
    $detectedId = [guid]::NewGuid().ToString()
    $detectedSql = @"
INSERT INTO payments (id, merchant_id, idempotency_key, request_hash, state, expected_amount, expected_asset, expected_decimals, network, expires_at) VALUES ('$detectedId', 'm-e2e', 'key-det', 'hash-det', 'creating', 1000, 'USDT', 2, 'trc20', now() - interval '1 minute');
INSERT INTO payment_transitions (payment_id, from_state, to_state, state_version, reason) VALUES ('$detectedId', '', 'creating', 1, 'created');
UPDATE payments SET state = 'created', state_version = 2 WHERE id = '$detectedId';
INSERT INTO payment_transitions (payment_id, from_state, to_state, state_version, reason) VALUES ('$detectedId', 'creating', 'created', 2, 'e2e');
UPDATE payments SET state = 'pending', state_version = 3 WHERE id = '$detectedId';
INSERT INTO payment_transitions (payment_id, from_state, to_state, state_version, reason) VALUES ('$detectedId', 'created', 'pending', 3, 'e2e');
UPDATE payments SET state = 'detected', state_version = 4 WHERE id = '$detectedId';
INSERT INTO payment_transitions (payment_id, from_state, to_state, state_version, reason) VALUES ('$detectedId', 'pending', 'detected', 4, 'e2e');
"@
    Invoke-Psql $detectedSql | Out-Null

    $janitorDone = $false
    foreach ($attempt in 1..20) {
        $expiredState = Invoke-Psql "SELECT state FROM payments WHERE id = '$expiredId'"
        $stuckState = Invoke-Psql "SELECT state FROM payments WHERE id = '$stuckCreateId'"
        if ($expiredState -eq "expired" -and $stuckState -eq "failed") { $janitorDone = $true; break }
        Start-Sleep -Seconds 2
    }
    if (-not $janitorDone) { throw "janitor did not process payments (expired=$expiredState, stuck=$stuckState)" }

    $expiredReason = Invoke-Psql "SELECT reason FROM payment_transitions WHERE payment_id = '$expiredId' AND to_state = 'expired'"
    if ($expiredReason -ne "janitor expired") { throw "unexpected expiry reason: $expiredReason" }
    $stuckReason = Invoke-Psql "SELECT reason FROM payment_transitions WHERE payment_id = '$stuckCreateId' AND to_state = 'failed'"
    if ($stuckReason -ne "janitor creating timeout") { throw "unexpected creating failure reason: $stuckReason" }
    $detectedState = Invoke-Psql "SELECT state || ':' || state_version FROM payments WHERE id = '$detectedId'"
    if ($detectedState -ne "detected:4") { throw "janitor must not touch detected payment: $detectedState" }

    # Outbox (4.7): переходы janitor-а породили события payment.expired/payment.failed,
    # relay публикует их в Kafka (топик создан выше; строки идут через outbox).
    $expiredEvents = Invoke-Psql "SELECT count(*) FROM outbox WHERE topic = 'payment.events' AND `"key`" = '$expiredId' AND headers->>'event_type' = 'payment.expired'"
    if ($expiredEvents -ne "1") { throw "expected one payment.expired outbox row: $expiredEvents" }
    $failedEvents = Invoke-Psql "SELECT count(*) FROM outbox WHERE topic = 'payment.events' AND `"key`" = '$stuckCreateId' AND headers->>'event_type' = 'payment.failed'"
    if ($failedEvents -ne "1") { throw "expected one payment.failed outbox row: $failedEvents" }

    $published = $false
    foreach ($attempt in 1..15) {
        $unpublished = Invoke-Psql "SELECT count(*) FROM outbox WHERE published_at IS NULL"
        if ($unpublished -eq "0") { $published = $true; break }
        Start-Sleep -Seconds 2
    }
    if (-not $published) { throw "relay did not publish outbox rows (pending=$unpublished)" }

    # gRPC-чтения (4.8): GetPayment отдает rate/locked_until и полный timeline;
    # ListPayments фильтрует по мерчанту, состоянию и периоду.
    $get = Invoke-PaymentGrpcurl "GetPayment" "{`"paymentId`":`"$paymentId`"}"
    if ($get.Code -ne 0) { throw "GetPayment failed: $($get.Output)" }
    $getJson = $get.Output | ConvertFrom-Json
    if ($getJson.payment.rate -ne "78.125") { throw "GetPayment rate mismatch: $($get.Output)" }
    if (-not $getJson.payment.lockedUntil) { throw "GetPayment lockedUntil is empty: $($get.Output)" }
    if ($getJson.payment.txid -ne "tx-e2e") { throw "GetPayment txid mismatch: $($get.Output)" }
    if ($getJson.payment.confirmations -ne 6) { throw "GetPayment confirmations mismatch: $($get.Output)" }
    if ($getJson.payment.timeline.Count -ne 6) { throw "GetPayment timeline mismatch: $($get.Output)" }

    $list = Invoke-PaymentGrpcurl "ListPayments" "{`"merchantId`":`"m-e2e`",`"stateFilter`":`"PAYMENT_STATE_COMPLETED`"}"
    if ($list.Code -ne 0) { throw "ListPayments failed: $($list.Output)" }
    $listJson = $list.Output | ConvertFrom-Json
    if ($listJson.payments.Count -ne 1 -or $listJson.payments[0].paymentId -ne $paymentId) {
        throw "ListPayments completed mismatch: $($list.Output)"
    }

    $future = Invoke-PaymentGrpcurl "ListPayments" "{`"merchantId`":`"m-e2e`",`"createdFrom`":`"2100-01-01T00:00:00Z`"}"
    if ($future.Code -ne 0) { throw "ListPayments future period failed: $($future.Output)" }
    $futureJson = $future.Output | ConvertFrom-Json
    if ($futureJson.payments) { throw "ListPayments future period must be empty: $($future.Output)" }

    $missing = Invoke-PaymentGrpcurl "GetPayment" "{`"paymentId`":`"$([guid]::NewGuid())`"}"
    if ($missing.Code -eq 0 -or $missing.Output -notmatch "NotFound") {
        throw "GetPayment unknown must return NotFound: $($missing.Output)"
    }

    Write-Host "OK: migrations=$version, chain=completed:6, history=6, forbidden transition and rewrite rejected, janitor expired+creating_failed+detected intact, outbox expired+failed published, read API rate/locked_until/list filters green"
} finally {
    docker compose -f $ComposeFile --profile app down -v | Out-Null
    Pop-Location
}
