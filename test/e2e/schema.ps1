# E2E CBL-4.1: схема платежа и автомат состояний на compose-стенде payment.
# Запуск из каталога сервиса: powershell -NoProfile -File test/e2e/schema.ps1
# Сценарий: compose up -> миграции (версия 3) -> полная цепочка состояний
# creating -> created -> pending -> detected -> confirming -> completed,
# запрещенные переходы и append-only журнал отклоняются вторым барьером БД.
$ErrorActionPreference = "Stop"

$RepoRoot = Resolve-Path (Join-Path $PSScriptRoot "..\..")
$ComposeFile = Join-Path $RepoRoot "deploy\docker-compose.yml"

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
            if ($version -eq "3") { $migrated = $true; break }
        } catch {
            Start-Sleep -Seconds 2
        }
    }
    if (-not $migrated) { throw "payment migrations are not applied (version $version)" }

    $paymentId = [guid]::NewGuid().ToString()
    Invoke-Psql "INSERT INTO payments (id, merchant_id, idempotency_key, request_hash, state, expected_amount, expected_asset, expected_decimals, network) VALUES ('$paymentId', 'm-e2e', 'key-e2e', 'hash-e2e', 'creating', 1000, 'USDT', 2, 'trc20')" | Out-Null
    Invoke-Psql "INSERT INTO payment_transitions (payment_id, from_state, to_state, state_version, reason) VALUES ('$paymentId', '', 'creating', 1, 'created')" | Out-Null

    # Полная цепочка автомата; каждая версия получает строку журнала.
    $chain = @(
        @{ From = "creating"; To = "created"; Version = 2; Extra = "" },
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

    Write-Host "OK: migrations=$version, chain=completed:6, history=6, forbidden transition and rewrite rejected"
} finally {
    docker compose -f $ComposeFile --profile app down -v | Out-Null
    Pop-Location
}
