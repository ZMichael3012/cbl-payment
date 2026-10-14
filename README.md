# cbl-payment

Автомат состояний платежа: создание счета, обработка канонических событий, пороги подтверждений.

Роль сервиса и контракты: `AGENTS.md`. Глобальные правила платформы: `../../AGENTS.md`,
`../../.specs/CBL-000-system-description.md`, `../../.specs/CBL-003-contracts.md`.

## Команды

```powershell
make build        # сборка
make test         # мокнутые unit-тесты
make vet          # go vet
make lint         # golangci-lint в Docker
make up           # зависимости + сервис в docker-compose
make down
```
