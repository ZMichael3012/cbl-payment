.PHONY: build test test-integration test-e2e vet lint fmt up down

build:
	go build ./...

test:
	go test ./...

test-integration:
	go test -tags integration -count=1 ./internal/...

# test-e2e runs the e2e tests on testcontainers: canonical Kafka events ->
# payment state machine (no compose stand needed).
test-e2e:
	go test -tags e2e -count=1 -timeout 12m ./test/e2e/...

# E2E: compose stand runs follow the test/e2e/E2E.md instruction
# (scenario E2E_Payment_Schema; artifacts go to test/e2e/.runs/<UTC mark>/);
# there are no PowerShell run scripts.

vet:
	go vet ./...

# lint mounts the repository; the modules come from the GitHub origin (Go proxy)
# resolves inside the container.
lint:
	docker run --rm -v "$(CURDIR)":/src -w /src golangci/golangci-lint:latest-alpine golangci-lint run ./...

fmt:
	gofmt -l .

up:
	docker compose -f deploy/docker-compose.yml --profile app up -d --build

down:
	docker compose -f deploy/docker-compose.yml down -v
