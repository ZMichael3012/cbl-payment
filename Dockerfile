# Контекст сборки: services/ (нужен доступ к replace на ../cbl-kit и ../cbl-protos).
# Команда: docker build -f cbl-payment/Dockerfile -t cbl/payment:dev .
FROM golang:1.26-alpine AS build
WORKDIR /src
COPY cbl-kit/ /src/cbl-kit/
COPY cbl-protos/ /src/cbl-protos/
COPY cbl-payment/ /src/cbl-payment/
WORKDIR /src/cbl-payment
RUN go mod download
RUN CGO_ENABLED=0 go build -trimpath -ldflags "-s -w" -o /out/payment ./cmd/payment

FROM gcr.io/distroless/static-debian12:nonroot
WORKDIR /app
COPY --from=build /out/payment /app/payment
COPY --from=build /src/cbl-payment/migrations /app/migrations
EXPOSE 8030 9030 9230
ENTRYPOINT ["/app/payment"]
