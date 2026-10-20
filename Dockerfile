# Build context: the repository root (CBL-0.9). The cbl-kit and cbl-protos
# modules are fetched from the GitHub origin; no sibling checkouts are needed.
FROM golang:1.26-alpine AS build
RUN apk add --no-cache git
RUN git config --global url."https://github.com/ZMichael3012/".insteadOf "https://github.com/cbl/"
ENV GOPRIVATE=github.com/cbl/*
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags "-s -w" -o /out/payment ./cmd/payment
FROM gcr.io/distroless/static-debian12:nonroot
WORKDIR /app
COPY --from=build /out/payment /app/payment
COPY --from=build /src/migrations /app/migrations
EXPOSE 8030 9030 9230
ENTRYPOINT ["/app/payment"]
