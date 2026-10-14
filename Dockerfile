FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags "-s -w" -o /out/payment ./cmd/payment

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/payment /app/payment
EXPOSE 8030 9030 9230
ENTRYPOINT ["/app/payment"]