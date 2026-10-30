# STAGE 1: Builder
FROM golang:1.27-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -o /app/bin/producer ./cmd/producer
RUN CGO_ENABLED=0 GOOS=linux go build -o /app/bin/worker ./cmd/worker
RUN CGO_ENABLED=0 GOOS=linux go build -o /app/bin/api ./cmd/api
# STAGE 2: Producer
FROM alpine:3.20 AS producer-runtime
WORKDIR /app
COPY --from=builder /app/bin/producer .
ENTRYPOINT ["./producer"]

# STAGE 3: Worker
FROM alpine:3.20 AS worker-runtime
WORKDIR /app
COPY --from=builder /app/bin/worker .
ENTRYPOINT ["./worker"]

# New stage:
FROM alpine:3.20 AS api-runtime
WORKDIR /app
COPY --from=builder /app/bin/api .
EXPOSE 8080
ENTRYPOINT ["./api"]