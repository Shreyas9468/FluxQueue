# FluxQueue

A distributed background job queue written in Go.

Supports worker pools, at-least-once delivery, exponential backoff retries, dead-letter queues, delayed/scheduled jobs, priority queues, and horizontal scaling of workers.

Built as a learning + portfolio project focused on real failure modes and operational concerns that appear in production systems (SQS-style job queues, internal task systems at product companies, etc.).

## Features

- **At-least-once delivery** with acknowledgment
- **Worker pools** with configurable concurrency
- **Exponential backoff** retries with jitter
- **Dead-letter queue (DLQ)** after max attempts
- **Delayed & scheduled jobs**
- **Priority queues** (high / normal / low)
- **Horizontal scaling** — multiple workers can run independently
- **gRPC API** for producers and admin operations
- **Redis** as the backing store (lists + sorted sets)
- **Basic observability** — Prometheus metrics endpoint
- Docker Compose for local multi-worker setup

## Architecture (High Level)
┌─────────────┐       gRPC        ┌──────────────────┐
│  Producer   │ ─────────────────▶│  FluxQueue API   │
└─────────────┘                   └────────┬─────────┘
│
▼
┌─────────────┐
│    Redis    │
│ (queues +   │
│  delayed +  │
│  DLQ)       │
└──────┬──────┘
│
┌──────────────────────┼──────────────────────┐
▼                      ▼                      ▼
┌────────────┐         ┌────────────┐         ┌────────────┐
│  Worker 1  │         │  Worker 2  │         │  Worker N  │
└────────────┘         └────────────┘         └────────────┘


### Key Design Decisions

| Decision              | Choice                        | Why                                                                 |
|-----------------------|-------------------------------|---------------------------------------------------------------------|
| Backing store         | Redis                         | Fast, supports lists + sorted sets (perfect for delayed jobs), widely used in production job queues |
| Delivery guarantee    | At-least-once                 | Simpler and more practical than exactly-once for most background jobs |
| Retries               | Exponential backoff + jitter  | Avoids thundering herd                                              |
| Visibility            | Lease / visibility timeout    | Workers must ACK; otherwise job becomes visible again               |
| API                   | gRPC                          | Type-safe, good for internal services, matches other projects in the portfolio |

## Quick Start

### Prerequisites
- Go 1.22+
- Docker & Docker Compose
- Redis (or use the one from docker-compose)

```bash
# Clone
git clone https://github.com/Shreyas9468/FluxQueue.git
cd FluxQueue

# Start Redis + 2 workers
docker compose up --build

# In another terminal – enqueue a job
go run ./cmd/producer --job "send-email" --payload '{"to":"user@example.com"}'
