# TaskQueue — Distributed Task Queue in Go

A production-grade, mini Amazon SQS built from scratch in Go.  
Producers push jobs over HTTP, concurrent goroutine workers pull and process them, failed jobs are retried with exponential backoff, and jobs that exhaust all retries land in a Dead Letter Queue (DLQ). Everything is persisted in Postgres.

---

## Why Go?

| Go feature | How it helps here |
|---|---|
| **Goroutines** | Spin up hundreds of lightweight workers with near-zero overhead — no thread pool configuration needed |
| **Channels + select** | Clean, deadlock-safe coordination between the poll ticker and the worker semaphore |
| **`context.Context`**  | First-class cancellation propagates from HTTP request → queue manager → DB query — graceful shutdown just works |
| **`net/http` (1.22 routing)** | Pattern-based routing (`GET /jobs/{id}`) without a framework |
| **`SELECT FOR UPDATE SKIP LOCKED`** | Combined with Go's concurrency model, delivers jobs to exactly one worker with zero double-processing |
| **Single binary** | `go build` produces one self-contained binary — no JVM, no runtime dependencies |

---

## Architecture

See [`docs/HLD.md`](docs/HLD.md) for the High-Level Design and [`docs/LLD.md`](docs/LLD.md) for the Low-Level Design with component diagrams.

---

## Project Structure

```
taskqueue/
├── cmd/
│   ├── server/main.go      # HTTP server + worker pool entrypoint
│   └── cli/main.go         # tq CLI binary
├── internal/
│   ├── models/job.go       # Job, DeadLetterJob, QueueStats types
│   ├── db/store.go         # All Postgres operations (pgx/v5)
│   ├── queue/manager.go    # Enqueue / Ack / Nack + exponential backoff
│   ├── worker/pool.go      # Goroutine worker pool
│   └── api/handlers.go     # HTTP route handlers
├── migrations/001_init.sql  # Schema — jobs + dead_letter_jobs
├── .env.example             # All supported environment variables
├── Dockerfile
├── docker-compose.yml
└── Makefile
```

---

## Environment Variables

| Variable | Required | Default | Description |
|---|---|---|---|
| `DATABASE_URL` | **Yes** | — | Postgres connection string |
| `ADDR` | No | `:8080` | Server listen address |
| `QUEUES` | No | `default,emails,notifications` | Comma-separated queues for workers to poll |
| `TQ_SERVER` | No (CLI only) | `http://localhost:8080` | Server base URL used by the CLI |

Copy `.env.example` → `.env` and fill in your values. **Never commit `.env`.**

---

## Running Locally

### Option A — Docker Compose (recommended)

```bash
# Start Postgres + server together
make docker-up

# Tear down
make docker-down
```

### Option B — Run server locally against Docker Postgres

```bash
cp .env.example .env
# Edit .env — set DATABASE_URL

# Start only Postgres in Docker, run server binary locally
make dev
```

### Option C — Fully manual

```bash
# 1. Start Postgres yourself, then:
cp .env.example .env   # fill in DATABASE_URL

# 2. Build
make build             # outputs bin/server and bin/tq

# 3. Run
export $(cat .env | xargs)
./bin/server
```

---

## REST API

| Method | Endpoint | Description |
|---|---|---|
| `POST` | `/jobs` | Submit a job |
| `GET` | `/jobs` | List jobs (`?queue=&status=&limit=&offset=`) |
| `GET` | `/jobs/:id` | Get a single job |
| `GET` | `/queues/:name/stats` | Queue statistics |
| `GET` | `/dlq` | List dead letter jobs (`?queue=`) |
| `POST` | `/dlq/:id/requeue` | Requeue a dead letter job |
| `GET` | `/health` | Health check |

### Submit a job

```bash
curl -X POST http://localhost:8080/jobs \
  -H "Content-Type: application/json" \
  -d '{"queue_name":"emails","payload":{"to":"user@example.com"},"max_retries":5}'
```

---

## CLI (`tq`)

```bash
# Submit a job
./bin/tq submit -q emails -p '{"to":"user@example.com","subject":"Welcome"}'

# Check job status
./bin/tq status <job-id>

# List jobs
./bin/tq list -q emails -s pending --limit 20

# Queue statistics
./bin/tq stats -q emails

# Dead letter queue
./bin/tq dlq list -q emails
./bin/tq dlq requeue <dlq-id>

# Point at a non-default server
./bin/tq --server http://prod:8080 stats -q emails
# or set TQ_SERVER=http://prod:8080
```

---

## Job Lifecycle

```
pending → running → completed
                 ↘ failed (retry with backoff) → pending (again)
                                               → dead (DLQ, after max retries)
```

Retry backoff: `2^attempt` seconds (2s, 4s, 8s, 16s…), capped at 1 hour.
