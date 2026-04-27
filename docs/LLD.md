# Low-Level Design — TaskQueue

## Package Dependency Graph

```mermaid
graph TD
    CLI["cmd/cli\nmain.go"]
    SRV["cmd/server\nmain.go"]
    API["internal/api\nhandlers.go"]
    Q["internal/queue\nmanager.go"]
    W["internal/worker\npool.go"]
    DB["internal/db\nstore.go"]
    M["internal/models\njob.go"]
    PG[("Postgres")]

    CLI -->|"HTTP over network\n(not a Go import)"| API
    SRV --> API
    SRV --> W
    API --> Q
    API --> DB
    W --> Q
    Q --> DB
    DB --> PG
    API --> M
    Q --> M
    DB --> M
    W --> M
```

---

## Database Schema

```mermaid
erDiagram
    jobs {
        uuid        id            PK
        varchar     queue_name
        jsonb       payload
        job_status  status
        int         priority
        int         max_retries
        int         retry_count
        timestamptz next_run_at
        timestamptz started_at
        timestamptz completed_at
        text        error_message
        timestamptz created_at
        timestamptz updated_at
    }

    dead_letter_jobs {
        uuid        id              PK
        uuid        original_job_id FK
        varchar     queue_name
        jsonb       payload
        text        error_message
        int         retry_count
        timestamptz created_at
    }

    jobs ||--o{ dead_letter_jobs : "moves to on exhaustion"
```

### DB Indexes

| Index | Columns | Purpose |
|---|---|---|
| `idx_jobs_queue_status_next_run` | `queue_name, status, priority DESC, next_run_at` | Fast dequeue — partial index on `status = pending` |
| `idx_jobs_status` | `status` | Fast filtering in list/stats queries |
| `idx_dlq_queue` | `queue_name` | Fast DLQ listing per queue |
| `idx_dlq_original` | `original_job_id` | Look up DLQ entry by original job |

---

## Worker Pool Internals

```mermaid
flowchart TD
    Start["Pool.Start(ctx)"]
    Ticker["time.Ticker\nevery 2s"]
    CtxDone{ctx.Done?}
    Drain["Drain semaphore\nwait for in-flight jobs"]
    Stop["Pool stopped"]
    Sem["Semaphore channel\ncap = concurrency"]
    Goroutine["go processOne(queue)"]
    Dequeue["store.DequeueJob\nSELECT FOR UPDATE SKIP LOCKED"]
    Empty{job == nil?}
    Run["safeRun(handler, job)\nwith 30s timeout"]
    OK{error?}
    Ack["store.MarkCompleted"]
    Nack["queue.Nack\n(backoff or DLQ)"]
    Done["release semaphore slot"]

    Start --> Ticker
    Ticker --> CtxDone
    CtxDone -->|shutdown signal| Drain --> Stop
    CtxDone -->|each tick, each queue| Sem
    Sem -->|acquire slot| Goroutine
    Goroutine --> Dequeue
    Dequeue --> Empty
    Empty -->|yes| Done
    Empty -->|no| Run
    Run --> OK
    OK -->|no error| Ack --> Done
    OK -->|error| Nack --> Done
```

---

## Concurrency Model

```mermaid
graph LR
    subgraph "Worker Pool (concurrency = 10)"
        S["Semaphore\nchan struct{} cap=10"]
        G1["goroutine 1"]
        G2["goroutine 2"]
        G3["goroutine 3"]
        GN["goroutine N"]
    end

    T["Ticker\nevery 2s"] -->|tick| S
    S --> G1
    S --> G2
    S --> G3
    S --> GN
    G1 -->|SKIP LOCKED| PG[("Postgres")]
    G2 -->|SKIP LOCKED| PG
    G3 -->|SKIP LOCKED| PG
    GN -->|SKIP LOCKED| PG
```

Each tick spawns one goroutine per queue. The semaphore ensures at most `concurrency` goroutines run simultaneously. `SKIP LOCKED` ensures no two goroutines pick the same job.

---

## API Layer

```mermaid
flowchart LR
    MW["loggingMiddleware\nmethod + path + status + latency"]
    MW --> MUX

    subgraph MUX["net/http ServeMux"]
        R1["POST /jobs"]
        R2["GET  /jobs"]
        R3["GET  /jobs/{id}"]
        R4["GET  /queues/{name}/stats"]
        R5["GET  /dlq"]
        R6["POST /dlq/{id}/requeue"]
        R7["GET  /health"]
    end

    R1 --> QM["queue.Manager\n.Enqueue()"]
    R2 --> ST["db.Store\n.ListJobs()"]
    R3 --> ST2["db.Store\n.GetJob()"]
    R4 --> ST3["db.Store\n.QueueStats()"]
    R5 --> ST4["db.Store\n.ListDLQ()"]
    R6 --> ST5["db.Store\n.RequeueDLQ()"]
    R7 --> OK["200 ok"]
```

---

## CLI → API Mapping

| CLI command | HTTP call |
|---|---|
| `tq submit -q <q> -p <json>` | `POST /jobs` |
| `tq status <id>` | `GET /jobs/<id>` |
| `tq list --queue --status` | `GET /jobs?queue=&status=` |
| `tq stats --queue <q>` | `GET /queues/<q>/stats` |
| `tq dlq list` | `GET /dlq` |
| `tq dlq requeue <id>` | `POST /dlq/<id>/requeue` |

---

## Retry Backoff Values

| Attempt | Delay before next retry |
|---|---|
| 1st failure | 2 seconds |
| 2nd failure | 4 seconds |
| 3rd failure | 8 seconds |
| 4th failure | 16 seconds |
| 5th failure | 32 seconds |
| 6th failure | 64 seconds |
| 7th failure | 128 seconds |
| 8th failure | 256 seconds |
| 9th failure | 512 seconds |
| 10th failure | 1024 seconds |
| 11th failure | 2048 seconds |
| **> 11 attempts** | capped at **1 hour** |

---

## Graceful Shutdown Sequence

```mermaid
sequenceDiagram
    participant OS as OS Signal\n(Ctrl+C)
    participant SRV as HTTP Server
    participant WP as Worker Pool
    participant DB as Postgres

    OS->>SRV: SIGINT / SIGTERM
    SRV->>WP: cancel workerCtx
    WP->>WP: stop accepting new jobs\n(ticker stops)
    WP->>WP: drain semaphore\n(wait for in-flight jobs)
    WP->>DB: finish current UPDATE / INSERT
    WP-->>SRV: all workers stopped
    SRV->>SRV: srv.Shutdown(15s timeout)
    SRV-->>OS: process exits cleanly
```
