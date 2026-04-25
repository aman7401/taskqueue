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

    CLI -->|HTTP calls only| SRV
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

---

## Worker Pool Internals

```mermaid
flowchart TD
    Start["Pool.Start(ctx)"]
    Ticker["time.Ticker\nevery 2s"]
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
    Ticker -->|each tick, each queue| Sem
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

## API Layer

```mermaid
flowchart LR
    subgraph net/http ServeMux
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

## Job Status Transitions

```mermaid
stateDiagram-v2
    [*] --> pending : POST /jobs
    pending --> running : worker dequeues\n(SKIP LOCKED)
    running --> completed : handler returns nil
    running --> failed : handler returns error\n(retries remain)
    failed --> pending : next_run_at elapsed
    running --> dead : handler returns error\n(max retries reached)
    dead --> pending : POST /dlq/:id/requeue
    completed --> [*]
```

---

## Retry Backoff Values

| Attempt | Delay before next retry |
|---|---|
| 1st failure | 2 seconds |
| 2nd failure | 4 seconds |
| 3rd failure | 8 seconds |
| 4th failure | 16 seconds |
| 5th failure | 32 seconds |
| … | … (doubles each time) |
| > ~36 attempts | capped at **1 hour** |
