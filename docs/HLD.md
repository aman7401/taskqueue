# High-Level Design — TaskQueue

## What it is

TaskQueue is a distributed background job processing system modelled after Amazon SQS. Clients submit work over HTTP; a pool of goroutine workers picks up and executes jobs concurrently; failed jobs are retried with exponential backoff; jobs that exhaust all retries land in a Dead Letter Queue.

---

## System Components

```mermaid
graph TD
    Producer["Producer\n(any HTTP client / CLI)"]
    API["HTTP API Server\n:8080"]
    DB[("Postgres\njobs + dead_letter_jobs")]
    WP["Worker Pool\n(goroutines)"]
    DLQ["Dead Letter Queue\n(dead_letter_jobs table)"]
    CLI["CLI — tq\n(submit / monitor)"]

    Producer -->|POST /jobs| API
    CLI -->|HTTP calls| API
    API -->|INSERT| DB
    WP -->|SELECT FOR UPDATE SKIP LOCKED| DB
    WP -->|UPDATE status| DB
    WP -->|on max retries exceeded| DLQ
    DLQ -->|POST /dlq/:id/requeue| API
```

---

## Request Flow

```mermaid
sequenceDiagram
    participant P as Producer
    participant A as API Server
    participant DB as Postgres
    participant W as Worker

    P->>A: POST /jobs {queue, payload}
    A->>DB: INSERT INTO jobs (status=pending)
    A-->>P: 201 {job_id}

    loop every 2 seconds
        W->>DB: SELECT FOR UPDATE SKIP LOCKED WHERE status=pending
        DB-->>W: job row (status updated to running)
        W->>W: execute handler(job)
        alt success
            W->>DB: UPDATE status=completed
        else failure, retries left
            W->>DB: UPDATE status=failed, next_run_at=now+backoff
        else failure, max retries reached
            W->>DB: UPDATE status=dead
            W->>DB: INSERT INTO dead_letter_jobs
        end
    end
```

---

## Retry & Backoff Strategy

```mermaid
flowchart LR
    A[Job fails] --> B{retry_count < max_retries?}
    B -- Yes --> C["status = failed\nnext_run_at = now + 2^attempt sec"]
    C --> D[Picked up again by worker]
    B -- No --> E["status = dead\nInserted into DLQ"]
    E --> F[Manual requeue via API/CLI]
```

---

## Deployment View

```mermaid
graph LR
    subgraph Docker Compose
        S["server\n(HTTP + Workers)"]
        PG[("postgres:16")]
    end
    CLI["tq CLI\n(local)"] -->|HTTP| S
    S <-->|pgx/v5| PG
```

---

## Key Design Decisions

| Decision | Rationale |
|---|---|
| Postgres as the queue backend | Durable, ACID transactions, no extra infrastructure |
| `SELECT FOR UPDATE SKIP LOCKED` | Prevents double-delivery under concurrent workers with no application-level locking |
| Polling (not LISTEN/NOTIFY) | Simpler, portable; poll interval is tunable |
| Workers embedded in server process | Reduces operational complexity for a single-node deployment |
| Exponential backoff capped at 1 hour | Avoids hammering a broken downstream while still retrying |
