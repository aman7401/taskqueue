# High-Level Design — TaskQueue

## What it is

TaskQueue is a distributed background job processing system modelled after Amazon SQS. Clients submit work over HTTP; a pool of goroutine workers picks up and executes jobs concurrently; failed jobs are retried with exponential backoff; jobs that exhaust all retries land in a Dead Letter Queue.

---

## Service Architecture

Shows all major layers and how they relate to each other.

```mermaid
graph TB
    subgraph Clients
        P["Producer\n(HTTP Client / App)"]
        CLI["tq CLI"]
    end

    subgraph TaskQueue Service
        subgraph HTTP Layer
            API["REST API\nhandlers.go\n:8080"]
        end

        subgraph Business Logic Layer
            QM["Queue Manager\nqueue/manager.go\n- Enqueue\n- Ack / Nack\n- Backoff logic"]
        end

        subgraph Worker Layer
            WP["Worker Pool\nworker/pool.go\n10 goroutines\npoll every 2s"]
            H["Job Handler\n(pluggable)"]
        end

        subgraph Data Layer
            ST["DB Store\ndb/store.go\n- CRUD\n- Dequeue\n- DLQ ops"]
        end
    end

    subgraph Infrastructure
        PG[("PostgreSQL\njobs\ndead_letter_jobs")]
    end

    P -->|POST /jobs\nGET /jobs| API
    CLI -->|HTTP| API
    API --> QM
    API --> ST
    QM --> ST
    WP --> QM
    WP --> H
    ST <-->|pgx/v5| PG
```

---

## Request Flow

End-to-end journey of a job from submission to completion.

```mermaid
sequenceDiagram
    participant P as Producer
    participant A as API Server
    participant DB as Postgres
    participant W as Worker

    P->>A: POST /jobs {queue, payload}
    A->>DB: persist job (status=pending)
    A-->>P: 201 {job_id}

    loop poll every 2 seconds
        W->>DB: claim next available job
        DB-->>W: job (status=running)
        W->>W: execute handler(job)
        alt success
            W->>DB: mark completed
        else failure, retries remain
            W->>DB: schedule retry with backoff
        else all retries exhausted
            W->>DB: move to Dead Letter Queue
        end
    end
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
| Postgres as the queue backend | Durable, ACID transactions, no extra infrastructure needed |
| Atomic dequeue with row-level locking | Prevents double-delivery under concurrent workers — see LLD for mechanics |
| Polling over push | Simpler, portable; poll interval is tunable per deployment |
| Workers embedded in server process | Reduces operational complexity for a single-node deployment |
| Exponential backoff with a cap | Avoids hammering a broken downstream — see LLD for exact values |
| Dead Letter Queue | Failed jobs are never silently dropped; always recoverable via requeue |
