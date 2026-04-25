package queue

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/aman7401/taskqueue/internal/db"
	"github.com/aman7401/taskqueue/internal/models"
	"github.com/google/uuid"
)

// Manager handles high-level queue operations.
type Manager struct {
	store *db.Store
}

func NewManager(store *db.Store) *Manager {
	return &Manager{store: store}
}

// Enqueue validates and persists a new job.
func (m *Manager) Enqueue(ctx context.Context, req *models.SubmitRequest) (*models.Job, error) {
	if req.QueueName == "" {
		return nil, fmt.Errorf("queue_name is required")
	}
	if req.Payload == nil {
		return nil, fmt.Errorf("payload is required")
	}

	payloadBytes, err := json.Marshal(req.Payload)
	if err != nil {
		return nil, fmt.Errorf("invalid payload: %w", err)
	}

	maxRetries := req.MaxRetries
	if maxRetries <= 0 {
		maxRetries = 3
	}

	nextRunAt := time.Now()
	if req.RunAt != "" {
		t, err := time.Parse(time.RFC3339, req.RunAt)
		if err != nil {
			return nil, fmt.Errorf("invalid run_at format, use RFC3339: %w", err)
		}
		nextRunAt = t
	}

	j := &models.Job{
		ID:         uuid.New(),
		QueueName:  req.QueueName,
		Payload:    payloadBytes,
		Status:     models.StatusPending,
		Priority:   req.Priority,
		MaxRetries: maxRetries,
		RetryCount: 0,
		NextRunAt:  nextRunAt,
	}

	if err := m.store.EnqueueJob(ctx, j); err != nil {
		return nil, fmt.Errorf("enqueue: %w", err)
	}
	return j, nil
}

// Dequeue claims the next available job for the given queue.
// Returns nil job (no error) when the queue is empty.
func (m *Manager) Dequeue(ctx context.Context, queueName string) (*models.Job, error) {
	return m.store.DequeueJob(ctx, queueName)
}

// Ack marks a job as successfully completed.
func (m *Manager) Ack(ctx context.Context, jobID uuid.UUID) error {
	return m.store.MarkCompleted(ctx, jobID)
}

// Nack records a failure for a job, scheduling retry or moving to DLQ.
func (m *Manager) Nack(ctx context.Context, job *models.Job, errMsg string) error {
	newRetryCount := job.RetryCount + 1
	isDead := newRetryCount >= job.MaxRetries

	var nextRunAt time.Time
	if isDead {
		nextRunAt = time.Now()
	} else {
		backoff := exponentialBackoff(newRetryCount)
		nextRunAt = time.Now().Add(backoff)
	}

	return m.store.MarkFailed(ctx, job.ID, errMsg, nextRunAt, isDead)
}

// exponentialBackoff returns the delay before retrying attempt n (1-based).
// Formula: 2^n seconds, capped at 1 hour.
func exponentialBackoff(attempt int) time.Duration {
	delay := time.Duration(1<<uint(attempt)) * time.Second
	const maxBackoff = time.Hour
	if delay > maxBackoff {
		delay = maxBackoff
	}
	return delay
}
