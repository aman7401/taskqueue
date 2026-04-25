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

type Manager struct {
	store *db.Store
}

func NewManager(store *db.Store) *Manager {
	return &Manager{store: store}
}

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

func (m *Manager) Dequeue(ctx context.Context, queueName string) (*models.Job, error) {
	return m.store.DequeueJob(ctx, queueName)
}

func (m *Manager) Ack(ctx context.Context, jobID uuid.UUID) error {
	return m.store.MarkCompleted(ctx, jobID)
}

func (m *Manager) Nack(ctx context.Context, job *models.Job, errMsg string) error {
	newRetryCount := job.RetryCount + 1
	isDead := newRetryCount >= job.MaxRetries

	var nextRunAt time.Time
	if isDead {
		nextRunAt = time.Now()
	} else {
		nextRunAt = time.Now().Add(exponentialBackoff(newRetryCount))
	}

	return m.store.MarkFailed(ctx, job.ID, errMsg, nextRunAt, isDead)
}

// exponentialBackoff returns 2^attempt seconds, capped at 1 hour.
func exponentialBackoff(attempt int) time.Duration {
	delay := time.Duration(1<<uint(attempt)) * time.Second
	if delay > time.Hour {
		delay = time.Hour
	}
	return delay
}
