package models

import (
	"time"

	"github.com/google/uuid"
)

type JobStatus string

const (
	StatusPending   JobStatus = "pending"
	StatusRunning   JobStatus = "running"
	StatusCompleted JobStatus = "completed"
	StatusFailed    JobStatus = "failed"
	StatusDead      JobStatus = "dead"
)

type Job struct {
	ID           uuid.UUID  `json:"id"`
	QueueName    string     `json:"queue_name"`
	Payload      []byte     `json:"payload"`
	Status       JobStatus  `json:"status"`
	Priority     int        `json:"priority"`
	MaxRetries   int        `json:"max_retries"`
	RetryCount   int        `json:"retry_count"`
	NextRunAt    time.Time  `json:"next_run_at"`
	StartedAt    *time.Time `json:"started_at,omitempty"`
	CompletedAt  *time.Time `json:"completed_at,omitempty"`
	ErrorMessage *string    `json:"error_message,omitempty"`
	CreatedAt    time.Time  `json:"created_at"`
	UpdatedAt    time.Time  `json:"updated_at"`
}

type DeadLetterJob struct {
	ID            uuid.UUID `json:"id"`
	OriginalJobID uuid.UUID `json:"original_job_id"`
	QueueName     string    `json:"queue_name"`
	Payload       []byte    `json:"payload"`
	ErrorMessage  string    `json:"error_message"`
	RetryCount    int       `json:"retry_count"`
	CreatedAt     time.Time `json:"created_at"`
}

type QueueStats struct {
	QueueName string `json:"queue_name"`
	Pending   int64  `json:"pending"`
	Running   int64  `json:"running"`
	Completed int64  `json:"completed"`
	Failed    int64  `json:"failed"`
	Dead      int64  `json:"dead"`
	Total     int64  `json:"total"`
}

// SubmitRequest is the HTTP request body for enqueuing a job.
type SubmitRequest struct {
	QueueName  string          `json:"queue_name"`
	Payload    interface{}     `json:"payload"`
	Priority   int             `json:"priority"`
	MaxRetries int             `json:"max_retries"`
	// RunAt schedules the job for a future time (RFC3339). Empty means now.
	RunAt      string          `json:"run_at,omitempty"`
}
