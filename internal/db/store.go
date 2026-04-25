package db

import (
	"context"
	"fmt"
	"time"

	"github.com/aman7401/taskqueue/internal/models"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Store struct {
	pool *pgxpool.Pool
}

func NewStore(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool}
}

func (s *Store) EnqueueJob(ctx context.Context, j *models.Job) error {
	const q = `
		INSERT INTO jobs (id, queue_name, payload, status, priority, max_retries, retry_count, next_run_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
	`
	_, err := s.pool.Exec(ctx, q,
		j.ID, j.QueueName, j.Payload, j.Status,
		j.Priority, j.MaxRetries, j.RetryCount, j.NextRunAt,
	)
	return err
}

// DequeueJob claims the next available job using SELECT FOR UPDATE SKIP LOCKED
// to prevent double-delivery under concurrent workers.
func (s *Store) DequeueJob(ctx context.Context, queueName string) (*models.Job, error) {
	const q = `
		UPDATE jobs
		SET status = 'running', started_at = NOW(), updated_at = NOW()
		WHERE id = (
			SELECT id FROM jobs
			WHERE queue_name = $1
			  AND status = 'pending'
			  AND next_run_at <= NOW()
			ORDER BY priority DESC, next_run_at ASC
			LIMIT 1
			FOR UPDATE SKIP LOCKED
		)
		RETURNING id, queue_name, payload, status, priority, max_retries, retry_count,
		          next_run_at, started_at, completed_at, error_message, created_at, updated_at
	`
	row := s.pool.QueryRow(ctx, q, queueName)
	return scanJob(row)
}

func (s *Store) MarkCompleted(ctx context.Context, jobID uuid.UUID) error {
	const q = `
		UPDATE jobs SET status = 'completed', completed_at = NOW(), updated_at = NOW()
		WHERE id = $1
	`
	_, err := s.pool.Exec(ctx, q, jobID)
	return err
}

func (s *Store) MarkFailed(ctx context.Context, jobID uuid.UUID, errMsg string, nextRunAt time.Time, isDead bool) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	newStatus := models.StatusFailed
	if isDead {
		newStatus = models.StatusDead
	}

	const updateJob = `
		UPDATE jobs
		SET status = $2, retry_count = retry_count + 1, error_message = $3,
		    next_run_at = $4, updated_at = NOW()
		WHERE id = $1
		RETURNING id, queue_name, payload, retry_count
	`
	var (
		id         uuid.UUID
		queueName  string
		payload    []byte
		retryCount int
	)
	err = tx.QueryRow(ctx, updateJob, jobID, string(newStatus), errMsg, nextRunAt).
		Scan(&id, &queueName, &payload, &retryCount)
	if err != nil {
		return err
	}

	if isDead {
		const insertDLQ = `
			INSERT INTO dead_letter_jobs (original_job_id, queue_name, payload, error_message, retry_count)
			VALUES ($1, $2, $3, $4, $5)
		`
		if _, err = tx.Exec(ctx, insertDLQ, id, queueName, payload, errMsg, retryCount); err != nil {
			return err
		}
	}

	return tx.Commit(ctx)
}

func (s *Store) GetJob(ctx context.Context, id uuid.UUID) (*models.Job, error) {
	const q = `
		SELECT id, queue_name, payload, status, priority, max_retries, retry_count,
		       next_run_at, started_at, completed_at, error_message, created_at, updated_at
		FROM jobs WHERE id = $1
	`
	return scanJob(s.pool.QueryRow(ctx, q, id))
}

func (s *Store) ListJobs(ctx context.Context, queueName, status string, limit, offset int) ([]*models.Job, error) {
	args := []interface{}{}
	where := "WHERE 1=1"
	i := 1

	if queueName != "" {
		where += fmt.Sprintf(" AND queue_name = $%d", i)
		args = append(args, queueName)
		i++
	}
	if status != "" {
		where += fmt.Sprintf(" AND status = $%d", i)
		args = append(args, status)
		i++
	}

	args = append(args, limit, offset)
	q := fmt.Sprintf(`
		SELECT id, queue_name, payload, status, priority, max_retries, retry_count,
		       next_run_at, started_at, completed_at, error_message, created_at, updated_at
		FROM jobs %s
		ORDER BY created_at DESC
		LIMIT $%d OFFSET $%d
	`, where, i, i+1)

	rows, err := s.pool.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var jobs []*models.Job
	for rows.Next() {
		j, err := scanJobRow(rows)
		if err != nil {
			return nil, err
		}
		jobs = append(jobs, j)
	}
	return jobs, rows.Err()
}

func (s *Store) QueueStats(ctx context.Context, queueName string) (*models.QueueStats, error) {
	const q = `
		SELECT
			COUNT(*) FILTER (WHERE status = 'pending')   AS pending,
			COUNT(*) FILTER (WHERE status = 'running')   AS running,
			COUNT(*) FILTER (WHERE status = 'completed') AS completed,
			COUNT(*) FILTER (WHERE status = 'failed')    AS failed,
			COUNT(*) FILTER (WHERE status = 'dead')      AS dead,
			COUNT(*)                                      AS total
		FROM jobs WHERE queue_name = $1
	`
	stats := &models.QueueStats{QueueName: queueName}
	err := s.pool.QueryRow(ctx, q, queueName).Scan(
		&stats.Pending, &stats.Running, &stats.Completed,
		&stats.Failed, &stats.Dead, &stats.Total,
	)
	return stats, err
}

func (s *Store) ListDLQ(ctx context.Context, queueName string, limit, offset int) ([]*models.DeadLetterJob, error) {
	where := ""
	args := []interface{}{}
	i := 1
	if queueName != "" {
		where = fmt.Sprintf("WHERE queue_name = $%d", i)
		args = append(args, queueName)
		i++
	}
	args = append(args, limit, offset)
	q := fmt.Sprintf(`
		SELECT id, original_job_id, queue_name, payload, error_message, retry_count, created_at
		FROM dead_letter_jobs %s
		ORDER BY created_at DESC
		LIMIT $%d OFFSET $%d
	`, where, i, i+1)

	rows, err := s.pool.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var dlqs []*models.DeadLetterJob
	for rows.Next() {
		d := &models.DeadLetterJob{}
		if err := rows.Scan(&d.ID, &d.OriginalJobID, &d.QueueName, &d.Payload,
			&d.ErrorMessage, &d.RetryCount, &d.CreatedAt); err != nil {
			return nil, err
		}
		dlqs = append(dlqs, d)
	}
	return dlqs, rows.Err()
}

func (s *Store) RequeueDLQ(ctx context.Context, dlqID uuid.UUID) (*models.Job, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	const fetchDLQ = `SELECT queue_name, payload FROM dead_letter_jobs WHERE id = $1`
	var queueName string
	var payload []byte
	if err := tx.QueryRow(ctx, fetchDLQ, dlqID).Scan(&queueName, &payload); err != nil {
		return nil, err
	}

	newID := uuid.New()
	const insertJob = `
		INSERT INTO jobs (id, queue_name, payload, status, max_retries, next_run_at)
		VALUES ($1, $2, $3, 'pending', 3, NOW())
		RETURNING id, queue_name, payload, status, priority, max_retries, retry_count,
		          next_run_at, started_at, completed_at, error_message, created_at, updated_at
	`
	j, err := scanJob(tx.QueryRow(ctx, insertJob, newID, queueName, payload))
	if err != nil {
		return nil, err
	}

	if _, err := tx.Exec(ctx, `DELETE FROM dead_letter_jobs WHERE id = $1`, dlqID); err != nil {
		return nil, err
	}

	return j, tx.Commit(ctx)
}

func scanJob(row pgx.Row) (*models.Job, error) {
	j := &models.Job{}
	err := row.Scan(
		&j.ID, &j.QueueName, &j.Payload, &j.Status,
		&j.Priority, &j.MaxRetries, &j.RetryCount,
		&j.NextRunAt, &j.StartedAt, &j.CompletedAt, &j.ErrorMessage,
		&j.CreatedAt, &j.UpdatedAt,
	)
	if err == pgx.ErrNoRows {
		return nil, nil
	}
	return j, err
}

func scanJobRow(rows pgx.Rows) (*models.Job, error) {
	j := &models.Job{}
	err := rows.Scan(
		&j.ID, &j.QueueName, &j.Payload, &j.Status,
		&j.Priority, &j.MaxRetries, &j.RetryCount,
		&j.NextRunAt, &j.StartedAt, &j.CompletedAt, &j.ErrorMessage,
		&j.CreatedAt, &j.UpdatedAt,
	)
	return j, err
}
