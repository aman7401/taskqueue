package worker

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/aman7401/taskqueue/internal/models"
	"github.com/aman7401/taskqueue/internal/queue"
)

type HandlerFunc func(ctx context.Context, job *models.Job) error

type Pool struct {
	mgr          *queue.Manager
	queues       []string
	concurrency  int
	handler      HandlerFunc
	pollInterval time.Duration
}

type Config struct {
	Queues       []string
	Concurrency  int
	Handler      HandlerFunc
	PollInterval time.Duration
}

func NewPool(mgr *queue.Manager, cfg Config) *Pool {
	if cfg.Concurrency <= 0 {
		cfg.Concurrency = 5
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = 2 * time.Second
	}
	return &Pool{
		mgr:          mgr,
		queues:       cfg.Queues,
		concurrency:  cfg.Concurrency,
		handler:      cfg.Handler,
		pollInterval: cfg.PollInterval,
	}
}

func (p *Pool) Start(ctx context.Context) {
	log.Printf("[pool] starting %d workers across queues %v", p.concurrency, p.queues)
	sem := make(chan struct{}, p.concurrency)

	ticker := time.NewTicker(p.pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Println("[pool] shutting down, draining in-flight jobs...")
			for i := 0; i < p.concurrency; i++ {
				sem <- struct{}{}
			}
			log.Println("[pool] all workers stopped")
			return
		case <-ticker.C:
			for _, q := range p.queues {
				sem <- struct{}{}
				go func(queueName string) {
					defer func() { <-sem }()
					p.processOne(ctx, queueName)
				}(q)
			}
		}
	}
}

func (p *Pool) processOne(ctx context.Context, queueName string) {
	job, err := p.mgr.Dequeue(ctx, queueName)
	if err != nil {
		log.Printf("[worker] dequeue error on %s: %v", queueName, err)
		return
	}
	if job == nil {
		return
	}

	log.Printf("[worker] processing job %s (queue=%s, attempt=%d/%d)",
		job.ID, job.QueueName, job.RetryCount+1, job.MaxRetries)

	jobCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	handlerErr := safeRun(jobCtx, p.handler, job)

	if handlerErr == nil {
		if err := p.mgr.Ack(ctx, job.ID); err != nil {
			log.Printf("[worker] ack failed for job %s: %v", job.ID, err)
		} else {
			log.Printf("[worker] job %s completed", job.ID)
		}
		return
	}

	log.Printf("[worker] job %s failed (attempt %d): %v", job.ID, job.RetryCount+1, handlerErr)
	if err := p.mgr.Nack(ctx, job, handlerErr.Error()); err != nil {
		log.Printf("[worker] nack failed for job %s: %v", job.ID, err)
	}
}

func safeRun(ctx context.Context, handler HandlerFunc, job *models.Job) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic: %v", r)
		}
	}()
	return handler(ctx, job)
}

// DefaultHandler is a demo handler that simulates work and randomly fails
// ~20% of the time so you can observe retry + DLQ behaviour out of the box.
func DefaultHandler(ctx context.Context, job *models.Job) error {
	log.Printf("[handler] executing job %s payload=%s", job.ID, string(job.Payload))
	time.Sleep(time.Duration(100+rand.Intn(400)) * time.Millisecond)
	if rand.Float32() < 0.2 {
		return fmt.Errorf("simulated transient failure")
	}
	return nil
}
