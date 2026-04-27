package worker

import (
	"context"
	"errors"
	"testing"

	"github.com/aman7401/taskqueue/internal/models"
	"github.com/google/uuid"
)

func TestSafeRun_Success(t *testing.T) {
	job := &models.Job{ID: uuid.New()}
	err := safeRun(context.Background(), func(_ context.Context, _ *models.Job) error {
		return nil
	}, job)
	if err != nil {
		t.Errorf("expected nil error, got %v", err)
	}
}

func TestSafeRun_HandlerReturnsError(t *testing.T) {
	job := &models.Job{ID: uuid.New()}
	want := errors.New("something broke")

	err := safeRun(context.Background(), func(_ context.Context, _ *models.Job) error {
		return want
	}, job)

	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if err.Error() != want.Error() {
		t.Errorf("got %v, want %v", err, want)
	}
}

func TestSafeRun_PanicIsRecovered(t *testing.T) {
	job := &models.Job{ID: uuid.New()}

	err := safeRun(context.Background(), func(_ context.Context, _ *models.Job) error {
		panic("unexpected crash")
	}, job)

	if err == nil {
		t.Fatal("expected error from panic recovery, got nil")
	}
	if err.Error() != "panic: unexpected crash" {
		t.Errorf("unexpected error message: %v", err)
	}
}

func TestSafeRun_ContextPassedToHandler(t *testing.T) {
	type key struct{}
	ctx := context.WithValue(context.Background(), key{}, "test-value")
	job := &models.Job{ID: uuid.New()}

	err := safeRun(ctx, func(c context.Context, _ *models.Job) error {
		if c.Value(key{}) != "test-value" {
			t.Error("context not passed correctly to handler")
		}
		return nil
	}, job)

	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}
