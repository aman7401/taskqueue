package api

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/aman7401/taskqueue/internal/models"
	"github.com/google/uuid"
)

// --- Mocks ---

type mockManager struct {
	job *models.Job
	err error
}

func (m *mockManager) Enqueue(_ context.Context, _ *models.SubmitRequest) (*models.Job, error) {
	return m.job, m.err
}

type mockStore struct {
	job    *models.Job
	jobs   []*models.Job
	stats  *models.QueueStats
	dlqs   []*models.DeadLetterJob
	err    error
}

func (m *mockStore) GetJob(_ context.Context, _ uuid.UUID) (*models.Job, error) {
	return m.job, m.err
}
func (m *mockStore) ListJobs(_ context.Context, _, _ string, _, _ int) ([]*models.Job, error) {
	return m.jobs, m.err
}
func (m *mockStore) QueueStats(_ context.Context, _ string) (*models.QueueStats, error) {
	return m.stats, m.err
}
func (m *mockStore) ListDLQ(_ context.Context, _ string, _, _ int) ([]*models.DeadLetterJob, error) {
	return m.dlqs, m.err
}
func (m *mockStore) RequeueDLQ(_ context.Context, _ uuid.UUID) (*models.Job, error) {
	return m.job, m.err
}

// --- Helpers ---

func newTestHandler(mgr jobManager, store jobStore) *Handler {
	return &Handler{mgr: mgr, store: store}
}

func doRequest(h *Handler, method, path string, body interface{}) *httptest.ResponseRecorder {
	var buf bytes.Buffer
	if body != nil {
		json.NewEncoder(&buf).Encode(body)
	}
	req := httptest.NewRequest(method, path, &buf)
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()

	mux := http.NewServeMux()
	h.RegisterRoutes(mux)
	mux.ServeHTTP(rr, req)
	return rr
}

// --- Tests: POST /jobs ---

func TestSubmitJob_Success(t *testing.T) {
	jobID := uuid.New()
	mgr := &mockManager{job: &models.Job{ID: jobID, QueueName: "default", Status: models.StatusPending}}
	h := newTestHandler(mgr, &mockStore{})

	rr := doRequest(h, http.MethodPost, "/jobs", map[string]interface{}{
		"queue_name": "default",
		"payload":    map[string]string{"task": "send_email"},
	})

	if rr.Code != http.StatusCreated {
		t.Errorf("expected 201, got %d", rr.Code)
	}

	var resp map[string]interface{}
	json.NewDecoder(rr.Body).Decode(&resp)
	if resp["id"] != jobID.String() {
		t.Errorf("expected job id %s, got %v", jobID, resp["id"])
	}
}

func TestSubmitJob_InvalidJSON(t *testing.T) {
	h := newTestHandler(&mockManager{}, &mockStore{})
	req := httptest.NewRequest(http.MethodPost, "/jobs", bytes.NewBufferString("not-json"))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()

	mux := http.NewServeMux()
	h.RegisterRoutes(mux)
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", rr.Code)
	}
}

// --- Tests: GET /jobs/{id} ---

func TestGetJob_Success(t *testing.T) {
	jobID := uuid.New()
	store := &mockStore{job: &models.Job{ID: jobID, QueueName: "default", Status: models.StatusCompleted}}
	h := newTestHandler(&mockManager{}, store)

	req := httptest.NewRequest(http.MethodGet, "/jobs/"+jobID.String(), nil)
	rr := httptest.NewRecorder()
	mux := http.NewServeMux()
	h.RegisterRoutes(mux)
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", rr.Code)
	}
}

func TestGetJob_NotFound(t *testing.T) {
	store := &mockStore{job: nil, err: nil}
	h := newTestHandler(&mockManager{}, store)

	req := httptest.NewRequest(http.MethodGet, "/jobs/"+uuid.New().String(), nil)
	rr := httptest.NewRecorder()
	mux := http.NewServeMux()
	h.RegisterRoutes(mux)
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusNotFound {
		t.Errorf("expected 404, got %d", rr.Code)
	}
}

func TestGetJob_InvalidID(t *testing.T) {
	h := newTestHandler(&mockManager{}, &mockStore{})
	rr := doRequest(h, http.MethodGet, "/jobs/not-a-uuid", nil)

	if rr.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", rr.Code)
	}
}

// --- Tests: GET /queues/{name}/stats ---

func TestQueueStats(t *testing.T) {
	store := &mockStore{stats: &models.QueueStats{
		QueueName: "default", Pending: 2, Completed: 10, Total: 12,
	}}
	h := newTestHandler(&mockManager{}, store)

	req := httptest.NewRequest(http.MethodGet, "/queues/default/stats", nil)
	rr := httptest.NewRecorder()
	mux := http.NewServeMux()
	h.RegisterRoutes(mux)
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", rr.Code)
	}

	var resp models.QueueStats
	json.NewDecoder(rr.Body).Decode(&resp)
	if resp.Total != 12 {
		t.Errorf("expected total 12, got %d", resp.Total)
	}
}

// --- Tests: GET /dlq ---

func TestListDLQ_Empty(t *testing.T) {
	h := newTestHandler(&mockManager{}, &mockStore{dlqs: nil})
	rr := doRequest(h, http.MethodGet, "/dlq", nil)

	if rr.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", rr.Code)
	}

	var resp map[string]interface{}
	json.NewDecoder(rr.Body).Decode(&resp)
	dlqs := resp["dead_letter_jobs"].([]interface{})
	if len(dlqs) != 0 {
		t.Errorf("expected empty DLQ, got %d items", len(dlqs))
	}
}

// --- Tests: GET /health ---

func TestHealthCheck(t *testing.T) {
	h := newTestHandler(&mockManager{}, &mockStore{})
	rr := doRequest(h, http.MethodGet, "/health", nil)

	if rr.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", rr.Code)
	}
}
