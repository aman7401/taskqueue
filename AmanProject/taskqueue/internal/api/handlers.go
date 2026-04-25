package api

import (
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/aman7401/taskqueue/internal/db"
	"github.com/aman7401/taskqueue/internal/models"
	"github.com/aman7401/taskqueue/internal/queue"
	"github.com/google/uuid"
)

type Handler struct {
	mgr   *queue.Manager
	store *db.Store
}

func NewHandler(mgr *queue.Manager, store *db.Store) *Handler {
	return &Handler{mgr: mgr, store: store}
}

// RegisterRoutes wires all routes onto mux.
func (h *Handler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("POST /jobs", h.submitJob)
	mux.HandleFunc("GET /jobs", h.listJobs)
	mux.HandleFunc("GET /jobs/{id}", h.getJob)
	mux.HandleFunc("GET /queues/{name}/stats", h.queueStats)
	mux.HandleFunc("GET /dlq", h.listDLQ)
	mux.HandleFunc("POST /dlq/{id}/requeue", h.requeueDLQ)
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
}

// POST /jobs
func (h *Handler) submitJob(w http.ResponseWriter, r *http.Request) {
	var req models.SubmitRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return
	}

	job, err := h.mgr.Enqueue(r.Context(), &req)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusCreated, job)
}

// GET /jobs?queue=&status=&limit=&offset=
func (h *Handler) listJobs(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	limit, _ := strconv.Atoi(q.Get("limit"))
	offset, _ := strconv.Atoi(q.Get("offset"))
	if limit <= 0 {
		limit = 50
	}

	jobs, err := h.store.ListJobs(r.Context(), q.Get("queue"), q.Get("status"), limit, offset)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if jobs == nil {
		jobs = []*models.Job{}
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"jobs":   jobs,
		"limit":  limit,
		"offset": offset,
	})
}

// GET /jobs/{id}
func (h *Handler) getJob(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid job id")
		return
	}

	job, err := h.store.GetJob(r.Context(), id)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if job == nil {
		writeError(w, http.StatusNotFound, "job not found")
		return
	}
	writeJSON(w, http.StatusOK, job)
}

// GET /queues/{name}/stats
func (h *Handler) queueStats(w http.ResponseWriter, r *http.Request) {
	stats, err := h.store.QueueStats(r.Context(), r.PathValue("name"))
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, stats)
}

// GET /dlq?queue=&limit=&offset=
func (h *Handler) listDLQ(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	limit, _ := strconv.Atoi(q.Get("limit"))
	offset, _ := strconv.Atoi(q.Get("offset"))
	if limit <= 0 {
		limit = 50
	}

	jobs, err := h.store.ListDLQ(r.Context(), q.Get("queue"), limit, offset)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if jobs == nil {
		jobs = []*models.DeadLetterJob{}
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"dead_letter_jobs": jobs,
		"limit":            limit,
		"offset":           offset,
	})
}

// POST /dlq/{id}/requeue
func (h *Handler) requeueDLQ(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid dlq id")
		return
	}

	job, err := h.store.RequeueDLQ(r.Context(), id)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusCreated, job)
}

func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}
