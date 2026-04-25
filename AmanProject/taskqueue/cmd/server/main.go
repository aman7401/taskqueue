package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/aman7401/taskqueue/internal/api"
	"github.com/aman7401/taskqueue/internal/db"
	"github.com/aman7401/taskqueue/internal/queue"
	"github.com/aman7401/taskqueue/internal/worker"
	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	dsn := env("DATABASE_URL", "postgres://postgres:postgres@localhost:5432/taskqueue?sslmode=disable")
	addr := env("ADDR", ":8080")
	queues := strings.Split(env("QUEUES", "default,emails,notifications"), ",")
	concurrency := 10

	// --- Database ---
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		log.Fatalf("db connect: %v", err)
	}
	defer pool.Close()

	if err := pool.Ping(ctx); err != nil {
		log.Fatalf("db ping: %v", err)
	}
	log.Println("connected to postgres")

	if err := runMigrations(pool, ctx); err != nil {
		log.Fatalf("migrations: %v", err)
	}

	// --- Wire up layers ---
	store := db.NewStore(pool)
	mgr := queue.NewManager(store)

	// --- HTTP server ---
	mux := http.NewServeMux()
	api.NewHandler(mgr, store).RegisterRoutes(mux)

	srv := &http.Server{
		Addr:         addr,
		Handler:      loggingMiddleware(mux),
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	// --- Worker pool ---
	workerCtx, workerCancel := context.WithCancel(ctx)
	wp := worker.NewPool(mgr, worker.Config{
		Queues:       queues,
		Concurrency:  concurrency,
		Handler:      worker.DefaultHandler,
		PollInterval: 2 * time.Second,
	})

	go wp.Start(workerCtx)

	// --- Graceful shutdown ---
	go func() {
		log.Printf("server listening on %s", addr)
		if err := srv.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("server: %v", err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Println("shutdown signal received")

	workerCancel()

	shutCtx, shutCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer shutCancel()
	if err := srv.Shutdown(shutCtx); err != nil {
		log.Printf("server shutdown error: %v", err)
	}
	log.Println("server stopped")
}

// runMigrations applies the schema idempotently on startup.
func runMigrations(pool *pgxpool.Pool, ctx context.Context) error {
	data, err := os.ReadFile("migrations/001_init.sql")
	if err != nil {
		// Try relative path from binary location.
		data, err = os.ReadFile("../../migrations/001_init.sql")
		if err != nil {
			return fmt.Errorf("read migration file: %w", err)
		}
	}
	_, err = pool.Exec(ctx, string(data))
	return err
}

func loggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rw := &responseWriter{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rw, r)
		log.Printf("%s %s %d %s", r.Method, r.URL.Path, rw.status, time.Since(start))
	})
}

type responseWriter struct {
	http.ResponseWriter
	status int
}

func (rw *responseWriter) WriteHeader(status int) {
	rw.status = status
	rw.ResponseWriter.WriteHeader(status)
}

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
