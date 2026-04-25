.PHONY: all build run dev tidy test docker-up docker-down cli

# Binaries
SERVER_BIN := bin/server
CLI_BIN    := bin/tq

all: build

build:
	@mkdir -p bin
	go build -o $(SERVER_BIN) ./cmd/server
	go build -o $(CLI_BIN)    ./cmd/cli
	@echo "Built: $(SERVER_BIN)  $(CLI_BIN)"

tidy:
	go mod tidy

test:
	go test ./...

# Run the server locally (requires Postgres running)
run: build
	DATABASE_URL="postgres://postgres:postgres@localhost:5432/taskqueue?sslmode=disable" \
	QUEUES="default,emails,notifications" \
	./$(SERVER_BIN)

# Full stack via Docker Compose
docker-up:
	docker compose up --build -d

docker-down:
	docker compose down -v

# Quick demo: start postgres only, run server locally
dev:
	docker compose up -d postgres
	@echo "Waiting for postgres..."
	@sleep 3
	$(MAKE) run

# CLI shortcuts
submit:
	./$(CLI_BIN) submit --queue=$(QUEUE) --payload='$(PAYLOAD)'

stats:
	./$(CLI_BIN) stats --queue=$(or $(QUEUE),default)

list:
	./$(CLI_BIN) list --queue=$(QUEUE) --limit=20

dlq:
	./$(CLI_BIN) dlq list --queue=$(QUEUE)
