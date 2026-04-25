.PHONY: all build run dev tidy test docker-up docker-down

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

run: build
	DATABASE_URL="postgres://postgres:postgres@localhost:5432/taskqueue?sslmode=disable" \
	QUEUES="default,emails,notifications" \
	./$(SERVER_BIN)

docker-up:
	docker compose up --build -d

docker-down:
	docker compose down -v

dev:
	docker compose up -d postgres
	@echo "Waiting for postgres..."
	@sleep 3
	$(MAKE) run
