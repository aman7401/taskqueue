FROM golang:1.22-alpine AS builder

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /server ./cmd/server

FROM alpine:3.19
WORKDIR /app
COPY --from=builder /server /app/server
COPY migrations/ /app/migrations/

EXPOSE 8080
ENTRYPOINT ["/app/server"]
