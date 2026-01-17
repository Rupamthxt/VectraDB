# CHANGE THIS LINE:
FROM golang:1.25-alpine AS builder

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
# ... rest of the file stays the same
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o vectradb ./cmd/server/main.go

FROM alpine:latest
WORKDIR /root/
COPY --from=builder /app/vectradb .
EXPOSE 8080
CMD ["./vectradb"]