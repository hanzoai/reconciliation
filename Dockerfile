# Build stage
FROM golang:1.24-alpine AS builder

WORKDIR /app

# Install build dependencies
RUN apk add --no-cache git

# Copy go mod files
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

# Build the binary
ARG VERSION=dev
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-X github.com/formancehq/reconciliation/cmd.Version=${VERSION}" -o /reconciliation .

# Final stage
FROM alpine:3.19

RUN apk add --no-cache ca-certificates tzdata

COPY --from=builder /reconciliation /bin/reconciliation

ENTRYPOINT ["/bin/reconciliation"]
CMD ["serve"]
