# Stage 1:
FROM golang:1.25.0 AS builder

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -ldflags="-s -w" -o /app/broker ./cmd/broker
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -ldflags="-s -w" -o /app/cli ./cmd/cli

# Stage 2:
FROM alpine:3.18

WORKDIR /root/
COPY --from=builder /app/broker .
COPY --from=builder /app/cli .

RUN apk add --no-cache bash curl

RUN chmod +x broker cli

COPY entrypoint.sh .
RUN chmod +x entrypoint.sh

ENTRYPOINT ["/root/entrypoint.sh"]
CMD []