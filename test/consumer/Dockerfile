FROM golang:1.25-alpine AS builder  
  
WORKDIR /app  
COPY go.mod go.sum ./  
RUN go mod download  
  
COPY . .  
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o consumer main.go  
  
FROM alpine:latest  
RUN apk --no-cache add ca-certificates  
WORKDIR /root/  
  
COPY --from=builder /app/consumer .  
ENTRYPOINT ["./consumer"]