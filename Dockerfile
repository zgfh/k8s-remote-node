FROM golang:1.26-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o /vk-node .

FROM alpine:3.21
RUN apk add --no-cache ca-certificates
COPY --from=builder /vk-node /usr/local/bin/vk-node
ENTRYPOINT ["vk-node"]
