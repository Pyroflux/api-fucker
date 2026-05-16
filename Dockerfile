FROM golang:1.23-alpine AS builder

WORKDIR /src
ARG TARGETOS=linux
ARG TARGETARCH

RUN apk add --no-cache ca-certificates

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -trimpath -ldflags="-s -w" -o /out/api-fucker ./cmd/server

FROM alpine:3.21

RUN apk add --no-cache ca-certificates && \
    addgroup -S app && \
    adduser -S -G app app && \
    mkdir -p /data && \
    chown app:app /data

WORKDIR /app

COPY --from=builder /out/api-fucker /app/api-fucker

ENV PORT=8080 \
    ADMIN_PORT=8081 \
    DATA_PATH=/data/data.db

EXPOSE 8080 8081
VOLUME ["/data"]

USER app
ENTRYPOINT ["/app/api-fucker"]
