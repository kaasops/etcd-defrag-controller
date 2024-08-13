## Build
FROM golang:1.18-buster AS builder

WORKDIR /app

COPY go.mod ./
COPY go.sum ./
RUN go mod download

COPY . ./

RUN CGO_ENABLED=0 go build -o /etcd-defrag-controller

## Deploy
FROM debian:buster-slim
WORKDIR /

COPY --from=builder /etcd-defrag-controller .

ENTRYPOINT ["/etcd-defrag-controller"]