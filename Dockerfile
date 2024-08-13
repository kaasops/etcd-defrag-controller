## Build
FROM golang:1.18-buster AS builder

WORKDIR /app

COPY go.mod ./
COPY go.sum ./
RUN go mod download

COPY . ./

RUN go build -o /etcd-defrag-controller

## Deploy
FROM gcr.io/distroless/static:nonroot
WORKDIR /

USER 65532:65532

COPY --from=builder /etcd-defrag-controller /etcd-defrag-controller

ENTRYPOINT ["/etcd-defrag-controller"]