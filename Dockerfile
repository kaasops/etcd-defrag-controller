## Build
FROM golang:1.18-buster AS build

WORKDIR /app

COPY go.mod ./
COPY go.sum ./
RUN go mod download

COPY . ./

RUN go build -o /etcd-defrag-controller

## Deploy
FROM debian:buster-slim

WORKDIR /

COPY --from=build /etcd-defrag-controller /etcd-defrag-controller

ENTRYPOINT ["/etcd-defrag-controller"]