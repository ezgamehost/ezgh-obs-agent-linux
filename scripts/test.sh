#!/bin/sh
set -eu
cd "$(dirname "$0")/.."
test -z "$(gofmt -l cmd internal)"
go vet ./...
go test -race ./...
go build ./cmd/ezgh-obs-agent-linux
rm -f ezgh-obs-agent-linux
