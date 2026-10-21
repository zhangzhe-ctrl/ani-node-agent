.PHONY: build test verify

build:
	GOWORK=off go build ./cmd/ani-node-agent

test:
	GOWORK=off go test -count=1 ./internal/state ./internal/gpu ./internal/telemetry

verify: test
	GOWORK=off go vet ./...
