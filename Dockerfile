FROM golang:1.26.10-bookworm AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN GOWORK=off go mod download
COPY cmd ./cmd
COPY internal ./internal
RUN GOWORK=off go build -trimpath -o /out/ani-node-agent ./cmd/ani-node-agent

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates systemd && rm -rf /var/lib/apt/lists/*
COPY --from=build /out/ani-node-agent /usr/local/bin/ani-node-agent
ENTRYPOINT ["/usr/local/bin/ani-node-agent"]
