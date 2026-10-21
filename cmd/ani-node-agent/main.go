package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	nodev1 "github.com/zhangzhe-ctrl/ani-accelerator-service/api/gen/go/accelerator/node/v1"
	"github.com/zhangzhe-ctrl/ani-node-agent/internal/gpu"
	"github.com/zhangzhe-ctrl/ani-node-agent/internal/state"
	"github.com/zhangzhe-ctrl/ani-node-agent/internal/telemetry"
)

func main() {
	if err := run(); err != nil && !errors.Is(err, context.Canceled) {
		slog.Error("node agent stopped", "reason", err.Error())
		os.Exit(1)
	}
}

func run() error {
	nodeUID := os.Getenv("ANI_NODE_UID")
	if nodeUID == "" && os.Getenv("ANI_NODE_UID_FILE") != "" {
		data, err := os.ReadFile(os.Getenv("ANI_NODE_UID_FILE"))
		if err != nil {
			return errors.New("node UID file unavailable")
		}
		nodeUID = strings.TrimSpace(string(data))
	}
	required := []string{"ANI_ACC_ENDPOINT", "ANI_ACC_SERVER_DNS", "ANI_TLS_CERT_FILE", "ANI_TLS_KEY_FILE", "ANI_TLS_CA_FILE", "ANI_SA_TOKEN_FILE", "ANI_CONNECTION_REF", "ANI_POD_NAMESPACE", "ANI_POD_NAME", "ANI_POD_UID", "ANI_NODE_NAME", "ANI_STATE_DIRECTORY", "ANI_CRI_SOCKET", "ANI_POD_RESOURCES_SOCKET", "ANI_SOURCE_FRESHNESS"}
	for _, name := range required {
		if os.Getenv(name) == "" {
			return fmt.Errorf("missing required configuration %s", name)
		}
	}
	for _, name := range []string{"ANI_STATE_DIRECTORY", "ANI_CRI_SOCKET", "ANI_POD_RESOURCES_SOCKET", "ANI_SA_TOKEN_FILE", "ANI_TLS_CERT_FILE", "ANI_TLS_KEY_FILE", "ANI_TLS_CA_FILE"} {
		if !filepath.IsAbs(os.Getenv(name)) {
			return fmt.Errorf("configuration %s must be an absolute path", name)
		}
	}
	snapshotInterval := 5 * time.Second
	if value := os.Getenv("ANI_SNAPSHOT_INTERVAL"); value != "" {
		duration, err := time.ParseDuration(value)
		if err != nil || duration <= 0 {
			return errors.New("invalid ANI_SNAPSHOT_INTERVAL")
		}
		snapshotInterval = duration
	}
	freshness, err := time.ParseDuration(os.Getenv("ANI_SOURCE_FRESHNESS"))
	if err != nil || freshness <= snapshotInterval+3*time.Second {
		return errors.New("snapshot interval must leave at least three seconds before source freshness")
	}
	instanceID, err := state.NewID()
	if err != nil {
		return err
	}
	bootFile := os.Getenv("ANI_BOOT_ID_FILE")
	if bootFile == "" {
		bootFile = "/proc/sys/kernel/random/boot_id"
	}
	bootBytes, err := os.ReadFile(bootFile)
	if err != nil {
		return err
	}
	bootID := strings.TrimSpace(string(bootBytes))
	if bootID == "" {
		return errors.New("host boot identity missing")
	}
	options := state.Options{}
	if value := os.Getenv("ANI_MAX_UNACKNOWLEDGED_BYTES"); value != "" {
		options.MaxUnacknowledgedBytes, err = strconv.ParseInt(value, 10, 64)
		if err != nil {
			return err
		}
	}
	log, err := state.Open(os.Getenv("ANI_STATE_DIRECTORY"), nodeUID, options)
	if err != nil {
		return err
	}
	defer log.Close()
	nodeUID = log.NodeUID()
	client := &telemetry.Client{Log: log, Capture: make(chan struct{}, 1), Recover: make(chan *nodev1.RecoverExitRequest, 16), Config: telemetry.Config{Endpoint: os.Getenv("ANI_ACC_ENDPOINT"), ServerDNS: os.Getenv("ANI_ACC_SERVER_DNS"), Certificate: os.Getenv("ANI_TLS_CERT_FILE"), PrivateKey: os.Getenv("ANI_TLS_KEY_FILE"), CA: os.Getenv("ANI_TLS_CA_FILE"), TokenFile: os.Getenv("ANI_SA_TOKEN_FILE"), ConnectionRef: os.Getenv("ANI_CONNECTION_REF"), PodNamespace: os.Getenv("ANI_POD_NAMESPACE"), PodName: os.Getenv("ANI_POD_NAME"), PodUID: os.Getenv("ANI_POD_UID"), NodeName: os.Getenv("ANI_NODE_NAME"), NodeUID: nodeUID, InstanceID: instanceID, BootID: bootID}}
	machineID := os.Getenv("ANI_MACHINE_ID")
	if machineID == "" && os.Getenv("ANI_MACHINE_ID_FILE") != "" {
		data, readErr := os.ReadFile(os.Getenv("ANI_MACHINE_ID_FILE"))
		if readErr != nil {
			return errors.New("machine identity unavailable")
		}
		machineID = strings.TrimSpace(string(data))
	}
	journalctl := os.Getenv("ANI_JOURNALCTL_PATH")
	if journalctl == "" {
		journalctl = "/usr/bin/journalctl"
	}
	if !filepath.IsAbs(journalctl) || filepath.Clean(journalctl) != journalctl || filepath.Base(journalctl) != "journalctl" {
		return errors.New("invalid journalctl deployment path")
	}
	collection := telemetry.CollectionConfig{SnapshotInterval: snapshotInterval, CRISocket: os.Getenv("ANI_CRI_SOCKET"), PodResourcesSocket: os.Getenv("ANI_POD_RESOURCES_SOCKET"), Journal: gpu.JournalConfig{Directory: os.Getenv("ANI_JOURNAL_DIRECTORY"), MachineID: machineID, NodeUID: client.Config.NodeUID, JournalctlPath: journalctl}}
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer cancel()
	results := make(chan error, 2)
	go func() { results <- client.Collect(ctx, collection) }()
	go func() { results <- client.Run(ctx) }()
	first := <-results
	cancel()
	<-results
	return first
}
