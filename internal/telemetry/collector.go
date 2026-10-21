package telemetry

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"time"

	nodev1 "github.com/zhangzhe-ctrl/ani-accelerator-service/api/gen/go/accelerator/node/v1"
	"github.com/zhangzhe-ctrl/ani-node-agent/internal/gpu"
	"github.com/zhangzhe-ctrl/ani-node-agent/internal/state"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type CollectionConfig struct {
	SnapshotInterval              time.Duration
	PodResourcesSocket, CRISocket string
	Journal                       gpu.JournalConfig
}

func (client *Client) Collect(ctx context.Context, config CollectionConfig) error {
	events := make(chan gpu.Event, 256)
	go func() {
		_ = gpu.WatchEvents(ctx, config.CRISocket, func(event gpu.Event) error {
			select {
			case events <- event:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		})
	}()
	ticker := time.NewTicker(config.SnapshotInterval)
	defer ticker.Stop()
	for {
		if client.Log.Degraded() {
			now := timestamppb.Now()
			gap := &nodev1.ImmutableRecord{Payload: &nodev1.ImmutableRecord_SourceGap{SourceGap: &nodev1.SourceGap{Sources: []nodev1.Source{nodev1.Source_SOURCE_NVML, nodev1.Source_SOURCE_POD_RESOURCES, nodev1.Source_SOURCE_CRI, nodev1.Source_SOURCE_CRI_EVENTS, nodev1.Source_SOURCE_JOURNAL}, Reason: "LOCAL_EVENT_STORE_RECOVERY", DiscoveredAt: now, End: now}}}
			if err := client.Append(gap, true); err != nil {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(time.Second):
					continue
				}
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case event := <-events:
			client.persistEvent(event)
		case request := <-client.Recover:
			config.Journal.NodeUID = client.Log.NodeUID()
			result := recoverJournal(ctx, config.Journal, client.Log.NodeUID(), request)
			if err := client.Append(&nodev1.ImmutableRecord{Payload: &nodev1.ImmutableRecord_JournalExit{JournalExit: result}}, false); err != nil {
				client.persistenceFailure(err)
			}
		case <-ticker.C:
			client.capture(ctx, config, events)
		case <-client.Capture:
			client.capture(ctx, config, events)
		}
	}
}

func (client *Client) persistEvent(event gpu.Event) {
	var record *nodev1.ImmutableRecord
	if event.Gap != nil {
		record = &nodev1.ImmutableRecord{Payload: &nodev1.ImmutableRecord_SourceGap{SourceGap: event.Gap}}
	} else {
		record = &nodev1.ImmutableRecord{Payload: &nodev1.ImmutableRecord_RuntimeFacts{RuntimeFacts: &nodev1.RuntimeFacts{Items: event.Facts}}}
	}
	if err := client.Append(record, false); err != nil {
		client.persistenceFailure(err)
	}
}

func (client *Client) capture(ctx context.Context, config CollectionConfig, events <-chan gpu.Event) {
	epoch := client.Epoch.Load()
	if epoch == 0 {
		return
	}
	snapshot := gpu.Snapshot(ctx, epoch, config.PodResourcesSocket, config.CRISocket)
	queued := len(events)
	for index := 0; index < queued; index++ {
		select {
		case event := <-events:
			client.persistEvent(event)
		case <-ctx.Done():
			return
		}
	}
	if err := client.Append(&nodev1.ImmutableRecord{Payload: &nodev1.ImmutableRecord_Snapshot{Snapshot: snapshot}}, false); err != nil {
		client.persistenceFailure(err)
	}
}

func (client *Client) persistenceFailure(err error) {
	reason := client.Log.Reason()
	if reason == "" {
		reason = "LOCAL_EVENT_STORE_WRITE_FAILED"
	}
	if errors.Is(err, state.ErrFull) {
		reason = state.ErrFull.Error()
	}
	if errors.Is(err, state.ErrTooLarge) {
		reason = state.ErrTooLarge.Error()
	}
	_ = client.Log.MarkGap(reason)
}

func recoverJournal(ctx context.Context, config gpu.JournalConfig, nodeUID string, request *nodev1.RecoverExitRequest) *nodev1.JournalExitResult {
	result := &nodev1.JournalExitResult{RequestId: request.RequestId}
	if request.RequestId == "" || request.NodeUid != nodeUID || len(request.KnownContainerIds) > 16 {
		result.Issues = []string{"JOURNAL_REQUEST_INVALID"}
		return result
	}
	for _, id := range request.KnownContainerIds {
		if len(id) != 64 || strings.Trim(id, "0123456789abcdef") != "" {
			result.Issues = []string{"JOURNAL_REQUEST_INVALID"}
			return result
		}
	}
	if config.Directory == "" {
		result.Issues = []string{"JOURNAL_UNAVAILABLE"}
		return result
	}
	statuses, found, err := gpu.RecoverJournalExit(ctx, config, gpu.JournalPodRef{UID: request.PodUid, Namespace: request.PodNamespace, Name: request.PodName, ContainerName: request.ContainerName})
	if err != nil {
		result.Issues = []string{"JOURNAL_RECOVERY_UNAVAILABLE"}
		return result
	}
	result.Found = found
	for _, status := range statuses {
		var provenance struct {
			MachineID        string `json:"machine_id"`
			BootID           string `json:"boot_id"`
			SandboxID        string `json:"sandbox_id"`
			SandboxCursor    string `json:"sandbox_cursor"`
			CreateCursor     string `json:"create_cursor"`
			ExitCursor       string `json:"exit_cursor"`
			FinishedAt       int64  `json:"finished_at"`
			CreatedAt        int64  `json:"created_at"`
			SandboxMonotonic int64  `json:"sandbox_monotonic"`
			CreateMonotonic  int64  `json:"create_monotonic"`
			ExitMonotonic    int64  `json:"exit_monotonic"`
			Exe              string `json:"exe"`
		}
		if err = json.Unmarshal([]byte(status.Info["acc.runtime_journal"]), &provenance); err != nil {
			result.Found = false
			result.Issues = []string{"JOURNAL_PROVENANCE_INVALID"}
			result.Attempts = nil
			result.Provenance = nil
			return result
		}
		result.Attempts = append(result.Attempts, gpu.FactFromStatus(status.Status, time.Now().UTC()))
		result.Provenance = append(result.Provenance, &nodev1.JournalProvenance{MachineId: provenance.MachineID, BootId: provenance.BootID, Unit: "containerd.service", Comm: "containerd", Uid: "0", Exe: provenance.Exe, Transport: "stdout", SandboxId: provenance.SandboxID, Cursor: provenance.ExitCursor, CreatedAt: timestamppb.New(time.Unix(0, provenance.CreatedAt)), ExitedAt: timestamppb.New(time.Unix(0, provenance.FinishedAt)), SandboxCursor: provenance.SandboxCursor, CreateCursor: provenance.CreateCursor, SandboxMonotonic: uint64(provenance.SandboxMonotonic), CreateMonotonic: uint64(provenance.CreateMonotonic), ExitMonotonic: uint64(provenance.ExitMonotonic)})
	}
	for _, known := range request.KnownContainerIds {
		covered := false
		for _, attempt := range result.Attempts {
			if attempt.ContainerId == known {
				covered = true
			}
		}
		if !covered {
			result.Found = false
			result.Attempts = nil
			result.Provenance = nil
			result.Issues = []string{"JOURNAL_KNOWN_ATTEMPT_MISSING"}
			return result
		}
	}
	return result
}
