package gpu

import (
	"context"
	"time"

	nodev1 "github.com/zhangzhe-ctrl/ani-accelerator-service/api/gen/go/accelerator/node/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
	cri "k8s.io/cri-api/pkg/apis/runtime/v1"
)

type Event struct {
	Facts []*nodev1.RuntimeFact
	Gap   *nodev1.SourceGap
}

func ExtractEvent(event *cri.ContainerEventResponse, observed time.Time) []*nodev1.RuntimeFact {
	if event == nil {
		return nil
	}
	var result []*nodev1.RuntimeFact
	seen := false
	for _, status := range event.ContainersStatuses {
		if status == nil {
			continue
		}
		fact := FactFromStatus(status, observed)
		if event.CreatedAt > 0 {
			fact.ObservedAt = timestamppb.New(time.Unix(0, event.CreatedAt))
		}
		if event.PodSandboxStatus != nil {
			fact.SandboxId = event.PodSandboxStatus.Id
			if pod := event.PodSandboxStatus.Metadata; pod != nil && (fact.PodUid != pod.Uid || fact.PodNamespace != pod.Namespace || fact.PodName != pod.Name) {
				fact.State = nodev1.ContainerState_CONTAINER_STATE_UNKNOWN
				fact.Issues = append(fact.Issues, "CRI_EVENT_IDENTITY_CONFLICT")
			}
		}
		if status.Id == event.ContainerId {
			seen = true
			kind := nodev1.RuntimeEventType(event.ContainerEventType + 1)
			fact.EventType = &kind
		}
		result = append(result, fact)
	}
	if !seen {
		kind := nodev1.RuntimeEventType(event.ContainerEventType + 1)
		fact := &nodev1.RuntimeFact{ContainerId: event.ContainerId, State: nodev1.ContainerState_CONTAINER_STATE_UNKNOWN, ObservedAt: timestamppb.New(observed), EventType: &kind, Issues: []string{"CRI_EVENT_STATUS_UNAVAILABLE"}}
		if event.CreatedAt > 0 {
			fact.ObservedAt = timestamppb.New(time.Unix(0, event.CreatedAt))
		}
		if event.PodSandboxStatus != nil {
			fact.SandboxId = event.PodSandboxStatus.Id
			if metadata := event.PodSandboxStatus.Metadata; metadata != nil {
				fact.PodUid = metadata.Uid
				fact.PodNamespace = metadata.Namespace
				fact.PodName = metadata.Name
			}
		}
		result = append(result, fact)
	}
	return result
}

func WatchEvents(ctx context.Context, socket string, emit func(Event) error) error {
	var gapStart time.Time
	for ctx.Err() == nil {
		connection, err := unixClient(socket)
		if err == nil {
			stream, streamErr := cri.NewRuntimeServiceClient(connection).GetContainerEvents(ctx, &cri.GetEventsRequest{})
			if streamErr == nil {
				now := time.Now().UTC()
				gap := &nodev1.SourceGap{Sources: []nodev1.Source{nodev1.Source_SOURCE_CRI_EVENTS}, Reason: "CRI_EVENT_SUBSCRIPTION_GAP", End: timestamppb.New(now), DiscoveredAt: timestamppb.New(now)}
				if !gapStart.IsZero() {
					gap.Start = timestamppb.New(gapStart)
				}
				if err = emit(Event{Gap: gap}); err != nil {
					connection.Close()
					return err
				}
				for {
					event, receiveErr := stream.Recv()
					if receiveErr != nil {
						break
					}
					facts := ExtractEvent(event, time.Now().UTC())
					valid := facts[:0]
					for _, fact := range facts {
						if fact.ContainerId == "" {
							now := timestamppb.Now()
							if err = emit(Event{Gap: &nodev1.SourceGap{Sources: []nodev1.Source{nodev1.Source_SOURCE_CRI_EVENTS}, Reason: "CRI_EVENT_CONTAINER_ID_UNAVAILABLE", Start: now, End: now, DiscoveredAt: now}}); err != nil {
								connection.Close()
								return err
							}
							continue
						}
						valid = append(valid, fact)
					}
					facts = valid
					for offset := 0; offset < len(facts); offset += 256 {
						end := min(offset+256, len(facts))
						if err = emit(Event{Facts: facts[offset:end]}); err != nil {
							connection.Close()
							return err
						}
					}
				}
				gapStart = time.Now().UTC()
			}
			connection.Close()
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second):
		}
	}
	return ctx.Err()
}
