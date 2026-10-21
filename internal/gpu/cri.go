package gpu

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"time"

	nodev1 "github.com/zhangzhe-ctrl/ani-accelerator-service/api/gen/go/accelerator/node/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
	cri "k8s.io/cri-api/pkg/apis/runtime/v1"
)

func GPUEnvironment(raw string) ([]*nodev1.GPUEnvironmentEntry, error) {
	if len(raw) == 0 || len(raw) > 4<<20 {
		return nil, errors.New("RUNTIME_INFO_UNAVAILABLE")
	}
	var info struct {
		RuntimeSpec struct {
			Process struct {
				Env []string `json:"env"`
			} `json:"process"`
		} `json:"runtimeSpec"`
	}
	if err := json.Unmarshal([]byte(raw), &info); err != nil {
		return nil, errors.New("RUNTIME_INFO_INVALID")
	}
	var result []*nodev1.GPUEnvironmentEntry
	var extractionError error
	for _, entry := range info.RuntimeSpec.Process.Env {
		name, value, found := strings.Cut(entry, "=")
		if name != "NVIDIA_VISIBLE_DEVICES" && name != "CUDA_DEVICE_SM_LIMIT" && !strings.HasPrefix(name, "CUDA_DEVICE_MEMORY_LIMIT_") {
			continue
		}
		result = append(result, &nodev1.GPUEnvironmentEntry{Name: name, Value: value})
		if !found {
			extractionError = errors.New("RUNTIME_ENV_ENTRY_INVALID")
		}
	}
	return result, extractionError
}

func ReadRuntime(ctx context.Context, socket string) ([]*nodev1.RuntimeFact, error) {
	connection, err := unixClient(socket)
	if err != nil {
		return nil, err
	}
	defer connection.Close()
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	client := cri.NewRuntimeServiceClient(connection)
	version, err := client.Version(ctx, &cri.VersionRequest{Version: "0.1.0"})
	if err != nil {
		return nil, err
	}
	if version.RuntimeName != "containerd" || version.RuntimeApiVersion != "v1" || version.RuntimeVersion == "" {
		return nil, errors.New("CRI_VERSION_UNSUPPORTED")
	}
	list, err := client.ListContainers(ctx, &cri.ListContainersRequest{})
	if err != nil {
		return nil, err
	}
	var result []*nodev1.RuntimeFact
	complete := true
	for _, container := range list.Containers {
		observedAt := time.Now().UTC()
		response, statusErr := client.ContainerStatus(ctx, &cri.ContainerStatusRequest{ContainerId: container.Id, Verbose: true})
		if statusErr != nil || response.Status == nil {
			complete = false
			fact := &nodev1.RuntimeFact{ContainerId: container.Id, SandboxId: container.PodSandboxId, PodUid: container.Labels["io.kubernetes.pod.uid"], PodNamespace: container.Labels["io.kubernetes.pod.namespace"], PodName: container.Labels["io.kubernetes.pod.name"], State: nodev1.ContainerState_CONTAINER_STATE_UNKNOWN, ObservedAt: timestamppb.New(observedAt), Issues: []string{"CRI_STATUS_UNAVAILABLE"}}
			if container.CreatedAt > 0 {
				fact.CreatedAt = timestamppb.New(time.Unix(0, container.CreatedAt))
			}
			if container.Metadata != nil {
				fact.ContainerName = container.Metadata.Name
				fact.Attempt = container.Metadata.Attempt
			}
			result = append(result, fact)
			continue
		}
		fact := FactFromStatus(response.Status, observedAt)
		fact.SandboxId = container.PodSandboxId
		fact.GpuEnvironment, err = GPUEnvironment(response.Info["info"])
		if err != nil {
			fact.Issues = append(fact.Issues, err.Error())
		}
		if fact.ContainerId != container.Id || container.Labels["io.kubernetes.pod.uid"] != fact.PodUid {
			fact.State = nodev1.ContainerState_CONTAINER_STATE_UNKNOWN
			fact.Issues = append(fact.Issues, "CRI_IDENTITY_CONFLICT")
			complete = false
		}
		result = append(result, fact)
	}
	if !complete {
		return result, errors.New("CRI_SNAPSHOT_INCOMPLETE")
	}
	return result, nil
}

func FactFromStatus(status *cri.ContainerStatus, observed time.Time) *nodev1.RuntimeFact {
	fact := &nodev1.RuntimeFact{ContainerId: status.Id, PodUid: status.Labels["io.kubernetes.pod.uid"], PodNamespace: status.Labels["io.kubernetes.pod.namespace"], PodName: status.Labels["io.kubernetes.pod.name"], State: nodev1.ContainerState_CONTAINER_STATE_UNKNOWN, ObservedAt: timestamppb.New(observed)}
	if status.Metadata != nil {
		fact.ContainerName = status.Metadata.Name
		fact.Attempt = status.Metadata.Attempt
	}
	switch status.State {
	case cri.ContainerState_CONTAINER_CREATED:
		fact.State = nodev1.ContainerState_CONTAINER_STATE_CREATED
	case cri.ContainerState_CONTAINER_RUNNING:
		fact.State = nodev1.ContainerState_CONTAINER_STATE_RUNNING
	case cri.ContainerState_CONTAINER_EXITED:
		if status.FinishedAt > 0 {
			fact.State = nodev1.ContainerState_CONTAINER_STATE_EXITED
		} else {
			fact.Issues = append(fact.Issues, "CRI_EXIT_TIME_MISSING")
		}
	}
	if status.CreatedAt > 0 {
		fact.CreatedAt = timestamppb.New(time.Unix(0, status.CreatedAt))
	}
	if status.StartedAt > 0 {
		fact.StartedAt = timestamppb.New(time.Unix(0, status.StartedAt))
	}
	if status.FinishedAt > 0 {
		fact.FinishedAt = timestamppb.New(time.Unix(0, status.FinishedAt))
	}
	if fact.PodUid == "" || fact.PodNamespace == "" || fact.PodName == "" || fact.ContainerName == "" {
		fact.State = nodev1.ContainerState_CONTAINER_STATE_UNKNOWN
		fact.Issues = append(fact.Issues, "CRI_IDENTITY_INCOMPLETE")
	}
	return fact
}
