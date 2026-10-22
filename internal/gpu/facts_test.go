package gpu

import (
	"context"
	"net"
	"path/filepath"
	"strings"
	"testing"
	"time"

	nodev1 "github.com/zhangzhe-ctrl/ani-accelerator-service/api/gen/go/accelerator/node/v1"
	"google.golang.org/grpc"
	cri "k8s.io/cri-api/pkg/apis/runtime/v1"
	pr "k8s.io/kubelet/pkg/apis/podresources/v1"
)

func TestGPUEnvironmentPreservesDuplicatesAndEveryMemoryIndex(testContext *testing.T) {
	raw := `{"runtimeSpec":{"process":{"env":["SECRET=must-stay-local","NVIDIA_VISIBLE_DEVICES=GPU-a","CUDA_DEVICE_MEMORY_LIMIT_0=1024m","CUDA_DEVICE_MEMORY_LIMIT_1=2048m","NVIDIA_VISIBLE_DEVICES=GPU-b","CUDA_DEVICE_SM_LIMIT=25"]}}}`
	entries, err := GPUEnvironment(raw)
	if err != nil {
		testContext.Fatal(err)
	}
	if len(entries) != 5 || entries[0].Name != "NVIDIA_VISIBLE_DEVICES" || entries[2].Name != "CUDA_DEVICE_MEMORY_LIMIT_1" || entries[3].Value != "GPU-b" {
		testContext.Fatalf("raw whitelist order or unsupported shape lost: %+v", entries)
	}
}

type runtimeContractSource struct {
	cri.UnimplementedRuntimeServiceServer
	version *cri.VersionResponse
}

func (source runtimeContractSource) Version(context.Context, *cri.VersionRequest) (*cri.VersionResponse, error) {
	return source.version, nil
}
func (runtimeContractSource) ListContainers(context.Context, *cri.ListContainersRequest) (*cri.ListContainersResponse, error) {
	return &cri.ListContainersResponse{}, nil
}

func TestRuntimeUsesContainerdV1ContractAcrossDiagnosticVersions(testContext *testing.T) {
	for _, scenario := range []struct {
		name, runtime, api, version string
		allow                       bool
	}{{"installed_2_3_4", "containerd", "v1", "2.3.4", true}, {"unknown_version", "containerd", "v1", "", false}, {"wrong_api", "containerd", "v1alpha2", "2.3.4", false}, {"wrong_runtime", "cri-o", "v1", "2.3.4", false}} {
		testContext.Run(scenario.name, func(caseContext *testing.T) {
			path := filepath.Join(caseContext.TempDir(), "cri.sock")
			listener, err := net.Listen("unix", path)
			if err != nil {
				caseContext.Fatal(err)
			}
			defer listener.Close()
			server := grpc.NewServer()
			cri.RegisterRuntimeServiceServer(server, runtimeContractSource{version: &cri.VersionResponse{RuntimeName: scenario.runtime, RuntimeApiVersion: scenario.api, RuntimeVersion: scenario.version}})
			go server.Serve(listener)
			defer server.Stop()
			facts, err := ReadRuntime(context.Background(), path)
			if (err == nil) != scenario.allow {
				caseContext.Fatalf("CRI contract acceptance mismatch: %v", err)
			}
			if scenario.allow && len(facts) != 0 {
				caseContext.Fatal("empty actual snapshot was changed")
			}
		})
	}
}

func TestRuntimeEventsKeepUnknownAttemptsAndDeletedDoesNotProveExit(testContext *testing.T) {
	for _, kind := range []cri.ContainerEventType{cri.ContainerEventType_CONTAINER_CREATED_EVENT, cri.ContainerEventType_CONTAINER_STARTED_EVENT, cri.ContainerEventType_CONTAINER_DELETED_EVENT} {
		event := &cri.ContainerEventResponse{ContainerId: "attempt-without-status", ContainerEventType: kind, CreatedAt: time.Now().UnixNano(), PodSandboxStatus: &cri.PodSandboxStatus{Id: "sandbox", Metadata: &cri.PodSandboxMetadata{Uid: "pod-uid", Namespace: "tenant", Name: "workspace"}}}
		facts := ExtractEvent(event, time.Now())
		if len(facts) != 1 || facts[0].ContainerId != event.ContainerId || facts[0].PodUid != "pod-uid" || facts[0].State != nodev1.ContainerState_CONTAINER_STATE_UNKNOWN || facts[0].FinishedAt != nil || facts[0].EventType == nil || int32(*facts[0].EventType) != int32(kind)+1 {
			testContext.Fatalf("raw event was dropped or promoted to exit: %+v", facts)
		}
	}
}

func TestRuntimeSandboxDeletedEventDoesNotInventContainerAttempt(testContext *testing.T) {
	uid := "481c72aa-da10-4544-ba08-29f090cf6e4d"
	sandbox, container := strings.Repeat("a", 64), strings.Repeat("b", 64)
	event := &cri.ContainerEventResponse{ContainerId: sandbox, ContainerEventType: cri.ContainerEventType_CONTAINER_DELETED_EVENT, CreatedAt: time.Now().UnixNano(), PodSandboxStatus: &cri.PodSandboxStatus{Id: sandbox, Metadata: &cri.PodSandboxMetadata{Uid: uid, Namespace: "tenant", Name: "workspace"}}, ContainersStatuses: []*cri.ContainerStatus{{Id: container, Metadata: &cri.ContainerMetadata{Name: "workspace"}, Labels: map[string]string{"io.kubernetes.pod.uid": uid, "io.kubernetes.pod.namespace": "tenant", "io.kubernetes.pod.name": "workspace"}, State: cri.ContainerState_CONTAINER_EXITED, FinishedAt: time.Now().Add(-time.Second).UnixNano()}}}
	facts := ExtractEvent(event, time.Now())
	if len(facts) != 1 || facts[0].ContainerId != container || facts[0].SandboxId != sandbox || facts[0].State != nodev1.ContainerState_CONTAINER_STATE_EXITED || facts[0].FinishedAt == nil || facts[0].EventType != nil {
		testContext.Fatalf("bound sandbox lifecycle was converted to an unexplained container: %+v", facts)
	}
	event.ContainersStatuses = nil
	if facts = ExtractEvent(event, time.Now()); len(facts) != 0 {
		testContext.Fatal("sandbox-only deletion fabricated a container or exit")
	}
	event.PodSandboxStatus.Metadata.Uid = ""
	if facts = ExtractEvent(event, time.Now()); len(facts) != 1 || facts[0].ContainerId != sandbox || facts[0].State != nodev1.ContainerState_CONTAINER_STATE_UNKNOWN || facts[0].FinishedAt != nil {
		testContext.Fatal("unbound sandbox-looking identity was silently ignored")
	}
}

func TestRuntimeUnknownEventTypeCannotBecomeUnsupportedWireEnum(testContext *testing.T) {
	event := &cri.ContainerEventResponse{ContainerId: strings.Repeat("a", 64), ContainerEventType: cri.ContainerEventType(4), CreatedAt: time.Now().UnixNano()}
	facts := ExtractEvent(event, time.Now())
	if len(facts) != 1 || facts[0].State != nodev1.ContainerState_CONTAINER_STATE_UNKNOWN || facts[0].EventType != nil || !containsIssue(facts[0].Issues, "CRI_EVENT_TYPE_UNKNOWN") {
		testContext.Fatalf("unknown raw event was dropped or encoded as an unsupported enum: %+v", facts)
	}
}

func containsIssue(issues []string, wanted string) bool {
	for _, issue := range issues {
		if issue == wanted {
			return true
		}
	}
	return false
}

func TestPodResourcesKeepsOnlyReportedIdentity(testContext *testing.T) {
	response := &pr.ListPodResourcesResponse{PodResources: []*pr.PodResources{{Name: "pod", Namespace: "tenant", Containers: []*pr.ContainerResources{{Name: "workspace", Devices: []*pr.ContainerDevices{{ResourceName: "volcano.sh/vgpu-number", DeviceIds: []string{"GPU-x-0", "GPU-x-1"}}}}}}}}
	allocations := ExtractPodResources(response)
	if len(allocations) != 1 || allocations[0].Namespace != "tenant" || allocations[0].PodName != "pod" || len(allocations[0].DeviceIds) != 2 || allocations[0].DeviceIds[0] != "GPU-x-0" {
		testContext.Fatalf("PodResources allocation changed: %+v", allocations)
	}
	if allocations[0].ProtoReflect().Descriptor().Fields().ByName("pod_uid") != nil {
		testContext.Fatal("PodResources contract fabricates a UID")
	}
}
