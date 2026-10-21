package gpu

import (
	"context"
	"net"
	"time"

	nodev1 "github.com/zhangzhe-ctrl/ani-accelerator-service/api/gen/go/accelerator/node/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	pr "k8s.io/kubelet/pkg/apis/podresources/v1"
)

func unixClient(path string) (*grpc.ClientConn, error) {
	return grpc.NewClient("passthrough:///local-node-source", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
		return (&net.Dialer{Timeout: 3 * time.Second}).DialContext(ctx, "unix", path)
	}), grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(16<<20)))
}

func ReadPodResources(ctx context.Context, socket string) ([]*nodev1.PodResourceAllocation, error) {
	connection, err := unixClient(socket)
	if err != nil {
		return nil, err
	}
	defer connection.Close()
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	response, err := pr.NewPodResourcesListerClient(connection).List(ctx, &pr.ListPodResourcesRequest{})
	if err != nil {
		return nil, err
	}
	return ExtractPodResources(response), nil
}

func ExtractPodResources(response *pr.ListPodResourcesResponse) []*nodev1.PodResourceAllocation {
	var result []*nodev1.PodResourceAllocation
	for _, pod := range response.GetPodResources() {
		for _, container := range pod.GetContainers() {
			for _, device := range container.GetDevices() {
				result = append(result, &nodev1.PodResourceAllocation{Namespace: pod.Namespace, PodName: pod.Name, ContainerName: container.Name, ResourceName: device.ResourceName, DeviceIds: append([]string(nil), device.DeviceIds...)})
			}
		}
	}
	return result
}
