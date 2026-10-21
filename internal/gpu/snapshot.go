package gpu

import (
	"context"
	"sync"
	"time"

	nodev1 "github.com/zhangzhe-ctrl/ani-accelerator-service/api/gen/go/accelerator/node/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func Snapshot(ctx context.Context, epoch uint64, podResourcesSocket, criSocket string) *nodev1.NodeSnapshot {
	snapshot := &nodev1.NodeSnapshot{CaptureEpoch: epoch, CaptureStart: timestamppb.Now()}
	health := func(source nodev1.Source, err error) *nodev1.SourceHealth {
		result := &nodev1.SourceHealth{Source: source, ObservedAt: timestamppb.Now(), Complete: err == nil}
		if err != nil {
			result.Issues = []string{source.String() + "_UNAVAILABLE"}
		}
		return result
	}
	var inventoryHealth, resourcesHealth, runtimeHealth *nodev1.SourceHealth
	var wait sync.WaitGroup
	wait.Add(3)
	go func() {
		defer wait.Done()
		devices, err := ReadInventory(ctx)
		snapshot.Devices = devices
		inventoryHealth = health(nodev1.Source_SOURCE_NVML, err)
	}()
	go func() {
		defer wait.Done()
		allocations, err := ReadPodResources(ctx, podResourcesSocket)
		snapshot.Allocations = allocations
		resourcesHealth = health(nodev1.Source_SOURCE_POD_RESOURCES, err)
	}()
	go func() {
		defer wait.Done()
		containers, err := ReadRuntime(ctx, criSocket)
		snapshot.Containers = containers
		runtimeHealth = health(nodev1.Source_SOURCE_CRI, err)
	}()
	wait.Wait()
	snapshot.Sources = []*nodev1.SourceHealth{inventoryHealth, resourcesHealth, runtimeHealth}
	snapshot.CaptureEnd = timestamppb.New(time.Now().UTC())
	return snapshot
}
