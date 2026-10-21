package gpu

import (
	"context"
	"errors"
	"regexp"
	"sync"

	"github.com/NVIDIA/go-nvml/pkg/nvml"
	nodev1 "github.com/zhangzhe-ctrl/ani-accelerator-service/api/gen/go/accelerator/node/v1"
)

var nvmlMu sync.Mutex
var physicalUUID = regexp.MustCompile(`^GPU-[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)

func ReadInventory(ctx context.Context) ([]*nodev1.GPUDevice, error) {
	nvmlMu.Lock()
	defer nvmlMu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if nvml.Init() != nvml.SUCCESS {
		return nil, errors.New("NVML_UNAVAILABLE")
	}
	defer nvml.Shutdown()
	count, result := nvml.DeviceGetCount()
	if result != nvml.SUCCESS {
		return nil, errors.New("NVML_UNAVAILABLE")
	}
	devices := make([]*nodev1.GPUDevice, 0, count)
	for index := 0; index < count; index++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		device, result := nvml.DeviceGetHandleByIndex(index)
		if result != nvml.SUCCESS {
			return nil, errors.New("NVML_UNAVAILABLE")
		}
		uuid, idResult := device.GetUUID()
		memory, memoryResult := device.GetMemoryInfo()
		name, nameResult := device.GetName()
		if idResult != nvml.SUCCESS || memoryResult != nvml.SUCCESS || nameResult != nvml.SUCCESS || !physicalUUID.MatchString(uuid) {
			return nil, errors.New("NVML_UNAVAILABLE")
		}
		devices = append(devices, &nodev1.GPUDevice{Uuid: uuid, Model: "NVIDIA-" + name, PhysicalMemoryBytes: memory.Total})
	}
	return devices, nil
}
