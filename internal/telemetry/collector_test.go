package telemetry

import (
	"context"
	"net"
	"path/filepath"
	"testing"
	"time"

	nodev1 "github.com/zhangzhe-ctrl/ani-accelerator-service/api/gen/go/accelerator/node/v1"
	"github.com/zhangzhe-ctrl/ani-node-agent/internal/state"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	cri "k8s.io/cri-api/pkg/apis/runtime/v1"
)

type bootstrapEvents struct {
	cri.UnimplementedRuntimeServiceServer
	started chan struct{}
}

func (source *bootstrapEvents) GetContainerEvents(_ *cri.GetEventsRequest, stream cri.RuntimeService_GetContainerEventsServer) error {
	close(source.started)
	<-stream.Context().Done()
	return stream.Context().Err()
}

func TestCollectionWaitsForDurableNodeBinding(testContext *testing.T) {
	directory := testContext.TempDir()
	socket := filepath.Join(directory, "cri.sock")
	listener, err := net.Listen("unix", socket)
	if err != nil {
		testContext.Fatal(err)
	}
	server := grpc.NewServer()
	source := &bootstrapEvents{started: make(chan struct{})}
	cri.RegisterRuntimeServiceServer(server, source)
	go server.Serve(listener)
	defer server.Stop()
	log, err := state.Open(filepath.Join(directory, "state"), "", state.Options{})
	if err != nil {
		testContext.Fatal(err)
	}
	defer log.Close()
	client := &Client{Log: log}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- client.Collect(ctx, CollectionConfig{SnapshotInterval: time.Hour, CRISocket: socket}) }()
	select {
	case <-source.started:
		testContext.Fatal("event subscription started before the actual Node UID was bound")
	case <-time.After(150 * time.Millisecond):
	}
	if log.Degraded() {
		testContext.Fatal("unbound bootstrap manufactured a durable source gap")
	}
	if err = log.Bind("actual-node"); err != nil {
		testContext.Fatal(err)
	}
	client.Epoch.Store(1)
	select {
	case <-source.started:
	case <-time.After(2 * time.Second):
		testContext.Fatal("bound collector did not start the real CRI subscription")
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		record, ready, nextErr := log.Next()
		if nextErr != nil {
			testContext.Fatal(nextErr)
		}
		if ready {
			var immutable nodev1.ImmutableRecord
			if proto.Unmarshal(record.Bytes, &immutable) != nil || immutable.GetSourceGap() == nil {
				testContext.Fatal("initial CRI subscription gap was not durably retained")
			}
			gap := immutable.GetSourceGap()
			if record.Sequence != 1 || len(gap.Sources) != 1 || gap.Sources[0] != nodev1.Source_SOURCE_CRI_EVENTS || gap.Reason != "CRI_EVENT_SUBSCRIPTION_GAP" || log.Degraded() {
				testContext.Fatal("bootstrap gap incorrectly poisoned current snapshot sources")
			}
			cancel()
			<-done
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	testContext.Fatal("subscription gap was not persisted after binding")
}
