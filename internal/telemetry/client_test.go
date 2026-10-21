package telemetry

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	nodev1 "github.com/zhangzhe-ctrl/ani-accelerator-service/api/gen/go/accelerator/node/v1"
	"github.com/zhangzhe-ctrl/ani-node-agent/internal/state"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type wireCenter struct {
	nodev1.UnimplementedNodeTelemetryServiceServer
	tokenFile string
	done      chan error
}

func TestOversizeReportEnvelopeNeverAppendsOrSyncs(testContext *testing.T) {
	syncs := 0
	log, err := state.Open(testContext.TempDir(), "node", state.Options{MaxRecordBytes: 8 << 20, Sync: func(file *os.File) error { syncs++; return file.Sync() }})
	if err != nil {
		testContext.Fatal(err)
	}
	defer log.Close()
	identity := "11111111-1111-4111-8111-111111111111"
	client := &Client{Log: log, Config: Config{PodUID: identity, InstanceID: identity, BootID: identity}}
	record := &nodev1.ImmutableRecord{ProtocolVersion: 1, LogId: log.LogID(), Sequence: 1, OriginPodUid: identity, OriginInstanceId: identity, OriginBootId: identity, PayloadDigest: strings.Repeat("a", 64), Payload: &nodev1.ImmutableRecord_SourceGap{SourceGap: &nodev1.SourceGap{Sources: []nodev1.Source{nodev1.Source_SOURCE_CRI}, Reason: "gap", DiscoveredAt: timestamppb.Now()}}}
	record.GetSourceGap().LastCursor = strings.Repeat("x", (8<<20)-proto.Size(record)-16)
	message := &nodev1.AgentMessage{Message: &nodev1.AgentMessage_Report{Report: &nodev1.AgentReport{Epoch: ^uint64(0), Record: record}}}
	if proto.Size(record) > 8<<20 || proto.Size(message) <= 8<<20 {
		testContext.Fatal("test does not isolate Report envelope overhead")
	}
	before := syncs
	if err = client.Append(record, false); !errors.Is(err, state.ErrTooLarge) {
		testContext.Fatalf("oversize envelope accepted: %v", err)
	}
	if syncs != before {
		testContext.Fatal("oversize message reached durable append")
	}
	if _, ready, nextErr := log.Next(); nextErr != nil || ready {
		testContext.Fatal("oversize message poisoned the ordered outbox")
	}
}

func (center *wireCenter) Connect(stream grpc.BidiStreamingServer[nodev1.AgentMessage, nodev1.CenterMessage]) error {
	helloMessage, err := stream.Recv()
	if err != nil {
		return err
	}
	hello := helloMessage.GetHello()
	if hello == nil || hello.ProtocolVersion != 1 || hello.ServiceAccountToken != "token-one" {
		center.done <- errors.New("invalid initial Hello")
		return errors.New("fixture identity rejected")
	}
	if err = stream.Send(&nodev1.CenterMessage{Message: &nodev1.CenterMessage_Welcome{Welcome: &nodev1.Welcome{NodeUid: "actual-node", NodeName: "node", Epoch: 7, LeaseExpiresAt: timestamppb.New(time.Now().Add(20 * time.Second)), ActiveLogId: hello.LogId}}}); err != nil {
		return err
	}
	if err = os.WriteFile(center.tokenFile, []byte("token-two"), 0600); err != nil {
		center.done <- err
		return err
	}
	acknowledged := false
	renewed := false
	for {
		message, receiveErr := stream.Recv()
		if receiveErr != nil {
			return receiveErr
		}
		switch body := message.Message.(type) {
		case *nodev1.AgentMessage_Report:
			record := body.Report.Record
			if body.Report.Epoch != 7 || record.Sequence != 1 || record.OriginPodUid != "pod" || record.OriginInstanceId != "instance" || record.GetRuntimeFacts() == nil {
				center.done <- errors.New("invalid record identity")
				return errors.New("fixture record rejected")
			}
			if err = stream.Send(&nodev1.CenterMessage{Message: &nodev1.CenterMessage_Ack{Ack: &nodev1.Ack{LogId: record.LogId, Sequence: record.Sequence, PayloadDigest: record.PayloadDigest}}}); err != nil {
				return err
			}
			acknowledged = true
		case *nodev1.AgentMessage_Renew:
			if body.Renew.Epoch != 7 || body.Renew.ServiceAccountToken != "token-two" {
				center.done <- errors.New("renewal did not reread projected token")
				return errors.New("fixture renewal rejected")
			}
			renewed = true
			if err = stream.Send(&nodev1.CenterMessage{Message: &nodev1.CenterMessage_Welcome{Welcome: &nodev1.Welcome{NodeUid: "actual-node", NodeName: "node", Epoch: 7, LeaseExpiresAt: timestamppb.New(time.Now().Add(20 * time.Second)), ActiveLogId: hello.LogId}}}); err != nil {
				return err
			}
		}
		if acknowledged && renewed {
			center.done <- nil
			<-stream.Context().Done()
			return stream.Context().Err()
		}
	}
}

func TestTLSWireBootstrapDurableAckAndTokenRotation(testContext *testing.T) {
	directory := testContext.TempDir()
	config, serverTLS := wireCertificates(testContext, directory)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		testContext.Fatal(err)
	}
	defer listener.Close()
	center := &wireCenter{tokenFile: config.TokenFile, done: make(chan error, 1)}
	server := grpc.NewServer(grpc.Creds(credentials.NewTLS(serverTLS)))
	nodev1.RegisterNodeTelemetryServiceServer(server, center)
	go server.Serve(listener)
	defer server.Stop()
	config.Endpoint = listener.Addr().String()
	config.ConnectionRef = "connection"
	config.PodNamespace = "ani-node-system"
	config.PodName = "agent"
	config.PodUID = "pod"
	config.NodeName = "node"
	config.InstanceID = "instance"
	config.BootID = "boot"
	config.RenewInterval = 50 * time.Millisecond
	config.HeartbeatInterval = 50 * time.Millisecond
	log, err := state.Open(filepath.Join(directory, "state"), "", state.Options{})
	if err != nil {
		testContext.Fatal(err)
	}
	defer log.Close()
	client := &Client{Config: config, Log: log, Capture: make(chan struct{}, 1), Recover: make(chan *nodev1.RecoverExitRequest, 1)}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	result := make(chan error, 1)
	go func() { result <- client.Run(ctx) }()
	select {
	case <-client.Capture:
	case err = <-result:
		testContext.Fatal(err)
	case <-ctx.Done():
		testContext.Fatal("no authenticated capture trigger")
	}
	if log.NodeUID() != "actual-node" {
		testContext.Fatal("actual Node UID was not durably bound")
	}
	if err = client.Append(&nodev1.ImmutableRecord{Payload: &nodev1.ImmutableRecord_RuntimeFacts{RuntimeFacts: &nodev1.RuntimeFacts{Items: []*nodev1.RuntimeFact{{ContainerId: "container", State: nodev1.ContainerState_CONTAINER_STATE_UNKNOWN, ObservedAt: timestamppb.Now()}}}}}, false); err != nil {
		testContext.Fatal(err)
	}
	select {
	case err = <-center.done:
		if err != nil {
			testContext.Fatal(err)
		}
	case <-ctx.Done():
		testContext.Fatal("wire exchange did not complete")
	}
	deadline := time.Now().Add(time.Second)
	for log.Watermark().Sequence != 1 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if log.Watermark().Sequence != 1 {
		testContext.Fatal("wire ACK not persisted")
	}
	select {
	case <-client.Capture:
		testContext.Fatal("ordinary renewal incorrectly created capture epoch")
	default:
	}
	cancel()
	<-result
}

func wireCertificates(testContext *testing.T, directory string) (Config, *tls.Config) {
	testContext.Helper()
	rootKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		testContext.Fatal(err)
	}
	rootTemplate := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "wire test CA"}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign}
	rootDER, err := x509.CreateCertificate(rand.Reader, rootTemplate, rootTemplate, &rootKey.PublicKey, rootKey)
	if err != nil {
		testContext.Fatal(err)
	}
	root, err := x509.ParseCertificate(rootDER)
	if err != nil {
		testContext.Fatal(err)
	}
	write := func(name string, data []byte) string {
		path := filepath.Join(directory, name)
		if err := os.WriteFile(path, data, 0600); err != nil {
			testContext.Fatal(err)
		}
		return path
	}
	rootPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: rootDER})
	makeLeaf := func(dns string, serial int64, usage x509.ExtKeyUsage) (tls.Certificate, string, string) {
		key, keyErr := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		if keyErr != nil {
			testContext.Fatal(keyErr)
		}
		template := &x509.Certificate{SerialNumber: big.NewInt(serial), DNSNames: []string{dns}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{usage}}
		der, certErr := x509.CreateCertificate(rand.Reader, template, root, &key.PublicKey, rootKey)
		if certErr != nil {
			testContext.Fatal(certErr)
		}
		keyDER, keyErr := x509.MarshalECPrivateKey(key)
		if keyErr != nil {
			testContext.Fatal(keyErr)
		}
		certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
		keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
		certificate, certErr := tls.X509KeyPair(certPEM, keyPEM)
		if certErr != nil {
			testContext.Fatal(certErr)
		}
		return certificate, write(dns+".crt", certPEM), write(dns+".key", keyPEM)
	}
	_, certPath, keyPath := makeLeaf("ani-node-agent", 2, x509.ExtKeyUsageClientAuth)
	serverCert, _, _ := makeLeaf("acc-center", 3, x509.ExtKeyUsageServerAuth)
	roots := x509.NewCertPool()
	roots.AppendCertsFromPEM(rootPEM)
	config := Config{Certificate: certPath, PrivateKey: keyPath, CA: write("ca.crt", rootPEM), TokenFile: write("token", []byte("token-one")), ServerDNS: "acc-center"}
	return config, &tls.Config{MinVersion: tls.VersionTLS13, MaxVersion: tls.VersionTLS13, Certificates: []tls.Certificate{serverCert}, ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: roots}
}
