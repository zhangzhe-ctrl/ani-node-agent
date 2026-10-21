package telemetry

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"errors"
	"log/slog"
	"os"
	"strings"
	"sync/atomic"
	"time"

	nodev1 "github.com/zhangzhe-ctrl/ani-accelerator-service/api/gen/go/accelerator/node/v1"
	"github.com/zhangzhe-ctrl/ani-node-agent/internal/state"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type Config struct {
	Endpoint, ServerDNS, Certificate, PrivateKey, CA, TokenFile                         string
	ConnectionRef, PodNamespace, PodName, PodUID, NodeName, NodeUID, InstanceID, BootID string
	HeartbeatInterval, RenewInterval                                                    time.Duration
}

type Client struct {
	Config  Config
	Log     *state.Log
	Epoch   atomic.Uint64
	Capture chan struct{}
	Recover chan *nodev1.RecoverExitRequest
}

func TLSConfig(config Config) (*tls.Config, error) {
	certificate, err := tls.LoadX509KeyPair(config.Certificate, config.PrivateKey)
	if err != nil {
		return nil, err
	}
	leaf, err := x509.ParseCertificate(certificate.Certificate[0])
	if err != nil {
		return nil, err
	}
	if len(leaf.DNSNames) != 1 || leaf.DNSNames[0] != "ani-node-agent" || len(leaf.URIs) != 0 || len(leaf.IPAddresses) != 0 {
		return nil, errors.New("AGENT_CERTIFICATE_SAN_INVALID")
	}
	rootBytes, err := os.ReadFile(config.CA)
	if err != nil {
		return nil, err
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(rootBytes) {
		return nil, errors.New("SERVER_CA_INVALID")
	}
	if config.ServerDNS == "" || strings.Contains(config.ServerDNS, "*") {
		return nil, errors.New("SERVER_DNS_REQUIRED")
	}
	return &tls.Config{MinVersion: tls.VersionTLS13, MaxVersion: tls.VersionTLS13, Certificates: []tls.Certificate{certificate}, RootCAs: roots, ServerName: config.ServerDNS, VerifyConnection: func(connection tls.ConnectionState) error {
		if len(connection.PeerCertificates) == 0 {
			return errors.New("SERVER_CERTIFICATE_MISSING")
		}
		server := connection.PeerCertificates[0]
		if len(server.DNSNames) != 1 || server.DNSNames[0] != config.ServerDNS || len(server.URIs) != 0 || len(server.IPAddresses) != 0 {
			return errors.New("SERVER_CERTIFICATE_SAN_INVALID")
		}
		return nil
	}}, nil
}

func (client *Client) Append(record *nodev1.ImmutableRecord, recovery bool) error {
	build := func(logID string, sequence uint64) ([]byte, string, error) {
		immutable := proto.Clone(record).(*nodev1.ImmutableRecord)
		immutable.ProtocolVersion = 1
		immutable.LogId = logID
		immutable.Sequence = sequence
		immutable.OriginPodUid = client.Config.PodUID
		immutable.OriginInstanceId = client.Config.InstanceID
		immutable.OriginBootId = client.Config.BootID
		immutable.PayloadDigest = ""
		body, err := (proto.MarshalOptions{Deterministic: true}).Marshal(immutable)
		if err != nil {
			return nil, "", err
		}
		digest := sha256.Sum256(body)
		immutable.PayloadDigest = hex.EncodeToString(digest[:])
		body, err = (proto.MarshalOptions{Deterministic: true}).Marshal(immutable)
		if err == nil && proto.Size(&nodev1.AgentMessage{Message: &nodev1.AgentMessage_Report{Report: &nodev1.AgentReport{Epoch: ^uint64(0), Record: immutable}}}) > 8<<20 {
			return nil, "", state.ErrTooLarge
		}
		return body, immutable.PayloadDigest, err
	}
	if recovery {
		_, err := client.Log.RecoverWithGap(build)
		return err
	}
	_, err := client.Log.Enqueue(build)
	return err
}

func (client *Client) Run(ctx context.Context) error {
	transport, err := TLSConfig(client.Config)
	if err != nil {
		return err
	}
	connection, err := grpc.NewClient(client.Config.Endpoint, grpc.WithTransportCredentials(credentials.NewTLS(transport)), grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(8<<20), grpc.MaxCallSendMsgSize(8<<20)))
	if err != nil {
		return err
	}
	defer connection.Close()
	for ctx.Err() == nil {
		if err = client.connect(ctx, connection); err != nil && ctx.Err() == nil {
			slog.Warn("telemetry stream reconnecting", "grpc_code", status.Code(err).String())
			if errors.Is(err, state.ErrAck) || errors.Is(err, state.ErrIdentity) {
				return err
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second):
		}
	}
	return ctx.Err()
}

func (client *Client) connect(parent context.Context, connection *grpc.ClientConn) error {
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	stream, err := nodev1.NewNodeTelemetryServiceClient(connection).Connect(ctx)
	if err != nil {
		return err
	}
	token, err := readToken(client.Config.TokenFile)
	if err != nil {
		return err
	}
	hello := &nodev1.AgentHello{ProtocolVersion: 1, ConnectionRef: client.Config.ConnectionRef, PodNamespace: client.Config.PodNamespace, PodName: client.Config.PodName, PodUid: client.Config.PodUID, InstanceId: client.Config.InstanceID, LogId: client.Log.LogID(), BootId: client.Config.BootID, ServiceAccountToken: token, NodeName: client.Config.NodeName, NodeUid: client.Config.NodeUID}
	if err = stream.Send(&nodev1.AgentMessage{Message: &nodev1.AgentMessage_Hello{Hello: hello}}); err != nil {
		return err
	}
	messages := make(chan *nodev1.CenterMessage, 1)
	failures := make(chan error, 1)
	go func() {
		for {
			message, receiveErr := stream.Recv()
			if receiveErr != nil {
				select {
				case failures <- receiveErr:
				case <-ctx.Done():
				}
				return
			}
			select {
			case messages <- message:
			case <-ctx.Done():
				return
			}
		}
	}()
	heartbeatInterval := client.Config.HeartbeatInterval
	if heartbeatInterval <= 0 {
		heartbeatInterval = 5 * time.Second
	}
	renewInterval := client.Config.RenewInterval
	if renewInterval <= 0 {
		renewInterval = 10 * time.Second
	}
	heartbeat := time.NewTicker(heartbeatInterval)
	defer heartbeat.Stop()
	renew := time.NewTicker(renewInterval)
	defer renew.Stop()
	send := time.NewTicker(100 * time.Millisecond)
	defer send.Stop()
	deadline := time.NewTimer(30 * time.Second)
	defer deadline.Stop()
	var epoch uint64
	var lease time.Time
	var inFlight bool
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-deadline.C:
			return errors.New("NODE_LEASE_EXPIRED")
		case err = <-failures:
			return err
		case message := <-messages:
			switch body := message.Message.(type) {
			case *nodev1.CenterMessage_Welcome:
				welcome := body.Welcome
				if welcome.NodeUid == "" || client.Config.NodeUID != "" && welcome.NodeUid != client.Config.NodeUID || welcome.NodeName != client.Config.NodeName || welcome.Epoch == 0 || welcome.LeaseExpiresAt == nil {
					return errors.New("CENTER_IDENTITY_INVALID")
				}
				if epoch != 0 && welcome.Epoch != epoch {
					return errors.New("CENTER_EPOCH_CHANGED")
				}
				if epoch == 0 {
					if err = client.Log.Bind(welcome.NodeUid); err != nil {
						return err
					}
					client.Config.NodeUID = welcome.NodeUid
					if err = client.Log.Reconcile(welcome.ActiveLogId, state.Receipt{Sequence: welcome.LastCommittedSequence, Digest: welcome.LastPayloadDigest}); err != nil {
						return err
					}
					client.Epoch.Store(welcome.Epoch)
					select {
					case client.Capture <- struct{}{}:
					default:
					}
				}
				epoch = welcome.Epoch
				lease = welcome.LeaseExpiresAt.AsTime()
				remaining := time.Until(lease)
				if remaining <= 0 || remaining > 31*time.Second {
					return errors.New("CENTER_LEASE_INVALID")
				}
				if !deadline.Stop() {
					select {
					case <-deadline.C:
					default:
					}
				}
				deadline.Reset(remaining)
			case *nodev1.CenterMessage_Ack:
				if !inFlight {
					return state.ErrAck
				}
				ack := body.Ack
				if err = client.Log.Acknowledge(ack.LogId, state.Receipt{Sequence: ack.Sequence, Digest: ack.PayloadDigest}); err != nil {
					return err
				}
				inFlight = false
			case *nodev1.CenterMessage_RecoverExit:
				if epoch == 0 || body.RecoverExit.NodeUid != client.Config.NodeUID {
					return errors.New("JOURNAL_REQUEST_NODE_INVALID")
				}
				select {
				case client.Recover <- body.RecoverExit:
				case <-ctx.Done():
					return ctx.Err()
				default:
					return errors.New("JOURNAL_REQUEST_BACKLOG")
				}
			default:
				return errors.New("CENTER_MESSAGE_INVALID")
			}
		case <-send.C:
			if epoch == 0 || inFlight {
				continue
			}
			record, ok, nextErr := client.Log.Next()
			if nextErr != nil {
				return nextErr
			}
			if !ok {
				continue
			}
			var immutable nodev1.ImmutableRecord
			if err = proto.Unmarshal(record.Bytes, &immutable); err != nil {
				return state.ErrCorrupt
			}
			if err = stream.Send(&nodev1.AgentMessage{Message: &nodev1.AgentMessage_Report{Report: &nodev1.AgentReport{Epoch: epoch, Record: &immutable}}}); err != nil {
				return err
			}
			inFlight = true
		case <-heartbeat.C:
			if epoch == 0 {
				continue
			}
			degraded := client.Log.Degraded()
			var issues []string
			if degraded {
				issues = []string{client.Log.Reason()}
			}
			if err = stream.Send(&nodev1.AgentMessage{Message: &nodev1.AgentMessage_Heartbeat{Heartbeat: &nodev1.AgentHeartbeat{Epoch: epoch, Degraded: degraded, Issues: issues}}}); err != nil {
				return err
			}
		case <-renew.C:
			if epoch == 0 {
				continue
			}
			token, err = readToken(client.Config.TokenFile)
			if err != nil {
				return err
			}
			if err = stream.Send(&nodev1.AgentMessage{Message: &nodev1.AgentMessage_Renew{Renew: &nodev1.AgentRenew{Epoch: epoch, ServiceAccountToken: token}}}); err != nil {
				return err
			}
		}
	}
}

func readToken(path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return "", err
	}
	if info.Size() > 16384 {
		return "", errors.New("SA_TOKEN_TOO_LARGE")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	token := strings.TrimSpace(string(data))
	if token == "" || len(token) > 16384 {
		return "", errors.New("SA_TOKEN_INVALID")
	}
	return token, nil
}
