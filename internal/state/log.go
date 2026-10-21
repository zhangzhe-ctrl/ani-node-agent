package state

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"syscall"
)

var (
	ErrLocked   = errors.New("LOCAL_EVENT_STORE_IN_USE")
	ErrIdentity = errors.New("LOCAL_LOG_ID_MISMATCH")
	ErrCorrupt  = errors.New("LOCAL_EVENT_STORE_CORRUPT")
	ErrFull     = errors.New("LOCAL_EVENT_STORE_FULL")
	ErrInFlight = errors.New("REPORT_IN_FLIGHT")
	ErrAck      = errors.New("ACK_WATERMARK_MISMATCH")
	ErrDegraded = errors.New("LOCAL_EVENT_STORE_DEGRADED")
	ErrTooLarge = errors.New("SOURCE_TOO_LARGE")
)

type Options struct {
	MaxUnacknowledgedBytes int64
	ReserveBytes           int64
	MaxRecordBytes         int
	Sync                   func(*os.File) error
}

type identity struct {
	Version int    `json:"version"`
	NodeUID string `json:"node_uid"`
	LogID   string `json:"log_id"`
}

type Receipt struct {
	Sequence uint64 `json:"sequence"`
	Digest   string `json:"digest"`
}

type Record struct {
	Sequence     uint64 `json:"sequence"`
	Digest       string `json:"digest"`
	Checksum     string `json:"checksum"`
	Bytes        []byte `json:"bytes"`
	encodedBytes int64
}

type Log struct {
	mu        sync.Mutex
	directory string
	identity  identity
	ack       Receipt
	pending   []Record
	bytes     int64
	inFlight  bool
	degraded  bool
	reason    string
	closed    bool
	lock      *os.File
	options   Options
}

func NewID() (string, error) {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", err
	}
	value[6] = value[6]&15 | 64
	value[8] = value[8]&63 | 128
	encoded := hex.EncodeToString(value[:])
	return encoded[:8] + "-" + encoded[8:12] + "-" + encoded[12:16] + "-" + encoded[16:20] + "-" + encoded[20:], nil
}

func Open(directory, nodeUID string, options Options) (*Log, error) {
	if !filepath.IsAbs(directory) {
		return nil, ErrIdentity
	}
	if options.MaxUnacknowledgedBytes == 0 {
		options.MaxUnacknowledgedBytes = 512 << 20
	}
	if options.ReserveBytes == 0 {
		options.ReserveBytes = 8 << 20
	}
	if options.MaxRecordBytes == 0 {
		options.MaxRecordBytes = (8 << 20) - 1024
	}
	if options.MaxUnacknowledgedBytes < 1 || options.ReserveBytes < 4096 || options.MaxRecordBytes < 1 {
		return nil, ErrFull
	}
	if options.Sync == nil {
		options.Sync = func(file *os.File) error { return file.Sync() }
	}
	if err := os.MkdirAll(directory, 0700); err != nil {
		return nil, err
	}
	info, err := os.Lstat(directory)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return nil, ErrIdentity
	}
	lock, err := os.OpenFile(filepath.Join(directory, ".writer.lock"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	if err = syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		lock.Close()
		return nil, ErrLocked
	}
	log := &Log{directory: directory, lock: lock, options: options}
	ready := false
	defer func() {
		if !ready {
			log.Close()
		}
	}()
	entries, err := os.ReadDir(directory)
	if err != nil {
		return nil, err
	}
	identityBytes, err := os.ReadFile(filepath.Join(directory, "identity.json"))
	if errors.Is(err, os.ErrNotExist) {
		for _, entry := range entries {
			if entry.Name() != ".writer.lock" {
				return nil, ErrIdentity
			}
		}
		logID, idErr := NewID()
		if idErr != nil {
			return nil, idErr
		}
		log.identity = identity{Version: 1, NodeUID: nodeUID, LogID: logID}
		if err = log.writeJSON("identity.json", log.identity); err != nil {
			return nil, err
		}
	} else if err != nil {
		return nil, err
	} else if json.Unmarshal(identityBytes, &log.identity) != nil || log.identity.Version != 1 || nodeUID != "" && log.identity.NodeUID != "" && log.identity.NodeUID != nodeUID || log.identity.LogID == "" {
		return nil, ErrIdentity
	}
	ackBytes, err := os.ReadFile(filepath.Join(directory, "ack.json"))
	if err == nil {
		if json.Unmarshal(ackBytes, &log.ack) != nil || log.ack.Sequence > 0 && log.ack.Digest == "" {
			return nil, ErrCorrupt
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	if faultBytes, readErr := os.ReadFile(filepath.Join(directory, "fault.json")); readErr == nil {
		log.degraded = true
		var fault struct {
			Reason string `json:"reason"`
		}
		if json.Unmarshal(faultBytes, &fault) != nil || fault.Reason == "" {
			return nil, ErrCorrupt
		}
		log.reason = fault.Reason
	} else if !errors.Is(readErr, os.ErrNotExist) {
		return nil, readErr
	}
	sort.Slice(entries, func(first, second int) bool { return entries[first].Name() < entries[second].Name() })
	for _, entry := range entries {
		if !strings.HasSuffix(entry.Name(), ".record") {
			continue
		}
		if entry.Type()&os.ModeSymlink != 0 || entry.IsDir() {
			return nil, ErrCorrupt
		}
		record, readErr := log.readRecord(entry.Name())
		if readErr != nil {
			return nil, readErr
		}
		if record.Sequence <= log.ack.Sequence {
			continue
		}
		if record.Sequence != log.ack.Sequence+uint64(len(log.pending))+1 {
			return nil, ErrCorrupt
		}
		log.bytes += record.encodedBytes
		record.Bytes = nil
		log.pending = append(log.pending, record)
	}
	if err = log.reclaim(); err != nil {
		return nil, err
	}
	if log.identity.NodeUID == "" && (log.ack.Sequence > 0 || len(log.pending) > 0) {
		return nil, ErrIdentity
	}
	if nodeUID != "" && log.identity.NodeUID == "" {
		log.identity.NodeUID = nodeUID
		if err = log.writeJSON("identity.json", log.identity); err != nil {
			return nil, err
		}
	}
	if err = log.reserve(); err != nil {
		log.degraded = true
		_ = log.fault("LOCAL_EVENT_STORE_FULL")
	}
	ready = true
	return log, nil
}

func (log *Log) LogID() string { return log.identity.LogID }

func (log *Log) NodeUID() string { log.mu.Lock(); defer log.mu.Unlock(); return log.identity.NodeUID }

func (log *Log) Bind(nodeUID string) error {
	log.mu.Lock()
	defer log.mu.Unlock()
	if nodeUID == "" || log.identity.NodeUID != "" && log.identity.NodeUID != nodeUID {
		return ErrIdentity
	}
	if log.identity.NodeUID == nodeUID {
		return nil
	}
	bound := log.identity
	bound.NodeUID = nodeUID
	if err := log.writeJSON("identity.json", bound); err != nil {
		return err
	}
	log.identity = bound
	return nil
}

func (log *Log) Watermark() Receipt { log.mu.Lock(); defer log.mu.Unlock(); return log.ack }

func (log *Log) Degraded() bool { log.mu.Lock(); defer log.mu.Unlock(); return log.degraded }

func (log *Log) Reason() string { log.mu.Lock(); defer log.mu.Unlock(); return log.reason }

func (log *Log) Enqueue(build func(string, uint64) ([]byte, string, error)) (Record, error) {
	return log.enqueue(build, false)
}

func (log *Log) RecoverWithGap(build func(string, uint64) ([]byte, string, error)) (Record, error) {
	return log.enqueue(build, true)
}

func (log *Log) enqueue(build func(string, uint64) ([]byte, string, error), recovery bool) (Record, error) {
	log.mu.Lock()
	defer log.mu.Unlock()
	if log.closed {
		return Record{}, os.ErrClosed
	}
	if log.identity.NodeUID == "" {
		return Record{}, ErrIdentity
	}
	if log.degraded && !recovery {
		return Record{}, ErrDegraded
	}
	sequence := log.ack.Sequence + uint64(len(log.pending)) + 1
	body, digest, err := build(log.identity.LogID, sequence)
	if err != nil {
		return Record{}, err
	}
	if len(body) > log.options.MaxRecordBytes {
		return Record{}, ErrTooLarge
	}
	if len(body) == 0 || digest == "" {
		return Record{}, ErrCorrupt
	}
	record := Record{Sequence: sequence, Digest: digest, Checksum: checksum(body), Bytes: append([]byte(nil), body...)}
	data, err := json.Marshal(record)
	if err != nil {
		return Record{}, err
	}
	if log.bytes+int64(len(data)) > log.options.MaxUnacknowledgedBytes {
		log.degraded = true
		_ = log.fault(ErrFull.Error())
		return Record{}, ErrFull
	}
	if err = log.writeAtomic(recordName(sequence), data); err != nil {
		log.degraded = true
		_ = log.fault("LOCAL_EVENT_STORE_WRITE_FAILED")
		return Record{}, err
	}
	index := record
	index.Bytes = nil
	index.encodedBytes = int64(len(data))
	log.pending = append(log.pending, index)
	log.bytes += int64(len(data))
	if recovery {
		if err = log.reserve(); err != nil {
			return record, err
		}
		if err = os.Remove(filepath.Join(log.directory, "fault.json")); err != nil && !errors.Is(err, os.ErrNotExist) {
			return record, err
		}
		if err = log.syncDirectory(); err != nil {
			return record, err
		}
		log.degraded = false
		log.reason = ""
	}
	return clone(record), nil
}

func (log *Log) Next() (Record, bool, error) {
	log.mu.Lock()
	defer log.mu.Unlock()
	if log.closed {
		return Record{}, false, os.ErrClosed
	}
	if log.inFlight {
		return Record{}, false, ErrInFlight
	}
	if len(log.pending) == 0 {
		return Record{}, false, nil
	}
	index := log.pending[0]
	record, err := log.readRecord(recordName(index.Sequence))
	if err != nil || record.Sequence != index.Sequence || record.Digest != index.Digest || record.Checksum != index.Checksum || record.encodedBytes != index.encodedBytes {
		log.degraded = true
		_ = log.fault("LOCAL_EVENT_STORE_READ_FAILED")
		if err == nil {
			err = ErrCorrupt
		}
		return Record{}, false, err
	}
	log.inFlight = true
	return record, true, nil
}

func (log *Log) readRecord(name string) (Record, error) {
	var record Record
	path := filepath.Join(log.directory, name)
	info, err := os.Lstat(path)
	if err != nil {
		return record, err
	}
	if !info.Mode().IsRegular() {
		return record, ErrCorrupt
	}
	file, err := os.Open(path)
	if err != nil {
		return record, err
	}
	defer file.Close()
	data, err := io.ReadAll(io.LimitReader(file, int64(log.options.MaxRecordBytes)*2+4096))
	if err != nil {
		return record, err
	}
	if json.Unmarshal(data, &record) != nil || record.Sequence == 0 || record.Digest == "" || len(record.Bytes) == 0 || len(record.Bytes) > log.options.MaxRecordBytes || record.Checksum != checksum(record.Bytes) || name != recordName(record.Sequence) {
		return Record{}, ErrCorrupt
	}
	record.encodedBytes = int64(len(data))
	return record, nil
}

func (log *Log) Reconnect() { log.mu.Lock(); defer log.mu.Unlock(); log.inFlight = false }

func (log *Log) Reconcile(logID string, remote Receipt) error {
	log.mu.Lock()
	defer log.mu.Unlock()
	if logID != log.identity.LogID {
		return ErrIdentity
	}
	if remote == log.ack {
		log.inFlight = false
		return nil
	}
	if len(log.pending) == 0 || remote.Sequence != log.ack.Sequence+1 || remote.Sequence != log.pending[0].Sequence || remote.Digest != log.pending[0].Digest {
		return ErrAck
	}
	return log.acknowledge(remote)
}

func (log *Log) Acknowledge(logID string, receipt Receipt) error {
	log.mu.Lock()
	defer log.mu.Unlock()
	if logID != log.identity.LogID {
		return ErrIdentity
	}
	if !log.inFlight {
		return ErrAck
	}
	return log.acknowledge(receipt)
}

func (log *Log) acknowledge(receipt Receipt) error {
	if log.closed {
		return os.ErrClosed
	}
	if len(log.pending) == 0 || receipt.Sequence != log.pending[0].Sequence || receipt.Digest != log.pending[0].Digest {
		return ErrAck
	}
	if err := log.writeJSON("ack.json", receipt); err != nil {
		log.degraded = true
		_ = log.fault("ACK_SYNC_FAILED")
		return err
	}
	log.bytes -= log.pending[0].encodedBytes
	log.ack = receipt
	log.pending[0] = Record{}
	log.pending = log.pending[1:]
	log.inFlight = false
	if err := log.reclaim(); err != nil {
		log.degraded = true
		_ = log.fault("ACK_RECLAIM_FAILED")
		return err
	}
	return nil
}

func (log *Log) MarkGap(reason string) error {
	log.mu.Lock()
	defer log.mu.Unlock()
	log.degraded = true
	return log.fault(reason)
}

func (log *Log) fault(reason string) error {
	log.reason = reason
	if err := os.Remove(filepath.Join(log.directory, ".reserve")); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return log.writeJSON("fault.json", struct {
		Reason string `json:"reason"`
	}{Reason: reason})
}

func (log *Log) reserve() error {
	file, err := os.OpenFile(filepath.Join(log.directory, ".reserve"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return err
	}
	defer file.Close()
	if err = syscall.Fallocate(int(file.Fd()), 0, 0, log.options.ReserveBytes); err != nil {
		return err
	}
	if err = log.options.Sync(file); err != nil {
		return err
	}
	return log.syncDirectory()
}

func (log *Log) reclaim() error {
	entries, err := os.ReadDir(log.directory)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if strings.HasSuffix(entry.Name(), ".record") && entry.Name() <= recordName(log.ack.Sequence) {
			if err = os.Remove(filepath.Join(log.directory, entry.Name())); err != nil {
				return err
			}
		}
	}
	return log.syncDirectory()
}

func (log *Log) writeJSON(name string, value any) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return log.writeAtomic(name, data)
}

func (log *Log) writeAtomic(name string, data []byte) error {
	file, err := os.CreateTemp(log.directory, ".pending-*")
	if err != nil {
		return err
	}
	temporary := file.Name()
	defer os.Remove(temporary)
	if _, err = file.Write(data); err == nil {
		err = log.options.Sync(file)
	}
	closeErr := file.Close()
	if err == nil {
		err = closeErr
	}
	if err != nil {
		return err
	}
	if err = os.Rename(temporary, filepath.Join(log.directory, name)); err != nil {
		return err
	}
	return log.syncDirectory()
}

func (log *Log) syncDirectory() error {
	directory, err := os.Open(log.directory)
	if err != nil {
		return err
	}
	defer directory.Close()
	return log.options.Sync(directory)
}

func (log *Log) Close() error {
	log.mu.Lock()
	defer log.mu.Unlock()
	if log.closed {
		return nil
	}
	log.closed = true
	if log.lock == nil {
		return nil
	}
	_ = syscall.Flock(int(log.lock.Fd()), syscall.LOCK_UN)
	return log.lock.Close()
}

func checksum(data []byte) string {
	digest := sha256.Sum256(data)
	return hex.EncodeToString(digest[:])
}
func recordName(sequence uint64) string { return fmt.Sprintf("%020d.record", sequence) }
func clone(record Record) Record        { record.Bytes = append([]byte(nil), record.Bytes...); return record }
