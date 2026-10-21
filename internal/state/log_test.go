package state

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func recordBuilder(content string) func(string, uint64) ([]byte, string, error) {
	return func(_ string, _ uint64) ([]byte, string, error) {
		return []byte(content), checksum([]byte(content)), nil
	}
}

func TestDurableReplaySingleFlightAndAckLoss(testContext *testing.T) {
	directory := testContext.TempDir()
	log, err := Open(directory, "node-a", Options{})
	if err != nil {
		testContext.Fatal(err)
	}
	first, err := log.Enqueue(recordBuilder("first immutable record"))
	if err != nil {
		testContext.Fatal(err)
	}
	if _, err = log.Enqueue(recordBuilder("second record")); err != nil {
		testContext.Fatal(err)
	}
	logID := log.LogID()
	sent, ok, err := log.Next()
	if err != nil || !ok || !bytes.Equal(first.Bytes, sent.Bytes) {
		testContext.Fatalf("first send: %+v %v %v", sent, ok, err)
	}
	if _, _, err = log.Next(); !errors.Is(err, ErrInFlight) {
		testContext.Fatalf("parallel report accepted: %v", err)
	}
	if err = log.Acknowledge(logID, Receipt{first.Sequence, "different"}); !errors.Is(err, ErrAck) {
		testContext.Fatalf("wrong content ACK accepted: %v", err)
	}
	if err = log.Close(); err != nil {
		testContext.Fatal(err)
	}
	log, err = Open(directory, "node-a", Options{})
	if err != nil {
		testContext.Fatal(err)
	}
	defer log.Close()
	if log.LogID() != logID {
		testContext.Fatal("restart reset log identity")
	}
	replayed, ok, err := log.Next()
	if err != nil || !ok || !bytes.Equal(first.Bytes, replayed.Bytes) {
		testContext.Fatalf("restart changed unacknowledged bytes: %+v %v %v", replayed, ok, err)
	}
	if err = log.Reconcile(logID, Receipt{Sequence: 2, Digest: checksum([]byte("second record"))}); !errors.Is(err, ErrAck) {
		testContext.Fatalf("larger welcome skipped records: %v", err)
	}
	if err = log.Reconcile(logID, Receipt{first.Sequence, first.Digest}); err != nil {
		testContext.Fatal(err)
	}
	next, ok, err := log.Next()
	if err != nil || !ok || next.Sequence != 2 {
		testContext.Fatalf("lost ACK did not advance durably: %+v %v %v", next, ok, err)
	}
	if _, err = os.Stat(filepath.Join(directory, recordName(1))); !errors.Is(err, os.ErrNotExist) {
		testContext.Fatal("ACKed file not reclaimed")
	}
}

func TestAckSyncFailureCannotAdvance(testContext *testing.T) {
	failAck := false
	options := Options{Sync: func(file *os.File) error {
		if failAck && strings.Contains(filepath.Base(file.Name()), ".pending-") {
			return errors.New("injected fsync failure")
		}
		return file.Sync()
	}}
	log, err := Open(testContext.TempDir(), "node-a", options)
	if err != nil {
		testContext.Fatal(err)
	}
	defer log.Close()
	first, err := log.Enqueue(recordBuilder("first"))
	if err != nil {
		testContext.Fatal(err)
	}
	if _, err = log.Enqueue(recordBuilder("second")); err != nil {
		testContext.Fatal(err)
	}
	if _, _, err = log.Next(); err != nil {
		testContext.Fatal(err)
	}
	failAck = true
	if err = log.Acknowledge(log.LogID(), Receipt{first.Sequence, first.Digest}); err == nil {
		testContext.Fatal("failed fsync acknowledged")
	}
	if log.Watermark().Sequence != 0 {
		testContext.Fatal("failed ACK advanced durable watermark")
	}
	if _, _, err = log.Next(); !errors.Is(err, ErrInFlight) {
		testContext.Fatalf("failed ACK allowed next: %v", err)
	}
	failAck = false
	if err = log.Acknowledge(log.LogID(), Receipt{first.Sequence, first.Digest}); err != nil {
		testContext.Fatal(err)
	}
	if !log.Degraded() {
		testContext.Fatal("storage gap erased without a durable gap record")
	}
	if _, err = log.RecoverWithGap(recordBuilder("source gap after disk failure")); err != nil {
		testContext.Fatal(err)
	}
	if log.Degraded() {
		testContext.Fatal("durable recovery gap did not restore sampling")
	}
}

func TestNodeIdentityWriterLockCorruptionAndFullLog(testContext *testing.T) {
	directory := testContext.TempDir()
	log, err := Open(directory, "node-a", Options{MaxUnacknowledgedBytes: 512})
	if err != nil {
		testContext.Fatal(err)
	}
	if _, err = Open(directory, "node-a", Options{}); !errors.Is(err, ErrLocked) {
		testContext.Fatalf("concurrent writer accepted: %v", err)
	}
	first, err := log.Enqueue(recordBuilder("durable"))
	if err != nil {
		testContext.Fatal(err)
	}
	if _, err = log.Enqueue(recordBuilder(strings.Repeat("x", 500))); !errors.Is(err, ErrFull) {
		testContext.Fatalf("full log not bounded: %v", err)
	}
	if _, _, err = log.Next(); err != nil {
		testContext.Fatalf("full log prevented existing delivery: %v", err)
	}
	if err = log.Acknowledge(log.LogID(), Receipt{first.Sequence, first.Digest}); err != nil {
		testContext.Fatalf("full log prevented durable ACK: %v", err)
	}
	if _, err = log.RecoverWithGap(recordBuilder("true collection gap")); err != nil {
		testContext.Fatal(err)
	}
	log.Close()
	if _, err = Open(directory, "node-replacement", Options{}); !errors.Is(err, ErrIdentity) {
		testContext.Fatalf("Node UID replacement reused history: %v", err)
	}
	if err = os.WriteFile(filepath.Join(directory, recordName(2)), []byte("corrupted"), 0600); err != nil {
		testContext.Fatal(err)
	}
	if _, err = Open(directory, "node-a", Options{}); !errors.Is(err, ErrCorrupt) {
		testContext.Fatalf("corruption silently discarded: %v", err)
	}
}
