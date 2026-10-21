package state

import (
	"errors"
	"testing"
)

func TestBootstrapBindsAuthenticatedNodeBeforeFacts(testContext *testing.T) {
	directory := testContext.TempDir()
	log, err := Open(directory, "", Options{})
	if err != nil {
		testContext.Fatal(err)
	}
	logID := log.LogID()
	if _, err = log.Enqueue(recordBuilder("unbound")); !errors.Is(err, ErrIdentity) {
		testContext.Fatalf("unbound node wrote facts: %v", err)
	}
	log.Close()
	log, err = Open(directory, "", Options{})
	if err != nil {
		testContext.Fatal(err)
	}
	defer log.Close()
	if logID != log.LogID() {
		testContext.Fatal("bootstrap restart changed log identity")
	}
	if err = log.Bind("authenticated-node-uid"); err != nil {
		testContext.Fatal(err)
	}
	if _, err = log.Enqueue(recordBuilder("bound fact")); err != nil {
		testContext.Fatal(err)
	}
	if err = log.Bind("replacement-node-uid"); !errors.Is(err, ErrIdentity) {
		testContext.Fatalf("identity replacement accepted: %v", err)
	}
}
