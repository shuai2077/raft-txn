package txnservice

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/eliben/raft/txnkv/txn"
)

// The wire types are a compatibility surface between clients and group members:
// they are what makes it possible to change the internal txn types without
// silently breaking a deployed client. These tests keep the two in step and pin
// the on-the-wire field names.

// TestLockInfoRoundTrip is the most important of them. A lock's key is what tells
// a resolver which key's group to write to, and it is exactly the field that is
// easy to forget when the wire type is edited: forgetting it produces a resolver
// that quietly clears nothing.
func TestLockInfoRoundTrip(t *testing.T) {
	internal := &txn.LockInfo{
		Exists:        true,
		Key:           "user:42",
		TxnID:         99,
		Primary:       "user:42",
		StartTS:       12,
		CommitTS:      34,
		Kind:          txn.OpDelete,
		ExpiresAt:     1234567,
		ActorCommitTS: 34,
		State:         txn.StateCommitted,
	}

	wire := lockFor(txn.TxnResult{Lock: internal})
	if wire == nil {
		t.Fatal("lockFor returned nil for a present lock")
	}

	// Through JSON, which is what actually happens on the wire.
	encoded, err := json.Marshal(wire)
	if err != nil {
		t.Fatalf("marshalling the wire lock: %v", err)
	}
	var decoded LockInfo
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatalf("unmarshalling the wire lock: %v", err)
	}

	back := lockInfoFromWire(&decoded)
	if *back != *internal {
		t.Fatalf("round trip lost information:\n got %+v\nwant %+v", *back, *internal)
	}

	// And the field names are part of the contract.
	for _, field := range []string{`"key"`, `"primary"`, `"start_ts"`, `"commit_ts"`, `"actor_commit_ts"`, `"state"`} {
		if !bytes.Contains(encoded, []byte(field)) {
			t.Errorf("wire lock is missing %s: %s", field, encoded)
		}
	}
}

func TestLockForNilWhenNoLock(t *testing.T) {
	if got := lockFor(txn.TxnResult{}); got != nil {
		t.Fatalf("lockFor returned %+v for a result with no lock", got)
	}
}

func TestStatusVocabularyIsStable(t *testing.T) {
	// These strings are the protocol. Changing them breaks every deployed client,
	// so the test states them explicitly rather than deriving them.
	want := map[string]txn.Status{
		WStatusOK:               txn.StatusOK,
		WStatusAlreadyCommitted: txn.StatusAlreadyCommitted,
		WStatusLocked:           txn.StatusLocked,
		WStatusWriteConflict:    txn.StatusWriteConflict,
		WStatusKeyNotInWriteSet: txn.StatusKeyNotInWriteSet,
		WStatusCommitTSOutOfOrd: txn.StatusCommitTSOutOfOrder,
		WStatusNotLeader:        txn.StatusNotLeader,
		WStatusInternal:         txn.StatusInternal,
	}
	for wire, status := range want {
		if got := statusFor(txn.TxnResult{Status: status}); got != wire {
			t.Errorf("statusFor(%v)=%q, want %q", status, got, wire)
		}
		// Unknown or invalid statuses must not be reported as success.
	}
	if got := statusFor(txn.TxnResult{Status: txn.StatusInvalid}); got != WStatusInternal {
		t.Errorf("statusFor(invalid)=%q, want %q", got, WStatusInternal)
	}
}

func TestStatusForAlwaysFailsClosed(t *testing.T) {
	// A status the client does not recognize must never look like success.
	for _, st := range []txn.Status{txn.StatusInvalid, txn.Status(42)} {
		got := statusFor(txn.TxnResult{Status: st})
		if got == WStatusOK || got == WStatusAlreadyCommitted {
			t.Errorf("statusFor(%v)=%q; an unknown status must not look successful", st, got)
		}
	}
}

func TestReadJSONRequiresJSONContentType(t *testing.T) {
	body := strings.NewReader(`{"key":"a"}`)
	req := httptest.NewRequest(http.MethodPost, "/txn/read/", body)
	req.Header.Set("Content-Type", "text/plain")

	var target ReadRequest
	if err := readJSON(req, &target); err == nil {
		t.Fatal("readJSON accepted a text/plain body")
	}
}

func TestReadJSONRejectsUnknownFields(t *testing.T) {
	// Strict decoding is what stops client and server from drifting apart: a
	// client that sends a field the server does not know is a client that will be
	// silently ignored otherwise.
	body := strings.NewReader(`{"key":"a","surprise":true}`)
	req := httptest.NewRequest(http.MethodPost, "/txn/read/", body)
	req.Header.Set("Content-Type", "application/json")

	var target ReadRequest
	if err := readJSON(req, &target); err == nil {
		t.Fatal("readJSON accepted an unknown field")
	}
}

func TestReadJSONAcceptsWellFormedRequest(t *testing.T) {
	body := strings.NewReader(`{"key":"a","start_ts":7}`)
	req := httptest.NewRequest(http.MethodPost, "/txn/read/", body)
	req.Header.Set("Content-Type", "application/json; charset=utf-8")

	var target ReadRequest
	if err := readJSON(req, &target); err != nil {
		t.Fatalf("readJSON: %v", err)
	}
	if target.Key != "a" || target.StartTS != 7 {
		t.Fatalf("decoded %+v, want key=a start_ts=7", target)
	}
}

func TestNotLeaderResponseIsRecognizable(t *testing.T) {
	// The client's retry loop recognizes a routing rejection by probing for these
	// two fields on any response, so they must be present on the error payload.
	payload := notLeaderPayload(2, 5)
	encoded, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	var probe notLeaderProbe
	if err := json.Unmarshal(encoded, &probe); err != nil {
		t.Fatal(err)
	}
	if probe.Status != WStatusNotLeader {
		t.Errorf("status=%q, want %q", probe.Status, WStatusNotLeader)
	}
	if probe.LeaderID != 2 {
		t.Errorf("leader_id=%d, want the hint 2", probe.LeaderID)
	}
}
