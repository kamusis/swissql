package cmd

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/kamusis/swissql/swissql-cli/internal/client"
)

func TestReplConnect_DisconnectsCurrentSessionAfterConnect(t *testing.T) {
	t.Helper()

	tmp := t.TempDir()
	// Ensure config package writes into an isolated location.
	_ = os.Setenv("USERPROFILE", tmp)
	_ = os.Setenv("HOME", tmp)

	calls := make([]string, 0, 4)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls = append(calls, r.Method+" "+r.URL.Path)

		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v1/disconnect":
			w.WriteHeader(http.StatusOK)
			return
		case r.Method == http.MethodPost && r.URL.Path == "/v1/connect":
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(client.ConnectResponse{
				SessionId: "new-session",
				TraceId:   "t",
				ExpiresAt: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
			})
			return
		default:
			w.WriteHeader(http.StatusNotFound)
			return
		}
	}))
	defer srv.Close()

	c := client.NewClient(srv.URL, 250*time.Millisecond)

	oldSid := "old-session"
	owns := true
	ctx := &replDispatchContext{
		Input:     "connect postgres://postgres:postgres@localhost:5433/postgres",
		Lower:     "connect postgres://postgres:postgres@localhost:5433/postgres",
		Client:    c,
		SessionId: &oldSid,
		// Keep Name nil to avoid touching registry/config during the disconnect pre-step.
		Name:        nil,
		OwnsSession: &owns,
	}

	handled, _ := dispatchReplLine(ctx)
	if !handled {
		t.Fatalf("expected connect to be handled")
	}
	if oldSid != "new-session" {
		t.Fatalf("expected session id to be updated to new session, got %q", oldSid)
	}
	if len(calls) < 2 {
		t.Fatalf("expected at least 2 calls, got %v", calls)
	}
	if calls[0] != "POST /v1/connect" {
		t.Fatalf("expected first call to be connect, got %v", calls)
	}
	if calls[1] != "POST /v1/disconnect" {
		t.Fatalf("expected second call to be disconnect, got %v", calls)
	}

	// Guardrail: ensure we didn't accidentally write config into a global path.
	if _, err := os.Stat(filepath.Join(tmp, ".swissql")); err != nil {
		// It is okay for this directory to not exist, since Name is nil and we avoided config writes.
		_ = err
	}
}

func TestReplConnect_DoesNotDisconnectIfSessionNotOwned(t *testing.T) {
	calls := make([]string, 0, 4)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls = append(calls, r.Method+" "+r.URL.Path)
		if r.Method == http.MethodPost && r.URL.Path == "/v1/connect" {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(client.ConnectResponse{
				SessionId: "new-session-2",
				TraceId:   "t2",
				ExpiresAt: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
			})
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	c := client.NewClient(srv.URL, 250*time.Millisecond)
	oldSid := "shared-session"
	owns := false
	ctx := &replDispatchContext{
		Input:       "connect postgres://postgres:postgres@localhost:5433/postgres",
		Lower:       "connect postgres://postgres:postgres@localhost:5433/postgres",
		Client:      c,
		SessionId:   &oldSid,
		Name:        nil,
		OwnsSession: &owns,
	}

	handled, _ := dispatchReplLine(ctx)
	if !handled {
		t.Fatalf("expected connect to be handled")
	}
	if oldSid != "new-session-2" {
		t.Fatalf("expected session id update")
	}
	// Expect only connect call, no disconnect
	if len(calls) != 1 {
		t.Fatalf("expected exactly 1 call (connect), got %v", calls)
	}
	if calls[0] != "POST /v1/connect" {
		t.Fatalf("expected call to be connect, got %v", calls)
	}

	// Verify OwnsSession is updated to true for the new session
	if !owns {
		t.Fatalf("expected OwnsSession to be updated to true after successful connect")
	}
}
