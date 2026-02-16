package auth

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestParseSubject(t *testing.T) {
	tests := []struct {
		in          string
		wantType    string
		wantID      string
		wantRel     string
		expectError bool
	}{
		{in: "user:alice", wantType: "user", wantID: "alice"},
		{in: "group:ops#member", wantType: "group", wantID: "ops", wantRel: "member"},
		{in: "", expectError: true},
		{in: "badsubject", expectError: true},
		{in: "user:#member", expectError: true},
	}
	for _, tc := range tests {
		subj, err := parseSubject(tc.in)
		if tc.expectError {
			if err == nil {
				t.Fatalf("expected parse error for %q", tc.in)
			}
			continue
		}
		if err != nil {
			t.Fatalf("unexpected parse error for %q: %v", tc.in, err)
		}
		if subj.Object.ObjectType != tc.wantType || subj.Object.ObjectID != tc.wantID || subj.OptionalRelation != tc.wantRel {
			t.Fatalf("parse mismatch for %q: got %#v", tc.in, subj)
		}
	}
}

func TestParseConsistency(t *testing.T) {
	if _, err := parseConsistency("minimize_latency"); err != nil {
		t.Fatalf("minimize_latency should be valid: %v", err)
	}
	if _, err := parseConsistency("fully_consistent"); err != nil {
		t.Fatalf("fully_consistent should be valid: %v", err)
	}
	if _, err := parseConsistency("at_least_as_fresh"); err == nil {
		t.Fatalf("expected unsupported consistency error")
	}
}

func TestSpiceDBAuthorizerCachesByCandidate(t *testing.T) {
	calls := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if got := r.Header.Get("Authorization"); got != "Bearer token" {
			t.Fatalf("unexpected authorization header: %q", got)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"permissionship":"PERMISSIONSHIP_HAS_PERMISSION"}`))
	}))
	defer srv.Close()

	az, err := NewSpiceDB(SpiceDBConfig{
		Endpoint:    srv.URL,
		Token:       "token",
		Subject:     "user:alice",
		Consistency: "minimize_latency",
	})
	if err != nil {
		t.Fatalf("new spicedb auth: %v", err)
	}
	c := CandidateKey{ObjectType: "metric_row", ObjectID: "orders_1", Permission: "read"}
	if !az.IsAllowed(c) {
		t.Fatalf("expected allowed on first check")
	}
	if !az.IsAllowed(c) {
		t.Fatalf("expected allowed on cached check")
	}
	if calls != 1 {
		t.Fatalf("expected 1 remote call, got %d", calls)
	}
}
