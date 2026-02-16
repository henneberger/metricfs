package auth

import (
	"os"
	"path/filepath"
	"testing"
)

func TestPermissionsFile(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "permissions.json")
	content := `{"allow":[{"object_type":"metric_row","object_id":"orders_1","permission":"read"},{"object_type":"metric_row","object_id":"orders_3"}]}`
	if err := os.WriteFile(p, []byte(content), 0o644); err != nil {
		t.Fatalf("write permissions file: %v", err)
	}

	a, err := NewFromPermissionsFile(p)
	if err != nil {
		t.Fatalf("load permissions: %v", err)
	}
	checks := []CandidateKey{
		{ObjectType: "metric_row", ObjectID: "orders_1", Permission: "read"},
		{ObjectType: "metric_row", ObjectID: "orders_3", Permission: "read"},
	}
	for _, c := range checks {
		if !a.IsAllowed(c) {
			t.Fatalf("expected allowed: %+v", c)
		}
	}
	if a.IsAllowed(CandidateKey{ObjectType: "metric_row", ObjectID: "orders_2", Permission: "read"}) {
		t.Fatalf("expected orders_2 denied")
	}
}
