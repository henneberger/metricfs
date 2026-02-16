package indexer

import (
	"bytes"
	"testing"

	"github.com/henneberger/metrics-fs/internal/auth"
)

func TestFilterOrdersForAlice(t *testing.T) {
	az, err := auth.NewFromPermissionsFile("../../examples/permissions-alice.json")
	if err != nil {
		t.Fatalf("auth: %v", err)
	}
	fi, err := BuildOrLoad("../../examples/metrics/orders.jsonl", Options{
		SourceDir:         "../../examples/metrics",
		MapperFileName:    ".metricfs-map.yaml",
		MapperInherit:     true,
		MissingMapperMode: "deny",
		MissingResource:   "deny",
	})
	if err != nil {
		t.Fatalf("build index: %v", err)
	}
	var b bytes.Buffer
	if err := FilterToWriter(fi, az, &b); err != nil {
		t.Fatalf("filter: %v", err)
	}
	out := b.String()
	if !bytes.Contains(b.Bytes(), []byte("orders_1")) || !bytes.Contains(b.Bytes(), []byte("orders_3")) {
		t.Fatalf("expected orders_1 and orders_3 in output: %s", out)
	}
	if bytes.Contains(b.Bytes(), []byte("orders_2")) {
		t.Fatalf("expected orders_2 filtered out: %s", out)
	}
}
