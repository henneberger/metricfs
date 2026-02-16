package projector

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/henneberger/metrics-fs/internal/auth"
)

func TestVirtualJSONLName(t *testing.T) {
	tests := []struct {
		in        string
		want      string
		projected bool
	}{
		{"a.jsonl", "a.jsonl", false},
		{"a.jsonl.gz", "a.jsonl", true},
		{"a.jsonl.tar.gz", "a.jsonl", true},
		{"a.parquet", "a.parquet", false},
		{"notes.txt", "notes.txt", false},
	}
	for _, tc := range tests {
		got, proj := VirtualJSONLName(tc.in)
		if got != tc.want || proj != tc.projected {
			t.Fatalf("VirtualJSONLName(%q) = (%q,%v), want (%q,%v)", tc.in, got, proj, tc.want, tc.projected)
		}
	}
}

func TestRenderFilteredGzipJSONL(t *testing.T) {
	dir := t.TempDir()
	sourceDir := filepath.Join(dir, "metrics")
	if err := os.MkdirAll(sourceDir, 0o755); err != nil {
		t.Fatalf("mkdir sourceDir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(sourceDir, ".metricfs-map.yaml"), []byte(`version: 1
rules:
  - match:
      glob: "*.jsonl"
    object_type: "metric_row"
    permission: "read"
    mapper:
      kind: "json_pointer"
      pointer: "/metric_row_id"
      canonical_template: "metric_row:{value}"
    missing_resource_key: "deny"
`), 0o644); err != nil {
		t.Fatalf("write mapper: %v", err)
	}

	gzPath := filepath.Join(sourceDir, "orders.jsonl.gz")
	if err := writeGzip(gzPath, []byte(
		"{\"metric_row_id\":\"orders_1\",\"value\":10}\n"+
			"{\"metric_row_id\":\"orders_2\",\"value\":20}\n"+
			"{\"metric_row_id\":\"orders_3\",\"value\":30}\n")); err != nil {
		t.Fatalf("write gzip: %v", err)
	}

	permPath := filepath.Join(dir, "permissions.json")
	if err := os.WriteFile(permPath, []byte(`{"allow":[{"object_type":"metric_row","object_id":"orders_1","permission":"read"},{"object_type":"metric_row","object_id":"orders_3","permission":"read"}]}`), 0o644); err != nil {
		t.Fatalf("write permissions: %v", err)
	}
	az, err := auth.NewFromPermissionsFile(permPath)
	if err != nil {
		t.Fatalf("new authorizer: %v", err)
	}

	var out bytes.Buffer
	err = RenderFiltered(gzPath, Options{
		SourceDir:         sourceDir,
		MapperFileName:    ".metricfs-map.yaml",
		MapperInherit:     true,
		MissingMapperMode: "deny",
		MissingResource:   "deny",
	}, az, &out)
	if err != nil {
		t.Fatalf("RenderFiltered: %v", err)
	}
	got := out.String()
	if strings.Contains(got, "orders_2") {
		t.Fatalf("unexpected unauthorized row in output: %s", got)
	}
	if !strings.Contains(got, "orders_1") || !strings.Contains(got, "orders_3") {
		t.Fatalf("missing expected rows: %s", got)
	}
}

func TestRenderFilteredTarGzipJSONL(t *testing.T) {
	dir := t.TempDir()
	sourceDir := filepath.Join(dir, "metrics")
	if err := os.MkdirAll(sourceDir, 0o755); err != nil {
		t.Fatalf("mkdir sourceDir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(sourceDir, ".metricfs-map.yaml"), []byte(`version: 1
rules:
  - match:
      glob: "*.jsonl"
    object_type: "metric_row"
    permission: "read"
    mapper:
      kind: "json_pointer"
      pointer: "/metric_row_id"
      canonical_template: "metric_row:{value}"
    missing_resource_key: "deny"
`), 0o644); err != nil {
		t.Fatalf("write mapper: %v", err)
	}

	tgzPath := filepath.Join(sourceDir, "orders.jsonl.tar.gz")
	if err := writeTarGzipJSONL(tgzPath, "inner/orders.jsonl", []byte(
		"{\"metric_row_id\":\"orders_1\",\"value\":10}\n"+
			"{\"metric_row_id\":\"orders_2\",\"value\":20}\n")); err != nil {
		t.Fatalf("write tar.gz: %v", err)
	}

	permPath := filepath.Join(dir, "permissions.json")
	if err := os.WriteFile(permPath, []byte(`{"allow":[{"object_type":"metric_row","object_id":"orders_1","permission":"read"}]}`), 0o644); err != nil {
		t.Fatalf("write permissions: %v", err)
	}
	az, err := auth.NewFromPermissionsFile(permPath)
	if err != nil {
		t.Fatalf("new authorizer: %v", err)
	}

	var out bytes.Buffer
	err = RenderFiltered(tgzPath, Options{
		SourceDir:         sourceDir,
		MapperFileName:    ".metricfs-map.yaml",
		MapperInherit:     true,
		MissingMapperMode: "deny",
		MissingResource:   "deny",
	}, az, &out)
	if err != nil {
		t.Fatalf("RenderFiltered: %v", err)
	}
	got := out.String()
	if !strings.Contains(got, "orders_1") || strings.Contains(got, "orders_2") {
		t.Fatalf("unexpected filtered output: %s", got)
	}
}

func writeGzip(path string, data []byte) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	gz := gzip.NewWriter(f)
	if _, err := gz.Write(data); err != nil {
		_ = gz.Close()
		return err
	}
	return gz.Close()
}

func writeTarGzipJSONL(path, name string, data []byte) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	gz := gzip.NewWriter(f)
	defer gz.Close()
	tw := tar.NewWriter(gz)
	defer tw.Close()
	h := &tar.Header{
		Name: name,
		Mode: 0o644,
		Size: int64(len(data)),
	}
	if err := tw.WriteHeader(h); err != nil {
		return err
	}
	_, err = tw.Write(data)
	return err
}
