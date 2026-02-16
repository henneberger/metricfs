package indexer

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/henneberger/metrics-fs/internal/auth"
)

func TestPassthroughWhenMapperMissing(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "raw.jsonl")
	want := []byte("{\"x\":1}\n{\"x\":2}\n")
	if err := os.WriteFile(p, want, 0o644); err != nil {
		t.Fatalf("write file: %v", err)
	}
	fi, err := BuildOrLoad(p, Options{
		SourceDir:         dir,
		MapperFileName:    ".metricfs-map.yaml",
		MapperInherit:     true,
		MissingMapperMode: "passthrough",
		MissingResource:   "deny",
	})
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	if !fi.Passthrough {
		t.Fatalf("expected passthrough index")
	}
	var b bytes.Buffer
	if err := FilterToWriter(fi, auth.NewDenyAll(), &b); err != nil {
		t.Fatalf("filter: %v", err)
	}
	if !bytes.Equal(b.Bytes(), want) {
		t.Fatalf("expected passthrough output %q, got %q", want, b.Bytes())
	}
}
