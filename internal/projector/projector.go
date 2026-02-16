package projector

import (
	"archive/tar"
	"bufio"
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/henneberger/metrics-fs/internal/auth"
	"github.com/henneberger/metrics-fs/internal/indexer"
	"github.com/henneberger/metrics-fs/internal/mapper"
)

type Options struct {
	SourceDir         string
	MapperFileName    string
	MapperInherit     bool
	MissingMapperMode string
	MissingResource   string
	IndexDir          string
	FormatVersion     int
}

func VirtualJSONLName(name string) (string, bool) {
	lower := strings.ToLower(name)
	switch {
	case strings.HasSuffix(lower, ".jsonl"):
		return name, false
	case strings.HasSuffix(lower, ".jsonl.gz"):
		return strings.TrimSuffix(name, ".gz"), true
	case strings.HasSuffix(lower, ".jsonl.tar.gz"):
		return strings.TrimSuffix(name, ".tar.gz"), true
	default:
		return name, false
	}
}

func RenderFiltered(sourcePath string, opts Options, az auth.Authorizer, w io.Writer) error {
	lower := strings.ToLower(sourcePath)
	if strings.HasSuffix(lower, ".jsonl") {
		fi, err := indexer.BuildOrLoad(sourcePath, indexer.Options{
			SourceDir:         opts.SourceDir,
			MapperFileName:    opts.MapperFileName,
			MapperInherit:     opts.MapperInherit,
			MissingMapperMode: opts.MissingMapperMode,
			MissingResource:   opts.MissingResource,
			IndexDir:          opts.IndexDir,
			FormatVersion:     opts.FormatVersion,
		})
		if err != nil {
			return err
		}
		return indexer.FilterToWriter(fi, az, w)
	}

	virtualPath := virtualPathForRule(sourcePath)
	rule, err := mapper.ResolveRuleForFile(virtualPath, mapper.Config{
		SourceDir:         opts.SourceDir,
		MapperFileName:    opts.MapperFileName,
		InheritParent:     opts.MapperInherit,
		MissingMapperMode: opts.MissingMapperMode,
		DefaultMissingKey: opts.MissingResource,
	})
	if err != nil {
		return err
	}

	switch {
	case strings.HasSuffix(lower, ".jsonl.gz"):
		return renderGzipJSONL(sourcePath, rule, az, w)
	case strings.HasSuffix(lower, ".jsonl.tar.gz"):
		return renderTarGzipJSONL(sourcePath, rule, az, w)
	default:
		return fmt.Errorf("unsupported file type for filtering: %s", sourcePath)
	}
}

func virtualPathForRule(sourcePath string) string {
	lower := strings.ToLower(sourcePath)
	switch {
	case strings.HasSuffix(lower, ".jsonl.gz"):
		return strings.TrimSuffix(sourcePath, ".gz")
	case strings.HasSuffix(lower, ".jsonl.tar.gz"):
		return strings.TrimSuffix(sourcePath, ".tar.gz")
	default:
		return sourcePath
	}
}

func renderGzipJSONL(path string, rule *mapper.SelectedRule, az auth.Authorizer, w io.Writer) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	gz, err := gzip.NewReader(f)
	if err != nil {
		return err
	}
	defer gz.Close()
	return streamJSONLLines(gz, rule, az, w)
}

func renderTarGzipJSONL(path string, rule *mapper.SelectedRule, az auth.Authorizer, w io.Writer) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	gz, err := gzip.NewReader(f)
	if err != nil {
		return err
	}
	defer gz.Close()
	tr := tar.NewReader(gz)
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		if hdr.Typeflag != tar.TypeReg {
			continue
		}
		if !strings.HasSuffix(strings.ToLower(hdr.Name), ".jsonl") {
			continue
		}
		if err := streamJSONLLines(tr, rule, az, w); err != nil {
			return err
		}
	}
}

func streamJSONLLines(r io.Reader, rule *mapper.SelectedRule, az auth.Authorizer, w io.Writer) error {
	br := bufio.NewReaderSize(r, 1<<20)
	for {
		line, err := br.ReadBytes('\n')
		if err != nil && err != io.EOF {
			return err
		}
		if len(line) > 0 {
			if isVisibleLine(rule, bytes.TrimRight(line, "\r\n"), az) {
				if _, err := w.Write(line); err != nil {
					return err
				}
			}
		}
		if err == io.EOF {
			return nil
		}
	}
}

func isVisibleLine(rule *mapper.SelectedRule, line []byte, az auth.Authorizer) bool {
	if rule == nil {
		return true
	}
	cands, err := mapper.EvaluateLine(rule, line)
	if err != nil {
		return false
	}
	if len(cands) == 0 {
		return false
	}
	if rule.Decision == "all" {
		for _, c := range cands {
			if !az.IsAllowed(c) {
				return false
			}
		}
		return true
	}
	for _, c := range cands {
		if az.IsAllowed(c) {
			return true
		}
	}
	return false
}
