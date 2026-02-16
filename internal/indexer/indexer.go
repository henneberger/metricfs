package indexer

import (
	"bufio"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/henneberger/metrics-fs/internal/auth"
	"github.com/henneberger/metrics-fs/internal/mapper"
)

type LineIndex struct {
	Start      int64               `json:"start"`
	End        int64               `json:"end"`
	Decision   string              `json:"decision"`
	Candidates []auth.CandidateKey `json:"candidates"`
}

type FileIndex struct {
	SourcePath  string      `json:"source_path"`
	Size        int64       `json:"size"`
	MtimeUnix   int64       `json:"mtime_unix"`
	RuleHash    string      `json:"rule_hash"`
	Passthrough bool        `json:"passthrough"`
	BuiltAt     time.Time   `json:"built_at"`
	Lines       []LineIndex `json:"lines"`
}

type Options struct {
	SourceDir         string
	MapperFileName    string
	MapperInherit     bool
	MissingMapperMode string
	MissingResource   string
	IndexDir          string
	FormatVersion     int
}

func BuildOrLoad(sourcePath string, opts Options) (*FileIndex, error) {
	rule, err := mapper.ResolveRuleForFile(sourcePath, mapper.Config{
		SourceDir:         opts.SourceDir,
		MapperFileName:    opts.MapperFileName,
		InheritParent:     opts.MapperInherit,
		MissingMapperMode: opts.MissingMapperMode,
		DefaultMissingKey: opts.MissingResource,
	})
	if err != nil {
		return nil, err
	}
	st, err := os.Stat(sourcePath)
	if err != nil {
		return nil, err
	}
	cachePath := ""
	if opts.IndexDir != "" {
		formatVersion := opts.FormatVersion
		if formatVersion <= 0 {
			formatVersion = 1
		}
		ruleHash := "passthrough"
		if rule != nil {
			ruleHash = rule.RuleHash
		}
		cachePath = cacheFilePath(opts.IndexDir, sourcePath, st.Size(), st.ModTime().UnixNano(), ruleHash, formatVersion)
		if fi, err := load(cachePath); err == nil {
			return fi, nil
		}
	}
	if rule == nil {
		fi := &FileIndex{
			SourcePath:  sourcePath,
			Size:        st.Size(),
			MtimeUnix:   st.ModTime().UnixNano(),
			RuleHash:    "passthrough",
			Passthrough: true,
			BuiltAt:     time.Now().UTC(),
		}
		if cachePath != "" {
			_ = save(cachePath, fi)
		}
		return fi, nil
	}
	fi, err := build(sourcePath, st, rule)
	if err != nil {
		return nil, err
	}
	if cachePath != "" {
		_ = save(cachePath, fi)
	}
	return fi, nil
}

func build(sourcePath string, st os.FileInfo, rule *mapper.SelectedRule) (*FileIndex, error) {
	f, err := os.Open(sourcePath)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	br := bufio.NewReaderSize(f, 1<<20)
	offset := int64(0)
	lines := make([]LineIndex, 0, 1024)
	for {
		chunk, err := br.ReadBytes('\n')
		if err != nil && err != io.EOF {
			return nil, err
		}
		if len(chunk) > 0 {
			start := offset
			end := offset + int64(len(chunk))
			lineTrim := strings.TrimRight(string(chunk), "\r\n")
			cands, evalErr := mapper.EvaluateLine(rule, []byte(lineTrim))
			if evalErr != nil {
				cands = nil
			}
			lines = append(lines, LineIndex{
				Start:      start,
				End:        end,
				Decision:   rule.Decision,
				Candidates: cands,
			})
			offset = end
		}
		if err == io.EOF {
			break
		}
	}
	return &FileIndex{
		SourcePath: sourcePath,
		Size:       st.Size(),
		MtimeUnix:  st.ModTime().UnixNano(),
		RuleHash:   rule.RuleHash,
		BuiltAt:    time.Now().UTC(),
		Lines:      lines,
	}, nil
}

func VisibleSegments(fi *FileIndex, az auth.Authorizer) [][2]int64 {
	if fi.Passthrough {
		return [][2]int64{{0, fi.Size}}
	}
	segments := make([][2]int64, 0)
	var current *[2]int64
	for _, ln := range fi.Lines {
		if isVisible(ln, az) {
			if current == nil {
				seg := [2]int64{ln.Start, ln.End}
				current = &seg
				continue
			}
			if current[1] == ln.Start {
				current[1] = ln.End
			} else {
				segments = append(segments, *current)
				seg := [2]int64{ln.Start, ln.End}
				current = &seg
			}
			continue
		}
		if current != nil {
			segments = append(segments, *current)
			current = nil
		}
	}
	if current != nil {
		segments = append(segments, *current)
	}
	return segments
}

func isVisible(ln LineIndex, az auth.Authorizer) bool {
	if len(ln.Candidates) == 0 {
		return false
	}
	if ln.Decision == "all" {
		for _, c := range ln.Candidates {
			if !az.IsAllowed(c) {
				return false
			}
		}
		return true
	}
	for _, c := range ln.Candidates {
		if az.IsAllowed(c) {
			return true
		}
	}
	return false
}

func FilterToWriter(fi *FileIndex, az auth.Authorizer, w io.Writer) error {
	if fi.Passthrough {
		f, err := os.Open(fi.SourcePath)
		if err != nil {
			return err
		}
		defer f.Close()
		_, err = io.Copy(w, f)
		return err
	}
	f, err := os.Open(fi.SourcePath)
	if err != nil {
		return err
	}
	defer f.Close()

	for _, ln := range fi.Lines {
		if !isVisible(ln, az) {
			continue
		}
		sz := ln.End - ln.Start
		if sz <= 0 {
			continue
		}
		buf := make([]byte, sz)
		if _, err := f.ReadAt(buf, ln.Start); err != nil && err != io.EOF {
			return err
		}
		if _, err := w.Write(buf); err != nil {
			return err
		}
	}
	return nil
}

func cacheFilePath(indexDir, sourcePath string, size int64, mtime int64, ruleHash string, formatVersion int) string {
	k := fmt.Sprintf("%d|%s|%d|%d|%s", formatVersion, sourcePath, size, mtime, ruleHash)
	h := sha1.Sum([]byte(k))
	return filepath.Join(indexDir, hex.EncodeToString(h[:])+".json")
}

func save(path string, fi *FileIndex) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	b, err := json.Marshal(fi)
	if err != nil {
		return err
	}
	return os.WriteFile(path, b, 0o644)
}

func load(path string) (*FileIndex, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var fi FileIndex
	if err := json.Unmarshal(b, &fi); err != nil {
		return nil, err
	}
	return &fi, nil
}
