//go:build !windows
// +build !windows

package fusefs

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/henneberger/metrics-fs/internal/auth"
	"github.com/henneberger/metrics-fs/internal/projector"
)

type Config struct {
	SourceDir          string
	MountDir           string
	MapperFileName     string
	MapperInherit      bool
	MissingMapperMode  string
	MissingResource    string
	IndexDir           string
	IndexFormatVersion int
	AllowOther         bool
	ReadOnly           bool
}

type Server struct {
	cfg Config
	az  auth.Authorizer
}

func New(cfg Config, az auth.Authorizer) *Server {
	return &Server{cfg: cfg, az: az}
}

func (s *Server) MountAndServe(ctx context.Context) error {
	root := &dirNode{cfg: s.cfg, az: s.az, sourcePath: s.cfg.SourceDir}
	opts := &fs.Options{
		MountOptions: fuse.MountOptions{
			AllowOther: s.cfg.AllowOther,
			Name:       "metricfs",
			FsName:     "metricfs",
			Options:    []string{"ro"},
		},
	}
	server, err := fs.Mount(s.cfg.MountDir, root, opts)
	if err != nil {
		return err
	}

	done := make(chan struct{})
	go func() {
		server.Wait()
		close(done)
	}()

	select {
	case <-ctx.Done():
		_ = server.Unmount()
		<-done
		return nil
	case <-done:
		return nil
	}
}

type dirNode struct {
	fs.Inode
	cfg        Config
	az         auth.Authorizer
	sourcePath string
}

type resolvedEntry struct {
	name      string
	source    string
	isDir     bool
	projected bool
}

func (d *dirNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	entries, err := d.resolveEntries()
	if err != nil {
		return nil, syscall.EIO
	}
	ent, ok := entries[name]
	if !ok {
		return nil, syscall.ENOENT
	}
	if ent.isDir {
		ch := &dirNode{cfg: d.cfg, az: d.az, sourcePath: ent.source}
		return d.NewInode(ctx, ch, fs.StableAttr{Mode: syscall.S_IFDIR}), 0
	}
	data, err := d.fileData(ent)
	if err != nil {
		return nil, syscall.EIO
	}
	file := &memFileNode{
		MemRegularFile: fs.MemRegularFile{
			Data: data,
			Attr: fuse.Attr{
				Mode: 0o444,
				Size: uint64(len(data)),
			},
		},
	}
	return d.NewInode(ctx, file, fs.StableAttr{Mode: syscall.S_IFREG}), 0
}

func (d *dirNode) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	entries, err := d.resolveEntries()
	if err != nil {
		return nil, syscall.EIO
	}
	names := make([]string, 0, len(entries))
	for name := range entries {
		names = append(names, name)
	}
	sort.Strings(names)

	out := make([]fuse.DirEntry, 0, len(entries))
	for _, name := range names {
		e := entries[name]
		mode := uint32(syscall.S_IFREG)
		if e.isDir {
			mode = syscall.S_IFDIR
		}
		out = append(out, fuse.DirEntry{
			Name: e.name,
			Mode: mode,
		})
	}
	return fs.NewListDirStream(out), 0
}

func (d *dirNode) Getattr(ctx context.Context, f fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	st, err := os.Stat(d.sourcePath)
	if err != nil {
		return syscall.ENOENT
	}
	out.Mode = uint32(st.Mode().Perm()) | syscall.S_IFDIR
	return 0
}

func (d *dirNode) fileData(ent resolvedEntry) ([]byte, error) {
	lower := strings.ToLower(ent.source)
	if !ent.projected && !strings.HasSuffix(lower, ".jsonl") {
		return os.ReadFile(ent.source)
	}
	var b bytes.Buffer
	if err := projector.RenderFiltered(ent.source, projector.Options{
		SourceDir:         d.cfg.SourceDir,
		MapperFileName:    d.cfg.MapperFileName,
		MapperInherit:     d.cfg.MapperInherit,
		MissingMapperMode: d.cfg.MissingMapperMode,
		MissingResource:   d.cfg.MissingResource,
		IndexDir:          d.cfg.IndexDir,
		FormatVersion:     d.cfg.IndexFormatVersion,
	}, d.az, &b); err != nil {
		return nil, err
	}
	return b.Bytes(), nil
}

func (d *dirNode) resolveEntries() (map[string]resolvedEntry, error) {
	dirEntries, err := os.ReadDir(d.sourcePath)
	if err != nil {
		return nil, err
	}
	out := map[string]resolvedEntry{}
	for _, e := range dirEntries {
		source := filepath.Join(d.sourcePath, e.Name())
		if e.IsDir() {
			out[e.Name()] = resolvedEntry{
				name:   e.Name(),
				source: source,
				isDir:  true,
			}
			continue
		}
		vname, projected := projector.VirtualJSONLName(e.Name())
		if existing, ok := out[vname]; ok && !existing.projected {
			continue
		}
		out[vname] = resolvedEntry{
			name:      vname,
			source:    source,
			isDir:     false,
			projected: projected,
		}
	}
	return out, nil
}

type memFileNode struct {
	fs.MemRegularFile
}

var _ fs.NodeGetattrer = (*dirNode)(nil)
var _ fs.NodeLookuper = (*dirNode)(nil)
var _ fs.NodeReaddirer = (*dirNode)(nil)
