package fusefs

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/henneberger/metrics-fs/internal/auth"
	"github.com/henneberger/metrics-fs/internal/indexer"
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

func (d *dirNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	next := filepath.Join(d.sourcePath, name)
	st, err := os.Stat(next)
	if err != nil {
		return nil, syscall.ENOENT
	}
	if st.IsDir() {
		ch := &dirNode{cfg: d.cfg, az: d.az, sourcePath: next}
		return d.NewInode(ctx, ch, fs.StableAttr{Mode: syscall.S_IFDIR}), 0
	}
	data, err := d.fileData(next)
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
	entries, err := os.ReadDir(d.sourcePath)
	if err != nil {
		return nil, syscall.EIO
	}
	out := make([]fuse.DirEntry, 0, len(entries))
	for _, e := range entries {
		mode := uint32(syscall.S_IFREG)
		if e.IsDir() {
			mode = syscall.S_IFDIR
		}
		out = append(out, fuse.DirEntry{
			Name: e.Name(),
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

func (d *dirNode) fileData(path string) ([]byte, error) {
	if !strings.HasSuffix(strings.ToLower(path), ".jsonl") {
		return os.ReadFile(path)
	}
	fi, err := indexer.BuildOrLoad(path, indexer.Options{
		SourceDir:         d.cfg.SourceDir,
		MapperFileName:    d.cfg.MapperFileName,
		MapperInherit:     d.cfg.MapperInherit,
		MissingMapperMode: d.cfg.MissingMapperMode,
		MissingResource:   d.cfg.MissingResource,
		IndexDir:          d.cfg.IndexDir,
		FormatVersion:     d.cfg.IndexFormatVersion,
	})
	if err != nil {
		return nil, err
	}
	var b bytes.Buffer
	if err := indexer.FilterToWriter(fi, d.az, &b); err != nil {
		return nil, err
	}
	return b.Bytes(), nil
}

type memFileNode struct {
	fs.MemRegularFile
}

var _ fs.NodeGetattrer = (*dirNode)(nil)
var _ fs.NodeLookuper = (*dirNode)(nil)
var _ fs.NodeReaddirer = (*dirNode)(nil)
