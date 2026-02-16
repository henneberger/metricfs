//go:build windows
// +build windows

package fusefs

import (
	"context"
	"errors"

	"github.com/henneberger/metrics-fs/internal/auth"
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
	_ = s
	_ = ctx
	return errors.New("fuse mount is not supported on windows; use metricfs render")
}
