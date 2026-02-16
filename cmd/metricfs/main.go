package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"time"

	"github.com/henneberger/metrics-fs/internal/auth"
	"github.com/henneberger/metrics-fs/internal/fusefs"
	"github.com/henneberger/metrics-fs/internal/indexer"
	"github.com/henneberger/metrics-fs/internal/projector"
)

type commonFlags struct {
	sourceDir           string
	mountDir            string
	authBackend         string
	subject             string
	readOnly            bool
	allowOther          bool
	spiceEndpoint       string
	spiceToken          string
	spiceTokenEnv       string
	spiceConsistency    string
	watchEnabled        bool
	watchBackoff        string
	reconcileInterval   time.Duration
	onSpiceUnavailable  string
	staleSnapshotTTL    time.Duration
	indexDir            string
	indexFormatVersion  int
	indexHash           string
	indexWorkers        int
	mapperFileName      string
	mapperResolution    string
	mapperInheritParent bool
	missingMapper       string
	missingResourceKey  string
	permissionsFile     string
	allowNoAuthz        bool
}

func addCommonFlags(fs *flag.FlagSet, c *commonFlags, needMountFields bool) {
	fs.StringVar(&c.sourceDir, "source-dir", "", "source directory")
	fs.StringVar(&c.mountDir, "mount-dir", "", "mount directory")
	fs.StringVar(&c.authBackend, "auth-backend", "file", "authorization backend: file|spicedb")
	fs.StringVar(&c.subject, "subject", "", "subject, e.g. user:alice")
	fs.BoolVar(&c.readOnly, "read-only", true, "read only")
	fs.BoolVar(&c.allowOther, "allow-other", false, "allow other users")
	fs.StringVar(&c.spiceEndpoint, "spicedb-endpoint", "", "spicedb endpoint")
	fs.StringVar(&c.spiceToken, "spicedb-token", "", "spicedb token")
	fs.StringVar(&c.spiceTokenEnv, "spicedb-token-env", "SPICEDB_TOKEN", "spicedb token env var")
	fs.StringVar(&c.spiceConsistency, "spicedb-consistency", "minimize_latency", "spicedb consistency")
	fs.BoolVar(&c.watchEnabled, "watch-enabled", true, "watch enabled")
	fs.StringVar(&c.watchBackoff, "watch-reconnect-backoff", "100ms..5s", "watch reconnect backoff range")
	fs.DurationVar(&c.reconcileInterval, "reconcile-interval", 30*time.Second, "reconcile interval")
	fs.StringVar(&c.onSpiceUnavailable, "on-spicedb-unavailable", "fail_closed", "fail_closed or serve_stale")
	fs.DurationVar(&c.staleSnapshotTTL, "stale-snapshot-ttl", 0, "stale ttl")
	fs.StringVar(&c.indexDir, "index-dir", defaultIndexDir(), "index directory")
	fs.IntVar(&c.indexFormatVersion, "index-format-version", 1, "index format version")
	fs.StringVar(&c.indexHash, "index-hash", "xxh3_64", "index hash")
	fs.IntVar(&c.indexWorkers, "index-workers", runtime.NumCPU(), "index workers")
	fs.StringVar(&c.mapperFileName, "mapper-file-name", ".metricfs-map.yaml", "mapper file name")
	fs.StringVar(&c.mapperResolution, "mapper-resolution", "nearest_ancestor", "mapper resolution")
	fs.BoolVar(&c.mapperInheritParent, "mapper-inherit-parent", true, "mapper inherit parent")
	fs.StringVar(&c.missingMapper, "missing-mapper", "deny", "missing mapper behavior")
	fs.StringVar(&c.missingResourceKey, "missing-resource-key", "deny", "default missing resource key behavior")
	fs.StringVar(&c.permissionsFile, "permissions-file", "", "explicit permissions file")
	fs.BoolVar(&c.allowNoAuthz, "allow-no-authz", false, "allow startup without auth source (denies all rows)")
}

func defaultIndexDir() string {
	if d := os.Getenv("XDG_CACHE_HOME"); d != "" {
		return filepath.Join(d, "metricfs")
	}
	home, _ := os.UserHomeDir()
	if home == "" {
		return ".metricfs-cache"
	}
	return filepath.Join(home, ".cache", "metricfs")
}

func validate(c *commonFlags, needMountFields bool) error {
	if c.sourceDir == "" {
		return fmt.Errorf("--source-dir is required")
	}
	if needMountFields && c.mountDir == "" {
		return fmt.Errorf("--mount-dir is required")
	}
	if c.mapperResolution != "nearest_ancestor" {
		return fmt.Errorf("--mapper-resolution supports nearest_ancestor only")
	}
	if c.missingMapper != "deny" && c.missingMapper != "passthrough" {
		return fmt.Errorf("--missing-mapper must be deny|passthrough")
	}
	if c.missingResourceKey != "deny" && c.missingResourceKey != "ignore" {
		return fmt.Errorf("--missing-resource-key must be deny|ignore")
	}
	if !c.readOnly {
		return fmt.Errorf("writable mode is not supported in MVP")
	}
	if st, err := os.Stat(c.sourceDir); err != nil || !st.IsDir() {
		return fmt.Errorf("source dir invalid: %s", c.sourceDir)
	}
	if needMountFields {
		if st, err := os.Stat(c.mountDir); err != nil || !st.IsDir() {
			return fmt.Errorf("mount dir invalid: %s", c.mountDir)
		}
	}
	if c.authBackend != "file" && c.authBackend != "spicedb" {
		return fmt.Errorf("--auth-backend must be file|spicedb")
	}
	if c.authBackend == "file" && c.permissionsFile == "" && !c.allowNoAuthz {
		return fmt.Errorf("file auth backend requires --permissions-file or --allow-no-authz")
	}
	if c.authBackend == "spicedb" {
		if c.spiceEndpoint == "" {
			return fmt.Errorf("spicedb auth backend requires --spicedb-endpoint")
		}
		if c.subject == "" {
			return fmt.Errorf("spicedb auth backend requires --subject")
		}
	}
	return nil
}

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	switch os.Args[1] {
	case "validate-flags":
		if err := runValidate(os.Args[2:]); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(2)
		}
	case "warm-index":
		if err := runWarmIndex(os.Args[2:]); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(2)
		}
	case "mount":
		if err := runMount(os.Args[2:]); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(3)
		}
	case "stats":
		if err := runStats(os.Args[2:]); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(2)
		}
	case "render":
		if err := runRender(os.Args[2:]); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(2)
		}
	default:
		usage()
		os.Exit(2)
	}
}

func usage() {
	fmt.Println("metricfs <mount|validate-flags|warm-index|stats|render>")
}

func runValidate(args []string) error {
	fs := flag.NewFlagSet("validate-flags", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	var c commonFlags
	addCommonFlags(fs, &c, true)
	if err := fs.Parse(args); err != nil {
		return err
	}
	return validate(&c, true)
}

func runWarmIndex(args []string) error {
	fs := flag.NewFlagSet("warm-index", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	var c commonFlags
	addCommonFlags(fs, &c, false)
	if err := fs.Parse(args); err != nil {
		return err
	}
	if err := validate(&c, false); err != nil {
		return err
	}
	count := 0
	err := filepath.WalkDir(c.sourceDir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		if !strings.HasSuffix(d.Name(), ".jsonl") {
			return nil
		}
		_, err = indexer.BuildOrLoad(path, indexer.Options{
			SourceDir:         c.sourceDir,
			MapperFileName:    c.mapperFileName,
			MapperInherit:     c.mapperInheritParent,
			MissingMapperMode: c.missingMapper,
			MissingResource:   c.missingResourceKey,
			IndexDir:          c.indexDir,
			FormatVersion:     c.indexFormatVersion,
		})
		if err != nil {
			return err
		}
		count++
		return nil
	})
	if err != nil {
		return err
	}
	fmt.Printf("warmed %d jsonl files\n", count)
	return nil
}

func runMount(args []string) error {
	fs := flag.NewFlagSet("mount", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	var c commonFlags
	addCommonFlags(fs, &c, true)
	if err := fs.Parse(args); err != nil {
		return err
	}
	if err := validate(&c, true); err != nil {
		return err
	}
	az, err := newAuthorizer(c)
	if err != nil {
		return err
	}
	if cl, ok := az.(io.Closer); ok {
		defer func() { _ = cl.Close() }()
	}
	srv := fusefs.New(fusefs.Config{
		SourceDir:          c.sourceDir,
		MountDir:           c.mountDir,
		MapperFileName:     c.mapperFileName,
		MapperInherit:      c.mapperInheritParent,
		MissingMapperMode:  c.missingMapper,
		MissingResource:    c.missingResourceKey,
		IndexDir:           c.indexDir,
		IndexFormatVersion: c.indexFormatVersion,
		AllowOther:         c.allowOther,
		ReadOnly:           c.readOnly,
	}, az)

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	fmt.Printf("mounted metricfs at %s\n", c.mountDir)
	return srv.MountAndServe(ctx)
}

func runStats(args []string) error {
	fs := flag.NewFlagSet("stats", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	mountDir := fs.String("mount", "", "mount path")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *mountDir == "" {
		return fmt.Errorf("--mount is required")
	}
	var files int
	var bytes int64
	err := filepath.WalkDir(*mountDir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		files++
		st, err := d.Info()
		if err == nil {
			bytes += st.Size()
		}
		return nil
	})
	if err != nil {
		return err
	}
	fmt.Printf("files=%d bytes=%d\n", files, bytes)
	return nil
}

func runRender(args []string) error {
	fs := flag.NewFlagSet("render", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	var c commonFlags
	addCommonFlags(fs, &c, false)
	filePath := fs.String("file", "", "source file to render filtered output")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *filePath == "" {
		return fmt.Errorf("--file is required")
	}
	if c.sourceDir == "" {
		c.sourceDir = filepath.Dir(*filePath)
	}
	if err := validate(&c, false); err != nil {
		return err
	}
	az, err := newAuthorizer(c)
	if err != nil {
		return err
	}
	if cl, ok := az.(io.Closer); ok {
		defer func() { _ = cl.Close() }()
	}
	return projector.RenderFiltered(*filePath, projector.Options{
		SourceDir:         c.sourceDir,
		MapperFileName:    c.mapperFileName,
		MapperInherit:     c.mapperInheritParent,
		MissingMapperMode: c.missingMapper,
		MissingResource:   c.missingResourceKey,
		IndexDir:          c.indexDir,
		FormatVersion:     c.indexFormatVersion,
	}, az, os.Stdout)
}

func newAuthorizer(c commonFlags) (auth.Authorizer, error) {
	switch c.authBackend {
	case "file":
		if c.permissionsFile == "" {
			return auth.NewDenyAll(), nil
		}
		return auth.New(c.permissionsFile)
	case "spicedb":
		token := strings.TrimSpace(c.spiceToken)
		if token == "" && c.spiceTokenEnv != "" {
			token = strings.TrimSpace(os.Getenv(c.spiceTokenEnv))
		}
		if token == "" {
			return nil, fmt.Errorf("spicedb auth backend requires --spicedb-token or %s env var", c.spiceTokenEnv)
		}
		return auth.NewSpiceDB(auth.SpiceDBConfig{
			Endpoint:    c.spiceEndpoint,
			Token:       token,
			Subject:     c.subject,
			Consistency: c.spiceConsistency,
		})
	default:
		return nil, fmt.Errorf("unsupported --auth-backend: %s", c.authBackend)
	}
}
