// regcachectl runs a local fleet of registry:2 pull-through caches — one
// per upstream registry (docker.io, ghcr.io, quay.io, repo.f5.com) — so
// repeatedly created/destroyed k3s-in-docker clusters (tmmlitectl,
// ocibnkctl, …) stop re-pulling the same images from the public and
// private registries on every rebuild.
//
// The fleet is tool-agnostic: it programs only the local container runtime
// and emits a k3s registries.yaml snippet. Caches use --restart=always +
// persistent volumes, so they survive cluster teardown AND host reboots.
//
//	regcachectl up [--far-key keys/f5-far-auth-key.tgz]
//	regcachectl status
//	regcachectl print-registries [--host host.docker.internal] [--no-fallback]
//	regcachectl gc
//	regcachectl down [--purge]
//	regcachectl install-systemd [--far-key …] [--write]
package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"text/tabwriter"
	"time"

	"github.com/mwiget/regcachectl/internal/blobcache"
	"github.com/mwiget/regcachectl/internal/cache"
)

const version = "0.1.0"

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	if err := run(ctx, os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "regcachectl:", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, argv []string) error {
	if len(argv) == 0 {
		usage()
		return fmt.Errorf("no command given")
	}
	cmd, rest := argv[0], argv[1:]

	// runtimeFlags registers the flags every runtime-touching command shares.
	type rtFlags struct {
		runtime, image, host *string
		portBase             *int
	}
	addRuntimeFlags := func(fs *flag.FlagSet) rtFlags {
		return rtFlags{
			runtime:  fs.String("runtime", "", "container runtime (docker|podman; autodetect if empty)"),
			image:    fs.String("image", "", "registry image override (default "+cache.RegistryImage+")"),
			host:     fs.String("host", cache.DefaultHost, "address k3s nodes use to reach the host"),
			portBase: fs.Int("port-base", cache.DefaultPortBase, "host port of the first cache"),
		}
	}

	switch cmd {
	case "up":
		fs := flag.NewFlagSet("up", flag.ExitOnError)
		f := addRuntimeFlags(fs)
		_ = fs.Parse(rest)
		e, err := buildEngine(ctx, *f.runtime, *f.image, *f.portBase)
		if err != nil {
			return err
		}
		fmt.Println("Bringing up pull-through cache fleet (" + e.Runtime + ", holds no credentials):")
		return e.Up(ctx)

	case "down":
		fs := flag.NewFlagSet("down", flag.ExitOnError)
		f := addRuntimeFlags(fs)
		purge := fs.Bool("purge", false, "also delete cached blobs (volumes)")
		_ = fs.Parse(rest)
		e, err := buildEngine(ctx, *f.runtime, *f.image, *f.portBase)
		if err != nil {
			return err
		}
		fmt.Println("Tearing down cache fleet:")
		return e.Down(ctx, *purge)

	case "status":
		fs := flag.NewFlagSet("status", flag.ExitOnError)
		f := addRuntimeFlags(fs)
		_ = fs.Parse(rest)
		e, err := buildEngine(ctx, *f.runtime, *f.image, *f.portBase)
		if err != nil {
			return err
		}
		return printStatus(ctx, e)

	case "gc":
		fs := flag.NewFlagSet("gc", flag.ExitOnError)
		f := addRuntimeFlags(fs)
		_ = fs.Parse(rest)
		e, err := buildEngine(ctx, *f.runtime, *f.image, *f.portBase)
		if err != nil {
			return err
		}
		fmt.Println("Garbage-collecting caches:")
		return e.GC(ctx)

	case "print-registries":
		fs := flag.NewFlagSet("print-registries", flag.ExitOnError)
		host := fs.String("host", cache.DefaultHost, "address k3s nodes use to reach the host")
		portBase := fs.Int("port-base", cache.DefaultPortBase, "host port of the first cache")
		noFallback := fs.Bool("no-fallback", false, "omit the direct-upstream fallback endpoint")
		_ = fs.Parse(rest)
		e := &cache.Engine{PortBase: *portBase}
		fmt.Print(e.RenderRegistries(*host, !*noFallback))
		return nil

	case "serve-blobcache":
		fs := flag.NewFlagSet("serve-blobcache", flag.ExitOnError)
		upstream := fs.String("upstream", "", "upstream registry base URL (e.g. https://repo.f5.com)")
		listen := fs.String("listen", ":5000", "listen address")
		cacheDir := fs.String("cache-dir", "/var/lib/blobcache", "blob cache directory")
		_ = fs.Parse(rest)
		if *upstream == "" {
			return fmt.Errorf("serve-blobcache: --upstream is required")
		}
		bp, err := blobcache.New(*upstream, *cacheDir)
		if err != nil {
			return err
		}
		return bp.ListenAndServe(*listen)

	case "install-systemd":
		fs := flag.NewFlagSet("install-systemd", flag.ExitOnError)
		write := fs.Bool("write", false, "write the unit to /etc/systemd/system (needs root)")
		_ = fs.Parse(rest)
		return installSystemd(*write)

	case "version", "-v", "--version":
		fmt.Println("regcachectl", version)
		return nil

	case "help", "-h", "--help":
		usage()
		return nil

	default:
		usage()
		return fmt.Errorf("unknown command %q", cmd)
	}
}

// buildEngine resolves the runtime and applies the image/port overrides.
func buildEngine(ctx context.Context, runtime, image string, portBase int) (*cache.Engine, error) {
	rt, err := cache.DetectRuntime(ctx, runtime)
	if err != nil {
		return nil, err
	}
	return &cache.Engine{Runtime: rt, Image: image, PortBase: portBase, Out: os.Stdout}, nil
}

func printStatus(ctx context.Context, e *cache.Engine) error {
	st, err := e.Status(ctx)
	if err != nil {
		return err
	}
	tw := tabwriter.NewWriter(os.Stdout, 0, 2, 2, ' ', 0)
	fmt.Fprintln(tw, "CACHE\tUPSTREAM\tPORT\tSTATE\tDISK\tREACHABLE")
	for _, s := range st {
		reach := "-"
		if s.State == "running" {
			reach = probe(ctx, s.Port)
		}
		disk := s.DiskUsage
		if disk == "" {
			disk = "-"
		}
		fmt.Fprintf(tw, "%s\t%s\t%d\t%s\t%s\t%s\n", s.Name, s.Host, s.Port, s.State, disk, reach)
	}
	return tw.Flush()
}

// probe checks the cache's /v2/ endpoint on the host. A pull-through cache
// answers 200 or 401 (registry:2 proxies anonymous to upstream) — both
// mean "reachable".
func probe(ctx context.Context, port int) string {
	url := fmt.Sprintf("http://localhost:%d/v2/", port)
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	cl := &http.Client{Timeout: 3 * time.Second}
	resp, err := cl.Do(req)
	if err != nil {
		return "DOWN"
	}
	defer resp.Body.Close()
	if resp.StatusCode == 200 || resp.StatusCode == 401 {
		return "OK"
	}
	return fmt.Sprintf("HTTP %d", resp.StatusCode)
}

func usage() {
	fmt.Fprint(os.Stderr, `regcachectl — local pull-through cache fleet for k3s-in-docker clusters

USAGE:
  regcachectl <command> [flags]

COMMANDS:
  up                 create/start the cache fleet (idempotent; holds no creds)
  down               stop & remove the fleet (--purge also drops cached blobs)
  status             show per-cache state, disk use, reachability
  gc                 run registry garbage-collect in each public cache
  print-registries   emit the k3s registries.yaml snippet to wire nodes
  serve-blobcache    (internal) run the credential-free blob cache; used by the
                     repo.f5.com container, not run by hand
  install-systemd    print/write a systemd unit so the fleet survives reboot
  version            print version

COMMON FLAGS:
  --runtime docker|podman   (autodetect if empty)
  --port-base 5000          host port of the first cache

The fleet stores NO registry credentials. Public registries are cached
anonymously; repo.f5.com is a credential-free blob cache — each client (k3s
cluster) supplies its own FAR key via its registries.yaml, so different BNK
versions can use different keys against the same cache.

EXAMPLE:
  make blobcache-image          # once, builds the repo.f5.com blob-cache image
  regcachectl up
  regcachectl print-registries > registries.yaml
  regcachectl status
`)
}
