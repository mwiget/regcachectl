// Package cache runs a fleet of registry:2 pull-through caches — one per
// upstream registry — on the host OCI runtime (docker or podman). It is
// tool-agnostic: it programs only the local container runtime and emits a
// k3s registries.yaml snippet, so any ctl tool (tmmlitectl, ocibnkctl, …)
// can point its k3s nodes at the same fleet.
//
// One container per upstream is deliberate: containerd's registry mirror
// is keyed by upstream host and forwards the original repo path with no
// host prefix, so a single endpoint serving multiple upstreams would be
// ambiguous (docker.io/library/redis vs quay.io/x/redis collide). One
// endpoint per upstream maps 1:1 to k3s mirror semantics.
package cache

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"strconv"
	"strings"
)

// RegistryImage is the pull-through cache image for the public, anonymous
// upstreams. registry:2 is the CNCF distribution registry; in proxy mode
// (REGISTRY_PROXY_REMOTEURL) it is a transparent caching mirror.
const RegistryImage = "registry:2.8.3"

// BlobcacheImage runs regcachectl's own credential-free, redirect-following
// blob cache (cmd `serve-blobcache`) for the private GAR-backed upstream. The
// client supplies its own credential via the cluster's registries.yaml, so the
// cache stores no secret. Build it with `make blobcache-image`.
const BlobcacheImage = "regcache-blobcache:latest"

const (
	containerPrefix = "regcache-"
	volumePrefix    = "tmm-regcache-"
	label           = "tmm-regcache=1"
	// DefaultHost is the address k3s nodes use to reach the host-published
	// caches. The ctl-tool wiring adds `--add-host host.docker.internal:
	// host-gateway` to every node so this resolves to the host.
	DefaultHost = "host.docker.internal"
	// DefaultPortBase: caches publish on PortBase, PortBase+1, … on the host.
	DefaultPortBase = 5000
)

// Upstream is one registry the fleet caches.
type Upstream struct {
	Name   string // short id and container/volume suffix
	Host   string // the registry hostname clients pull from
	Remote string // the upstream v2 API base
	// Blobcache runs the credential-free, redirect-following blob cache
	// (serve-blobcache) instead of an anonymous registry:2 proxy. Used for the
	// private GAR-backed upstream where the CLIENT supplies the credential and
	// the upstream serves layers via signed-URL redirects.
	Blobcache bool
}

// Upstreams is the fixed set of registries every supported ctl tool pulls
// from (see tmmlitectl AGENTS.md image inventory). The public three are
// anonymous registry:2 pull-through caches; repo.f5.com is the credential-free
// blob cache.
var Upstreams = []Upstream{
	{Name: "dockerhub", Host: "docker.io", Remote: "https://registry-1.docker.io"},
	{Name: "ghcr", Host: "ghcr.io", Remote: "https://ghcr.io"},
	{Name: "quay", Host: "quay.io", Remote: "https://quay.io"},
	{Name: "f5", Host: "repo.f5.com", Remote: "https://repo.f5.com", Blobcache: true},
	// nvcr.io (NGC) is auth-required for every pull. It runs as an anonymous
	// registry:2 proxy like the public three — it holds no credentials and
	// relays the client's bearer token upstream. Seed it with `regcachectl pull
	// nvcr.io/...` (which supplies the NGC token, all platforms) on a host that
	// can reach nvcr.io, then `export`/`import` carries the multi-arch image to a
	// WAF-blocked host where it serves cached HITs without re-contacting nvcr.io.
	{Name: "nvcr", Host: "nvcr.io", Remote: "https://nvcr.io"},
}

// Engine drives the fleet against a chosen runtime.
type Engine struct {
	Runtime  string // "docker" or "podman"
	Image    string // override RegistryImage
	PortBase int
	// PortBaseSet reports that PortBase came from an explicit --port-base
	// flag. When false, DiscoverPorts asks the running fleet which ports it
	// actually publishes, so a fleet brought up on a non-default base stays
	// addressable without repeating the flag on every command.
	PortBaseSet bool
	Out         io.Writer

	// ports maps an upstream name to the host port its container publishes,
	// filled by DiscoverPorts. Empty until then (and for absent caches).
	ports map[string]int
}

// ImageName is the effective registry image (override or default).
func (e *Engine) ImageName() string {
	if e.Image != "" {
		return e.Image
	}
	return RegistryImage
}

func (e *Engine) portBase() int {
	if e.PortBase != 0 {
		return e.PortBase
	}
	return DefaultPortBase
}

// Port returns the host port for upstream index i: the port the existing
// container actually publishes (see DiscoverPorts) when one was found and
// --port-base was not given explicitly, else portBase+i.
func (e *Engine) Port(i int) int {
	if !e.PortBaseSet && i >= 0 && i < len(Upstreams) {
		if p, ok := e.ports[Upstreams[i].Name]; ok && p > 0 {
			return p
		}
	}
	return e.portBase() + i
}

// DiscoverPorts records the host port each existing cache container publishes,
// so read-only commands (list/status/print-registries/…) address the fleet
// that is actually running instead of the compiled-in default base. Reading
// the port from the container — the `tmm-regcache.port` label, falling back to
// the 5000/tcp binding for containers created before that label existed —
// keeps the fleet self-describing: no state file to go stale, and a fleet
// created from another shell is still found.
//
// Best-effort: a runtime that cannot be queried leaves the defaults in place.
func (e *Engine) DiscoverPorts(ctx context.Context) {
	if e.Runtime == "" {
		return
	}
	found := map[string]int{}
	for _, u := range Upstreams {
		const format = `{{index .Config.Labels "tmm-regcache.port"}}|` +
			`{{range $b := index .HostConfig.PortBindings "5000/tcp"}}{{$b.HostPort}}{{end}}`
		out, err := e.run(ctx, "inspect", "--format", format, container(u))
		if err != nil {
			continue // absent container: nothing to discover
		}
		labelled, bound, _ := strings.Cut(lastLine(out), "|")
		for _, cand := range []string{labelled, bound} {
			if p, err := strconv.Atoi(strings.TrimSpace(cand)); err == nil && p > 0 {
				found[u.Name] = p
				break
			}
		}
	}
	e.ports = found
}

// PortMismatch reports the caches whose running container publishes a port
// other than the one an explicit --port-base asks for, so a caller can warn
// instead of silently probing ports nothing listens on.
func (e *Engine) PortMismatch() []string {
	var out []string
	for i, u := range Upstreams {
		if p, ok := e.ports[u.Name]; ok && p > 0 && p != e.portBase()+i {
			out = append(out, fmt.Sprintf("%s runs on :%d, not :%d", u.Name, p, e.portBase()+i))
		}
	}
	return out
}

func container(u Upstream) string { return containerPrefix + u.Name }
func volume(u Upstream) string    { return volumePrefix + u.Name }

// DetectRuntime returns the first available runtime, preferring `prefer`.
func DetectRuntime(ctx context.Context, prefer string) (string, error) {
	cands := []string{prefer, "docker", "podman"}
	var firstErr error
	for _, rt := range cands {
		if rt == "" {
			continue
		}
		if _, err := exec.LookPath(rt); err != nil {
			if firstErr == nil {
				firstErr = fmt.Errorf("%s not found on PATH", rt)
			}
			continue
		}
		if err := exec.CommandContext(ctx, rt, "version").Run(); err != nil {
			if firstErr == nil {
				firstErr = fmt.Errorf("%s version: %w", rt, err)
			}
			continue
		}
		return rt, nil
	}
	if firstErr == nil {
		return "", errors.New("no container runtime found (tried docker and podman)")
	}
	return "", firstErr
}

// run executes the runtime CLI and returns trimmed stdout.
func (e *Engine) run(ctx context.Context, args ...string) (string, error) {
	cmd := exec.CommandContext(ctx, e.Runtime, args...)
	var out, errb bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &errb
	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("%s %s: %w (%s)", e.Runtime, strings.Join(args, " "), err, strings.TrimSpace(errb.String()))
	}
	return strings.TrimSpace(out.String()), nil
}

func (e *Engine) logf(format string, a ...any) {
	if e.Out != nil {
		fmt.Fprintf(e.Out, format+"\n", a...)
	}
}

// Up brings up (or reconciles) the whole fleet. No credentials are needed or
// stored: the public upstreams are anonymous registry:2 caches and the private
// upstream is the credential-free blob cache (clients supply their own key via
// the cluster's registries.yaml). Idempotent: existing containers are left
// running. The blob cache requires its image (see `make blobcache-image`).
func (e *Engine) Up(ctx context.Context) error {
	for i, u := range Upstreams {
		if u.Blobcache {
			if ok, err := e.imageExists(ctx, BlobcacheImage); err != nil {
				return err
			} else if !ok {
				e.logf("  ! %-9s skipped — blob-cache image %s not found (run `make blobcache-image`); public caches still up", u.Name, BlobcacheImage)
				continue
			}
		}
		if err := e.ensureVolume(ctx, volume(u)); err != nil {
			return err
		}
		exists, running, err := e.containerState(ctx, container(u))
		if err != nil {
			return err
		}
		port := e.Port(i)
		if exists {
			if !running {
				if _, err := e.run(ctx, "start", container(u)); err != nil {
					return err
				}
				e.logf("  ↑ %-9s started   %s → :%d", u.Name, u.Host, port)
			} else {
				e.logf("  = %-9s running   %s → :%d", u.Name, u.Host, port)
			}
			continue
		}
		if _, err := e.run(ctx, e.runArgs(u, port)...); err != nil {
			return fmt.Errorf("start %s: %w", u.Name, err)
		}
		kind := "proxy"
		if u.Blobcache {
			kind = "blobcache, no creds"
		}
		e.logf("  + %-9s created   %s → :%d  (%s %s)", u.Name, u.Host, port, kind, u.Remote)
	}
	return nil
}

// runArgs builds the `<runtime> run` argv for one upstream cache.
func (e *Engine) runArgs(u Upstream, port int) []string {
	base := []string{
		"run", "-d",
		"--name", container(u),
		"--label", label,
		"--label", "tmm-regcache.host=" + u.Host,
		// The published port, so later commands can discover where this
		// fleet lives instead of assuming DefaultPortBase (DiscoverPorts).
		"--label", "tmm-regcache.port=" + strconv.Itoa(port),
		"--restart=always",
		"-p", fmt.Sprintf("%d:5000", port),
	}
	if u.Blobcache {
		// credential-free blob cache: client supplies auth via registries.yaml.
		return append(base,
			"-v", volume(u)+":/var/lib/blobcache",
			BlobcacheImage,
			"serve-blobcache", "--upstream", u.Remote,
			"--listen", ":5000", "--cache-dir", "/var/lib/blobcache",
		)
	}
	// anonymous registry:2 pull-through cache.
	return append(base,
		"-v", volume(u)+":/var/lib/registry",
		"-e", "REGISTRY_PROXY_REMOTEURL="+u.Remote,
		"-e", "REGISTRY_STORAGE_DELETE_ENABLED=true",
		e.ImageName(),
	)
}

// imageExists reports whether the runtime has the named image locally.
func (e *Engine) imageExists(ctx context.Context, ref string) (bool, error) {
	out, err := e.run(ctx, "images", "-q", ref)
	if err != nil {
		return false, err
	}
	return out != "", nil
}

// Down stops and removes the fleet. purge also deletes the cached blobs.
func (e *Engine) Down(ctx context.Context, purge bool) error {
	for _, u := range Upstreams {
		exists, _, err := e.containerState(ctx, container(u))
		if err != nil {
			return err
		}
		if exists {
			if _, err := e.run(ctx, "rm", "-f", container(u)); err != nil {
				return err
			}
			e.logf("  - %-9s removed", u.Name)
		}
		if purge {
			// Ignore "no such volume".
			_, _ = e.run(ctx, "volume", "rm", volume(u))
			e.logf("  x %-9s volume purged", u.Name)
		}
	}
	return nil
}

// Status reports per-cache state.
type Status struct {
	Name      string
	Host      string
	Port      int
	State     string // running / stopped / absent
	DiskUsage string // human-readable, "" if unknown
	Reachable string // OK / error text
}

// Status collects the state of every cache in the fleet.
func (e *Engine) Status(ctx context.Context) ([]Status, error) {
	var out []Status
	for i, u := range Upstreams {
		s := Status{Name: u.Name, Host: u.Host, Port: e.Port(i), State: "absent"}
		exists, running, err := e.containerState(ctx, container(u))
		if err != nil {
			return nil, err
		}
		switch {
		case exists && running:
			s.State = "running"
			if u.Blobcache {
				// distroless container — size comes from the /_cache endpoint.
				var st struct {
					TotalBytes int64 `json:"total_bytes"`
				}
				if err := e.httpJSON(ctx, s.Port, "/_cache", &st); err == nil {
					s.DiskUsage = HumanBytes(st.TotalBytes)
				}
			} else if du, err := e.run(ctx, "exec", container(u), "du", "-sh", "/var/lib/registry"); err == nil {
				s.DiskUsage = strings.Fields(du)[0]
			}
		case exists:
			s.State = "stopped"
		}
		out = append(out, s)
	}
	return out, nil
}

// GC runs registry garbage-collect inside every running cache.
func (e *Engine) GC(ctx context.Context) error {
	for _, u := range Upstreams {
		_, running, err := e.containerState(ctx, container(u))
		if err != nil {
			return err
		}
		if !running {
			continue
		}
		if u.Blobcache {
			// The blob cache is digest-keyed and immutable; nothing to mark/
			// sweep. (Reclaim space by purging its volume on `down --purge`.)
			e.logf("  · %-9s blobcache (digest-keyed; no gc)", u.Name)
			continue
		}
		out, err := e.run(ctx, "exec", container(u),
			"registry", "garbage-collect", "/etc/docker/registry/config.yml", "--delete-untagged")
		if err != nil {
			// An empty cache has no repositories dir yet — not an error.
			if strings.Contains(err.Error(), "Path not found") || strings.Contains(err.Error(), "repositories") {
				e.logf("  · %-9s empty", u.Name)
				continue
			}
			return fmt.Errorf("gc %s: %w", u.Name, err)
		}
		e.logf("  ♻ %-9s %s", u.Name, lastLine(out))
	}
	return nil
}

func (e *Engine) ensureVolume(ctx context.Context, name string) error {
	if out, _ := e.run(ctx, "volume", "ls", "-q", "-f", "name=^"+name+"$"); out == name {
		return nil
	}
	_, err := e.run(ctx, "volume", "create", name)
	return err
}

// containerState returns (exists, running). Absent containers are not an
// error.
func (e *Engine) containerState(ctx context.Context, name string) (exists, running bool, err error) {
	out, err := e.run(ctx, "ps", "-a", "--filter", "name=^"+name+"$", "--format", "{{.State}}")
	if err != nil {
		return false, false, err
	}
	if out == "" {
		return false, false, nil
	}
	return true, out == "running", nil
}

func lastLine(s string) string {
	lines := strings.Split(strings.TrimSpace(s), "\n")
	return lines[len(lines)-1]
}
