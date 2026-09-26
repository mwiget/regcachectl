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
	"net/http"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// RegistryImage is the pull-through cache image for the public, anonymous
// upstreams: the CNCF distribution registry, which in proxy mode
// (REGISTRY_PROXY_REMOTEURL) is a transparent caching mirror.
//
// distribution v3 rather than registry:2.8.3, because 2.8's proxy deletes every
// blob and manifest it cached a fixed 7 days after first fetching it (a
// hard-coded repositoryTTL, not refreshed by HITs), so a warmed OKD payload or
// an imported air-gap bundle silently expires. v3 makes that TTL configurable
// (proxy.ttl, 0 disables it) and reads the same on-disk layout, so an existing
// 2.8.3 volume serves unchanged (and back again, if rolled back).
const RegistryImage = "registry:3.1.2"

// DefaultProxyTTL is how long a registry proxy keeps content it fetched: 0
// keeps it until `gc --delete-untagged` or `down --purge`. Tags are still
// re-resolved against the upstream on every pull, so a moved tag is followed.
const DefaultProxyTTL time.Duration = 0

// cacheWriteTimeout bounds how long distribution v3 may take to store one
// blob it is proxying (its own default is 5m, which a multi-GB layer over a
// slow upstream exceeds, leaving the blob served but never cached).
const cacheWriteTimeout = "1h"

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

	// transport, when set, carries the registry HTTP requests of pull and
	// pull-release (tests observe how bodies are consumed through it).
	transport http.RoundTripper

	// ProxyTTL is the registry proxy's content expiry (proxy.ttl); 0 never
	// expires. Only distribution v3 honours it — registry:2.8 ignores the
	// variable and keeps its fixed 7 days.
	ProxyTTL time.Duration
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
			if !u.Blobcache {
				e.noteImageDrift(ctx, u)
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
	// anonymous registry pull-through cache.
	return append(base,
		"-v", volume(u)+":/var/lib/registry",
		"-e", "REGISTRY_PROXY_REMOTEURL="+u.Remote,
		"-e", "REGISTRY_PROXY_TTL="+e.ProxyTTL.String(),
		"-e", "REGISTRY_PROXY_CACHEWRITETIMEOUT="+cacheWriteTimeout,
		"-e", "REGISTRY_STORAGE_DELETE_ENABLED=true",
		// the v3 image's bundled config logs at debug.
		"-e", "REGISTRY_LOG_LEVEL=info",
		e.ImageName(),
	)
}

// noteImageDrift says when an existing cache runs another image than the one
// configured: `up` never recreates a running cache, so a new default image (or
// --image) only takes effect after `down` + `up`, which keeps the volume.
func (e *Engine) noteImageDrift(ctx context.Context, u Upstream) {
	img, err := e.containerImage(ctx, container(u))
	if err != nil || img == "" || img == e.ImageName() {
		return
	}
	e.logf("    %-9s runs %s, configured %s — `regcachectl down && regcachectl up` switches it (cached data is kept)",
		"", img, e.ImageName())
}

// containerImage is the image a container was created from.
func (e *Engine) containerImage(ctx context.Context, name string) (string, error) {
	return e.run(ctx, "inspect", "--format", "{{.Config.Image}}", name)
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

// GC runs registry garbage-collect against every running registry cache.
//
// It never deletes a manifest the cache holds by default. `--delete-untagged`
// would: a pull-through cache records a tag only when a client pulls by tag,
// so everything pulled by digest — an OKD release payload, a digest-pinned
// helm chart image — is "untagged" and is swept with its layers. registry:2.8
// additionally aborts the whole run ("failed to retrieve tags unknown
// repository") on a repository that holds no tag at all. deleteUntagged opts
// into it anyway, for reclaiming space when losing digest pins is acceptable.
//
// Each cache is stopped for the run and started again afterwards, even when
// the run fails or is interrupted. garbage-collect against a serving registry
// is unsafe: distribution's docs require it "in read-only mode or not running
// at all", since layers written during the run can be deleted — and in a proxy
// every cache miss is a write. The serving process also keeps a stale
// in-memory descriptor of every blob it deletes and answers 200 with the full
// Content-Length and an empty body (a client's "unexpected EOF") until it
// restarts.
func (e *Engine) GC(ctx context.Context, deleteUntagged bool) error {
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
		if err := e.gcOne(ctx, u, deleteUntagged); err != nil {
			return fmt.Errorf("gc %s: %w", u.Name, err)
		}
	}
	return nil
}

// gcScript runs garbage-collect with whichever config the image ships:
// distribution v3 moved it from /etc/docker/registry to /etc/distribution.
const gcScript = `cfg=/etc/distribution/config.yml; [ -f "$cfg" ] || cfg=/etc/docker/registry/config.yml; exec registry garbage-collect "$cfg" "$@"`

func (e *Engine) gcOne(ctx context.Context, u Upstream, deleteUntagged bool) (err error) {
	img, err := e.containerImage(ctx, container(u))
	if err != nil {
		return err
	}
	if _, err := e.run(ctx, "stop", container(u)); err != nil {
		return err
	}
	defer func() {
		// not ctx: an interrupted gc must not leave the cache stopped (a
		// stopped container is not revived by --restart=always).
		if _, serr := e.run(context.Background(), "start", container(u)); serr != nil && err == nil {
			err = serr
		}
	}()
	args := []string{"run", "--rm", "-v", volume(u) + ":/var/lib/registry",
		"--entrypoint", "sh", img, "-c", gcScript, "gc"}
	if deleteUntagged {
		args = append(args, "--delete-untagged")
	}
	out, err := e.run(ctx, args...)
	if err != nil {
		// An empty cache has no repositories dir yet — not an error.
		if strings.Contains(err.Error(), "Path not found") {
			e.logf("  · %-9s empty", u.Name)
			return nil
		}
		return err
	}
	e.logf("  ♻ %-9s %s", u.Name, gcSummary(out))
	return nil
}

var gcCounts = regexp.MustCompile(`(\d+) blobs marked, (\d+) blobs and (\d+) manifests eligible for deletion`)

// gcSummary is garbage-collect's closing count line, or its last line.
func gcSummary(out string) string {
	if m := gcCounts.FindStringSubmatch(out); m != nil {
		marked, _ := strconv.Atoi(m[1])
		blobs, _ := strconv.Atoi(m[2])
		manifests, _ := strconv.Atoi(m[3])
		return fmt.Sprintf("%d blobs kept, %d blobs and %d manifests deleted", marked, blobs, manifests)
	}
	return lastLine(out)
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
