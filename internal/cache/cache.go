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
	"strings"

	"github.com/mwiget/regcachectl/internal/far"
)

// RegistryImage is the pull-through cache image. registry:2 is the CNCF
// distribution registry; in proxy mode (REGISTRY_PROXY_REMOTEURL) it is a
// transparent caching mirror. Override with --image / RegcacheImage.
const RegistryImage = "registry:2.8.3"

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
	Name    string // short id and container/volume suffix
	Host    string // the registry hostname clients pull from
	Remote  string // the v2 API base registry:2 proxies to
	AuthFAR bool   // pull upstream creds from the FAR tgz
}

// Upstreams is the fixed set of registries every supported ctl tool pulls
// from (see tmmlitectl AGENTS.md image inventory).
var Upstreams = []Upstream{
	{Name: "dockerhub", Host: "docker.io", Remote: "https://registry-1.docker.io"},
	{Name: "ghcr", Host: "ghcr.io", Remote: "https://ghcr.io"},
	{Name: "quay", Host: "quay.io", Remote: "https://quay.io"},
	{Name: "f5", Host: "repo.f5.com", Remote: "https://repo.f5.com", AuthFAR: true},
}

// Engine drives the fleet against a chosen runtime.
type Engine struct {
	Runtime  string // "docker" or "podman"
	Image    string // override RegistryImage
	PortBase int
	Out      io.Writer
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

// Port returns the host port for upstream index i.
func (e *Engine) Port(i int) int { return e.portBase() + i }

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

// Up brings up (or reconciles) the whole fleet. farKey may be empty, in
// which case the F5 cache is skipped with a warning and the public caches
// still come up. Idempotent: existing containers are left running.
func (e *Engine) Up(ctx context.Context, farKey string) error {
	var creds far.Creds
	haveCreds := false
	if farKey != "" {
		c, err := far.Extract(farKey)
		if err != nil {
			return fmt.Errorf("FAR key: %w", err)
		}
		creds, haveCreds = c, true
	}

	for i, u := range Upstreams {
		if u.AuthFAR && !haveCreds {
			e.logf("  ! %-9s skipped — needs --far-key for %s (public caches still up)", u.Name, u.Host)
			continue
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
		args := []string{
			"run", "-d",
			"--name", container(u),
			"--label", label,
			"--label", "tmm-regcache.host=" + u.Host,
			"--restart=always",
			"-p", fmt.Sprintf("%d:5000", port),
			"-v", volume(u) + ":/var/lib/registry",
			"-e", "REGISTRY_PROXY_REMOTEURL=" + u.Remote,
			"-e", "REGISTRY_STORAGE_DELETE_ENABLED=true",
		}
		if u.AuthFAR {
			args = append(args,
				"-e", "REGISTRY_PROXY_USERNAME="+creds.Username,
				"-e", "REGISTRY_PROXY_PASSWORD="+creds.Password,
			)
		}
		args = append(args, e.ImageName())
		if _, err := e.run(ctx, args...); err != nil {
			return fmt.Errorf("start %s: %w", u.Name, err)
		}
		e.logf("  + %-9s created   %s → :%d  (proxy %s)", u.Name, u.Host, port, u.Remote)
	}
	return nil
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
			if du, err := e.run(ctx, "exec", container(u), "du", "-sh", "/var/lib/registry"); err == nil {
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
