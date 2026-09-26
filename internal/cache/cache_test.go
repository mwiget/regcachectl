package cache

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// fakeRuntime installs a shell script standing in for docker: it appends each
// invocation's argv to a log and answers the queries the engine makes. Only
// the dockerhub and f5 caches exist and run; the dockerhub container runs
// registry:2.8.3. FAKE_GC_FAIL=1 makes the garbage-collect helper fail.
func fakeRuntime(t *testing.T) (e *Engine, log func() []string, out *bytes.Buffer) {
	t.Helper()
	dir := t.TempDir()
	logf := filepath.Join(dir, "calls")
	script := `#!/bin/sh
echo "$*" >> ` + logf + `
case "$1" in
ps)
  case "$*" in
    *regcache-dockerhub*|*regcache-f5*) echo running ;;
  esac ;;
inspect) echo registry:2.8.3 ;;
exec)
  case "$*" in
    *" find "*) [ -n "$FAKE_FIND" ] && cat "$FAKE_FIND" ;;
    *"/okd/content/_layers/sha256") [ -n "$FAKE_LAYERS" ] && cat "$FAKE_LAYERS" ;;
    *"/okd/content/_manifests/revisions/sha256") [ -n "$FAKE_REVS" ] && cat "$FAKE_REVS" ;;
  esac ;;
images) [ -z "$FAKE_NO_IMAGE" ] && echo deadbeef ;;
volume) shift; case "$1" in ls) echo "$4" | sed 's/^name=^//; s/\$$//' ;; esac ;;
run)
  case "$*" in
    *garbage-collect*)
      if [ -n "$FAKE_GC_FAIL" ]; then echo "failed to garbage collect" >&2; exit 1; fi
      [ -n "$FAKE_GC_SLEEP" ] && sleep "$FAKE_GC_SLEEP"
      echo "25 blobs marked, 3 blobs and 1 manifests eligible for deletion" ;;
  esac ;;
esac
exit 0
`
	rt := filepath.Join(dir, "docker")
	if err := os.WriteFile(rt, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	out = &bytes.Buffer{}
	e = &Engine{Runtime: rt, Out: out}
	log = func() []string {
		b, _ := os.ReadFile(logf)
		return strings.Split(strings.TrimSpace(string(b)), "\n")
	}
	return e, log, out
}

// index of the first logged call starting with prefix, or -1.
func callIndex(calls []string, prefix string) int {
	for i, c := range calls {
		if strings.HasPrefix(c, prefix) {
			return i
		}
	}
	return -1
}

func TestGC_KeepsDigestPulledImagesByDefault(t *testing.T) {
	e, log, out := fakeRuntime(t)
	if err := e.GC(context.Background(), false); err != nil {
		t.Fatal(err)
	}
	calls := log()
	gc := callIndex(calls, "run --rm -v tmm-regcache-dockerhub:/var/lib/registry")
	if gc < 0 {
		t.Fatalf("no garbage-collect helper run:\n%s", strings.Join(calls, "\n"))
	}
	if strings.Contains(calls[gc], "--delete-untagged") {
		t.Errorf("default gc passes --delete-untagged, which deletes every digest-pulled image:\n%s", calls[gc])
	}
	if !strings.Contains(calls[gc], " registry:2.8.3 ") {
		t.Errorf("gc helper does not run the cache's own image:\n%s", calls[gc])
	}
	if !strings.Contains(out.String(), "25 blobs kept, 3 blobs and 1 manifests deleted") {
		t.Errorf("gc summary not reported:\n%s", out.String())
	}
}

func TestGC_DeleteUntaggedIsOptIn(t *testing.T) {
	e, log, _ := fakeRuntime(t)
	if err := e.GC(context.Background(), true); err != nil {
		t.Fatal(err)
	}
	calls := log()
	gc := callIndex(calls, "run --rm -v tmm-regcache-dockerhub:/var/lib/registry")
	if gc < 0 || !strings.HasSuffix(calls[gc], " --delete-untagged") {
		t.Errorf("--delete-untagged not passed through when asked for:\n%s", strings.Join(calls, "\n"))
	}
}

// garbage-collect must not run against a serving registry: stop, collect,
// start — and start again even when the collection fails.
func TestGC_StopsTheCacheAroundTheRun(t *testing.T) {
	for _, fail := range []bool{false, true} {
		e, log, _ := fakeRuntime(t)
		if fail {
			t.Setenv("FAKE_GC_FAIL", "1")
		}
		err := e.GC(context.Background(), false)
		if fail != (err != nil) {
			t.Errorf("fail=%v: GC error = %v", fail, err)
		}
		calls := log()
		stop := callIndex(calls, "stop regcache-dockerhub")
		gc := callIndex(calls, "run --rm -v tmm-regcache-dockerhub:/var/lib/registry")
		start := callIndex(calls, "start regcache-dockerhub")
		if !(stop >= 0 && stop < gc && gc < start) {
			t.Errorf("fail=%v: want stop < gc < start, got %d < %d < %d:\n%s", fail, stop, gc, start, strings.Join(calls, "\n"))
		}
		if callIndex(calls, "exec") >= 0 {
			t.Errorf("fail=%v: gc still execs into the serving container:\n%s", fail, strings.Join(calls, "\n"))
		}
		// the blob cache is never stopped.
		if callIndex(calls, "stop regcache-f5") >= 0 {
			t.Errorf("fail=%v: gc stopped the blob cache", fail)
		}
	}
}

func TestGCScript_PicksTheImagesConfig(t *testing.T) {
	// v3 moved the config; 2.8 has only the old path.
	for _, p := range []string{"/etc/distribution/config.yml", "/etc/docker/registry/config.yml"} {
		if !strings.Contains(gcScript, p) {
			t.Errorf("gc script does not consider %s", p)
		}
	}
	if !strings.HasPrefix(gcScript, "cfg=/etc/distribution/config.yml;") {
		t.Errorf("gc script must prefer the v3 path, falling back to 2.8's: %s", gcScript)
	}
}

func TestGCSummary(t *testing.T) {
	out := "noise\n12 blobs marked, 0 blobs and 0 manifests eligible for deletion\n"
	if got, want := gcSummary(out), "12 blobs kept, 0 blobs and 0 manifests deleted"; got != want {
		t.Errorf("gcSummary = %q, want %q", got, want)
	}
	if got := gcSummary("a\nlast"); got != "last" {
		t.Errorf("gcSummary fallback = %q, want the last line", got)
	}
}

func TestRunArgs_ProxyTTL(t *testing.T) {
	u := Upstreams[2]
	for _, c := range []struct {
		ttl  time.Duration
		want string
	}{
		{0, "REGISTRY_PROXY_TTL=0s"},
		{720 * time.Hour, "REGISTRY_PROXY_TTL=720h0m0s"},
	} {
		args := strings.Join((&Engine{ProxyTTL: c.ttl}).runArgs(u, 5002), " ")
		for _, w := range []string{c.want, "REGISTRY_PROXY_CACHEWRITETIMEOUT=1h", "REGISTRY_LOG_LEVEL=info", " " + RegistryImage} {
			if !strings.Contains(args, w) {
				t.Errorf("ttl %v: run args missing %q:\n%s", c.ttl, w, args)
			}
		}
	}
}

func TestRunArgs_BlobcacheHasNoProxyEnv(t *testing.T) {
	args := strings.Join((&Engine{}).runArgs(Upstreams[3], 5003), " ")
	if strings.Contains(args, "REGISTRY_") {
		t.Errorf("blob cache got registry proxy settings:\n%s", args)
	}
}

func TestUp_NotesImageDrift(t *testing.T) {
	e, log, out := fakeRuntime(t)
	if err := e.Up(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "runs registry:2.8.3, configured "+RegistryImage) {
		t.Errorf("up does not say the running cache uses another image:\n%s", out.String())
	}
	// it is a note: the running cache is not touched.
	for _, verb := range []string{"stop", "rm", "start regcache-dockerhub"} {
		if i := callIndex(log(), verb); i >= 0 {
			t.Errorf("up ran %q on an existing cache", log()[i])
		}
	}
	e2, _, out2 := fakeRuntime(t)
	e2.Image = "registry:2.8.3"
	if err := e2.Up(context.Background()); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(out2.String(), "configured") {
		t.Errorf("up reports drift when the image matches:\n%s", out2.String())
	}
}

// A fleet created under the old default image has no copy of the new one:
// export pulls it rather than failing.
func TestExport_PullsAMissingHelperImage(t *testing.T) {
	e, log, _ := fakeRuntime(t)
	t.Setenv("FAKE_NO_IMAGE", "1")
	if err := e.Export(context.Background(), filepath.Join(t.TempDir(), "x.tgz"), []string{"quay"}); err != nil {
		t.Fatal(err)
	}
	pull := callIndex(log(), "pull "+RegistryImage)
	run := callIndex(log(), "run --rm -v tmm-regcache-quay:/caches/quay:ro "+RegistryImage+" tar czf")
	if pull < 0 || run < pull {
		t.Errorf("export did not pull the helper before using it:\n%s", strings.Join(log(), "\n"))
	}
}

// Ctrl-C during the collection must still start the cache again.
func TestGC_InterruptedStillStartsTheCache(t *testing.T) {
	e, log, _ := fakeRuntime(t)
	t.Setenv("FAKE_GC_SLEEP", "2")
	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(300*time.Millisecond, cancel)
	if err := e.GC(ctx, false); err == nil {
		t.Fatal("interrupted gc reported success")
	}
	if callIndex(log(), "start regcache-dockerhub") < 0 {
		t.Errorf("interrupted gc left the cache stopped:\n%s", strings.Join(log(), "\n"))
	}
}
