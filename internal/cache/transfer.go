package cache

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
)

// Export bundles every cache volume's on-disk data into a single .tgz at
// outPath, so the whole warm cache can be copied to another host and imported
// there (seeding it offline, no upstream re-pull). It streams through a
// throwaway helper container that mounts each volume read-only and tars them
// under <cacheName>/… — the helper is the already-present registry image (it is
// Alpine, so it has tar), adding no new dependency. Nothing here needs the
// caches to be stopped; the volumes are mounted read-only.
// only restricts the export to the named caches (e.g. {"nvcr"}); empty means all.
func (e *Engine) Export(ctx context.Context, outPath string, only []string) error {
	want := map[string]bool{}
	for _, n := range only {
		want[n] = true
	}
	var mounts, names []string
	for _, u := range Upstreams {
		if len(want) > 0 && !want[u.Name] {
			continue
		}
		if e.volumeExists(ctx, volume(u)) {
			mounts = append(mounts, "-v", volume(u)+":/caches/"+u.Name+":ro")
			names = append(names, u.Name)
		}
	}
	if len(names) == 0 {
		if len(want) > 0 {
			return fmt.Errorf("no matching cache volumes for %s (known: %s)", strings.Join(only, ", "), knownCaches())
		}
		return errors.New("no cache volumes to export — run `up` and pull some images first")
	}
	helper := e.ImageName()
	if ok, err := e.imageExists(ctx, helper); err != nil {
		return err
	} else if !ok {
		return fmt.Errorf("helper image %s not present — run `regcachectl up` first", helper)
	}

	f, err := os.Create(outPath)
	if err != nil {
		return err
	}
	defer f.Close()

	args := append([]string{"run", "--rm"}, mounts...)
	args = append(args, helper, "tar", "czf", "-", "-C", "/caches", ".")
	e.logf("exporting %s → %s ...", strings.Join(names, ", "), outPath)
	if err := e.runStream(ctx, nil, f, args...); err != nil {
		os.Remove(outPath) // don't leave a partial bundle
		return err
	}
	if fi, err := f.Stat(); err == nil {
		e.logf("wrote %s (%s) — copy it to another host and `regcachectl import` there", outPath, HumanBytes(fi.Size()))
	}
	return nil
}

// Import unpacks a bundle produced by Export into this host's cache volumes,
// creating them if missing. Cache data is content-addressed and immutable, so
// the unpack is a safe union: it seeds a fresh fleet or merges into an existing
// one without clobbering blobs. (Repo/tag sidecars on the F5 cache are
// last-writer for an overlapping digest — cosmetic only; the blobs are intact.)
// Run `regcachectl up` afterwards to serve the imported data.
func (e *Engine) Import(ctx context.Context, inPath string) error {
	f, err := os.Open(inPath)
	if err != nil {
		return err
	}
	defer f.Close()

	helper := e.ImageName()
	if ok, _ := e.imageExists(ctx, helper); !ok {
		e.logf("helper image %s absent — pulling ...", helper)
		if _, err := e.run(ctx, "pull", helper); err != nil {
			return fmt.Errorf("helper image %s not present and pull failed (%w) — run `regcachectl up` first", helper, err)
		}
	}

	var mounts []string
	for _, u := range Upstreams {
		if err := e.ensureVolume(ctx, volume(u)); err != nil {
			return err
		}
		mounts = append(mounts, "-v", volume(u)+":/caches/"+u.Name)
	}
	args := append([]string{"run", "--rm", "-i"}, mounts...)
	args = append(args, helper, "tar", "xzf", "-", "-C", "/caches")
	e.logf("importing %s into the cache volumes ...", inPath)
	if err := e.runStream(ctx, f, io.Discard, args...); err != nil {
		return err
	}
	e.logf("imported. run `regcachectl up` to (re)start the fleet serving the data.")
	return nil
}

// runStream runs the runtime CLI wiring stdin/stdout to the given streams (so a
// multi-GB bundle never buffers in memory), capturing stderr for the error.
func (e *Engine) runStream(ctx context.Context, stdin io.Reader, stdout io.Writer, args ...string) error {
	cmd := exec.CommandContext(ctx, e.Runtime, args...)
	cmd.Stdin = stdin
	cmd.Stdout = stdout
	var errb bytes.Buffer
	cmd.Stderr = &errb
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("%s %s: %w (%s)", e.Runtime, strings.Join(args, " "), err, strings.TrimSpace(errb.String()))
	}
	return nil
}

func (e *Engine) volumeExists(ctx context.Context, name string) bool {
	out, _ := e.run(ctx, "volume", "ls", "-q", "-f", "name=^"+name+"$")
	return out == name
}
