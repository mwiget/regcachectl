package cache

import (
	"archive/tar"
	"bufio"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"
)

// imageReferencesPath is where an OpenShift/OKD release image lists its
// payload: an ImageStream whose tags name every component image by digest.
const imageReferencesPath = "release-manifests/image-references"

// DefaultReleasePlatform is the platform pull-release warms by default.
const DefaultReleasePlatform = "linux/amd64"

// PullRelease warms the caches with an OKD/OpenShift release: the release
// image itself and every payload image its release-manifests/image-references
// names, each through the cache for its registry, jobs at a time.
//
// Why: a cold cache fetching one blob for several nodes at once is where
// installs failed ("unexpected EOF (after reconnecting, server did not process
// a Range: header)"). Warmed ahead, every node pull is a HIT, and `export
// --cache quay` then carries an installable, offline release.
//
// A blob shared by several payload images is fetched once. platform selects
// the child of a multi-arch image ("" warms all); a single-platform image is
// warmed whole.
func (e *Engine) PullRelease(ctx context.Context, ref, platform string, jobs int, creds string) error {
	if jobs < 1 {
		return fmt.Errorf("pull-release: -j must be at least 1")
	}
	rf, err := parseRef(ref)
	if err != nil {
		return err
	}
	idx, ok := upstreamIndexForHost(rf.host)
	if !ok {
		return fmt.Errorf("no cache for host %q (known: %s)", rf.host, knownHosts())
	}
	seen := &sync.Map{}
	pc := e.pullClient(idx, rf.repo, creds, seen)

	start := time.Now()
	e.logf("reading %s from %s/%s:%s via :%d ...", imageReferencesPath, rf.host, rf.repo, rf.ref, e.Port(idx))
	manifest, err := pc.platformManifest(ctx, rf.ref, platform)
	if err != nil {
		return err
	}
	raw, err := pc.findFileInLayers(ctx, manifest, imageReferencesPath)
	if err != nil {
		return err
	}
	refs, err := parseImageReferences(raw)
	if err != nil {
		return err
	}
	// the release image's own layers (read above) and config.
	if _, err := pc.warmManifestBlobs(ctx, manifest); err != nil {
		return fmt.Errorf("warm release image: %w", err)
	}
	e.logf("release lists %d payload images; warming %s with -j %d ...", len(refs), platformLabel(platform), jobs)

	results := e.warmAll(ctx, refs, platform, jobs, creds, seen)
	warmErr := map[string]error{}
	var blobs int
	for _, r := range results {
		if r.err != nil {
			warmErr[r.ref] = r.err
			continue
		}
		blobs += r.stats.blobs
	}
	e.logf("warmed %d/%d payload images (%d blob references, %d blobs fetched) in %s",
		len(refs)-len(warmErr), len(refs), blobs, countSeen(seen), time.Since(start).Round(time.Second))

	// A 200 for every blob does not mean the cache kept it: the registry
	// proxy streams a blob another client is already fetching straight from
	// the upstream without storing it, and when that other client hangs up the
	// blob is stored by nobody. So count what is on disk, the way `list
	// --okd-release` does, and re-warm what is missing.
	all := append([]string{ref}, refs...)
	missing, verr := e.missingOnDisk(ctx, all, platform)
	for round := 1; verr == nil && len(missing) > 0 && round <= verifyRounds; round++ {
		e.logf("%d image(s) not fully on disk after warming; re-warming them (round %d/%d) ...", len(missing), round, verifyRounds)
		time.Sleep(reWarmPause)
		for _, r := range e.warmAll(ctx, missing, platform, jobs, creds, &sync.Map{}) {
			if r.err != nil {
				warmErr[r.ref] = r.err
			} else {
				delete(warmErr, r.ref)
			}
		}
		missing, verr = e.missingOnDisk(ctx, all, platform)
	}
	if verr != nil {
		// cannot read the store (no runtime, another registry on this port):
		// report what the warm itself saw, and say it is unverified.
		e.logf("done (NOT verified on disk: %v)", verr)
		return e.reportFailures(warmErr, len(refs), "failed")
	}
	notOnDisk := map[string]error{}
	for _, m := range missing {
		err := warmErr[m]
		if err == nil {
			err = fmt.Errorf("served, but not stored by the cache")
		}
		notOnDisk[m] = err
	}
	payloadMissing := len(missing)
	if _, releaseMissing := notOnDisk[ref]; releaseMissing {
		payloadMissing--
	}
	e.logf("done: %d/%d payload images verified on disk in %s", len(refs)-payloadMissing, len(refs), time.Since(start).Round(time.Second))
	return e.reportFailures(notOnDisk, len(refs), "not on disk")
}

// verifyRounds is how often pull-release re-warms images it found missing
// from the on-disk store before reporting them.
const verifyRounds = 2

// reWarmPause lets the fetch that left a blob unstored end first: while
// another client's fetch of it is in flight, the proxy keeps serving it
// without storing it. (Against a throwaway registry:3, one of three such
// images still needed the second round without a pause.)
var reWarmPause = 2 * time.Second

func (e *Engine) reportFailures(failed map[string]error, total int, what string) error {
	if len(failed) == 0 {
		return nil
	}
	var lines []string
	for r, err := range failed {
		lines = append(lines, fmt.Sprintf("%s: %v", r, err))
	}
	sort.Strings(lines)
	for _, l := range lines {
		e.logf("  ✗ %s", l)
	}
	return fmt.Errorf("pull-release: %d of %d payload images %s", len(failed), total, what)
}

// missingOnDisk returns the refs whose image is not completely in its cache's
// on-disk store (manifest, wanted platforms, every config and layer linked
// into the repository) — the check `list --okd-release` makes.
func (e *Engine) missingOnDisk(ctx context.Context, refs []string, platform string) ([]string, error) {
	views := map[string]storeView{}
	var missing []string
	for _, r := range refs {
		pr, err := parseRef(r)
		if err != nil {
			missing = append(missing, r)
			continue
		}
		idx, ok := upstreamIndexForHost(pr.host)
		if !ok || Upstreams[idx].Blobcache {
			missing = append(missing, r)
			continue
		}
		u := Upstreams[idx]
		v, ok := views[u.Name]
		if !ok {
			if v, err = e.openStore(ctx, u); err != nil {
				return nil, err
			}
			views[u.Name] = v
		}
		digest := pr.ref
		if !strings.HasPrefix(digest, "sha256:") {
			if digest, ok = v.tagDigest(pr.repo, pr.ref); !ok {
				missing = append(missing, r)
				continue
			}
		}
		if full, _ := imageOnDisk(v, pr.repo, digest, platform); !full {
			missing = append(missing, r)
		}
	}
	return missing, nil
}

func platformLabel(p string) string {
	if p == "" {
		return "all platforms"
	}
	return p
}

func countSeen(m *sync.Map) int {
	n := 0
	m.Range(func(_, _ any) bool { n++; return true })
	return n
}

func (e *Engine) pullClient(idx int, repo, creds string, seen *sync.Map) *pullClient {
	return &pullClient{
		hc:    &http.Client{Transport: e.transport},
		base:  fmt.Sprintf("http://localhost:%d", e.Port(idx)),
		repo:  repo,
		creds: creds,
		seen:  seen,
	}
}

type warmResult struct {
	ref   string
	stats warmStats
	err   error
}

// warmAll warms every ref through its registry's cache, at most jobs at a
// time, and returns one result per ref in input order.
func (e *Engine) warmAll(ctx context.Context, refs []string, platform string, jobs int, creds string, seen *sync.Map) []warmResult {
	results := make([]warmResult, len(refs))
	if jobs < 1 {
		jobs = 1 // zero workers would block on the first send forever
	}
	work := make(chan int)
	var wg sync.WaitGroup
	var mu sync.Mutex
	done := 0
	for w := 0; w < jobs; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range work {
				r := warmResult{ref: refs[i]}
				r.stats, r.err = e.warmRef(ctx, refs[i], platform, creds, seen)
				results[i] = r
				mu.Lock()
				done++
				status := fmt.Sprintf("%d blobs", r.stats.blobs)
				if r.err != nil {
					status = "FAILED: " + r.err.Error()
				}
				e.logf("  [%d/%d] %s — %s", done, len(refs), refs[i], status)
				mu.Unlock()
			}
		}()
	}
	for i := range refs {
		work <- i
	}
	close(work)
	wg.Wait()
	return results
}

func (e *Engine) warmRef(ctx context.Context, ref, platform, creds string, seen *sync.Map) (warmStats, error) {
	rf, err := parseRef(ref)
	if err != nil {
		return warmStats{}, err
	}
	idx, ok := upstreamIndexForHost(rf.host)
	if !ok {
		return warmStats{}, fmt.Errorf("no cache for host %q", rf.host)
	}
	if Upstreams[idx].Blobcache {
		return warmStats{}, fmt.Errorf("%s is served by the blob cache, which pull does not warm", rf.host)
	}
	return e.pullClient(idx, rf.repo, creds, seen).warmImage(ctx, rf.ref, platform, nil)
}

// platformManifest resolves ref to a single-platform image manifest: ref
// itself, or the child of its index that matches platform.
func (pc *pullClient) platformManifest(ctx context.Context, ref, platform string) ([]byte, error) {
	body, ctype, err := pc.getManifest(ctx, ref)
	if err != nil {
		return nil, err
	}
	if !isIndex(ctype) {
		return body, nil
	}
	ix, err := parseIndex(body)
	if err != nil {
		return nil, err
	}
	want := platform
	if want == "" {
		want = DefaultReleasePlatform
	}
	for _, m := range ix {
		if m.plat != "attestation" && platformMatches(want, m.plat) {
			child, _, err := pc.getManifest(ctx, m.digest)
			return child, err
		}
	}
	return nil, fmt.Errorf("%s has no manifest for platform %s", ref, want)
}

// findFileInLayers returns the content of path from the image's layers,
// reading the top layer first (the one that wins in the image's filesystem).
// Every layer it opens is read to the end, so the cache stores it whole.
func (pc *pullClient) findFileInLayers(ctx context.Context, manifest []byte, path string) ([]byte, error) {
	var m struct {
		Layers []struct {
			Digest string `json:"digest"`
		} `json:"layers"`
	}
	if err := json.Unmarshal(manifest, &m); err != nil {
		return nil, fmt.Errorf("parse manifest: %w", err)
	}
	for i := len(m.Layers) - 1; i >= 0; i-- {
		d := m.Layers[i].Digest
		content, found, err := pc.fileInBlob(ctx, d, path)
		if err != nil {
			return nil, fmt.Errorf("layer %s: %w", ShortDigest(d), err)
		}
		if found {
			return content, nil
		}
	}
	return nil, fmt.Errorf("%s not found in any of %d layers — not a release image?", path, len(m.Layers))
}

// fileInBlob streams one layer blob through the cache and looks for path in
// it. The blob is marked seen: it has been fetched in full either way.
func (pc *pullClient) fileInBlob(ctx context.Context, digest, path string) ([]byte, bool, error) {
	body, err := pc.openBlob(ctx, digest)
	if err != nil {
		return nil, false, err
	}
	defer body.Close()
	content, found, err := fileInLayer(body, path)
	if err != nil {
		return nil, false, err
	}
	// drain, so the proxy finishes storing the blob it is streaming to us.
	if _, err := io.Copy(io.Discard, body); err != nil {
		return nil, false, fmt.Errorf("stream blob: %w", err)
	}
	if pc.seen != nil {
		pc.seen.Store(pc.seenKey(digest), true)
	}
	return content, found, nil
}

// fileInLayer scans a (gzip-compressed or plain) tar layer for path. A layer
// that is not gzip and not a readable tar (zstd, say) is reported as not
// containing it; a gzip layer that breaks off is an error.
func fileInLayer(r io.Reader, path string) ([]byte, bool, error) {
	br := bufio.NewReader(r)
	var tr *tar.Reader
	gzipped := false
	if magic, err := br.Peek(2); err == nil && magic[0] == 0x1f && magic[1] == 0x8b {
		zr, err := gzip.NewReader(br)
		if err != nil {
			return nil, false, err
		}
		defer zr.Close()
		tr, gzipped = tar.NewReader(zr), true
	} else {
		tr = tar.NewReader(br)
	}
	for {
		h, err := tr.Next()
		if errors.Is(err, io.EOF) {
			return nil, false, nil
		}
		if err != nil {
			if gzipped {
				return nil, false, err
			}
			// not a tar we can read (e.g. zstd): not here, try the next layer.
			return nil, false, nil
		}
		if strings.TrimPrefix(h.Name, "./") == path && h.Typeflag == tar.TypeReg {
			b, err := io.ReadAll(tr)
			return b, err == nil, err
		}
	}
}

// openBlob GETs a blob through the cache and returns its body.
func (pc *pullClient) openBlob(ctx context.Context, digest string) (io.ReadCloser, error) {
	url := fmt.Sprintf("%s/v2/%s/blobs/%s", pc.base, pc.repo, digest)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	if pc.token != "" {
		req.Header.Set("Authorization", "Bearer "+pc.token)
	}
	resp, err := pc.hc.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		snippet, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		resp.Body.Close()
		return nil, fmt.Errorf("GET blob %s: HTTP %d: %s", ShortDigest(digest), resp.StatusCode, strings.TrimSpace(string(snippet)))
	}
	return resp.Body, nil
}

// parseImageReferences returns the distinct image pull specs a release's
// image-references ImageStream names, in the order it lists them.
func parseImageReferences(raw []byte) ([]string, error) {
	var is struct {
		Kind string `json:"kind"`
		Spec struct {
			Tags []struct {
				Name string `json:"name"`
				From struct {
					Kind string `json:"kind"`
					Name string `json:"name"`
				} `json:"from"`
			} `json:"tags"`
		} `json:"spec"`
	}
	if err := json.Unmarshal(raw, &is); err != nil {
		return nil, fmt.Errorf("parse %s: %w", imageReferencesPath, err)
	}
	if is.Kind != "ImageStream" {
		return nil, fmt.Errorf("%s is a %q, want an ImageStream", imageReferencesPath, is.Kind)
	}
	seen := map[string]bool{}
	var refs []string
	for _, t := range is.Spec.Tags {
		if t.From.Kind != "DockerImage" || t.From.Name == "" || seen[t.From.Name] {
			continue
		}
		seen[t.From.Name] = true
		refs = append(refs, t.From.Name)
	}
	if len(refs) == 0 {
		return nil, fmt.Errorf("%s lists no images", imageReferencesPath)
	}
	return refs, nil
}
