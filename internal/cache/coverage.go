package cache

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"path"
	"strings"
)

// ReleaseCoverage is how much of an OKD/OpenShift release payload one cache
// holds on disk.
type ReleaseCoverage struct {
	Cache        string   // cache name, e.g. quay
	Total        int      // payload images the release lists
	Full         int      // manifest and every config/layer blob on disk
	ManifestOnly int      // manifest on disk, a blob missing
	Missing      []string // not fully on disk (includes ManifestOnly)
	Elsewhere    int      // payload images on another registry (not checked)
}

// storeView is read-only access to what one registry cache holds on disk. has*
// look at the on-disk store; read* fetch through the cache only what has*
// confirmed is on disk, so the proxy answers locally and never goes upstream.
type storeView interface {
	hasManifest(repo, digest string) bool
	hasBlob(repo, digest string) bool // stored, and linked into repo
	tagDigest(repo, tag string) (string, bool)
	readManifest(repo, digest string) (body []byte, contentType string, err error)
	openBlob(repo, digest string) (io.ReadCloser, error)
}

// ReleaseCoverage reports how many of a release's payload images the cache
// for its registry holds completely. It reads the store without warming it: a
// release image that is not cached is an error, not a pull.
func (e *Engine) ReleaseCoverage(ctx context.Context, ref, platform string) (ReleaseCoverage, error) {
	rf, err := parseRef(ref)
	if err != nil {
		return ReleaseCoverage{}, err
	}
	idx, ok := upstreamIndexForHost(rf.host)
	if !ok || Upstreams[idx].Blobcache {
		return ReleaseCoverage{}, fmt.Errorf("no registry cache for host %q", rf.host)
	}
	u := Upstreams[idx]
	v, err := e.openStore(ctx, u)
	if err != nil {
		return ReleaseCoverage{}, err
	}
	return releaseCoverage(v, u, rf, platform)
}

// openStore gives read access to what the cache for u holds on disk: its
// running container, read by exec. It refuses when that container does not
// publish the port this engine addresses (an explicit --port-base pointing at
// another registry), since the disk read would describe a different store.
func (e *Engine) openStore(ctx context.Context, u Upstream) (storeView, error) {
	if e.storeFor != nil {
		return e.storeFor(ctx, u)
	}
	if _, running, err := e.containerState(ctx, container(u)); err != nil {
		return nil, err
	} else if !running {
		return nil, fmt.Errorf("cache %s is not running", u.Name)
	}
	idx := indexOf(u)
	if p, ok := e.ports[u.Name]; ok && p > 0 && p != e.Port(idx) {
		return nil, fmt.Errorf("%s publishes :%d, not the :%d addressed here", container(u), p, e.Port(idx))
	}
	v := &diskView{e: e, ctx: ctx, u: u, name: container(u), revs: map[string]map[string]bool{}, layers: map[string]map[string]bool{}}
	if err := v.loadBlobs(); err != nil {
		return nil, err
	}
	return v, nil
}

func releaseCoverage(v storeView, u Upstream, rf imageRef, platform string) (ReleaseCoverage, error) {
	cov := ReleaseCoverage{Cache: u.Name}
	digest := rf.ref
	if !strings.HasPrefix(digest, "sha256:") {
		d, ok := v.tagDigest(rf.repo, rf.ref)
		if !ok {
			return cov, fmt.Errorf("release %s/%s:%s has no cached tag (a release pulled by digest has none: pass @sha256:…)", rf.host, rf.repo, rf.ref)
		}
		digest = d
	}
	manifest, err := localPlatformManifest(v, rf.repo, digest, platform)
	if err != nil {
		return cov, fmt.Errorf("release image: %w", err)
	}
	raw, err := localFileInLayers(v, rf.repo, manifest, imageReferencesPath)
	if err != nil {
		return cov, fmt.Errorf("release image: %w", err)
	}
	refs, err := parseImageReferences(raw)
	if err != nil {
		return cov, err
	}
	cov.Total = len(refs)
	for _, r := range refs {
		pr, err := parseRef(r)
		if err != nil {
			return cov, err
		}
		if pr.host != u.Host {
			cov.Elsewhere++
			continue
		}
		full, manifestOnDisk := imageOnDisk(v, pr.repo, pr.ref, platform)
		switch {
		case full:
			cov.Full++
		case manifestOnDisk:
			cov.ManifestOnly++
			cov.Missing = append(cov.Missing, r)
		default:
			cov.Missing = append(cov.Missing, r)
		}
	}
	return cov, nil
}

// imageOnDisk reports whether an image is fully on disk (its manifest, the
// wanted platform's child manifests, and every config/layer blob), and whether
// at least its top manifest is.
func imageOnDisk(v storeView, repo, digest, platform string) (full, manifest bool) {
	if !v.hasManifest(repo, digest) {
		return false, false
	}
	body, ctype, err := v.readManifest(repo, digest)
	if err != nil {
		return false, true
	}
	if !isIndex(ctype) {
		return blobsOnDisk(v, repo, body), true
	}
	ix, err := parseIndex(body)
	if err != nil {
		return false, true
	}
	matched := 0
	for _, m := range ix {
		if !platformMatches(platform, m.plat) {
			continue
		}
		matched++
		if !v.hasManifest(repo, m.digest) {
			return false, true
		}
		child, _, err := v.readManifest(repo, m.digest)
		if err != nil || !blobsOnDisk(v, repo, child) {
			return false, true
		}
	}
	return matched > 0, true
}

func blobsOnDisk(v storeView, repo string, manifest []byte) bool {
	var m struct {
		Config struct {
			Digest string `json:"digest"`
		} `json:"config"`
		Layers []struct {
			Digest string `json:"digest"`
		} `json:"layers"`
	}
	if json.Unmarshal(manifest, &m) != nil {
		return false
	}
	if m.Config.Digest != "" && !v.hasBlob(repo, m.Config.Digest) {
		return false
	}
	for _, l := range m.Layers {
		if !v.hasBlob(repo, l.Digest) {
			return false
		}
	}
	return true
}

func localPlatformManifest(v storeView, repo, digest, platform string) ([]byte, error) {
	if !v.hasManifest(repo, digest) {
		return nil, fmt.Errorf("%s@%s is not cached", repo, ShortDigest(digest))
	}
	body, ctype, err := v.readManifest(repo, digest)
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
			return localPlatformManifest(v, repo, m.digest, "")
		}
	}
	return nil, fmt.Errorf("%s@%s has no manifest for platform %s", repo, ShortDigest(digest), want)
}

func localFileInLayers(v storeView, repo string, manifest []byte, file string) ([]byte, error) {
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
		if !v.hasBlob(repo, d) {
			return nil, fmt.Errorf("layer %s is not cached", ShortDigest(d))
		}
		body, err := v.openBlob(repo, d)
		if err != nil {
			return nil, err
		}
		content, found, err := fileInLayer(body, file)
		body.Close()
		if err != nil {
			return nil, fmt.Errorf("layer %s: %w", ShortDigest(d), err)
		}
		if found {
			return content, nil
		}
	}
	return nil, fmt.Errorf("%s not found in any of %d layers — not a release image?", file, len(m.Layers))
}

// diskView is storeView over a running registry cache: the on-disk store read
// by exec in the container, content fetched over its HTTP port.
type diskView struct {
	e      *Engine
	ctx    context.Context
	u      Upstream
	name   string // the cache container read by exec
	blobs  map[string]bool
	revs   map[string]map[string]bool // repo → manifest digests with a revision link
	layers map[string]map[string]bool // repo → blob digests linked into it
}

const registryRoot = "/var/lib/registry/docker/registry/v2"

func (v *diskView) loadBlobs() error {
	out, err := v.e.run(v.ctx, "exec", v.name, "find", registryRoot+"/blobs", "-name", "data", "-type", "f")
	if err != nil {
		return fmt.Errorf("list %s blobs: %w", v.u.Name, err)
	}
	v.blobs = map[string]bool{}
	for _, p := range strings.Fields(out) {
		// …/blobs/sha256/ab/<hex>/data
		dir := path.Dir(p)
		v.blobs[path.Base(path.Dir(path.Dir(dir)))+":"+path.Base(dir)] = true
	}
	return nil
}

// hasBlob: the blob's data is stored and repo links it — a registry serves a
// blob locally only through a repository that links it.
func (v *diskView) hasBlob(repo, digest string) bool {
	return v.blobs[digest] && v.links(v.layers, repo, "_layers/sha256")[digest]
}

// hasManifest: the manifest revision is linked into repo and its data stored.
func (v *diskView) hasManifest(repo, digest string) bool {
	return v.blobs[digest] && v.links(v.revs, repo, "_manifests/revisions/sha256")[digest]
}

// links lists (once per repo) the digests linked under a repository subdir.
func (v *diskView) links(cache map[string]map[string]bool, repo, sub string) map[string]bool {
	if l, ok := cache[repo]; ok {
		return l
	}
	out, _ := v.e.run(v.ctx, "exec", v.name, "ls", "-1", registryRoot+"/repositories/"+repo+"/"+sub)
	l := map[string]bool{}
	for _, h := range strings.Fields(out) {
		l["sha256:"+h] = true
	}
	cache[repo] = l
	return l
}

func (v *diskView) tagDigest(repo, tag string) (string, bool) {
	out, err := v.e.run(v.ctx, "exec", v.name, "cat", registryRoot+"/repositories/"+repo+"/_manifests/tags/"+tag+"/current/link")
	if err != nil || !strings.HasPrefix(out, "sha256:") {
		return "", false
	}
	return out, true
}

func (v *diskView) readManifest(repo, digest string) ([]byte, string, error) {
	pc := v.e.pullClient(indexOf(v.u), repo, "", nil)
	return pc.getManifest(v.ctx, digest)
}

func (v *diskView) openBlob(repo, digest string) (io.ReadCloser, error) {
	pc := v.e.pullClient(indexOf(v.u), repo, "", nil)
	return pc.openBlob(v.ctx, digest)
}

func indexOf(u Upstream) int {
	i, _ := upstreamIndexForHost(u.Host)
	return i
}
