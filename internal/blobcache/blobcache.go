// Package blobcache is a credential-free, redirect-following, digest-keyed
// caching reverse proxy for a single OCI registry upstream (built for GAR-
// backed repo.f5.com).
//
// Why it exists: registry:2 proxy mode caches GAR blobs correctly (it
// follows GAR's 302-to-signed-URL redirects server-side) but only with a
// STATIC upstream credential baked into the cache. We want the opposite —
// the CLIENT supplies its own credential (so different BNK versions can use
// different FAR keys) and the cache holds none. So this proxy:
//
//   - passes every request through to the upstream, RELAYING the client's
//     Authorization header (manifests, token challenges, tags) — never
//     caching them, so auth is always enforced upstream;
//   - for blob GETs (content-addressed, immutable), serves a disk HIT by
//     sha256 digest, or on a MISS forwards the client's auth, FOLLOWS the
//     upstream 302 to the pre-signed download URL (no auth — it's signed),
//     and streams the body to the client while teeing it into the cache,
//     keyed and verified by digest.
//
// It therefore holds no credentials. The same trust posture as a registry:2
// pull-through cache: a cached blob is served on a HIT without re-checking
// upstream auth, so bind it to a trusted local host.
package blobcache

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
)

// blobRe matches a registry v2 blob path and captures the repo name and the
// sha256 digest. The repo (the greedy `.+` before /blobs/) is recorded per
// digest so `list --objects` can name otherwise-anonymous blobs; it is never a
// credential, so capturing it preserves the credential-free posture.
var blobRe = regexp.MustCompile(`^/v2/(.+)/blobs/(sha256:[0-9a-f]{64})$`)

// manifestRe matches a registry v2 manifest path and captures the repo and the
// reference (a tag or a digest). The reference is relayed, never cached; we only
// note repo→tag from it so the listing can show repo:tag (the tag never appears
// on a blob request). A tag is not a credential.
var manifestRe = regexp.MustCompile(`^/v2/(.+)/manifests/(.+)$`)

// Proxy is the caching reverse proxy for one upstream.
type Proxy struct {
	upstream *url.URL
	cacheDir string
	rp       *httputil.ReverseProxy // transparent passthrough (relays auth)
	upClient *http.Client           // talks to upstream; does NOT auto-follow redirects
	dlClient *http.Client           // fetches the pre-signed redirect target (no auth)
	log      *log.Logger
	mu       sync.Mutex // guards repo-sidecar writes
}

// New builds a Proxy for upstream (e.g. https://repo.f5.com), caching blobs
// under cacheDir.
func New(upstream, cacheDir string) (*Proxy, error) {
	u, err := url.Parse(upstream)
	if err != nil {
		return nil, fmt.Errorf("parse upstream %q: %w", upstream, err)
	}
	if u.Scheme == "" || u.Host == "" {
		return nil, fmt.Errorf("upstream %q must be an absolute URL", upstream)
	}
	if err := os.MkdirAll(cacheDir, 0o755); err != nil {
		return nil, err
	}
	rp := httputil.NewSingleHostReverseProxy(u)
	orig := rp.Director
	rp.Director = func(r *http.Request) {
		orig(r)
		r.Host = u.Host // present the upstream vhost, not the cache's
	}
	noFollow := func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	return &Proxy{
		upstream: u,
		cacheDir: cacheDir,
		rp:       rp,
		upClient: &http.Client{CheckRedirect: noFollow},
		dlClient: &http.Client{},
		log:      log.New(os.Stdout, "", log.LstdFlags),
	}, nil
}

func (p *Proxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/_cache" {
		p.serveStats(w)
		return
	}
	if m := manifestRe.FindStringSubmatch(r.URL.Path); m != nil &&
		(r.Method == http.MethodGet || r.Method == http.MethodHead) {
		p.recordTag(m[1], m[2]) // note repo:tag, then fall through to relay
	}
	if m := blobRe.FindStringSubmatch(r.URL.Path); m != nil && r.Method == http.MethodGet {
		p.serveBlob(w, r, m[1], m[2])
		return
	}
	// Everything else (manifests, /v2/, token, blob HEAD) is a transparent,
	// auth-relaying passthrough — never cached.
	p.rp.ServeHTTP(w, r)
}

func (p *Proxy) blobPath(digest string) string {
	return filepath.Join(p.cacheDir, "blobs", strings.TrimPrefix(digest, "sha256:"))
}

func (p *Proxy) reposPath(digest string) string {
	return filepath.Join(p.cacheDir, "repos", strings.TrimPrefix(digest, "sha256:"))
}

// recordRepo notes that digest was requested under repo, persisting a
// newline-separated sidecar (deduped) so the listing can name the blob. The
// repo is taken from the request path, so a HIT names already-cached blobs too.
func (p *Proxy) recordRepo(digest, repo string) {
	if repo == "" {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	rp := p.reposPath(digest)
	for _, e := range readRepos(rp) {
		if e == repo {
			return
		}
	}
	if err := os.MkdirAll(filepath.Dir(rp), 0o755); err != nil {
		return
	}
	f, err := os.OpenFile(rp, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return
	}
	defer f.Close()
	f.WriteString(repo + "\n")
}

// readRepos returns the deduped repo names recorded for a blob (empty if none).
func readRepos(path string) []string {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil
	}
	var out []string
	seen := map[string]bool{}
	for _, ln := range strings.Split(string(b), "\n") {
		ln = strings.TrimSpace(ln)
		if ln == "" || seen[ln] {
			continue
		}
		seen[ln] = true
		out = append(out, ln)
	}
	sort.Strings(out)
	return out
}

func (p *Proxy) tagsPath(repo string) string {
	return filepath.Join(p.cacheDir, "tags", url.QueryEscape(repo))
}

// recordTag notes the reference repo was requested at — a tag or a digest
// (`sha256:…`). Both are kept (the listing prefers tags and falls back to a
// `@digest` for digest-pinned images, so they don't read as "missing a tag").
// Fires on every manifest request (relayed, never cached), so a warm re-pull
// still records it.
func (p *Proxy) recordTag(repo, ref string) {
	if repo == "" || ref == "" {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	tp := p.tagsPath(repo)
	for _, e := range readRepos(tp) { // same newline-deduped sidecar format
		if e == ref {
			return
		}
	}
	if err := os.MkdirAll(filepath.Dir(tp), 0o755); err != nil {
		return
	}
	f, err := os.OpenFile(tp, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return
	}
	defer f.Close()
	f.WriteString(ref + "\n")
}

func (p *Proxy) serveBlob(w http.ResponseWriter, r *http.Request, repo, digest string) {
	p.recordRepo(digest, repo)
	path := p.blobPath(digest)
	if fi, err := os.Stat(path); err == nil {
		f, err := os.Open(path)
		if err == nil {
			defer f.Close()
			w.Header().Set("Docker-Content-Digest", digest)
			w.Header().Set("Content-Type", "application/octet-stream")
			w.Header().Set("Content-Length", fmt.Sprint(fi.Size()))
			w.Header().Set("X-Cache", "HIT")
			w.WriteHeader(http.StatusOK)
			io.Copy(w, f)
			return
		}
	}

	// MISS: ask upstream WITH the client's credentials.
	ureq, err := http.NewRequestWithContext(r.Context(), http.MethodGet, p.upstream.String()+r.URL.Path, nil)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	ureq.Host = p.upstream.Host
	if a := r.Header.Get("Authorization"); a != "" {
		ureq.Header.Set("Authorization", a)
	}
	if a := r.Header.Get("Accept"); a != "" {
		ureq.Header.Set("Accept", a)
	}
	resp, err := p.upClient.Do(ureq)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()

	// GAR redirects large blobs to a pre-signed download URL — follow it
	// ourselves (no auth) so the bytes flow through (and into) the cache.
	if resp.StatusCode >= 300 && resp.StatusCode < 400 {
		loc := resp.Header.Get("Location")
		if loc == "" {
			http.Error(w, "redirect without Location", http.StatusBadGateway)
			return
		}
		dreq, err := http.NewRequestWithContext(r.Context(), http.MethodGet, loc, nil)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadGateway)
			return
		}
		dresp, err := p.dlClient.Do(dreq)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadGateway)
			return
		}
		defer dresp.Body.Close()
		p.streamAndCache(w, dresp, digest, path)
		return
	}
	p.streamAndCache(w, resp, digest, path)
}

// streamAndCache copies an upstream 200 body to the client while teeing it to
// a temp file; on a verified digest match it atomically commits the blob to
// the cache. A non-200 is relayed verbatim and never cached. A client
// disconnect or digest mismatch leaves the cache untouched.
func (p *Proxy) streamAndCache(w http.ResponseWriter, resp *http.Response, digest, path string) {
	if resp.StatusCode != http.StatusOK {
		copyHeader(w.Header(), resp.Header)
		w.WriteHeader(resp.StatusCode)
		io.Copy(w, resp.Body)
		return
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), "dl-")
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer os.Remove(tmp.Name()) // no-op once renamed away
	defer tmp.Close()

	w.Header().Set("Docker-Content-Digest", digest)
	w.Header().Set("Content-Type", "application/octet-stream")
	if cl := resp.Header.Get("Content-Length"); cl != "" {
		w.Header().Set("Content-Length", cl)
	}
	w.Header().Set("X-Cache", "MISS")
	w.WriteHeader(http.StatusOK)

	h := sha256.New()
	if _, err := io.Copy(io.MultiWriter(w, tmp, h), resp.Body); err != nil {
		// client gone or upstream truncated — serve what we sent, cache nothing
		return
	}
	if "sha256:"+hex.EncodeToString(h.Sum(nil)) != digest {
		p.log.Printf("digest mismatch for %s — not caching", digest)
		return
	}
	tmp.Close()
	if err := os.Rename(tmp.Name(), path); err != nil {
		p.log.Printf("cache commit %s: %v", digest, err)
	}
}

func copyHeader(dst, src http.Header) {
	for k, vs := range src {
		for _, v := range vs {
			dst.Add(k, v)
		}
	}
}

// Stats is the cached-blob inventory served at /_cache, so `regcachectl list`
// can report the cached objects and their exact (digest-keyed) sizes — the
// container is distroless, so an in-container `du` is not available.
type Stats struct {
	Count      int                 `json:"count"`
	TotalBytes int64               `json:"total_bytes"`
	Blobs      []BlobInfo          `json:"blobs"`
	Tags       map[string][]string `json:"tags,omitempty"` // repo → tag(s) seen on manifest requests
}

// BlobInfo is one cached blob, with the repo name(s) it was served under (if
// any were recorded). A blob can be shared across repos, hence a slice.
type BlobInfo struct {
	Digest string   `json:"digest"`
	Size   int64    `json:"size"`
	Repos  []string `json:"repos,omitempty"`
}

func (p *Proxy) stats() Stats {
	var st Stats
	entries, err := os.ReadDir(filepath.Join(p.cacheDir, "blobs"))
	if err != nil {
		return st // missing dir → empty cache
	}
	for _, e := range entries {
		if e.IsDir() || strings.HasPrefix(e.Name(), "dl-") {
			continue // skip in-flight temp downloads
		}
		fi, err := e.Info()
		if err != nil {
			continue
		}
		st.Blobs = append(st.Blobs, BlobInfo{
			Digest: "sha256:" + e.Name(),
			Size:   fi.Size(),
			Repos:  readRepos(p.reposPath("sha256:" + e.Name())),
		})
		st.TotalBytes += fi.Size()
		st.Count++
	}
	sort.Slice(st.Blobs, func(i, j int) bool { return st.Blobs[i].Size > st.Blobs[j].Size })

	// repo → tag(s), decoded from the tags/ sidecar filenames.
	if tagEntries, err := os.ReadDir(filepath.Join(p.cacheDir, "tags")); err == nil {
		st.Tags = map[string][]string{}
		for _, e := range tagEntries {
			if e.IsDir() {
				continue
			}
			repo, err := url.QueryUnescape(e.Name())
			if err != nil {
				continue
			}
			st.Tags[repo] = readRepos(p.tagsPath(repo))
		}
	}
	return st
}

func (p *Proxy) serveStats(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(p.stats())
}

// ListenAndServe runs the proxy until the process exits.
func (p *Proxy) ListenAndServe(addr string) error {
	p.log.Printf("blobcache: upstream=%s cache=%s listen=%s", p.upstream, p.cacheDir, addr)
	return http.ListenAndServe(addr, p)
}
