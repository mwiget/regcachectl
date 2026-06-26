package cache

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	neturl "net/url"
	"os"
	"strings"
)

// Pull warms the pull-through cache for each ref with EVERY platform of the
// image. It performs a full registry-v2 pull through the cache endpoint — token
// flow → manifest index → every child manifest's config + layers — so the cache
// ends up holding the complete multi-arch image.
//
// This is the seam that makes `export`/`import` carry all architectures: a plain
// `docker pull` + `docker save` keeps only the host platform (an arm64 Mac would
// strip the amd64 image a server needs), whereas warming the cache fetches the
// index and every child, so the bundle is multi-arch.
//
// creds is an optional "user:password" for the upstream's token endpoint. For
// nvcr.io it defaults to "$oauthtoken:<~/.ngc>" when left empty. Public images
// warm anonymously.
func (e *Engine) Pull(ctx context.Context, refs []string, creds string) error {
	for _, raw := range refs {
		if err := e.pullOne(ctx, raw, creds); err != nil {
			return fmt.Errorf("pull %s: %w", raw, err)
		}
	}
	return nil
}

type imageRef struct {
	host string
	repo string
	ref  string // tag or digest
}

func parseRef(s string) (imageRef, error) {
	r := imageRef{}
	name := s
	// host: first path segment iff it looks like a registry (has a dot/port or
	// is localhost); otherwise the ref is an (implicit) docker.io image.
	if slash := strings.Index(name, "/"); slash >= 0 &&
		(strings.ContainsAny(name[:slash], ".:") || name[:slash] == "localhost") {
		r.host = name[:slash]
		name = name[slash+1:]
	} else {
		r.host = "docker.io"
	}
	// digest pins win over tags; default to :latest.
	if i := strings.Index(name, "@"); i >= 0 {
		r.ref, name = name[i+1:], name[:i]
	} else if i := strings.LastIndex(name, ":"); i >= 0 {
		r.ref, name = name[i+1:], name[:i]
	} else {
		r.ref = "latest"
	}
	if r.host == "docker.io" && !strings.Contains(name, "/") {
		name = "library/" + name
	}
	r.repo = name
	if r.repo == "" {
		return r, fmt.Errorf("could not parse a repository from %q", s)
	}
	return r, nil
}

func upstreamIndexForHost(host string) (int, bool) {
	for i, u := range Upstreams {
		if u.Host == host {
			return i, true
		}
	}
	return 0, false
}

func knownHosts() string {
	hs := make([]string, len(Upstreams))
	for i, u := range Upstreams {
		hs[i] = u.Host
	}
	return strings.Join(hs, ", ")
}

func (e *Engine) pullOne(ctx context.Context, raw, creds string) error {
	rf, err := parseRef(raw)
	if err != nil {
		return err
	}
	idx, ok := upstreamIndexForHost(rf.host)
	if !ok {
		return fmt.Errorf("no cache for host %q (known: %s)", rf.host, knownHosts())
	}
	port := e.Port(idx)

	// nvcr.io is always auth-required; default its creds from ~/.ngc.
	if creds == "" && rf.host == "nvcr.io" {
		if key := readNGCKey(); key != "" {
			creds = "$oauthtoken:" + key
		} else {
			return fmt.Errorf("nvcr.io needs credentials: pass --creds '$oauthtoken:<key>' or put the key in ~/.ngc")
		}
	}

	pc := &pullClient{hc: &http.Client{}, base: fmt.Sprintf("http://localhost:%d", port), repo: rf.repo, creds: creds}

	e.logf("warming %s/%s:%s (all platforms) via :%d ...", rf.host, rf.repo, rf.ref, port)
	body, ctype, err := pc.getManifest(ctx, rf.ref)
	if err != nil {
		return err
	}

	var manifests, blobs int
	if isIndex(ctype) {
		var ix struct {
			Manifests []struct {
				Digest   string `json:"digest"`
				Platform struct {
					OS   string `json:"os"`
					Arch string `json:"architecture"`
				} `json:"platform"`
			} `json:"manifests"`
		}
		if err := json.Unmarshal(body, &ix); err != nil {
			return fmt.Errorf("parse manifest index: %w", err)
		}
		for _, m := range ix.Manifests {
			child, _, err := pc.getManifest(ctx, m.Digest)
			if err != nil {
				return err
			}
			n, err := pc.warmManifestBlobs(ctx, child)
			if err != nil {
				return err
			}
			plat := strings.TrimPrefix(m.Platform.OS+"/"+m.Platform.Arch, "/")
			if plat == "" || plat == "unknown/unknown" {
				plat = "attestation"
			}
			e.logf("  • %-18s %s (%d blobs)", plat, ShortDigest(m.Digest), n)
			manifests++
			blobs += n
		}
	} else {
		n, err := pc.warmManifestBlobs(ctx, body)
		if err != nil {
			return err
		}
		e.logf("  • single-platform manifest (%d blobs)", n)
		manifests, blobs = 1, n
	}
	e.logf("done: %s/%s — %d manifest(s), %d blobs cached on :%d", rf.host, rf.repo, manifests, blobs, port)
	return nil
}

// --- minimal registry-v2 client (stdlib only, mirrors list.go's httpJSON) ---

const (
	mtOCIIndex       = "application/vnd.oci.image.index.v1+json"
	mtDockerList     = "application/vnd.docker.distribution.manifest.list.v2+json"
	mtOCIManifest    = "application/vnd.oci.image.manifest.v1+json"
	mtDockerManifest = "application/vnd.docker.distribution.manifest.v2+json"
)

func manifestAccept() string {
	return strings.Join([]string{mtOCIIndex, mtDockerList, mtOCIManifest, mtDockerManifest}, ", ")
}

func isIndex(contentType string) bool {
	return strings.HasPrefix(contentType, mtOCIIndex) || strings.HasPrefix(contentType, mtDockerList)
}

type pullClient struct {
	hc    *http.Client
	base  string // http://localhost:PORT
	repo  string
	creds string // "user:password" or ""
	token string // cached bearer, minted on the first 401
}

// getManifest GETs repo/manifests/<ref>, doing the bearer token dance on a 401.
// The cache relays the upstream's challenge, so the token is minted against the
// real upstream auth realm with the client's creds and replayed through the cache.
func (pc *pullClient) getManifest(ctx context.Context, ref string) (body []byte, contentType string, err error) {
	url := fmt.Sprintf("%s/v2/%s/manifests/%s", pc.base, pc.repo, neturl.PathEscape(ref))
	b, ct, status, hdr, err := pc.get(ctx, url, manifestAccept())
	if err != nil {
		return nil, "", err
	}
	if status == http.StatusUnauthorized {
		if err := pc.authenticate(ctx, hdr.Get("Www-Authenticate")); err != nil {
			return nil, "", err
		}
		b, ct, status, _, err = pc.get(ctx, url, manifestAccept())
		if err != nil {
			return nil, "", err
		}
	}
	if status != http.StatusOK {
		return nil, "", fmt.Errorf("GET manifest %s: HTTP %d: %s", ref, status, strings.TrimSpace(string(b)))
	}
	return b, ct, nil
}

func (pc *pullClient) warmManifestBlobs(ctx context.Context, manifest []byte) (int, error) {
	var m struct {
		Config struct {
			Digest string `json:"digest"`
		} `json:"config"`
		Layers []struct {
			Digest string `json:"digest"`
		} `json:"layers"`
	}
	if err := json.Unmarshal(manifest, &m); err != nil {
		return 0, fmt.Errorf("parse manifest: %w", err)
	}
	digs := make([]string, 0, len(m.Layers)+1)
	if m.Config.Digest != "" {
		digs = append(digs, m.Config.Digest)
	}
	for _, l := range m.Layers {
		if l.Digest != "" {
			digs = append(digs, l.Digest)
		}
	}
	for _, d := range digs {
		if err := pc.warmBlob(ctx, d); err != nil {
			return 0, err
		}
	}
	return len(digs), nil
}

// warmBlob GETs a blob and discards it — the proxy caches it on the way through.
func (pc *pullClient) warmBlob(ctx context.Context, digest string) error {
	url := fmt.Sprintf("%s/v2/%s/blobs/%s", pc.base, pc.repo, digest)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	if pc.token != "" {
		req.Header.Set("Authorization", "Bearer "+pc.token)
	}
	resp, err := pc.hc.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		snippet, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return fmt.Errorf("GET blob %s: HTTP %d: %s", ShortDigest(digest), resp.StatusCode, strings.TrimSpace(string(snippet)))
	}
	if _, err := io.Copy(io.Discard, resp.Body); err != nil {
		return fmt.Errorf("stream blob %s: %w", ShortDigest(digest), err)
	}
	return nil
}

func (pc *pullClient) get(ctx context.Context, url, accept string) (body []byte, contentType string, status int, hdr http.Header, err error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, "", 0, nil, err
	}
	if accept != "" {
		req.Header.Set("Accept", accept)
	}
	if pc.token != "" {
		req.Header.Set("Authorization", "Bearer "+pc.token)
	}
	resp, err := pc.hc.Do(req)
	if err != nil {
		return nil, "", 0, nil, err
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, "", resp.StatusCode, resp.Header, err
	}
	return b, resp.Header.Get("Content-Type"), resp.StatusCode, resp.Header, nil
}

// authenticate mints a bearer token from a `Bearer realm=...,service=...,scope=...`
// challenge using the client's basic creds, and caches it for subsequent requests.
func (pc *pullClient) authenticate(ctx context.Context, challenge string) error {
	if !strings.HasPrefix(strings.ToLower(challenge), "bearer ") {
		return fmt.Errorf("unsupported auth challenge %q", challenge)
	}
	p := parseChallenge(challenge[len("Bearer "):])
	realm := p["realm"]
	if realm == "" {
		return fmt.Errorf("auth challenge missing realm: %q", challenge)
	}
	q := neturl.Values{}
	if p["service"] != "" {
		q.Set("service", p["service"])
	}
	if p["scope"] != "" {
		q.Set("scope", p["scope"])
	}
	tokenURL := realm
	if len(q) > 0 {
		tokenURL += "?" + q.Encode()
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, tokenURL, nil)
	if err != nil {
		return err
	}
	if pc.creds != "" {
		user, pass, _ := strings.Cut(pc.creds, ":")
		req.SetBasicAuth(user, pass)
	}
	resp, err := pc.hc.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("token request to %s: HTTP %d: %s", realm, resp.StatusCode, strings.TrimSpace(string(b)))
	}
	var tok struct {
		Token       string `json:"token"`
		AccessToken string `json:"access_token"`
	}
	if err := json.Unmarshal(b, &tok); err != nil {
		return fmt.Errorf("parse token response: %w", err)
	}
	if pc.token = tok.Token; pc.token == "" {
		pc.token = tok.AccessToken
	}
	if pc.token == "" {
		return fmt.Errorf("empty token from %s", realm)
	}
	return nil
}

// parseChallenge splits `k="v",k2="v2",...` honoring quoted commas (scope values
// can contain them, e.g. scope="repository:x:pull,push").
func parseChallenge(s string) map[string]string {
	m := map[string]string{}
	var cur strings.Builder
	inQ := false
	flush := func() {
		part := cur.String()
		cur.Reset()
		if k, v, ok := strings.Cut(strings.TrimSpace(part), "="); ok {
			m[strings.ToLower(strings.TrimSpace(k))] = strings.Trim(strings.TrimSpace(v), `"`)
		}
	}
	for _, r := range s {
		switch {
		case r == '"':
			inQ = !inQ
			cur.WriteRune(r)
		case r == ',' && !inQ:
			flush()
		default:
			cur.WriteRune(r)
		}
	}
	flush()
	return m
}

func readNGCKey() string {
	home, err := os.UserHomeDir()
	if err != nil {
		return ""
	}
	b, err := os.ReadFile(home + "/.ngc")
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(b))
}
