package cache

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// fakeRegistry is a registry-v2 endpoint standing in for the quay cache. It
// records which paths were fetched and how often, how many blob GETs ran at
// once, and whether a client hung up on a blob before reading all of it.
type fakeRegistry struct {
	manifests map[string]fakeManifest // "repo@digest" → manifest
	blobs     map[string][]byte       // digest → content
	delay     time.Duration           // per blob GET, to make concurrency observable

	mu        sync.Mutex
	gets      map[string]int
	stored    map[string]bool // "repo@digest": blobs the cache kept (and linked)
	lose      map[string]int  // digest → how many more GETs are served but not kept
	inflight  int32
	maxFlight int32
	truncated []string // blobs a client closed before reading to the end
}

type fakeManifest struct {
	ctype string
	body  []byte
}

func newFakeRegistry() *fakeRegistry {
	return &fakeRegistry{manifests: map[string]fakeManifest{}, blobs: map[string][]byte{}, gets: map[string]int{},
		stored: map[string]bool{}, lose: map[string]int{}}
}

func digestOf(b []byte) string { return fmt.Sprintf("sha256:%x", sha256.Sum256(b)) }

func (f *fakeRegistry) addBlob(b []byte) string {
	d := digestOf(b)
	f.blobs[d] = b
	return d
}

// addImage stores a single-platform image with the given layers and returns
// its manifest digest.
func (f *fakeRegistry) addImage(repo string, layers ...[]byte) string {
	cfg := f.addBlob([]byte(fmt.Sprintf(`{"architecture":"amd64","os":"linux","n":%d}`, len(f.blobs))))
	type desc struct {
		MediaType string `json:"mediaType"`
		Digest    string `json:"digest"`
		Size      int    `json:"size"`
	}
	m := struct {
		SchemaVersion int    `json:"schemaVersion"`
		MediaType     string `json:"mediaType"`
		Config        desc   `json:"config"`
		Layers        []desc `json:"layers"`
	}{2, mtDockerManifest, desc{"application/vnd.docker.container.image.v1+json", cfg, 1}, nil}
	for _, l := range layers {
		m.Layers = append(m.Layers, desc{"application/vnd.docker.image.rootfs.diff.tar.gzip", f.addBlob(l), len(l)})
	}
	body, _ := json.Marshal(m)
	d := digestOf(body)
	f.manifests[repo+"@"+d] = fakeManifest{mtDockerManifest, body}
	return d
}

// addIndex stores a manifest list over the given platform → manifest digests.
func (f *fakeRegistry) addIndex(repo string, children map[string]string) string {
	var ms []map[string]any
	for plat, d := range children {
		p := strings.SplitN(plat, "/", 2)
		ms = append(ms, map[string]any{"mediaType": mtDockerManifest, "digest": d,
			"platform": map[string]string{"os": p[0], "architecture": p[1]}})
	}
	body, _ := json.Marshal(map[string]any{"schemaVersion": 2, "mediaType": mtDockerList, "manifests": ms})
	d := digestOf(body)
	f.manifests[repo+"@"+d] = fakeManifest{mtDockerList, body}
	return d
}

func (f *fakeRegistry) count(path string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.gets[path]
}

func (f *fakeRegistry) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	f.gets[r.URL.Path]++
	f.mu.Unlock()
	p := strings.TrimPrefix(r.URL.Path, "/v2/")
	if i := strings.Index(p, "/manifests/"); i >= 0 {
		m, ok := f.manifests[p[:i]+"@"+p[i+len("/manifests/"):]]
		if !ok {
			http.Error(w, `{"errors":[{"code":"MANIFEST_UNKNOWN"}]}`, http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", m.ctype)
		w.Write(m.body)
		return
	}
	if i := strings.Index(p, "/blobs/"); i >= 0 {
		d := p[i+len("/blobs/"):]
		b, ok := f.blobs[d]
		if !ok {
			http.Error(w, "blob unknown", http.StatusNotFound)
			return
		}
		n := atomic.AddInt32(&f.inflight, 1)
		defer atomic.AddInt32(&f.inflight, -1)
		for {
			m := atomic.LoadInt32(&f.maxFlight)
			if n <= m || atomic.CompareAndSwapInt32(&f.maxFlight, m, n) {
				break
			}
		}
		time.Sleep(f.delay)
		w.Header().Set("Content-Length", strconv.Itoa(len(b)))
		// write in chunks and flush, so a client that stops reading is seen.
		for off := 0; off < len(b); off += 64 << 10 {
			end := off + 64<<10
			if end > len(b) {
				end = len(b)
			}
			if _, err := w.Write(b[off:end]); err != nil {
				return
			}
			w.(http.Flusher).Flush()
		}
		// like the registry proxy: a blob served while another client's fetch
		// of it fails is streamed to this client but stored by nobody.
		f.mu.Lock()
		if f.lose[d] > 0 {
			f.lose[d]--
		} else {
			f.stored[p[:i]+"@"+d] = true
		}
		f.mu.Unlock()
		return
	}
	http.NotFound(w, r)
}

// serveAsQuay starts f and returns an Engine whose quay cache port is f's.
func serveAsQuay(t *testing.T, f *fakeRegistry) *Engine {
	t.Helper()
	reWarmPause = 0
	srv := httptest.NewServer(f)
	t.Cleanup(srv.Close)
	_, portStr, _ := net.SplitHostPort(strings.TrimPrefix(srv.URL, "http://"))
	port, _ := strconv.Atoi(portStr)
	idx, _ := upstreamIndexForHost("quay.io")
	return &Engine{PortBase: port - idx, Out: io.Discard, transport: &eofTracker{f: f},
		storeFor: func(context.Context, Upstream) (storeView, error) { return &servedStore{f}, nil }}
}

// servedStore is the on-disk view of a fakeRegistry: every manifest, and the
// blobs it actually kept.
type servedStore struct{ f *fakeRegistry }

func (s *servedStore) hasManifest(repo, d string) bool {
	_, ok := s.f.manifests[repo+"@"+d]
	return ok
}
func (s *servedStore) hasBlob(repo, d string) bool {
	s.f.mu.Lock()
	defer s.f.mu.Unlock()
	return s.f.stored[repo+"@"+d]
}
func (s *servedStore) tagDigest(repo, tag string) (string, bool) { return "", false }
func (s *servedStore) readManifest(repo, d string) ([]byte, string, error) {
	m := s.f.manifests[repo+"@"+d]
	return m.body, m.ctype, nil
}
func (s *servedStore) openBlob(repo, d string) (io.ReadCloser, error) {
	return io.NopCloser(bytes.NewReader(s.f.blobs[d])), nil
}

// eofTracker records, client side, each blob body closed before its end was
// read. (Server-side write errors miss it: loopback socket buffers can hold a
// whole test blob the client never reads.)
type eofTracker struct{ f *fakeRegistry }

func (tr *eofTracker) RoundTrip(req *http.Request) (*http.Response, error) {
	resp, err := http.DefaultTransport.RoundTrip(req)
	if err != nil || !strings.Contains(req.URL.Path, "/blobs/") {
		return resp, err
	}
	resp.Body = &eofBody{ReadCloser: resp.Body, f: tr.f, digest: req.URL.Path[strings.LastIndex(req.URL.Path, "/")+1:]}
	return resp, nil
}

type eofBody struct {
	io.ReadCloser
	f      *fakeRegistry
	digest string
	eof    bool
}

func (b *eofBody) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	if err == io.EOF {
		b.eof = true
	}
	return n, err
}

func (b *eofBody) Close() error {
	if !b.eof {
		b.f.mu.Lock()
		b.f.truncated = append(b.f.truncated, b.digest)
		b.f.mu.Unlock()
	}
	return b.ReadCloser.Close()
}

func tarGz(t *testing.T, files map[string][]byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	tw := tar.NewWriter(zw)
	names := make([]string, 0, len(files))
	for name := range files {
		names = append(names, name)
	}
	sort.Strings(names) // deterministic order: tests put the file they look for first
	for _, name := range names {
		content := files[name]
		if err := tw.WriteHeader(&tar.Header{Name: name, Mode: 0o644, Size: int64(len(content)), Typeflag: tar.TypeReg}); err != nil {
			t.Fatal(err)
		}
		tw.Write(content)
	}
	tw.Close()
	zw.Close()
	return buf.Bytes()
}

func imageStream(refs ...string) []byte {
	var tags []map[string]any
	for i, r := range refs {
		tags = append(tags, map[string]any{"name": fmt.Sprintf("c%d", i), "from": map[string]string{"kind": "DockerImage", "name": r}})
	}
	b, _ := json.Marshal(map[string]any{"kind": "ImageStream", "apiVersion": "image.openshift.io/v1", "spec": map[string]any{"tags": tags}})
	return b
}

// release builds a fake release: payload images in okd/content sharing one
// base layer, one of them multi-arch, and a release image whose TOP layer
// lists them. A lower layer carries a stale image-references naming an image
// that does not exist: reading bottom-up would pick it.
type release struct {
	ref       string
	payload   []string
	shared    string // the base layer every payload image has
	arm64     string // the arm64 child of the multi-arch payload image
	armLayer  string
	armConfig string
	topLayer  string
}

func buildRelease(t *testing.T, f *fakeRegistry, n int) release {
	t.Helper()
	var r release
	base := []byte("shared base layer")
	r.shared = digestOf(base)
	for i := 0; i < n; i++ {
		r.payload = append(r.payload, "quay.io/okd/content@"+f.addImage("okd/content", base, []byte(fmt.Sprintf("layer of image %d", i))))
	}
	amd := f.addImage("okd/content", base, []byte("amd64 layer"))
	armLayer := []byte("arm64 layer")
	r.armLayer = digestOf(armLayer)
	r.arm64 = f.addImage("okd/content", base, armLayer)
	var am struct {
		Config struct{ Digest string } `json:"config"`
	}
	json.Unmarshal(f.manifests["okd/content@"+r.arm64].body, &am)
	r.armConfig = am.Config.Digest
	r.payload = append(r.payload, "quay.io/okd/content@"+f.addIndex("okd/content", map[string]string{"linux/amd64": amd, "linux/arm64": r.arm64}))

	// the top layer is large and incompressible: a client that stops reading
	// once it has image-references leaves most of it unsent.
	pad := make([]byte, 8<<20)
	rand.Read(pad)
	refs := append(append([]string{}, r.payload...), r.payload[0]) // a duplicate entry
	top := tarGz(t, map[string][]byte{"./" + imageReferencesPath: imageStream(refs...), "zz-padding": pad})
	r.topLayer = digestOf(top)
	stale := tarGz(t, map[string][]byte{imageReferencesPath: imageStream("quay.io/okd/content@sha256:" + strings.Repeat("0", 64))})
	r.ref = "quay.io/okd/release@" + f.addImage("okd/release", []byte("not a tar at all"), stale, top)
	return r
}

func TestPullRelease_WarmsEveryPayloadImage(t *testing.T) {
	f := newFakeRegistry()
	f.delay = 20 * time.Millisecond
	r := buildRelease(t, f, 12)
	e := serveAsQuay(t, f)

	if err := e.PullRelease(context.Background(), r.ref, "linux/amd64", 4, ""); err != nil {
		t.Fatal(err)
	}
	for _, p := range r.payload {
		d := p[strings.Index(p, "@")+1:]
		if f.count("/v2/okd/content/manifests/"+d) == 0 {
			t.Errorf("payload manifest %s never fetched", ShortDigest(d))
		}
	}
	// every blob of every amd64 payload image went through the cache.
	for d := range f.blobs {
		if d == r.armLayer || d == r.armConfig {
			continue
		}
		if f.count("/v2/okd/content/blobs/"+d)+f.count("/v2/okd/release/blobs/"+d) == 0 {
			t.Errorf("blob %s never fetched", ShortDigest(d))
		}
	}
	// a layer shared by all payload images is fetched once, not per image:
	// concurrent warmers must not pull the same cold blob at once.
	if n := f.count("/v2/okd/content/blobs/" + r.shared); n != 1 {
		t.Errorf("shared layer fetched %d times, want 1", n)
	}
	// the platform filter: the arm64 child and its layer are left alone.
	if n := f.count("/v2/okd/content/manifests/"+r.arm64) + f.count("/v2/okd/content/blobs/"+r.armLayer) +
		f.count("/v2/okd/content/blobs/"+r.armConfig); n != 0 {
		t.Errorf("arm64 content fetched %d times with --platform linux/amd64", n)
	}
	// bounded concurrency, and actually concurrent.
	if m := atomic.LoadInt32(&f.maxFlight); m > 4 || m < 2 {
		t.Errorf("max concurrent blob GETs = %d, want 2..4 with -j 4", m)
	}
	// the top layer was read to the end, so the cache stored all of it.
	if len(f.truncated) > 0 {
		t.Errorf("blobs abandoned mid-stream: %v", f.truncated)
	}
	// top layer first: the stale lower image-references named a digest that
	// does not exist, which would have failed the run.
	if f.count("/v2/okd/content/manifests/sha256:"+strings.Repeat("0", 64)) != 0 {
		t.Error("read image-references from a lower layer instead of the top one")
	}
}

func TestPullRelease_AllPlatforms(t *testing.T) {
	f := newFakeRegistry()
	r := buildRelease(t, f, 2)
	e := serveAsQuay(t, f)
	if err := e.PullRelease(context.Background(), r.ref, "", 2, ""); err != nil {
		t.Fatal(err)
	}
	if f.count("/v2/okd/content/blobs/"+r.armLayer) != 1 {
		t.Error("--platform '' did not warm the arm64 child")
	}
}

func TestPullRelease_ReportsFailuresAndWarmsTheRest(t *testing.T) {
	f := newFakeRegistry()
	r := buildRelease(t, f, 5)
	gone := r.payload[2][strings.Index(r.payload[2], "@")+1:]
	delete(f.manifests, "okd/content@"+gone)
	e := serveAsQuay(t, f)
	err := e.PullRelease(context.Background(), r.ref, "linux/amd64", 3, "")
	if err == nil || !strings.Contains(err.Error(), "1 of 6 payload images not on disk") {
		t.Fatalf("err = %v, want 1 of 6 not on disk", err)
	}
	for i, p := range r.payload {
		d := p[strings.Index(p, "@")+1:]
		if i != 2 && f.count("/v2/okd/content/manifests/"+d) == 0 {
			t.Errorf("payload %d not warmed after another failed", i)
		}
	}
}

func TestPullRelease_RejectsBadJobs(t *testing.T) {
	f := newFakeRegistry()
	r := buildRelease(t, f, 1)
	e := serveAsQuay(t, f)
	err := e.PullRelease(context.Background(), r.ref, "", 0, "")
	if err == nil || !strings.Contains(err.Error(), "-j") {
		t.Errorf("-j 0: err = %v, want it refused", err)
	}
	if len(f.gets) != 0 {
		t.Errorf("-j 0 still contacted the cache: %v", f.gets)
	}
}

func TestParseImageReferences(t *testing.T) {
	refs, err := parseImageReferences(imageStream("quay.io/a@sha256:1", "quay.io/b@sha256:2", "quay.io/a@sha256:1"))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(refs, ",") != "quay.io/a@sha256:1,quay.io/b@sha256:2" {
		t.Errorf("refs = %v, want the two distinct refs in order", refs)
	}
	if _, err := parseImageReferences([]byte(`{"kind":"ConfigMap","spec":{"tags":[{"from":{"kind":"DockerImage","name":"quay.io/a@sha256:1"}}]}}`)); err == nil {
		t.Error("a non-ImageStream was accepted")
	}
	if _, err := parseImageReferences([]byte(`{"kind":"ImageStream","spec":{"tags":[]}}`)); err == nil {
		t.Error("an empty ImageStream was accepted")
	}
}

func TestFileInLayer(t *testing.T) {
	want := []byte("hello")
	gz := tarGz(t, map[string][]byte{"./" + imageReferencesPath: want})
	if got, ok, err := fileInLayer(bytes.NewReader(gz), imageReferencesPath); err != nil || !ok || !bytes.Equal(got, want) {
		t.Errorf("gzip layer: %q %v %v", got, ok, err)
	}
	// uncompressed tar.
	var plain bytes.Buffer
	tw := tar.NewWriter(&plain)
	tw.WriteHeader(&tar.Header{Name: imageReferencesPath, Mode: 0o644, Size: 5, Typeflag: tar.TypeReg})
	tw.Write(want)
	tw.Close()
	if got, ok, err := fileInLayer(&plain, imageReferencesPath); err != nil || !ok || !bytes.Equal(got, want) {
		t.Errorf("plain tar layer: %q %v %v", got, ok, err)
	}
	// unreadable (zstd, say): not found, not an error — try the next layer.
	if _, ok, err := fileInLayer(bytes.NewReader([]byte{0x28, 0xb5, 0x2f, 0xfd, 1, 2, 3}), imageReferencesPath); ok || err != nil {
		t.Errorf("non-tar layer: found=%v err=%v", ok, err)
	}
	// a gzip layer cut short is an error, not "not here".
	if _, _, err := fileInLayer(bytes.NewReader(gz[:len(gz)/2]), "absent"); err == nil {
		t.Error("truncated gzip layer read as complete")
	}
}

func TestPlatformMatches(t *testing.T) {
	for _, c := range []struct {
		want, plat string
		ok         bool
	}{
		{"", "attestation", true},
		{"linux/amd64", "linux/amd64", true},
		{"linux/amd64", "linux/arm64", false},
		{"linux/arm64", "linux/arm64/v8", true},
		{"linux/arm64/v8", "linux/arm64", false},
		{"linux/arm", "linux/arm64", false},
		{"linux/amd64", "attestation", false},
	} {
		if got := platformMatches(c.want, c.plat); got != c.ok {
			t.Errorf("platformMatches(%q, %q) = %v, want %v", c.want, c.plat, got, c.ok)
		}
	}
}

// A blob is served locally only through a repository that links it, so a
// layer shared by images in two repositories is warmed once in each.
func TestPullRelease_SharedBlobWarmedPerRepository(t *testing.T) {
	f := newFakeRegistry()
	base := []byte("shared base layer")
	a := f.addImage("okd/content", base, []byte("a"))
	b := f.addImage("okd/other", base, []byte("b"))
	top := tarGz(t, map[string][]byte{imageReferencesPath: imageStream("quay.io/okd/content@"+a, "quay.io/okd/other@"+b)})
	rel := f.addImage("okd/release", top)
	e := serveAsQuay(t, f)
	var out bytes.Buffer
	e.Out = &out
	if err := e.PullRelease(context.Background(), "quay.io/okd/release@"+rel, "", 2, ""); err != nil {
		t.Fatal(err)
	}
	// in the first pass: the on-disk re-warm must not be what stores it.
	if strings.Contains(out.String(), "re-warming") {
		t.Errorf("the first pass left the shared layer unwarmed in one repository:\n%s", out.String())
	}
	for _, repo := range []string{"okd/content", "okd/other"} {
		if n := f.count("/v2/" + repo + "/blobs/" + digestOf(base)); n != 1 {
			t.Errorf("shared layer fetched %d times through %s, want 1", n, repo)
		}
	}
}

// A blob the cache served but did not keep is found by the on-disk check and
// warmed again; the run succeeds only once it is stored.
func TestPullRelease_ReWarmsWhatTheCacheDidNotKeep(t *testing.T) {
	f := newFakeRegistry()
	r := buildRelease(t, f, 6)
	f.lose[r.shared] = 1
	e := serveAsQuay(t, f)
	var out bytes.Buffer
	e.Out = &out
	if err := e.PullRelease(context.Background(), r.ref, "linux/amd64", 3, ""); err != nil {
		t.Fatalf("%v\n%s", err, out.String())
	}
	if !f.stored["okd/content@"+r.shared] {
		t.Error("the lost shared layer is still not stored")
	}
	if n := f.count("/v2/okd/content/blobs/" + r.shared); n != 2 {
		t.Errorf("lost layer fetched %d times, want 2 (warm + re-warm)", n)
	}
	if !strings.Contains(out.String(), "re-warming") || !strings.Contains(out.String(), "done: 7/7 payload images verified on disk") {
		t.Errorf("output does not show the re-warm and the verified count:\n%s", out.String())
	}
}

// A blob the cache never keeps is reported, however many 200s it served.
func TestPullRelease_ReportsWhatNeverGetsStored(t *testing.T) {
	f := newFakeRegistry()
	r := buildRelease(t, f, 3)
	var m struct {
		Layers []struct{ Digest string } `json:"layers"`
	}
	json.Unmarshal(f.manifests["okd/content@"+r.payload[1][strings.Index(r.payload[1], "@")+1:]].body, &m)
	f.lose[m.Layers[1].Digest] = 1000
	e := serveAsQuay(t, f)
	var out bytes.Buffer
	e.Out = &out
	err := e.PullRelease(context.Background(), r.ref, "linux/amd64", 2, "")
	if err == nil || !strings.Contains(err.Error(), "1 of 4 payload images not on disk") {
		t.Fatalf("err = %v, want 1 of 4 not on disk\n%s", err, out.String())
	}
	if !strings.Contains(out.String(), "done: 3/4 payload images verified on disk") ||
		!strings.Contains(out.String(), r.payload[1]+": served, but not stored by the cache") {
		t.Errorf("output does not name the unstored image honestly:\n%s", out.String())
	}
}

// Without access to the store the result is reported as unverified, not as
// verified.
func TestPullRelease_SaysWhenItCannotVerify(t *testing.T) {
	f := newFakeRegistry()
	r := buildRelease(t, f, 2)
	e := serveAsQuay(t, f)
	e.storeFor = func(context.Context, Upstream) (storeView, error) { return nil, fmt.Errorf("no runtime") }
	var out bytes.Buffer
	e.Out = &out
	if err := e.PullRelease(context.Background(), r.ref, "", 2, ""); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "NOT verified on disk: no runtime") || strings.Contains(out.String(), "verified on disk in") {
		t.Errorf("unverifiable run not reported as such:\n%s", out.String())
	}
	// unverified, a warm failure is still reported.
	delete(f.manifests, "okd/content@"+r.payload[0][strings.Index(r.payload[0], "@")+1:])
	if err := e.PullRelease(context.Background(), r.ref, "", 2, ""); err == nil || !strings.Contains(err.Error(), "1 of 3 payload images failed") {
		t.Errorf("unverified run with a failed image: err = %v", err)
	}
}
