package cache

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"testing"
)

// fakeStore is a storeView over a fakeRegistry, with what is "on disk" chosen
// per test. Reading content that is not on disk fails the test: coverage must
// never make the cache fetch anything.
type fakeStore struct {
	t         *testing.T
	f         *fakeRegistry
	manifests map[string]bool // "repo@digest"
	blobs     map[string]bool
	tags      map[string]string // "repo:tag" → digest
}

func newFakeStore(t *testing.T, f *fakeRegistry) *fakeStore {
	s := &fakeStore{t: t, f: f, manifests: map[string]bool{}, blobs: map[string]bool{}, tags: map[string]string{}}
	for k := range f.manifests {
		s.manifests[k] = true
	}
	for d := range f.blobs {
		s.blobs[d] = true
	}
	return s
}

func (s *fakeStore) hasManifest(repo, d string) bool { return s.manifests[repo+"@"+d] }
func (s *fakeStore) hasBlob(d string) bool           { return s.blobs[d] }
func (s *fakeStore) tagDigest(repo, tag string) (string, bool) {
	d, ok := s.tags[repo+":"+tag]
	return d, ok
}
func (s *fakeStore) readManifest(repo, d string) ([]byte, string, error) {
	if !s.hasManifest(repo, d) {
		s.t.Errorf("read manifest %s@%s that is not on disk", repo, ShortDigest(d))
		return nil, "", fmt.Errorf("not on disk")
	}
	m := s.f.manifests[repo+"@"+d]
	return m.body, m.ctype, nil
}
func (s *fakeStore) openBlob(repo, d string) (io.ReadCloser, error) {
	if !s.hasBlob(d) {
		s.t.Errorf("read blob %s that is not on disk", ShortDigest(d))
		return nil, fmt.Errorf("not on disk")
	}
	return io.NopCloser(bytes.NewReader(s.f.blobs[d])), nil
}

func quayUpstream() Upstream { i, _ := upstreamIndexForHost("quay.io"); return Upstreams[i] }

func digestPart(ref string) string { return ref[strings.Index(ref, "@")+1:] }

func TestReleaseCoverage(t *testing.T) {
	f := newFakeRegistry()
	r := buildRelease(t, f, 4) // payload: 4 single-platform + 1 multi-arch
	s := newFakeStore(t, f)
	rf, _ := parseRef(r.ref)

	cov, err := releaseCoverage(s, quayUpstream(), rf, "linux/amd64")
	if err != nil {
		t.Fatal(err)
	}
	if cov.Total != 5 || cov.Full != 5 || len(cov.Missing) != 0 {
		t.Fatalf("all on disk: %+v, want 5/5", cov)
	}

	// payload 0: manifest gone. payload 1: a layer gone. payload 4 (the index):
	// its arm64 child gone, which linux/amd64 does not need.
	delete(s.manifests, "okd/content@"+digestPart(r.payload[0]))
	var m struct {
		Layers []struct{ Digest string } `json:"layers"`
	}
	jsonUnmarshal(t, f.manifests["okd/content@"+digestPart(r.payload[1])].body, &m)
	delete(s.blobs, m.Layers[1].Digest)
	delete(s.manifests, "okd/content@"+r.arm64)

	cov, err = releaseCoverage(s, quayUpstream(), rf, "linux/amd64")
	if err != nil {
		t.Fatal(err)
	}
	if cov.Full != 3 || cov.ManifestOnly != 1 || len(cov.Missing) != 2 {
		t.Errorf("amd64: %+v, want 3 full, 1 manifest-only, 2 missing", cov)
	}
	// all platforms: the index now lacks its arm64 child.
	cov, _ = releaseCoverage(s, quayUpstream(), rf, "")
	if cov.Full != 2 || cov.ManifestOnly != 2 {
		t.Errorf("all platforms: %+v, want 2 full, 2 manifest-only", cov)
	}
}

func TestReleaseCoverage_ByTagAndUncached(t *testing.T) {
	f := newFakeRegistry()
	r := buildRelease(t, f, 1)
	s := newFakeStore(t, f)
	byTag, _ := parseRef("quay.io/okd/release:4.22")
	if _, err := releaseCoverage(s, quayUpstream(), byTag, ""); err == nil || !strings.Contains(err.Error(), "@sha256") {
		t.Errorf("uncached tag: err = %v, want a hint to pass the digest", err)
	}
	s.tags["okd/release:4.22"] = digestPart(r.ref)
	if cov, err := releaseCoverage(s, quayUpstream(), byTag, ""); err != nil || cov.Full != cov.Total {
		t.Errorf("cached tag: %+v, %v", cov, err)
	}
	// the release's top layer is gone: an error, never a fetch.
	delete(s.blobs, r.topLayer)
	rf, _ := parseRef(r.ref)
	if _, err := releaseCoverage(s, quayUpstream(), rf, ""); err == nil {
		t.Error("coverage without the release's top layer on disk succeeded")
	}
}

func TestReleaseCoverage_OtherRegistriesNotChecked(t *testing.T) {
	f := newFakeRegistry()
	d := f.addImage("okd/content", []byte("l"))
	top := tarGz(t, map[string][]byte{imageReferencesPath: imageStream("quay.io/okd/content@"+d, "ghcr.io/x/y@sha256:"+strings.Repeat("1", 64))})
	rel := f.addImage("okd/release", top)
	s := newFakeStore(t, f)
	rf, _ := parseRef("quay.io/okd/release@" + rel)
	cov, err := releaseCoverage(s, quayUpstream(), rf, "")
	if err != nil {
		t.Fatal(err)
	}
	if cov.Total != 2 || cov.Full != 1 || cov.Elsewhere != 1 || len(cov.Missing) != 0 {
		t.Errorf("%+v, want 1 full on quay and 1 elsewhere", cov)
	}
}

func jsonUnmarshal(t *testing.T, b []byte, v any) {
	t.Helper()
	if err := json.Unmarshal(b, v); err != nil {
		t.Fatal(err)
	}
}
