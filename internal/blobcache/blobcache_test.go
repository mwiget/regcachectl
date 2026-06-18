package blobcache

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

func digestOf(b []byte) string {
	s := sha256.Sum256(b)
	return "sha256:" + hex.EncodeToString(s[:])
}

// TestServeBlob_InlineAndHit: upstream serves a blob inline (200); first GET is
// a MISS that caches, second GET is a HIT served from disk with the upstream
// never touched again.
func TestServeBlob_InlineAndHit(t *testing.T) {
	blob := []byte("hello-layer-bytes")
	dg := digestOf(blob)
	var upstreamHits int32
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&upstreamHits, 1)
		w.Write(blob)
	}))
	defer up.Close()

	p, err := New(up.URL, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(p)
	defer srv.Close()

	url := srv.URL + "/v2/images/x/blobs/" + dg
	if got, xc := get(t, url); got != string(blob) || xc != "MISS" {
		t.Fatalf("first GET: body=%q x-cache=%q", got, xc)
	}
	if got, xc := get(t, url); got != string(blob) || xc != "HIT" {
		t.Fatalf("second GET: body=%q x-cache=%q (want HIT)", got, xc)
	}
	if n := atomic.LoadInt32(&upstreamHits); n != 1 {
		t.Errorf("upstream hit %d times, want 1 (second should be a cache HIT)", n)
	}
}

// TestServeBlob_FollowsRedirect: upstream 302s the blob to a signed URL that
// requires NO auth — the proxy must follow it and cache the result.
func TestServeBlob_FollowsRedirect(t *testing.T) {
	blob := strings.Repeat("L", 4096)
	dg := digestOf([]byte(blob))

	storage := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "" {
			t.Errorf("signed-URL fetch must carry NO Authorization, got %q", r.Header.Get("Authorization"))
		}
		io.WriteString(w, blob)
	}))
	defer storage.Close()

	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer tok" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		http.Redirect(w, r, storage.URL+"/signed", http.StatusFound)
	}))
	defer up.Close()

	p, err := New(up.URL, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(p)
	defer srv.Close()

	// client supplies its OWN bearer; proxy relays it to upstream, follows 302.
	req, _ := http.NewRequest(http.MethodGet, srv.URL+"/v2/images/x/blobs/"+dg, nil)
	req.Header.Set("Authorization", "Bearer tok")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if string(body) != blob {
		t.Fatalf("redirected blob body mismatch (len %d)", len(body))
	}
	if resp.Header.Get("Docker-Content-Digest") != dg {
		t.Errorf("missing/incorrect Docker-Content-Digest")
	}
}

// TestPassthrough_RelaysAuth: non-blob paths (manifests) are proxied with the
// client's Authorization forwarded and are not cached.
func TestPassthrough_RelaysAuth(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer abc" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		io.WriteString(w, "manifest-json")
	}))
	defer up.Close()

	p, _ := New(up.URL, t.TempDir())
	srv := httptest.NewServer(p)
	defer srv.Close()

	req, _ := http.NewRequest(http.MethodGet, srv.URL+"/v2/images/x/manifests/latest", nil)
	req.Header.Set("Authorization", "Bearer abc")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if string(body) != "manifest-json" {
		t.Fatalf("passthrough body=%q", body)
	}
}

// TestStatsEndpoint: after caching a blob, /_cache reports it with its size.
func TestStatsEndpoint(t *testing.T) {
	blob := []byte("some-cached-bytes")
	dg := digestOf(blob)
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write(blob)
	}))
	defer up.Close()
	p, _ := New(up.URL, t.TempDir())
	srv := httptest.NewServer(p)
	defer srv.Close()

	// empty cache first
	var st Stats
	getJSON(t, srv.URL+"/_cache", &st)
	if st.Count != 0 {
		t.Fatalf("fresh cache Count=%d, want 0", st.Count)
	}
	// cache one blob, then re-check
	get(t, srv.URL+"/v2/images/x/blobs/"+dg)
	getJSON(t, srv.URL+"/_cache", &st)
	if st.Count != 1 || st.TotalBytes != int64(len(blob)) {
		t.Fatalf("after caching: Count=%d TotalBytes=%d (want 1, %d)", st.Count, st.TotalBytes, len(blob))
	}
	if len(st.Blobs) != 1 || st.Blobs[0].Digest != dg {
		t.Fatalf("blob inventory wrong: %+v", st.Blobs)
	}
}

// TestRecordsRepoNames: the repo from the blob path is recorded per digest and
// surfaced via /_cache — including a HIT under a *second* repo (shared blob),
// which must add that repo without re-fetching upstream.
func TestRecordsRepoNames(t *testing.T) {
	blob := []byte("shared-layer")
	dg := digestOf(blob)
	var upstreamHits int32
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&upstreamHits, 1)
		w.Write(blob)
	}))
	defer up.Close()
	p, _ := New(up.URL, t.TempDir())
	srv := httptest.NewServer(p)
	defer srv.Close()

	// MISS under repo A (caches + records A), then HIT under repo B (records B).
	get(t, srv.URL+"/v2/images/tmm-img/blobs/"+dg)
	get(t, srv.URL+"/v2/images/dssm-store/blobs/"+dg)

	if n := atomic.LoadInt32(&upstreamHits); n != 1 {
		t.Fatalf("upstream hit %d times, want 1 (second is a HIT)", n)
	}
	var st Stats
	getJSON(t, srv.URL+"/_cache", &st)
	if len(st.Blobs) != 1 {
		t.Fatalf("want 1 blob, got %d", len(st.Blobs))
	}
	got := strings.Join(st.Blobs[0].Repos, ",")
	if got != "images/dssm-store,images/tmm-img" { // readRepos sorts
		t.Fatalf("repos = %q, want both repos (sorted)", got)
	}
	// idempotent: re-requesting an already-recorded repo adds no duplicate.
	get(t, srv.URL+"/v2/images/tmm-img/blobs/"+dg)
	getJSON(t, srv.URL+"/_cache", &st)
	if len(st.Blobs[0].Repos) != 2 {
		t.Fatalf("repos not deduped: %v", st.Blobs[0].Repos)
	}
}

func getJSON(t *testing.T, url string, v any) {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if err := json.NewDecoder(resp.Body).Decode(v); err != nil {
		t.Fatal(err)
	}
}

func get(t *testing.T, url string) (body, xcache string) {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return string(b), resp.Header.Get("X-Cache")
}
