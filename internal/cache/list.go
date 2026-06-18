package cache

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"
)

// Listing is the cached-object inventory of one cache.
type Listing struct {
	Name    string
	Host    string
	Port    int
	Engine  string // "registry:2" or "blobcache"
	State   string // running / stopped / absent
	Size    string // human-readable total ("-" if unknown)
	Bytes   int64  // total bytes (0 if unknown)
	Objects []Object
	Note    string // shared-store caveat etc.
}

// Object is one cached item: a repo:tag (registry:2) or a blob digest
// (blobcache, with an exact size).
type Object struct {
	Name   string
	Size   string // "" when size is not attributable per-object
	Detail string // optional trailing column (e.g. repo names for a blob)
}

// List gathers the cached objects + space for every cache in the fleet.
func (e *Engine) List(ctx context.Context) ([]Listing, error) {
	var out []Listing
	for i, u := range Upstreams {
		l := Listing{Name: u.Name, Host: u.Host, Port: e.Port(i), State: "absent", Size: "-"}
		l.Engine = "registry:2"
		if u.Blobcache {
			l.Engine = "blobcache"
		}
		exists, running, err := e.containerState(ctx, container(u))
		if err != nil {
			return nil, err
		}
		switch {
		case exists && running:
			l.State = "running"
			if u.Blobcache {
				e.fillBlobcache(ctx, &l)
			} else {
				e.fillRegistry2(ctx, u, &l)
			}
		case exists:
			l.State = "stopped"
		}
		out = append(out, l)
	}
	return out, nil
}

// fillRegistry2 lists the truly-cached repo:tag set and the total store size.
// The catalog API lists locally-cached repos, but the tags/list API PROXIES to
// upstream (returning every upstream tag, not what's cached) — so cached tags
// are read from the on-disk manifest store instead. registry:2 shares blobs
// across repos, so per-repo sizes aren't attributable: only the cache total is.
func (e *Engine) fillRegistry2(ctx context.Context, u Upstream, l *Listing) {
	l.Note = "shared blob store; size is the cache total"
	if du, err := e.run(ctx, "exec", container(u), "du", "-sh", "/var/lib/registry"); err == nil {
		if f := strings.Fields(du); len(f) > 0 {
			l.Size = f[0]
		}
	}
	var cat struct {
		Repositories []string `json:"repositories"`
	}
	if err := e.httpJSON(ctx, l.Port, "/v2/_catalog", &cat); err != nil {
		return
	}
	sort.Strings(cat.Repositories)
	for _, repo := range cat.Repositories {
		// Cached tags = the manifest tag dirs on disk (NOT the proxied API).
		tagsDir := "/var/lib/registry/docker/registry/v2/repositories/" + repo + "/_manifests/tags"
		out, err := e.run(ctx, "exec", container(u), "ls", "-1", tagsDir)
		if err != nil || strings.TrimSpace(out) == "" {
			l.Objects = append(l.Objects, Object{Name: repo}) // repo cached, no resolved tag
			continue
		}
		tags := strings.Fields(out)
		sort.Strings(tags)
		for _, t := range tags {
			l.Objects = append(l.Objects, Object{Name: repo + ":" + t})
		}
	}
}

// fillBlobcache reads the digest-keyed inventory from the blob cache's /_cache
// endpoint — exact per-blob sizes.
func (e *Engine) fillBlobcache(ctx context.Context, l *Listing) {
	var st struct {
		Count      int   `json:"count"`
		TotalBytes int64 `json:"total_bytes"`
		Blobs      []struct {
			Digest string   `json:"digest"`
			Size   int64    `json:"size"`
			Repos  []string `json:"repos"`
		} `json:"blobs"`
	}
	if err := e.httpJSON(ctx, l.Port, "/_cache", &st); err != nil {
		return
	}
	l.Bytes = st.TotalBytes
	l.Size = HumanBytes(st.TotalBytes)
	for _, b := range st.Blobs {
		l.Objects = append(l.Objects, Object{
			Name:   b.Digest,
			Size:   HumanBytes(b.Size),
			Detail: strings.Join(b.Repos, ", "),
		})
	}
}

func (e *Engine) httpJSON(ctx context.Context, port int, path string, v any) error {
	url := fmt.Sprintf("http://localhost:%d%s", port, path)
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	resp, err := (&http.Client{Timeout: 5 * time.Second}).Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("HTTP %d", resp.StatusCode)
	}
	return json.NewDecoder(resp.Body).Decode(v)
}

// HumanBytes renders a byte count as a compact human-readable string.
func HumanBytes(n int64) string {
	if n < 1024 {
		return fmt.Sprintf("%dB", n)
	}
	const units = "KMGTP"
	f := float64(n)
	i := -1
	for f >= 1024 && i < len(units)-1 {
		f /= 1024
		i++
	}
	return fmt.Sprintf("%.1f%cB", f, units[i])
}
