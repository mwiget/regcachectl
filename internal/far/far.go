// Package far extracts repo.f5.com registry credentials from the
// operator-supplied FAR (F5 Application Registry) tgz, so the F5
// pull-through cache can authenticate upstream on the clients' behalf.
//
// This is a self-contained port of tmmlitectl's internal/deploy/license.go
// (ExtractFARDockerConfig) reduced to "give me a username/password pair",
// which is what registry:2's REGISTRY_PROXY_USERNAME/PASSWORD want.
package far

import (
	"archive/tar"
	"compress/gzip"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
)

// Creds is an upstream Basic-auth credential pair.
type Creds struct {
	Username string
	Password string
}

// Extract opens the FAR tgz and returns the repo.f5.com upstream
// credentials. Three on-disk formats are handled, matching the live F5
// packs:
//
//  1. A literal .dockerconfigjson — we pull auths[*].auth and split it.
//  2. A base64-encoded GCP service-account JSON (current format, filename
//     typically cne_pull_64.json) — username "_json_key_base64", password
//     is the base64 blob verbatim.
//  3. A raw service-account JSON — same scheme, we base64 it ourselves.
//
// GAR accepts "_json_key_base64:<base64-of-sa-json>" as Basic auth, which
// is exactly what registry:2 forwards upstream when proxying.
func Extract(tgzPath string) (Creds, error) {
	f, err := os.Open(tgzPath)
	if err != nil {
		return Creds{}, fmt.Errorf("open far %s: %w", tgzPath, err)
	}
	defer f.Close()

	gz, err := gzip.NewReader(f)
	if err != nil {
		return Creds{}, fmt.Errorf("gzip %s: %w", tgzPath, err)
	}
	defer gz.Close()

	t := tar.NewReader(gz)
	for {
		hdr, err := t.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return Creds{}, fmt.Errorf("tar read %s: %w", tgzPath, err)
		}
		if hdr.Typeflag != tar.TypeReg {
			continue
		}
		body, err := io.ReadAll(t)
		if err != nil {
			return Creds{}, err
		}

		// Format 1: already a dockerconfigjson.
		if c, ok := credsFromDockerConfig(body); ok {
			return c, nil
		}
		// Format 2: base64-encoded GCP SA JSON.
		trimmed := strings.TrimSpace(string(body))
		if decoded, err := base64.StdEncoding.DecodeString(trimmed); err == nil && isServiceAccountJSON(decoded) {
			return Creds{Username: "_json_key_base64", Password: trimmed}, nil
		}
		// Format 3: raw SA JSON.
		if isServiceAccountJSON(body) {
			return Creds{
				Username: "_json_key_base64",
				Password: base64.StdEncoding.EncodeToString(body),
			}, nil
		}

		base := hdr.Name
		if i := strings.LastIndexByte(base, '/'); i >= 0 {
			base = base[i+1:]
		}
		return Creds{}, fmt.Errorf("far tgz %s entry %s is neither a dockerconfigjson nor a (base64-encoded) GCP service_account JSON", tgzPath, base)
	}
	return Creds{}, fmt.Errorf("far tgz %s: no regular files inside", tgzPath)
}

// credsFromDockerConfig parses a dockerconfigjson and returns the first
// auth entry decoded into user/pass.
func credsFromDockerConfig(b []byte) (Creds, bool) {
	var doc struct {
		Auths map[string]struct {
			Auth     string `json:"auth"`
			Username string `json:"username"`
			Password string `json:"password"`
		} `json:"auths"`
	}
	if err := json.Unmarshal(b, &doc); err != nil || len(doc.Auths) == 0 {
		return Creds{}, false
	}
	// Prefer repo.f5.com, else take any entry.
	pick := func() (string, struct {
		Auth     string `json:"auth"`
		Username string `json:"username"`
		Password string `json:"password"`
	}) {
		if e, ok := doc.Auths["repo.f5.com"]; ok {
			return "repo.f5.com", e
		}
		for k, e := range doc.Auths {
			return k, e
		}
		return "", struct {
			Auth     string `json:"auth"`
			Username string `json:"username"`
			Password string `json:"password"`
		}{}
	}
	_, e := pick()
	if e.Username != "" {
		return Creds{Username: e.Username, Password: e.Password}, true
	}
	if e.Auth != "" {
		dec, err := base64.StdEncoding.DecodeString(e.Auth)
		if err != nil {
			return Creds{}, false
		}
		user, pass, ok := strings.Cut(string(dec), ":")
		if !ok {
			return Creds{}, false
		}
		return Creds{Username: user, Password: pass}, true
	}
	return Creds{}, false
}

func isServiceAccountJSON(b []byte) bool {
	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		return false
	}
	t, _ := m["type"].(string)
	return t == "service_account"
}
