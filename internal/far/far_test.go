package far

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"encoding/base64"
	"os"
	"path/filepath"
	"testing"
)

// writeTgz packs a single file into a gzip'd tar at a temp path.
func writeTgz(t *testing.T, name string, body []byte) string {
	t.Helper()
	dir := t.TempDir()
	p := filepath.Join(dir, "far.tgz")
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gz)
	if err := tw.WriteHeader(&tar.Header{Name: name, Mode: 0o644, Size: int64(len(body)), Typeflag: tar.TypeReg}); err != nil {
		t.Fatal(err)
	}
	if _, err := tw.Write(body); err != nil {
		t.Fatal(err)
	}
	tw.Close()
	gz.Close()
	if err := os.WriteFile(p, buf.Bytes(), 0o644); err != nil {
		t.Fatal(err)
	}
	return p
}

const saJSON = `{"type":"service_account","project_id":"example-project","private_key":"x"}`

func TestExtract_Base64SA(t *testing.T) {
	b64 := base64.StdEncoding.EncodeToString([]byte(saJSON))
	c, err := Extract(writeTgz(t, "cne_pull_64.json", []byte(b64)))
	if err != nil {
		t.Fatal(err)
	}
	if c.Username != "_json_key_base64" {
		t.Errorf("username = %q, want _json_key_base64", c.Username)
	}
	if c.Password != b64 {
		t.Errorf("password not the verbatim base64 blob")
	}
}

func TestExtract_RawSA(t *testing.T) {
	c, err := Extract(writeTgz(t, "sa.json", []byte(saJSON)))
	if err != nil {
		t.Fatal(err)
	}
	if c.Username != "_json_key_base64" {
		t.Errorf("username = %q", c.Username)
	}
	if got, _ := base64.StdEncoding.DecodeString(c.Password); string(got) != saJSON {
		t.Errorf("password does not base64-decode back to the SA JSON")
	}
}

func TestExtract_DockerConfigAuth(t *testing.T) {
	auth := base64.StdEncoding.EncodeToString([]byte("_json_key_base64:ABC123"))
	cfg := `{"auths":{"repo.f5.com":{"auth":"` + auth + `"}}}`
	c, err := Extract(writeTgz(t, "config.json", []byte(cfg)))
	if err != nil {
		t.Fatal(err)
	}
	if c.Username != "_json_key_base64" || c.Password != "ABC123" {
		t.Errorf("got %q/%q, want _json_key_base64/ABC123", c.Username, c.Password)
	}
}

func TestExtract_Garbage(t *testing.T) {
	if _, err := Extract(writeTgz(t, "x.txt", []byte("not json, not base64-sa"))); err == nil {
		t.Fatal("expected error for unrecognized FAR content")
	}
}
