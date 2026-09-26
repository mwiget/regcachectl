package cache

import (
	"strconv"
	"strings"
	"testing"
)

func TestRenderRegistries_FallbackAndPorts(t *testing.T) {
	e := &Engine{PortBase: 5000}
	got := e.RenderRegistries("host.docker.internal", true)

	want := []string{
		`"docker.io":`,
		`- "http://host.docker.internal:5000"`,
		`- "https://registry-1.docker.io"`, // fallback for dockerhub
		`"ghcr.io":`,
		`- "http://host.docker.internal:5001"`,
		`"quay.io":`,
		`- "http://host.docker.internal:5002"`,
		`"repo.f5.com":`,
		`- "http://host.docker.internal:5003"`,
		`- "https://repo.f5.com"`,
	}
	for _, w := range want {
		if !strings.Contains(got, w) {
			t.Errorf("registries.yaml missing %q\n---\n%s", w, got)
		}
	}
}

func TestRenderRegistries_NoFallback(t *testing.T) {
	e := &Engine{PortBase: 6000}
	got := e.RenderRegistries("10.0.0.1", false)
	if strings.Contains(got, "registry-1.docker.io") {
		t.Errorf("no-fallback output still contains the upstream endpoint:\n%s", got)
	}
	if !strings.Contains(got, `- "http://10.0.0.1:6003"`) {
		t.Errorf("custom host/port-base not honored:\n%s", got)
	}
}

func TestPortLayout(t *testing.T) {
	e := &Engine{}
	if e.Port(0) != DefaultPortBase {
		t.Errorf("Port(0) = %d, want %d", e.Port(0), DefaultPortBase)
	}
	if e.Port(3) != DefaultPortBase+3 {
		t.Errorf("Port(3) = %d, want %d", e.Port(3), DefaultPortBase+3)
	}
}

// The default format is the k3s registries.yaml, byte for byte: consumers
// that parse `print-registries` without --format must see no change.
func TestRender_DefaultIsK3sUnchanged(t *testing.T) {
	e := &Engine{PortBase: 5000}
	for _, f := range []string{"", FormatK3s} {
		got, err := e.Render(f, "host.docker.internal", true)
		if err != nil {
			t.Fatal(err)
		}
		if want := e.RenderRegistries("host.docker.internal", true); got != want {
			t.Errorf("Render(%q) differs from the k3s output:\n%s\n--- want\n%s", f, got, want)
		}
	}
}

func TestRender_UnknownFormat(t *testing.T) {
	if _, err := (&Engine{}).Render("docker", "h", true); err == nil {
		t.Error("Render accepted an unknown format")
	}
}

// crioBlocks splits registries.conf into its [[registry]] blocks.
func crioBlocks(conf string) []string {
	parts := strings.Split(conf, "[[registry]]\n")
	return parts[1:]
}

func TestRenderCRIO(t *testing.T) {
	e := &Engine{PortBase: 6000}
	got, err := e.Render(FormatCRIO, "10.240.0.2", true)
	if err != nil {
		t.Fatal(err)
	}
	blocks := crioBlocks(got)
	if len(blocks) != len(Upstreams) {
		t.Fatalf("%d [[registry]] blocks, want one per cache (%d):\n%s", len(blocks), len(Upstreams), got)
	}
	for i, u := range Upstreams {
		b := blocks[i]
		for _, w := range []string{
			`location = "` + u.Host + `"`,
			"[[registry.mirror]]",
			`location = "10.240.0.2:` + itoa(6000+i) + `"`,
			"insecure = true",
		} {
			if !strings.Contains(b, w) {
				t.Errorf("%s block missing %q:\n%s", u.Host, w, b)
			}
		}
		// the mirror is the bare cache host: a pull-through cache keeps paths.
		if strings.Contains(b, "http://") {
			t.Errorf("%s mirror location carries a scheme:\n%s", u.Host, b)
		}
	}
}

func TestRenderCRIO_NoFallbackRefused(t *testing.T) {
	if _, err := (&Engine{}).Render(FormatCRIO, "h", false); err == nil {
		t.Error("crio accepted --no-fallback, which registries.conf cannot express")
	}
}

func TestRenderOKD(t *testing.T) {
	e := &Engine{PortBase: 5000}
	got, err := e.Render(FormatOKD, "10.240.0.2", true)
	if err != nil {
		t.Fatal(err)
	}
	docs := strings.Split(got, "---\n")
	if len(docs) != 3 {
		t.Fatalf("%d YAML documents, want 3:\n%s", len(docs), got)
	}
	for i, kind := range []string{"ImageDigestMirrorSet", "ImageTagMirrorSet", "Image"} {
		if !strings.Contains(docs[i], "apiVersion: config.openshift.io/v1\nkind: "+kind+"\n") {
			t.Errorf("document %d is not a %s:\n%s", i, kind, docs[i])
		}
	}
	for i, u := range Upstreams {
		mirror := "  - source: " + u.Host + "\n    mirrors:\n    - 10.240.0.2:" + itoa(5000+i) + "\n"
		for d, field := range []string{"imageDigestMirrors", "imageTagMirrors"} {
			if !strings.Contains(docs[d], field+":\n") || !strings.Contains(docs[d], mirror) {
				t.Errorf("%s missing %s mirror %q:\n%s", field, u.Host, mirror, docs[d])
			}
		}
		if !strings.Contains(docs[2], `    - "10.240.0.2:`+itoa(5000+i)+`"`) {
			t.Errorf("insecureRegistries missing the %s cache:\n%s", u.Host, docs[2])
		}
	}
	if !strings.Contains(docs[2], "metadata:\n  name: cluster\n") || !strings.Contains(docs[2], "registrySources:\n    insecureRegistries:\n") {
		t.Errorf("Image is not the cluster image.config with insecureRegistries:\n%s", docs[2])
	}
	if strings.Contains(got, "NeverContactSource") {
		t.Errorf("fallback output forbids contacting the source:\n%s", got)
	}
}

func TestRenderOKD_NoFallbackNeverContactsSource(t *testing.T) {
	got, err := (&Engine{}).Render(FormatOKD, "h", false)
	if err != nil {
		t.Fatal(err)
	}
	// one policy per mirror entry, in both mirror sets.
	if n := strings.Count(got, "    mirrorSourcePolicy: NeverContactSource\n"); n != 2*len(Upstreams) {
		t.Errorf("%d NeverContactSource policies, want %d:\n%s", n, 2*len(Upstreams), got)
	}
}

// Every format lists exactly the configured caches, at their port by index —
// nothing hard-coded beside Upstreams.
func TestRender_OnlyConfiguredCaches(t *testing.T) {
	saved := Upstreams
	defer func() { Upstreams = saved }()
	Upstreams = []Upstream{
		{Name: "ghcr", Host: "ghcr.io", Remote: "https://ghcr.io"},
		{Name: "quay", Host: "quay.io", Remote: "https://quay.io"},
	}
	e := &Engine{PortBase: 7000}
	for _, f := range Formats {
		got, err := e.Render(f, "h", true)
		if err != nil {
			t.Fatal(err)
		}
		for _, absent := range []string{"docker.io", "repo.f5.com", "nvcr.io", "7002"} {
			if strings.Contains(got, absent) {
				t.Errorf("%s output lists %q, which is not configured:\n%s", f, absent, got)
			}
		}
		if !strings.Contains(got, "h:7001") || !strings.Contains(got, "quay.io") {
			t.Errorf("%s output lost quay.io at index 1 (h:7001):\n%s", f, got)
		}
	}
}

func itoa(i int) string { return strconv.Itoa(i) }

// crio and okd address the ports the running fleet publishes (#7's
// discovery), not portBase+index, unless --port-base was given explicitly.
func TestRender_UsesDiscoveredPorts(t *testing.T) {
	e := &Engine{ports: map[string]int{"quay": 5102}}
	for _, f := range []string{FormatCRIO, FormatOKD} {
		got, err := e.Render(f, "10.240.0.2", true)
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(got, "10.240.0.2:5102") || strings.Contains(got, "10.240.0.2:5002") {
			t.Errorf("%s ignores the discovered quay port 5102:\n%s", f, got)
		}
	}
	e.PortBase, e.PortBaseSet = 6000, true
	got, _ := e.Render(FormatOKD, "h", true)
	if !strings.Contains(got, "h:6002") {
		t.Errorf("explicit --port-base does not win over discovery:\n%s", got)
	}
}
