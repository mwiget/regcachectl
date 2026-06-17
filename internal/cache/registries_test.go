package cache

import "strings"

import "testing"

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
