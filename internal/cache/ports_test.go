package cache

import "testing"

// discovered ports win over the compiled-in default base, so a fleet brought
// up on `--port-base 5100` answers `list`/`status`/`print-registries` with no
// flag repeated.
func TestPortUsesDiscovered(t *testing.T) {
	e := &Engine{ports: map[string]int{"dockerhub": 5100, "quay": 5102}}
	if got := e.Port(0); got != 5100 { // dockerhub, discovered
		t.Errorf("Port(0) = %d, want 5100", got)
	}
	if got := e.Port(2); got != 5102 { // quay, discovered
		t.Errorf("Port(2) = %d, want 5102", got)
	}
	// ghcr was not discovered (absent container): fall back to the base.
	if got := e.Port(1); got != DefaultPortBase+1 {
		t.Errorf("Port(1) = %d, want %d", got, DefaultPortBase+1)
	}
}

// An explicit --port-base is an override: it must win even when a container
// publishes something else, so `--port-base 5000` still probes 5000.
func TestPortExplicitFlagWins(t *testing.T) {
	e := &Engine{PortBase: 5000, PortBaseSet: true, ports: map[string]int{"dockerhub": 5100}}
	if got := e.Port(0); got != 5000 {
		t.Errorf("Port(0) = %d, want 5000 (explicit flag)", got)
	}
}

func TestPortMismatch(t *testing.T) {
	e := &Engine{PortBase: 5000, PortBaseSet: true, ports: map[string]int{"dockerhub": 5100, "ghcr": 5001}}
	got := e.PortMismatch()
	if len(got) != 1 {
		t.Fatalf("PortMismatch() = %v, want 1 entry (ghcr:5001 matches base+1)", got)
	}
	if want := "dockerhub runs on :5100, not :5000"; got[0] != want {
		t.Errorf("PortMismatch()[0] = %q, want %q", got[0], want)
	}
}

// No discovery (no fleet, or a runtime that could not be queried) leaves the
// documented default layout untouched.
func TestPortNoDiscoveryKeepsDefaults(t *testing.T) {
	e := &Engine{}
	for i := range Upstreams {
		if got := e.Port(i); got != DefaultPortBase+i {
			t.Errorf("Port(%d) = %d, want %d", i, got, DefaultPortBase+i)
		}
	}
}
