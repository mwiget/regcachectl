package main

import (
	"context"
	"flag"
	"strings"
	"testing"
)

func TestParseArgs_FlagsEitherSideOfPositionals(t *testing.T) {
	for _, args := range [][]string{
		{"-j", "8", "--platform", "", "ref"},
		{"ref", "-j", "8", "--platform", ""},
		{"-j", "8", "ref", "--platform", ""},
	} {
		fs := flag.NewFlagSet("t", flag.ContinueOnError)
		j := fs.Int("j", 4, "")
		p := fs.String("platform", "linux/amd64", "")
		pos := parseArgs(fs, args)
		if strings.Join(pos, ",") != "ref" || *j != 8 || *p != "" {
			t.Errorf("%q: positionals %q, -j %d, --platform %q; want [ref], 8, \"\"", args, pos, *j, *p)
		}
	}
	fs := flag.NewFlagSet("t", flag.ContinueOnError)
	j := fs.Int("j", 4, "")
	if pos := parseArgs(fs, []string{"a", "--", "b", "-j", "8"}); strings.Join(pos, ",") != "a,b,-j,8" || *j != 4 {
		t.Errorf("after --: positionals %q, -j %d; want everything positional", pos, *j)
	}
}

// The real command line, in both orders. -j 0 is refused before any runtime
// is touched, so the flag demonstrably reached the command either way.
func TestPullRelease_FlagAfterRef(t *testing.T) {
	for _, argv := range [][]string{
		{"pull-release", "-j", "0", "quay.io/okd/scos-release@sha256:1"},
		{"pull-release", "quay.io/okd/scos-release@sha256:1", "-j", "0"},
	} {
		err := run(context.Background(), argv)
		if err == nil || !strings.Contains(err.Error(), "-j must be at least 1") {
			t.Errorf("%q: err = %v, want the -j check (flag parsed)", argv, err)
		}
	}
}
