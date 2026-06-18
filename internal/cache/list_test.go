package cache

import "testing"

func TestHumanBytes(t *testing.T) {
	cases := []struct {
		n    int64
		want string
	}{
		{0, "0B"},
		{512, "512B"},
		{1024, "1.0KB"},
		{1536, "1.5KB"},
		{61612093, "58.8MB"},
		{1 << 30, "1.0GB"},
	}
	for _, c := range cases {
		if got := HumanBytes(c.n); got != c.want {
			t.Errorf("HumanBytes(%d) = %q, want %q", c.n, got, c.want)
		}
	}
}

func TestShortDigest(t *testing.T) {
	long := "sha256:2eb67991eaa9368ba199c2fac2c573cb0ffdeb79184533344f42fc9a7ff6af3c"
	if got, want := ShortDigest(long), "sha256:2eb67991eaa9"; got != want {
		t.Errorf("ShortDigest(long) = %q, want %q", got, want)
	}
	short := "sha256:abc" // shorter than the abbreviation → returned as-is
	if got := ShortDigest(short); got != short {
		t.Errorf("ShortDigest(short) = %q, want %q", got, short)
	}
}
