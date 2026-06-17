package main

import (
	"fmt"
	"os"
	"path/filepath"
)

const unitPath = "/etc/systemd/system/tmm-regcache.service"

// installSystemd prints (or writes) a systemd unit that reconciles the
// cache fleet on boot. The caches already carry --restart=always, so docker
// restarts them after a reboot on its own; this unit is the belt-and-
// suspenders option that also re-creates anything missing and pins the
// far-key into ExecStart.
func installSystemd(write bool) error {
	self, err := os.Executable()
	if err != nil || self == "" {
		self = "/usr/local/bin/regcachectl"
	} else {
		self, _ = filepath.Abs(self)
	}
	unit := fmt.Sprintf(`[Unit]
Description=tmm registry pull-through cache fleet (regcachectl)
Requires=docker.service
After=docker.service network-online.target
Wants=network-online.target

[Service]
Type=oneshot
RemainAfterExit=yes
ExecStart=%s up
ExecStop=%s down
TimeoutStartSec=300

[Install]
WantedBy=multi-user.target
`, self, self)

	if !write {
		fmt.Print(unit)
		fmt.Fprintf(os.Stderr, "\n# to install:\n#   sudo regcachectl install-systemd --write\n#   sudo systemctl enable --now tmm-regcache.service\n")
		return nil
	}
	if err := os.WriteFile(unitPath, []byte(unit), 0o644); err != nil {
		return fmt.Errorf("write %s: %w (run as root?)", unitPath, err)
	}
	fmt.Println("wrote", unitPath)
	fmt.Println("enable with: sudo systemctl enable --now tmm-regcache.service")
	return nil
}
