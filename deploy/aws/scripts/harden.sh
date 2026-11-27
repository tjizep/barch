#!/bin/bash
# SSH and account settings that AWS Marketplace requires. Runs as root.
set -euo pipefail

install -m 644 /tmp/barch-files/00-barch-hardening.conf /etc/ssh/sshd_config.d/00-barch-hardening.conf
install -d -m 755 /run/sshd
sshd -t

# No password on root, and no login with one. Buyers use the ubuntu user and sudo.
passwd -l root
# cloud-init must not turn password login back on
cat >/etc/cloud/cloud.cfg.d/99-barch.cfg <<'CFG'
ssh_pwauth: false
disable_root: true
CFG
