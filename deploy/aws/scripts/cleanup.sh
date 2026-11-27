#!/bin/bash
# Removes everything specific to the build instance. Runs as root, last.
# After this the instance can't be logged into again, so nothing may follow it.
set -euo pipefail

export DEBIAN_FRONTEND=noninteractive
apt-get -y autoremove --purge
apt-get clean
rm -rf /var/lib/apt/lists/* /tmp/barch-files

# cloud-init state, so the next boot is a first boot: new host keys, the buyer's
# key pair, a new machine id
cloud-init clean --logs --seed
rm -f /etc/ssh/ssh_host_*
truncate -s 0 /etc/machine-id
rm -f /var/lib/dbus/machine-id

# keys and history. The Packer key is in ubuntu's authorized_keys.
find / -xdev \( -name authorized_keys -o -name authorized_keys2 \) -path '*/.ssh/*' -delete
rm -f /root/.bash_history /home/*/.bash_history /root/.lesshst /home/*/.lesshst
rm -rf /root/.cache /home/*/.cache

# logs
journalctl --rotate || true
journalctl --vacuum-time=1s || true
find /var/log -type f \( -name '*.gz' -o -name '*.[0-9]' \) -delete
find /var/log -type f -exec truncate -s 0 {} +
sync
