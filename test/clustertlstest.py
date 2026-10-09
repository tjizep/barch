#!/usr/bin/env python3
"""
The Raft ports over TLS - TODO 620.

What has to hold:
  - with raft_tls on, a Raft port answers a TLS handshake with the node's
    certificate;
  - three nodes form a cluster and replicate a space over it, and a leader killed
    under writes loses no acknowledged write;
  - a certificate that won't load stops the group, and says why.

Ports are BARCH_TEST_PORT and the 19 after it.
"""
import os
import shutil
import socket
import ssl
import subprocess
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import scale  # noqa: E402
import clusternodes  # noqa: E402
from clusternodes import wait_for, leader_of  # noqa: E402

scale.workdir()
BASE = scale.port(default=26500)
BINARY = os.environ.get("BARCHD", os.path.join(os.getcwd(), "barchd"))
if not os.path.exists(BINARY):
    print("SKIP: no barchd at %s" % BINARY)
    sys.exit(0)
if not shutil.which("openssl"):
    print("SKIP: no openssl to make a certificate with")
    sys.exit(0)

SPACE = "orders"
failures = 0


def check(ok, what):
    global failures
    print("  %-62s %s" % (what, "pass" if ok else "FAIL"), flush=True)
    if not ok:
        failures += 1


# one self-signed certificate for every node, so it is its own CA
cert = os.path.abspath("raft.crt")
key = os.path.abspath("raft.key")
subprocess.run(["openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "1",
                "-subj", "/CN=barch-raft-test", "-keyout", key, "-out", cert],
               check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
TLS = ["raft_tls=on", "tls_pem_certificate_chain_file=" + cert, "tls_private_key_file=" + key]
nodes = [clusternodes.Node(i, BASE, BINARY, TLS) for i in range(3)]


def tls_handshake(port):
    ctx = ssl.create_default_context(cafile=cert)
    ctx.check_hostname = False
    try:
        with socket.create_connection(("127.0.0.1", port), timeout=5) as s:
            with ctx.wrap_socket(s) as t:
                return t.getpeercert().get("subject", ())
    except (OSError, ssl.SSLError):
        return None


try:
    print("a cluster over TLS")
    for n in nodes:
        n.start()
    nodes[0].client().execute_command("CLUSTER", "INIT")
    for n in nodes[1:]:
        n.client().execute_command("CLUSTER", "JOIN", "127.0.0.1", str(nodes[0].port))
    nodes[0].client("configuration").execute_command("SET", SPACE + ".raft", "on")
    check(wait_for(60, lambda: leader_of(nodes, SPACE) is not None and all(
        "members 3" in n.group_line(SPACE) and "learners 0" in n.group_line(SPACE) for n in nodes)),
        "three voters and a leader")
    subject = tls_handshake(nodes[1].raft)
    check(subject is not None and "barch-raft-test" in str(subject),
          "the cluster group's port answers TLS with the node's certificate")

    print("the leader is killed while clients write")
    w = clusternodes.Writers(nodes, SPACE, count=4, prefix="t")
    w.start()
    time.sleep(scale.scaled_seconds(1.5))
    old = leader_of(nodes, SPACE)
    old.kill()
    check(wait_for(30, lambda: leader_of(nodes, SPACE) not in (None, old)), "another node leads")
    time.sleep(scale.scaled_seconds(1.5))
    w.stop()
    print("    acknowledged %d, uncertain %d, told %s" % (len(w.acked), len(w.maybe), w.errors))
    new = leader_of(nodes, SPACE)
    c = new.client(SPACE)
    missing = sum(1 for k, v in w.acked.items() if c.execute_command("GET", k) != v)
    check(len(w.acked) > 0 and missing == 0,
          "every acknowledged write is there (%d, %d missing)" % (len(w.acked), missing))
    old.start()
    check(wait_for(60, lambda: len(set(n.digest(SPACE)[0] for n in nodes)) == 1),
          "the killed node comes back and the copies match")

    print("a certificate that won't load")
    n = nodes[2]
    n.stop()
    n.settings = TLS + ["tls_pem_certificate_chain_file=" + os.path.abspath("no-such.crt")]
    try:
        n.start()
        started = True
    except AssertionError:
        started = False
        n.proc = None
    check(not started, "stops the node")
    check("could not start raft group" in open(n.dir + ".log").read(), "and its log says why")
    n.settings = TLS
    n.start()
finally:
    for n in nodes:
        n.stop()

print("FAILED" if failures else "all passed")
sys.exit(1 if failures else 0)
