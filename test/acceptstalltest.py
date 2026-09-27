# The accept path in src/rpc/server.cpp - TODO 509.
#
# Only one accept waits at a time, and it used to be set up again only after the
# new connection's first byte had been read, with a blocking read and no timeout.
# So a client that connected and sent nothing (a health check, a port scanner, a
# pooled connection) held up every connection after it. Separately, an accept
# error returned without setting up another accept, so running out of file
# descriptors once meant nobody new could connect until a restart.
#
# Checked here:
#   1. a PING answers while another client sits connected and silent
#   2. a binary protocol ping whose command comes in two packets still answers
#   3. after accept fails for lack of fds, new clients get in once fds are free
import os
import resource
import socket
import struct
import subprocess
import sys
import time

import scale

scale.workdir()
PORT = scale.port(default=14360)

BINARY = os.environ.get("BARCHD", os.path.join(os.getcwd(), "barchd"))
if not os.path.exists(BINARY):
    print("SKIP: no barchd at %s" % BINARY)
    sys.exit(0)

DATA = os.path.join(os.getcwd(), "acceptstall_data")
os.makedirs(DATA, exist_ok=True)
FDS = 256


def low_fds():
    resource.setrlimit(resource.RLIMIT_NOFILE, (FDS, FDS))


def ping(timeout=5):
    s = socket.create_connection(("127.0.0.1", PORT), timeout)
    s.settimeout(timeout)
    try:
        s.sendall(b"*1\r\n$4\r\nPING\r\n")
        return s.recv(64)
    except socket.timeout:
        return None
    finally:
        s.close()


print("start accept stall test with %s" % BINARY, flush=True)
p = subprocess.Popen([BINARY, "--port", str(PORT), "--bind", "127.0.0.1", "--dir", DATA],
                     stdout=subprocess.PIPE, stderr=subprocess.STDOUT, preexec_fn=low_fds)
held = []
try:
    end = time.time() + 60
    while True:
        assert p.poll() is None, "barchd exited with %s" % p.returncode
        try:
            if ping(1) == b"+PONG\r\n":
                break
        except OSError:
            pass
        assert time.time() < end, "barchd never answered"
        time.sleep(0.1)

    # --- 1. a silent client doesn't hold anyone else up ---------------------------
    silent = socket.create_connection(("127.0.0.1", PORT), 5)
    time.sleep(0.3)
    began = time.perf_counter()
    got = ping()
    assert got == b"+PONG\r\n", "a PING behind a silent client answered %r" % (got,)
    print("PING behind a silent client answered in %.2fs" % (time.perf_counter() - began))

    # --- 2. the binary protocol, its command split over two packets --------------
    b = socket.create_connection(("127.0.0.1", PORT), 5)
    b.settimeout(5)
    b.sendall(b"\x00" + struct.pack("<I", 1)[:2])
    time.sleep(0.3)
    b.sendall(struct.pack("<I", 1)[2:])
    version = b""
    while len(version) < 4:
        chunk = b.recv(4 - len(version))
        assert chunk, "the binary ping closed after %d bytes" % len(version)
        version += chunk
    b.close()
    assert struct.unpack("<I", version)[0] > 0, "binary ping answered %r" % (version,)
    silent.close()
    print("binary ping answered version %d" % struct.unpack("<I", version)[0])

    # --- 3. running out of fds doesn't end accepting ------------------------------
    for _ in range(FDS + 100):
        try:
            s = socket.create_connection(("127.0.0.1", PORT), 1)
            s.sendall(b"*1\r\n$4\r\nPING\r\n")
            held.append(s)
        except OSError:
            break
    time.sleep(1)
    for s in held:
        s.close()
    held.clear()
    time.sleep(1.5)
    got = ping()
    assert got == b"+PONG\r\n", "after running out of fds a new client got %r" % (got,)
    print("a new client got in after the fds came back")
finally:
    for s in held:
        s.close()
    p.kill()
    out = p.stdout.read().decode(errors="replace")
    p.wait()

assert "Too many open files" in out, "the fd limit was never reached, so part 3 tested nothing"
# killed rather than stopped, so a sanitizer's exit code never shows: read its reports
assert "WARNING: ThreadSanitizer" not in out and "ERROR: AddressSanitizer" not in out, \
    out[out.find("Sanitizer") - 200:][:4000]
print("accept stall test passed")
