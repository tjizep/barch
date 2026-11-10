# Requests that arrive in pieces are answered whole - TODO 855.
#
# The second piece of a split request, and an EOF after a short read, reach the
# session through the reactor's wait, not the read that took the first piece.
# Written for an asio patch that was later dropped (DONE 806); the cases still
# hold for any reader.
#
# Each split case sends a PING with the first piece in one write, and reads the
# PONG before it sends the rest. The PONG says the server read the first piece,
# so the rest can only arrive through the wait. No sleeps.
#
# Every case runs on each way in: TCP, a unix socket, and TLS. START moves the
# listener from one to the next. A unix read can stop short in ways a TCP read
# can't, and a TLS session reads the socket through the ssl stream. TLS is skipped
# when there is no openssl to make a certificate with, and the unix socket where
# Python has no AF_UNIX (Windows).
#
# Cases:
#   splits inside a bulk value, between \r and \n, in the command name,
#   at the end of a run of pipelined GETs
#   a pipeline sent one byte per write
#   requests bigger than the 32 KB read buffer, and one that fills it exactly
#   64 connections each holding half a request at once
#   EOF after a whole request, and EOF in the middle of one
import os
import shutil
import signal
import socket
import ssl
import subprocess
import sys
import tempfile
import time

import scale

scale.workdir()
PORT = scale.port(default=14491)
TLS_PORT = PORT + 1

BINARY = os.environ.get("BARCHD", os.path.join(os.getcwd(), "barchd"))
if not os.path.exists(BINARY):
    print("SKIP: no barchd at %s" % BINARY)
    sys.exit(0)

DATA = os.path.join(os.getcwd(), "splitread_data")
READ_BUFFER = 32 * 1024  # rpc_io_buffer_size in src/rpc/constants.h

# sun_path holds 107 bytes; a run from a deep directory gets a short one instead
SOCK_DIR = DATA if len(DATA) < 90 else tempfile.mkdtemp(prefix="barch-split-")
SOCK = os.path.join(SOCK_DIR, "barchd.sock")


def start():
    os.makedirs(DATA, exist_ok=True)
    for name in os.listdir(DATA):
        os.remove(os.path.join(DATA, name))
    p = subprocess.Popen([BINARY, "--port", str(PORT), "--bind", "127.0.0.1",
                          "--dir", DATA, "--no-save-on-exit"],
                         stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    wait_listening(p, lambda: socket.create_connection(("127.0.0.1", PORT), timeout=0.5))
    return p


def wait_listening(p, connect):
    end = time.time() + 60
    while time.time() < end:
        if p.poll() is not None:
            raise AssertionError("barchd exited with %s" % p.returncode)
        try:
            connect().close()
            return
        except OSError:
            time.sleep(0.1)
    p.kill()
    raise AssertionError("barchd did not start listening")


def stop(p):
    p.terminate()
    try:
        p.wait(timeout=60)
    except subprocess.TimeoutExpired:
        scale.stop_hung(p, signal.SIGTERM, 60)


def resp(*args):
    out = b"*%d\r\n" % len(args)
    for a in args:
        a = a if isinstance(a, bytes) else a.encode()
        out += b"$%d\r\n%s\r\n" % (len(a), a)
    return out


class Conn:
    def __init__(self, sock):
        self.sock = sock
        self.buf = b""

    def fill(self):
        chunk = self.sock.recv(65536)
        if not chunk:
            raise AssertionError("connection closed")
        self.buf += chunk

    def replies(self, count):
        """the raw replies: enough RESP2 for these commands"""
        got = []
        while len(got) < count:
            while b"\r\n" not in self.buf:
                self.fill()
            at = self.buf.index(b"\r\n")
            head = self.buf[:at]
            if head[:1] == b"$" and head != b"$-1":
                n = int(head[1:])
                while len(self.buf) < at + 2 + n + 2:
                    self.fill()
                got.append(self.buf[at + 2:at + 2 + n])
                self.buf = self.buf[at + 2 + n + 2:]
            else:
                got.append(head)
                self.buf = self.buf[at + 2:]
        return got

    def closed(self):
        """True once the server ends the connection"""
        try:
            while True:
                chunk = self.sock.recv(65536)
                if not chunk:
                    return True
                self.buf += chunk
        except ConnectionResetError:
            return True
        except socket.timeout:
            return False

    def close(self):
        self.sock.close()


def tcp_socket(port):
    s = socket.create_connection(("127.0.0.1", port), timeout=60)
    s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    return s


def tcp():
    return Conn(tcp_socket(PORT))


def unix_socket(timeout=60):
    s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    s.settimeout(timeout)
    try:
        s.connect(SOCK)
    except OSError:
        s.close()
        raise
    return s


def unix():
    return Conn(unix_socket())


TLS_CTX = None


def tls():
    return Conn(TLS_CTX.wrap_socket(tcp_socket(TLS_PORT)))


def make_tls_files():
    """a self-signed certificate, its key and DH parameters; None with no openssl"""
    if not shutil.which("openssl"):
        return None
    cert, key, dh = (os.path.join(DATA, n) for n in ("split.crt", "split.key", "split.dh"))
    quiet = dict(check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    subprocess.run(["openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "1",
                    "-subj", "/CN=barch-split-test", "-keyout", key, "-out", cert], **quiet)
    # a named group, so nothing has to be generated
    subprocess.run(["openssl", "genpkey", "-genparam", "-algorithm", "DH",
                    "-pkeyopt", "group:ffdhe2048", "-out", dh], **quiet)
    return cert, key, dh


failures = 0


def check(ok, what):
    global failures
    print("  %-62s %s" % (what, "pass" if ok else "FAIL"), flush=True)
    if not ok:
        failures += 1


def split(c, first, rest, count):
    """first piece behind a PING; the rest only after the PONG; count replies"""
    c.sock.sendall(resp("PING") + first)
    pong = c.replies(1)
    if pong != [b"+PONG"]:
        return [b"PING got %r" % pong]
    c.sock.sendall(rest)
    return c.replies(count)


def cases(connect, name):
    """every split case on connections from connect(); keys are kept apart per name"""
    def key(k):
        return "%s:%s" % (name, k)

    c = connect()

    req = resp("SET", key("k"), "hello")
    at = req.index(b"hello") + 2
    check(split(c, req[:at], req[at:], 1) == [b"+OK"], "split inside a bulk value")

    req = resp("GET", key("k"))
    at = req.index(b"\r\n", 4) + 1
    check(split(c, req[:at], req[at:], 1) == [b"hello"], "split between \\r and \\n")

    at = req.index(b"GET") + 2
    check(split(c, req[:at], req[at:], 1) == [b"hello"], "split in the command name")

    keys = [key("g%02d" % i) for i in range(10)]
    c.sock.sendall(b"".join(resp("SET", k, k + "v") for k in keys))
    c.replies(len(keys))
    run = b"".join(resp("GET", k) for k in keys)
    last = resp("GET", key("k"))
    got = split(c, run + last[:7], last[7:], len(keys) + 1)
    check(got == [(k + "v").encode() for k in keys] + [b"hello"],
          "split at the end of 10 pipelined GETs")

    cmds = [resp("SET", key("b%d" % i), "v%d" % i) for i in range(10)] + \
           [resp("GET", key("b%d" % i)) for i in range(10)]
    for byte in b"".join(cmds):
        c.sock.sendall(bytes([byte]))
    got = c.replies(len(cmds))
    want = [b"+OK"] * 10 + [b"v%d" % i for i in range(10)]
    check(got == want, "20 commands sent one byte per write")

    big = "x" * (3 * READ_BUFFER + 17)
    c.sock.sendall(resp("SET", key("big"), big) + resp("GET", key("big")))
    check(c.replies(2) == [b"+OK", big.encode()], "a request 3 read buffers long")

    req = resp("GET", key("big"))
    got = split(c, req[:9], req[9:], 1)
    check(got == [big.encode()], "split after a request bigger than the buffer")

    # SET exact <value> is READ_BUFFER bytes on the wire, so the read that takes
    # it can fill the buffer to the last byte
    frame = len(resp("SET", key("exact"), ""))
    n = READ_BUFFER - frame
    n -= len(str(n)) - 1  # the length prefix grows with n
    exact = "e" * n
    assert len(resp("SET", key("exact"), exact)) == READ_BUFFER
    c.sock.sendall(resp("SET", key("exact"), exact))
    check(c.replies(1) == [b"+OK"], "a request exactly one read buffer long")
    req = resp("GET", key("exact"))
    check(split(c, req[:5], req[5:], 1) == [exact.encode()], "split after the exact one")
    c.close()

    many = [connect() for _ in range(64)]
    sets = [resp("SET", key("m%d" % i), "w%d" % i) for i in range(64)]
    for m, s in zip(many, sets):
        m.sock.sendall(resp("PING") + s[:12])
    pongs = sum(1 for m in many if m.replies(1) == [b"+PONG"])
    for i, m in enumerate(many):
        m.sock.sendall(sets[i][12:] + resp("GET", key("m%d" % i)))
    good = sum(1 for i, m in enumerate(many) if m.replies(2) == [b"+OK", b"w%d" % i])
    check(pongs == 64 and good == 64, "64 connections split at once, %d right" % good)
    for m in many:
        m.close()

    c = connect()
    c.sock.sendall(resp("PING"))
    c.replies(1)
    c.sock.shutdown(socket.SHUT_WR)
    check(c.closed(), "EOF after a whole request closes the connection")
    c.close()

    c = connect()
    c.sock.sendall(resp("PING") + resp("SET", key("half"), "never")[:10])
    c.replies(1)
    c.sock.shutdown(socket.SHUT_WR)
    check(c.closed(), "EOF in the middle of a request closes the connection")
    c.close()

    c = connect()
    c.sock.sendall(resp("GET", key("half")) + resp("GET", key("k")))
    check(c.replies(2) == [b"$-1", b"hello"], "the half request was dropped, the server is fine")
    c.close()


def command(connect, *args):
    c = connect()
    c.sock.sendall(resp(*args))
    got = c.replies(1)
    c.close()
    return got


proc = start()
try:
    print("over TCP")
    cases(tcp, "tcp")

    # START from a connection is asynchronous: it answers, then a thread stops the
    # TCP and TLS listeners and starts the one asked for. A unix listener is left
    # alone, so the TLS one can start beside it later, from the unix socket. Where
    # there is no unix socket, the TCP listener is still up and TLS starts from it.
    tls_files = make_tls_files()
    if tls_files:
        for name, path in zip(("tls_pem_certificate_chain_file", "tls_private_key_file",
                               "tls_tmp_dh_file"), tls_files):
            check(command(tcp, "CONFIG", "SET", name, path) == [b"+OK"], "CONFIG SET " + name)
    via = tcp
    if hasattr(socket, "AF_UNIX"):
        check(command(tcp, "START", SOCK, "0") == [b"+OK"], "START on a unix socket")
        wait_listening(proc, lambda: unix_socket(timeout=0.5))

        print("over a unix socket")
        cases(unix, "unix")
        via = unix
    else:
        print("SKIP unix socket: this Python has no AF_UNIX")

    if tls_files:
        check(command(via, "START", "127.0.0.1", str(TLS_PORT), "SSL") == [b"+OK"],
              "START with TLS")
        TLS_CTX = ssl.create_default_context(cafile=tls_files[0])
        TLS_CTX.check_hostname = False
        wait_listening(proc, lambda: TLS_CTX.wrap_socket(
            socket.create_connection(("127.0.0.1", TLS_PORT), timeout=0.5)))

        print("over TLS")
        cases(tls, "tls")
    else:
        print("SKIP TLS: no openssl to make a certificate with")
finally:
    stop(proc)
    if SOCK_DIR != DATA:
        shutil.rmtree(SOCK_DIR, ignore_errors=True)

print("\n%s" % ("all split read checks pass" if failures == 0 else "FAILURES above"))
sys.exit(0 if failures == 0 else 1)
