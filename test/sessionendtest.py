"""
A session whose read chain ends for good shuts its socket down - TODO 932.

The collector frees a session on let_go, or when a peek of its socket reads 0. A TLS
session that read a bad record, or a close_notify, stopped reading and did neither.
The TCP stayed open, so the peek never read 0. The session, its fd and its CLIENT
LIST line stayed for as long as the client did.

Checked:
  - a TLS record with a bad MAC: barchd ends the connection, and CLIENT LIST drops it;
  - a TLS close_notify with the TCP left open: the same;
  - a TCP client that half closes after a pipeline gets every reply, then the end;
  - a BLPOP parked when its client half closes is let go, and so is its session;
  - a TCP client that resets is dropped from CLIENT LIST.
TLS is skipped when there is no openssl to make a certificate with.
"""
import os
import shutil
import socket
import ssl
import struct
import subprocess
import sys
import time

import scale

scale.workdir()
PORT = scale.port(0, default=14850)
TLS_PORT = scale.port(1, default=14851)
BINARY = os.environ.get("BARCHD", os.path.join(os.getcwd(), "barchd"))
if not os.path.exists(BINARY):
    print("SKIP: no barchd at %s" % BINARY)
    sys.exit(0)

# failsafes only: a broken run ends here instead of hanging
READ_FAILSAFE = 20
WAIT_FAILSAFE = 60

failures = 0


def check(ok, what):
    global failures
    print("  %-74s %s" % (what, "pass" if ok else "FAIL"), flush=True)
    if not ok:
        failures += 1


def cmd(*args):
    out = b"*%d\r\n" % len(args)
    for a in args:
        a = a.encode() if isinstance(a, str) else a
        out += b"$%d\r\n%s\r\n" % (len(a), a)
    return out


def read_to_end(s):
    """everything up to the server's end, and whether the end came"""
    got = b""
    s.settimeout(READ_FAILSAFE)
    try:
        while True:
            chunk = s.recv(65536)
            if not chunk:
                return got, True
            got += chunk
    except socket.timeout:
        return got, False
    except ConnectionResetError:
        return got, True


def read_until(s, want):
    got = b""
    s.settimeout(READ_FAILSAFE)
    try:
        while len(got) < len(want):
            chunk = s.recv(65536)
            if not chunk:
                break
            got += chunk
    except socket.timeout:
        pass
    return got


class Observer:
    """one connection that asks; over TLS once START has moved barchd there"""

    def __init__(self, s):
        self.s = s
        self.buf = b""

    def line(self):
        while b"\r\n" not in self.buf:
            chunk = self.s.recv(65536)
            if not chunk:
                raise ConnectionError("observer lost")
            self.buf += chunk
        l, self.buf = self.buf.split(b"\r\n", 1)
        return l

    def reply(self):
        l = self.line()
        if l[:1] in (b"+", b"-", b":"):
            return l
        if l[:1] == b"$":
            n = int(l[1:])
            while len(self.buf) < n + 2:
                self.buf += self.s.recv(65536)
            v, self.buf = self.buf[:n], self.buf[n + 2:]
            return v
        if l[:1] == b"=":  # verbatim string, RESP3
            n = int(l[1:])
            while len(self.buf) < n + 2:
                self.buf += self.s.recv(65536)
            v, self.buf = self.buf[4:n], self.buf[n + 2:]
            return v
        raise ValueError(l)

    def ask(self, *args):
        self.s.sendall(cmd(*args))
        return self.reply()

    def listed(self, addr):
        """whether CLIENT LIST has the session whose client is at `addr`"""
        return any(addr in l.split() for l in self.ask("CLIENT", "LIST").decode().splitlines())

    def blocked(self):
        info = self.ask("INFO", "clients").decode()
        for l in info.splitlines():
            if l.startswith("blocked_clients:"):
                return int(l.split(":")[1])
        return -1

    def wait_gone(self, addr):
        """
        until CLIENT LIST drops the session at `addr`. The collector runs on its own
        clock. One session, not a count: a count taken while wait_listening's probe
        was still listed was one too many after the sweep, and waited for ever on
        Windows - TODO 956
        """
        end = time.time() + WAIT_FAILSAFE
        while time.time() < end:
            if not self.listed(addr):
                return True
            time.sleep(0.05)
        return False

    def wait_blocked(self, n):
        end = time.time() + WAIT_FAILSAFE
        while time.time() < end:
            if self.blocked() == n:
                return True
            time.sleep(0.02)
        return False


def make_tls_files(data):
    if not shutil.which("openssl"):
        return None
    cert, key, dh = (os.path.join(data, n) for n in ("end.crt", "end.key", "end.dh"))
    quiet = dict(check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    subprocess.run(["openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "1",
                    "-subj", "/CN=barch-end-test", "-keyout", key, "-out", cert], **quiet)
    subprocess.run(["openssl", "genpkey", "-genparam", "-algorithm", "DH",
                    "-pkeyopt", "group:ffdhe2048", "-out", dh], **quiet)
    return cert, key, dh


def wait_listening(connect):
    end = time.time() + 60
    while time.time() < end:
        try:
            connect().close()
            return True
        except OSError:
            time.sleep(0.1)
    return False


def addr(s):
    """the client's end, as CLIENT LIST's addr= has it"""
    return "addr=%s:%d" % s.getsockname()[:2]


def tcp():
    return socket.create_connection(("127.0.0.1", PORT), timeout=READ_FAILSAFE)


DATA = os.path.join(os.getcwd(), "sessionend_data")
os.makedirs(DATA, exist_ok=True)
log = open(os.path.join(os.getcwd(), "sessionend.log"), "w")
p = subprocess.Popen([BINARY, "--port", str(PORT), "--bind", "127.0.0.1", "--dir", DATA, "--no-save-on-exit",
                      "-c", "save_interval=86400000"],
                     stdout=log, stderr=log)
try:
    wait_listening(tcp)
    obs = Observer(tcp())

    print("a TCP client that half closes after a pipeline")
    s = tcp()
    me = addr(s)
    s.sendall(cmd("SET", "a", "1") + cmd("GET", "a") + cmd("KEYS", "a") + cmd("PING"))
    s.shutdown(socket.SHUT_WR)
    got, ended = read_to_end(s)
    want = b"+OK\r\n$1\r\n1\r\n*1\r\n$1\r\na\r\n+PONG\r\n"
    check(got == want, "every reply comes (%r)" % got)
    check(ended, "then the end")
    s.close()
    check(obs.wait_gone(me), "and CLIENT LIST drops it")

    print("a BLPOP parked when its client half closes")
    obs.ask("DEL", "qk")
    s = tcp()
    me = addr(s)
    s.sendall(cmd("BLPOP", "qk", "0"))
    check(obs.wait_blocked(1), "BLPOP parks")
    s.shutdown(socket.SHUT_WR)
    check(obs.wait_blocked(0), "the park is let go")
    got, ended = read_to_end(s)
    check(ended and got == b"", "the connection ends with no reply (%r)" % got)
    s.close()
    check(obs.wait_gone(me), "and CLIENT LIST drops it")
    obs.ask("RPUSH", "qk", "v")
    check(obs.ask("LPOP", "qk") == b"v", "a push after is not taken by it")

    print("a TCP client that resets")
    s = tcp()
    me = addr(s)
    s.sendall(cmd("PING"))
    check(read_until(s, b"+PONG\r\n") == b"+PONG\r\n", "PING is answered")
    check(obs.listed(me), "CLIENT LIST shows it")
    s.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0))
    s.close()
    check(obs.wait_gone(me), "CLIENT LIST drops it")

    tls_files = make_tls_files(DATA)
    if tls_files:
        for name, path in zip(("tls_pem_certificate_chain_file", "tls_private_key_file",
                               "tls_tmp_dh_file"), tls_files):
            check(obs.ask("CONFIG", "SET", name, path) == b"+OK", "CONFIG SET " + name)
        # START moves barchd to the TLS port and closes the TCP listener
        check(obs.ask("START", "127.0.0.1", str(TLS_PORT), "SSL") == b"+OK", "START with TLS")
        ctx = ssl.create_default_context(cafile=tls_files[0])
        ctx.check_hostname = False

        def tls():
            return ctx.wrap_socket(socket.create_connection(("127.0.0.1", TLS_PORT),
                                                            timeout=READ_FAILSAFE))

        check(wait_listening(tls), "TLS listens")
        obs.s.close()
        obs = Observer(tls())

        print("a TLS record with a bad MAC")
        s = tls()
        me = addr(s)
        s.sendall(cmd("PING"))
        check(read_until(s, b"+PONG\r\n") == b"+PONG\r\n", "PING is answered")
        check(obs.listed(me), "CLIENT LIST shows it")
        # the TLS object lets go of the socket, and raw takes it: Windows can't
        # os.dup a socket - TODO 954
        raw = socket.socket(fileno=s.detach())
        raw.sendall(b"\x17\x03\x03\x00\x20" + b"x" * 32)
        got, ended = read_to_end(raw)
        check(ended, "barchd ends the connection (%d bytes of alert first)" % len(got))
        check(obs.wait_gone(me), "and CLIENT LIST drops it")
        raw.close()
        s.close()

        print("a TLS close_notify with the TCP left open")
        s = tls()
        me = addr(s)
        s.sendall(cmd("PING"))
        check(read_until(s, b"+PONG\r\n") == b"+PONG\r\n", "PING is answered")
        check(obs.listed(me), "CLIENT LIST shows it")
        # sends close_notify, and then wants barchd's: not blocking, so it only sends
        s.setblocking(False)
        try:
            s.unwrap()
        except (ssl.SSLWantReadError, ssl.SSLError, OSError):
            pass
        raw = socket.socket(fileno=s.detach())
        raw.setblocking(True)
        got, ended = read_to_end(raw)
        check(ended, "barchd ends the connection (%d bytes first)" % len(got))
        check(obs.wait_gone(me), "and CLIENT LIST drops it")
        raw.close()
        s.close()
    else:
        print("SKIP TLS: no openssl to make a certificate with")
    obs.s.close()
finally:
    p.kill()
    p.wait()

print("FAILED" if failures else "all passed")
sys.exit(1 if failures else 0)
