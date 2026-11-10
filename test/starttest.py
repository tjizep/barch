# START moves the listener, and two of them in a row don't hang barchd - TODO 856.
#
# START from a connection is asynchronous: it answers, and a restart thread stops
# the TCP and TLS listeners and starts the one asked for. That thread joins the io
# threads of the listener it stops. A START used to join the previous restart
# thread first, on its own io thread. So a second START that came in while the
# first was restarting waited on a thread that was waiting on it, and SIGTERM
# then hung behind both.
#
# Two STARTs pipelined in one write make that certain, not a race: one io thread
# reads both, and the restart the first one began must join that same thread.
#
# CLIENT LIST shows TLS sessions too - TODO 859. They weren't registered, so it
# didn't, and a bad handshake must still close its socket now the server holds one.
#
# Also the flags. `START iface port SSL ASYNCH` used to start a plain listener, and
# `START iface port ASYNCH` was refused.
#
# No sleeps. Waits are polls for a listener, bounded only so a hang fails.
import os
import shutil
import signal
import socket
import ssl
import subprocess
import sys
import time

import scale

scale.workdir()
PORT = scale.port(default=19700)
NEXT = [scale.port(i, default=19700) for i in range(1, 6)]

BINARY = os.environ.get("BARCHD", os.path.join(os.getcwd(), "barchd"))
if not os.path.exists(BINARY):
    print("SKIP: no barchd at %s" % BINARY)
    sys.exit(0)

DATA = os.path.join(os.getcwd(), "start_data")
LOG = os.path.join(os.getcwd(), "start_barchd.log")


def start():
    os.makedirs(DATA, exist_ok=True)
    for name in os.listdir(DATA):
        os.remove(os.path.join(DATA, name))
    p = subprocess.Popen([BINARY, "--port", str(PORT), "--bind", "127.0.0.1",
                          "--dir", DATA, "--no-save-on-exit"],
                         stdout=open(LOG, "w"), stderr=subprocess.STDOUT)
    p.log_path = LOG
    wait_listening(p, lambda: plain(PORT, timeout=0.5))
    return p


def wait_listening(p, connect):
    end = time.time() + 60
    while time.time() < end:
        if p.poll() is not None:
            raise AssertionError("barchd exited with %s" % p.returncode)
        try:
            connect().close()
            return True
        except OSError:
            time.sleep(0.1)
    return False


def stop(p):
    """SIGTERM, and barchd has to exit: a hang here is the deadlock"""
    p.terminate()
    try:
        p.wait(timeout=60)
    except subprocess.TimeoutExpired:
        scale.stop_hung(p, signal.SIGTERM, 60)


def resp(*args):
    out = b"*%d\r\n" % len(args)
    for a in args:
        a = a.encode()
        out += b"$%d\r\n%s\r\n" % (len(a), a)
    return out


def plain(port, timeout=60):
    return socket.create_connection(("127.0.0.1", port), timeout=timeout)


def reply(s):
    """one simple reply or error line"""
    got = b""
    while not got.endswith(b"\r\n"):
        chunk = s.recv(4096)
        if not chunk:
            return b"<closed>"
        got += chunk
    return got[:-2]


def command(s, *args):
    s.sendall(resp(*args))
    return reply(s)


def bulk(s):
    """one bulk string reply"""
    got = b""
    while b"\r\n" not in got:
        chunk = s.recv(4096)
        if not chunk:
            return b"<closed>"
        got += chunk
    head, _, got = got.partition(b"\r\n")
    if not head.startswith(b"$"):
        return head
    size = int(head[1:])
    while len(got) < size + 2:
        chunk = s.recv(4096)
        if not chunk:
            return b"<closed>"
        got += chunk
    return got[:size]


def client_id(s):
    s.sendall(resp("CLIENT", "INFO"))
    return bulk(s).split()[0]


def client_ids(s):
    s.sendall(resp("CLIENT", "LIST"))
    return [line.split()[0] for line in bulk(s).splitlines() if line]


def until(test):
    """poll, bounded only so a hang fails"""
    end = time.time() + 60
    while time.time() < end:
        if test():
            return True
        time.sleep(0.1)
    return False


def closed_by_peer(s):
    """reads to EOF; False if barchd keeps the socket open"""
    s.settimeout(60)
    try:
        while s.recv(4096):
            pass
        return True
    except socket.timeout:
        return False
    except OSError:
        return True


def refused(port):
    try:
        plain(port, timeout=5).close()
        return False
    except OSError:
        return True


def make_tls_files():
    """a self-signed certificate, its key and DH parameters; None with no openssl"""
    if not shutil.which("openssl"):
        return None
    cert, key, dh = (os.path.join(DATA, n) for n in ("start.crt", "start.key", "start.dh"))
    quiet = dict(check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    subprocess.run(["openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "1",
                    "-subj", "/CN=barch-start-test", "-keyout", key, "-out", cert], **quiet)
    subprocess.run(["openssl", "genpkey", "-genparam", "-algorithm", "DH",
                    "-pkeyopt", "group:ffdhe2048", "-out", dh], **quiet)
    return cert, key, dh


failures = 0


def check(ok, what):
    global failures
    print("  %-62s %s" % (what, "pass" if ok else "FAIL"), flush=True)
    if not ok:
        failures += 1


def need(ok, what):
    """a check the rest of the test stands on"""
    check(ok, what)
    if not ok:
        raise AssertionError("can't go on: " + what)


def tls_ping(ctx, port):
    """PONG over a TLS session, or why not"""
    try:
        s = ctx.wrap_socket(plain(port, timeout=10))
    except (OSError, ssl.SSLError) as e:
        return "handshake: %s" % e
    try:
        return command(s, "PING")
    finally:
        s.close()


proc = start()
try:
    c = plain(PORT)
    check(command(c, "START", "127.0.0.1", str(PORT), "BOGUS") == b"-ERR invalid argument",
          "an unknown flag is refused")
    check(command(c, "START", "127.0.0.1", str(PORT), "SSL", "SSL") == b"-ERR invalid argument",
          "a flag twice is refused")

    tls_files = make_tls_files()
    if tls_files:
        for name, path in zip(("tls_pem_certificate_chain_file", "tls_private_key_file",
                               "tls_tmp_dh_file"), tls_files):
            check(command(c, "CONFIG", "SET", name, path) == b"+OK", "CONFIG SET " + name)
        ctx = ssl.create_default_context(cafile=tls_files[0])
        ctx.check_hostname = False

        # SSL with ASYNCH after it, then before it: a TLS listener both times
        port = NEXT[0]
        check(command(c, "START", "127.0.0.1", str(port), "SSL", "ASYNCH") == b"+OK",
              "START SSL ASYNCH")
        c.close()
        need(wait_listening(proc, lambda: plain(port, timeout=0.5)), "it listens")
        need(tls_ping(ctx, port) == b"+PONG", "SSL ASYNCH gives a TLS listener")

        c = ctx.wrap_socket(plain(port))
        port = NEXT[1]
        need(command(c, "START", "127.0.0.1", str(port), "ASYNCH", "SSL") == b"+OK",
             "START ASYNCH SSL, from a TLS session")
        c.close()
        need(wait_listening(proc, lambda: plain(port, timeout=0.5)), "it listens")
        need(tls_ping(ctx, port) == b"+PONG", "ASYNCH SSL gives a TLS listener")
        c = ctx.wrap_socket(plain(port))

        # TLS sessions in CLIENT LIST
        me = client_id(c)
        check(me.startswith(b"id=") and me in client_ids(c),
              "CLIENT LIST shows the TLS session asking")
        other = ctx.wrap_socket(plain(port))
        gone = client_id(other)
        check(gone in client_ids(c), "and another TLS session")
        other.close()
        check(until(lambda: gone not in client_ids(c)), "and drops it once it closes")

        # not TLS on a TLS port: the handshake fails and the socket closes
        bad = plain(port)
        bad.sendall(resp("PING"))
        check(closed_by_peer(bad), "a failed handshake is closed")
        bad.close()
    else:
        print("SKIP TLS: no openssl to make a certificate with")

    # ASYNCH alone: a plain listener
    port = NEXT[2]
    need(command(c, "START", "127.0.0.1", str(port), "ASYNCH") == b"+OK", "START ASYNCH")
    c.close()
    need(wait_listening(proc, lambda: plain(port, timeout=0.5)), "it listens")
    c = plain(port)
    check(command(c, "PING") == b"+PONG", "ASYNCH gives a plain listener")

    # two STARTs in one write, on one io thread
    first, last = NEXT[3], NEXT[4]
    c.sendall(resp("START", "127.0.0.1", str(first)) + resp("START", "127.0.0.1", str(last)))
    need(wait_listening(proc, lambda: plain(last, timeout=0.5)),
         "two STARTs in one write: the second one listens")
    s = plain(last)
    check(command(s, "PING") == b"+PONG", "and answers")
    s.close()
    check(refused(first), "the first one was moved on")
    c.close()
finally:
    stop(proc)

check(proc.returncode == 0, "SIGTERM after it ends barchd (exit %s)" % proc.returncode)
if failures:
    print("%d FAILED" % failures)
    sys.exit(1)
print("all passed")
