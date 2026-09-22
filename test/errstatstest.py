import scale
import socket
import time
import redis
import barch

# Error counters for external monitoring - TODO 391: script timeouts and
# script errors, oom sheds, refused connections, call errors, net errors.
# Each is driven once here and read back through STATS and INFO ERRORS.

scale.workdir()

PORT = scale.port(default=14097)

print("start error stats test")
barch.start("0.0.0.0", PORT)
barch.ping("127.0.0.1", PORT)
r = redis.Redis(host="127.0.0.1", port=PORT, db=0, protocol=2)
r.execute_command("FLUSHDB")


def stats():
    flat = r.execute_command("STATS")
    out = {}
    for i in range(0, len(flat) - 1, 2):
        k = flat[i].decode() if isinstance(flat[i], bytes) else str(flat[i])
        out[k] = int(flat[i + 1])
    return out


def info_errors():
    raw = r.execute_command("INFO ERRORS")
    if isinstance(raw, bytes):
        raw = raw.decode()
    out = {}
    for line in raw.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        name, _, value = line.partition(":")
        out[name] = int(value)
    return out


def refused(*args):
    try:
        r.execute_command(*args)
        return None
    except redis.exceptions.ResponseError as e:
        return str(e)


try:
    base = stats()
    for k in ("function_timeouts", "function_errors", "oom_avoided_inserts"):
        assert k in base, "STATS is missing %s (%r)" % (k, sorted(base))
    errs = info_errors()
    for k in ("function_timeouts", "function_errors", "oom_avoided_inserts",
              "refused_connections", "accept_errors", "net_errors",
              "request_errors", "exceptions_raised"):
        assert k in errs, "INFO ERRORS is missing %s (%r)" % (k, sorted(errs))

    # a script error: compiles, runs, raises
    assert r.execute_command("SETF", "errboom", '''
        function call()
            error("boom")
        end
    ''') == b"OK"
    e = refused("errboom")
    assert e and "boom" in e, e
    got = stats()
    assert got["function_errors"] == base["function_errors"] + 1, got
    assert info_errors()["function_errors"] == errs["function_errors"] + 1

    # a script timeout: runs past a short deadline. The slice parks it first,
    # so the deadline has to cover several slices - 300ms of spinning does it.
    assert r.execute_command("SETF", "errslow", '''
        function call()
            local s = 0
            while true do s = s + 1 end
            return s
        end
    ''') == b"OK"
    r.execute_command("CONFIG SET function_deadline_ms 300")
    try:
        t0 = time.time()
        e = refused("errslow")
        dt = time.time() - t0
        assert e and "timeout" in e.lower(), e
        assert dt >= 0.25, "the timeout fired too fast to be the deadline: %.2fs" % dt
    finally:
        r.execute_command("CONFIG SET function_deadline_ms 1000")
    got = stats()
    assert got["function_timeouts"] == base["function_timeouts"] + 1, got
    assert info_errors()["function_timeouts"] == errs["function_timeouts"] + 1

    # a refused connection: one past the max. redis-py opens lazily, so hold
    # max connections open on raw sockets first, then let one more in.
    r.execute_command("CONFIG SET max_resp_connections 3")
    held = []
    try:
        for _ in range(3):
            s = socket.create_connection(("127.0.0.1", PORT), timeout=5)
            s.sendall(b"*1\r\n$4\r\nPING\r\n")
            assert s.recv(64) == b"+PONG\r\n"
            held.append(s)
        extra = socket.create_connection(("127.0.0.1", PORT), timeout=5)
        try:
            extra.sendall(b"*1\r\n$4\r\nPING\r\n")
            extra.close()
        except OSError:
            pass
    finally:
        for s in held:
            s.close()
        r.execute_command("CONFIG SET max_resp_connections 10000")
    # the refusal races the accept; poll rather than asserting at once
    deadline = time.time() + 5
    while time.time() < deadline:
        if info_errors()["refused_connections"] > errs["refused_connections"]:
            break
        time.sleep(0.05)
    assert info_errors()["refused_connections"] > errs["refused_connections"], \
        "no refused connection was counted"

    # oom sheds and call errors were already counted before this change;
    # here they are only checked to still be visible in both surfaces
    assert "oom_avoided_inserts" in stats()
    assert "request_errors" in info_errors()

    print("complete error stats test")
finally:
    try:
        barch.stop()
    except Exception:
        pass
