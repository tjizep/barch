"""
A test's redis client sends a command once - TODO 957.

redis-py 8 times a read out at 5 s, and its default retry sends the command again on
a new connection. barch keeps the space a client chose with USE per connection, so
the resend ran in the default space (TODO 900). scale.py makes every redis.Redis a
test builds send once and wait for ever, unless the test asks for its own.

Checked against a server that reads one command and closes without an answer:
  - a client with default arguments fails at that command, on one connection;
  - its read timeout is none, and an explicit one, by keyword or position, holds;
  - an explicit retry still retries.
No barchd: the server here is a socket in a thread.
"""
import socket
import sys
import threading

import scale

import redis
from redis.backoff import NoBackoff
from redis.retry import Retry

PORT = scale.port(default=14495)
MARK = b"MARK"

failures = 0


def check(ok, what):
    global failures
    print("  %-74s %s" % (what, "pass" if ok else "FAIL"), flush=True)
    if not ok:
        failures += 1


def hang_up_after_one(listener, seen):
    """
    one connection at a time: read a command, then close with no answer. Stops at a
    connection that sends MARK, so every connection before it has been counted
    """
    while True:
        c, _ = listener.accept()
        with c:
            data = c.recv(65536)
            if data.startswith(MARK):
                return
            seen.append(data)


def connections_for(client):
    """how many connections one PING on `client` took, and whether it failed"""
    listener = socket.create_server(("127.0.0.1", PORT))
    seen = []
    t = threading.Thread(target=hang_up_after_one, args=(listener, seen))
    t.start()
    try:
        client.ping()
        failed = False
    except (redis.ConnectionError, redis.TimeoutError):
        failed = True
    finally:
        with socket.create_connection(("127.0.0.1", PORT)) as m:
            m.sendall(MARK)
        t.join()
        listener.close()
    return len(seen), failed


def kwargs_of(client):
    return client.connection_pool.connection_kwargs


print("a client with default arguments")
plain = redis.Redis(host="127.0.0.1", port=PORT)
n, failed = connections_for(plain)
check(failed, "a PING the server hangs up on fails")
check(n == 1, "on one connection, not resent (%d)" % n)
check(kwargs_of(plain)["socket_timeout"] is None, "no read timeout")

print("a client that asks for its own")
check(kwargs_of(redis.Redis(host="127.0.0.1", port=PORT, socket_timeout=60))["socket_timeout"] == 60,
      "socket_timeout by keyword holds")
check(kwargs_of(redis.Redis("127.0.0.1", PORT, 0, None, 30))["socket_timeout"] == 30,
      "socket_timeout by position holds")
n, failed = connections_for(redis.Redis(host="127.0.0.1", port=PORT, retry=Retry(NoBackoff(), 2)))
check(failed and n == 3, "an explicit retry still retries (%d connections)" % n)

print("FAILED" if failures else "all passed")
sys.exit(1 if failures else 0)
