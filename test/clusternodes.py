"""
barchd processes in a Raft cluster, for the cluster tests - TODO 610, 611.

Node i listens on base + i for RESP, and on base + 4 + 4i up for its Raft groups
(the cluster group and up to three data spaces), so four nodes fit in the block of
20 ports ctest gives a test.
"""
import atexit
import os
import re
import shutil
import signal
import subprocess
import time

import redis
from redis.backoff import NoBackoff
from redis.retry import Retry


# every node's, unless a test's own settings say otherwise: those come after it on
# the command line - TODO 620
SECRET = "clusternodes test secret"

# nodes that didn't exit 0 when stopped - TODO 619. A clean stop is 0; a TSan
# report makes it 66, and a crash or an abort is a signal. Nothing else looks at
# that, so a test fails here, at exit, whatever it printed
bad_exits = []


def _check_exits():
    if bad_exits:
        print("FAILED: barchd exited badly - %s (see n<i>.log)" % ", ".join(bad_exits), flush=True)
        os._exit(1)


atexit.register(_check_exits)


class Node:
    def __init__(self, i, base, binary, settings=(), env=None):
        self.i = i
        self.port = base + i
        self.raft = base + 4 + 4 * i
        self.binary = binary
        self.settings = list(settings)
        self.env = dict(env or {})
        self.dir = os.path.join(os.getcwd(), "n%d" % (i + 1))
        self.proc = None
        # a run before this one left its cluster here
        shutil.rmtree(self.dir, ignore_errors=True)
        if os.path.exists(self.dir + ".log"):
            os.remove(self.dir + ".log")
        os.makedirs(self.dir, exist_ok=True)

    def start(self):
        # output to a file: an unread pipe fills and stops barchd
        log = open(self.dir + ".log", "a")
        args = [self.binary, "--port", str(self.port), "--bind", "127.0.0.1", "--dir", self.dir,
                "-c", "raft_port=%d" % self.raft, "-c", "external_host=127.0.0.1",
                "-c", "cluster_heartbeat_ms=500", "-c", "cluster_secret=" + SECRET,
                # 32, not the default 128 - TODO 631: copying a space to a member
                # holds every one of its shards' latches at once, and TSan's deadlock
                # detector stops the process past 64 held by one thread.
                # BARCH_TEST_RAFT_SHARDS runs the tests with another count
                "-c", "raft_shards=" + os.environ.get("BARCH_TEST_RAFT_SHARDS", "32")]
        for s in self.settings:
            args += ["-c", s]
        e = dict(os.environ)
        e.update(self.env)
        self.proc = subprocess.Popen(args, stdout=log, stderr=subprocess.STDOUT, env=e)
        end = time.time() + 60
        while time.time() < end:
            if self.proc.poll() is not None:
                raise AssertionError("node %d exited with %s" % (self.i + 1, self.proc.returncode))
            try:
                self.client().ping()
                return
            except redis.RedisError:
                time.sleep(0.1)
        raise AssertionError("node %d didn't come up" % (self.i + 1))

    def kill(self):
        self.proc.send_signal(signal.SIGKILL)
        self.proc.wait()
        self.proc = None

    def pause(self):
        self.proc.send_signal(signal.SIGSTOP)

    def resume(self):
        self.proc.send_signal(signal.SIGCONT)

    def stop(self):
        if self.proc:
            self.proc.send_signal(signal.SIGTERM)
            try:
                code = self.proc.wait(30)
                if code != 0:
                    bad_exits.append("node %d exited with %d" % (self.i + 1, code))
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait()
                bad_exits.append("node %d didn't stop within 30s" % (self.i + 1))
            self.proc = None

    def client(self, space=None):
        # no retries: redis-py backs off and tries a dead node again for seconds,
        # and finding the next leader is the test's to do
        c = redis.Redis(host="127.0.0.1", port=self.port, single_connection_client=True,
                        socket_timeout=30, socket_connect_timeout=2, decode_responses=True,
                        retry=Retry(NoBackoff(), 0))
        if space:
            c.execute_command("USE", space)
        return c

    def info(self):
        return self.client().execute_command("CLUSTER", "INFO")

    def group_line(self, space):
        for line in self.info():
            if line.startswith("group ") and (" spaces %s" % space) in line:
                return line
        return ""

    def field(self, space, name):
        """a number from this node's CLUSTER INFO line for the group of `space`"""
        m = re.search(r"\b%s (\d+)" % name, self.group_line(space))
        return int(m.group(1)) if m else -1

    def leads(self, space):
        return "(this node)" in self.group_line(space)

    def digest(self, space):
        return self.client().execute_command("CLUSTER", "DIGEST", space)


def wait_for(seconds, f):
    end = time.time() + seconds
    while time.time() < end:
        try:
            if f():
                return True
        except redis.RedisError:
            pass
        time.sleep(0.1)
    return False


def leader_of(nodes, space):
    for n in nodes:
        if n.proc:
            try:
                if n.leads(space):
                    return n
            except redis.RedisError:
                pass
    return None


def on_leader(nodes, space, f, seconds=60):
    """f(leader) for the space's leader, tried again while there's none or it stops
    leading - a slow disk can cost a group its leader for half a second or so. What
    f returns, or None if no leader answered in time."""
    end = time.time() + seconds
    while time.time() < end:
        n = leader_of(nodes, space)
        if n is not None:
            try:
                return f(n)
            except redis.RedisError:
                pass
        time.sleep(0.1)
    return None


class Writers:
    """
    Clients writing keys of their own to `space` through whichever node leads it,
    following NOTLEADER. `acked` is what the cluster acknowledged; `maybe` is what
    may or may not have happened (a connection that died, an UNKNOWN).
    """

    def __init__(self, nodes, space, count=4, prefix="w"):
        import threading
        self.nodes = nodes
        self.space = space
        self.prefix = prefix
        self.acked = {}
        self.maybe = set()
        self.errors = {}
        self.lock = threading.Lock()
        self.stopping = threading.Event()
        # daemons, so a test that dies with them running exits instead of hanging
        # until ctest's timeout
        self.threads = [threading.Thread(target=self.run, args=(w,), daemon=True) for w in range(count)]

    def note(self, kind):
        with self.lock:
            self.errors[kind] = self.errors.get(kind, 0) + 1

    def by_port(self, port):
        for n in self.nodes:
            if n.port == port:
                return n
        return None

    def run(self, w):
        n = 0
        target = leader_of(self.nodes, self.space) or self.nodes[0]
        cl = None
        while not self.stopping.is_set():
            key = "%s%d-%d" % (self.prefix, w, n)
            try:
                if cl is None:
                    cl = target.client(self.space)
                cl.execute_command("SET", key, key)
                with self.lock:
                    self.acked[key] = key
                n += 1
            except redis.ResponseError as e:
                self.note(str(e).split(" ")[0])
                m = re.match(r"NOTLEADER 127\.0\.0\.1:(\d+)", str(e))
                if m and self.by_port(int(m.group(1))):
                    target = self.by_port(int(m.group(1)))
                    cl = None
                    continue                  # refused: it didn't happen, try again
                if str(e).startswith("UNKNOWN"):
                    with self.lock:
                        self.maybe.add(key)
                    n += 1
                time.sleep(0.05)              # TRYAGAIN, or no leader yet
            except (redis.ConnectionError, redis.TimeoutError) as e:
                self.note(type(e).__name__)
                with self.lock:
                    self.maybe.add(key)       # the node went with it: either way
                n += 1
                cl = None
                time.sleep(0.1)
                l = leader_of(self.nodes, self.space)
                target = l if l else target

    def start(self):
        for t in self.threads:
            t.start()

    def stop(self):
        self.stopping.set()
        for t in self.threads:
            t.join()
