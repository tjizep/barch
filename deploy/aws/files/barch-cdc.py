#!/usr/bin/env python3
"""Applies Debezium change events to barchd.

Debezium Server reads the Postgres WAL (pgoutput) and posts each change, as
JSON, to this service with its HTTP sink. This service logs in to barchd and
changes the cached row:

  invalidate  DEL the row's key. The application reloads it on the next miss.
  upsert      SET the row's key to the new row as JSON. DEL on delete.

A TRUNCATE deletes every key under the table's key prefix.

It answers 200 only after barchd has taken the change. Anything else makes
Debezium send the event again, so no change is lost while barchd restarts. It
serves one request at a time, so changes are applied in WAL order.

Only the Python standard library is used. See /opt/barch/CDC.md.
"""

import argparse
import configparser
import http.server
import json
import logging
import socket
import string
import sys

log = logging.getLogger("barch-cdc")


class BarchError(Exception):
    pass


class Barch:
    """A RESP connection to barchd that logs in, and reconnects when it drops."""

    def __init__(self, host, port, user, password, timeout):
        self.addr = (host, port)
        self.user = user
        self.password = password
        self.timeout = timeout
        self.sock = None
        self.buf = b""

    def close(self):
        if self.sock is not None:
            self.sock.close()
        self.sock = None
        self.buf = b""

    def _connect(self):
        self.sock = socket.create_connection(self.addr, self.timeout)
        self.buf = b""
        if self.user:
            self._call("AUTH", self.user, self.password)

    def call(self, *args):
        """Runs one command. Reconnects once if the connection has gone."""
        for attempt in (1, 2):
            try:
                if self.sock is None:
                    self._connect()
                return self._call(*args)
            except (OSError, EOFError):
                self.close()
                if attempt == 2:
                    raise

    def _call(self, *args):
        out = [b"*%d\r\n" % len(args)]
        for a in args:
            b = a if isinstance(a, bytes) else str(a).encode()
            out.append(b"$%d\r\n%s\r\n" % (len(b), b))
        self.sock.sendall(b"".join(out))
        reply = self._read()
        if isinstance(reply, BarchError):
            raise reply
        return reply

    def _line(self):
        while b"\r\n" not in self.buf:
            chunk = self.sock.recv(65536)
            if not chunk:
                raise EOFError("barchd closed the connection")
            self.buf += chunk
        line, self.buf = self.buf.split(b"\r\n", 1)
        return line

    def _exact(self, n):
        while len(self.buf) < n + 2:
            chunk = self.sock.recv(65536)
            if not chunk:
                raise EOFError("barchd closed the connection")
            self.buf += chunk
        data, self.buf = self.buf[:n], self.buf[n + 2:]
        return data

    def _read(self):
        line = self._line()
        kind, rest = line[:1], line[1:]
        if kind == b"+":
            return rest.decode()
        if kind == b"-":
            return BarchError(rest.decode(errors="replace"))
        if kind == b":":
            return int(rest)
        if kind == b"$":
            n = int(rest)
            return None if n < 0 else self._exact(n)
        if kind == b"*":
            n = int(rest)
            return None if n < 0 else [self._read() for _ in range(n)]
        if kind == b"_":
            return None
        raise BarchError("unexpected reply %r" % line[:40])


class Table:
    """How one table's rows map to barch keys."""

    def __init__(self, name, key, mode, ttl):
        if mode not in ("invalidate", "upsert"):
            raise ValueError("%s: mode must be invalidate or upsert, not %r" % (name, mode))
        self.name = name
        self.key = key
        self.mode = mode
        self.ttl = ttl
        self.fields = [f for _, f, _, _ in string.Formatter().parse(key) if f]
        if not self.fields:
            raise ValueError("%s: key %r names no column" % (name, key))

    def key_for(self, schema, table, row):
        values = {"schema": schema, "table": table}
        for f in self.fields:
            if f in ("schema", "table"):
                continue
            if row is None or f not in row:
                raise KeyError("%s: column %r is not in the event" % (self.name, f))
            values[f] = row[f]
        return self.key.format(**values)

    def prefix(self, schema, table):
        """The fixed start of every key, or '' if a row column comes first."""
        head = ""
        for literal, field, _, _ in string.Formatter().parse(self.key):
            head += literal
            if field is None:
                continue
            if field == "schema":
                head += schema
            elif field == "table":
                head += table
            else:
                return head
        return head


class Applier:
    def __init__(self, barch, tables, default, range_batch=1000):
        self.barch = barch
        self.tables = tables
        self.default = default
        self.range_batch = range_batch

    def table(self, schema, table):
        t = self.tables.get("%s.%s" % (schema, table)) or self.tables.get(table)
        return t or self.default

    def apply(self, event):
        """Applies one event. Returns a short note for the log."""
        event = unwrap(event)
        if event is None:
            return "skip: empty"
        op = event.get("op")
        source = event.get("source") or {}
        schema, table = source.get("schema", ""), source.get("table", "")
        if not table:
            return "skip: no table"
        t = self.table(schema, table)
        if t is None:
            return "skip: %s.%s has no key" % (schema, table)
        before, after = event.get("before"), event.get("after")

        if op == "t":
            return self.truncate(t, schema, table)
        if op == "d":
            self.barch.call("DEL", t.key_for(schema, table, before))
            return "del"
        if op not in ("c", "u", "r"):
            return "skip: op %r" % op

        key = t.key_for(schema, table, after)
        # A changed primary key leaves the old row's key behind. pgoutput sends
        # the old key columns in before when that happens.
        if before is not None:
            try:
                old = t.key_for(schema, table, before)
            except KeyError:
                old = key
            if old != key:
                self.barch.call("DEL", old)
        if t.mode == "invalidate":
            self.barch.call("DEL", key)
            return "del"
        value = json.dumps(after, separators=(",", ":"), ensure_ascii=False)
        if t.ttl > 0:
            self.barch.call("SET", key, value, "EX", t.ttl)
        else:
            self.barch.call("SET", key, value)
        return "set"

    def truncate(self, t, schema, table):
        prefix = t.prefix(schema, table)
        if not prefix:
            log.error("TRUNCATE %s.%s: key %r has no fixed prefix, keys are left as they are",
                      schema, table, t.key)
            return "skip: truncate without prefix"
        lower = prefix.encode()
        upper = after_prefix(lower)
        removed = 0
        while True:
            # RANGE includes lower and leaves out upper. Keys found are deleted,
            # so the next round starts at the prefix again.
            keys = self.barch.call("RANGE", lower, upper, self.range_batch) or []
            if not keys:
                break
            removed += self.barch.call("DEL", *keys)
        return "truncate: %d keys" % removed


def after_prefix(prefix):
    """The smallest byte string greater than every string that starts with prefix."""
    b = bytearray(prefix)
    while b and b[-1] == 0xFF:
        b.pop()
    if not b:
        return b"\xff" * (len(prefix) + 1)
    b[-1] += 1
    return bytes(b)


def unwrap(event):
    """Gets the change out of a CloudEvent or a schema envelope."""
    if isinstance(event, dict) and "specversion" in event and "data" in event:
        event = event["data"]
        if isinstance(event, str):
            event = json.loads(event)
    if isinstance(event, dict) and "payload" in event and "schema" in event:
        event = event["payload"]
    return event


def make_handler(applier):
    class Handler(http.server.BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def reply(self, code, text):
            body = (text + "\n").encode()
            self.send_response(code)
            self.send_header("Content-Type", "text/plain")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self):
            if self.path != "/health":
                return self.reply(404, "not found")
            try:
                applier.barch.call("EXISTS", "barch-cdc-health")
            except Exception as e:  # noqa: BLE001 - any failure is unhealthy
                return self.reply(503, "barchd: %s" % e)
            return self.reply(200, "ok")

        def do_POST(self):
            n = int(self.headers.get("Content-Length") or 0)
            body = self.rfile.read(n)
            try:
                doc = json.loads(body) if body.strip() else None
            except ValueError as e:
                # Sending it again won't help, and Debezium sends again on
                # anything but a 2xx. Log it loudly and move on.
                log.error("bad JSON, event dropped: %s: %r", e, body[:200])
                return self.reply(200, "dropped: bad JSON")
            events = doc if isinstance(doc, list) else [doc]
            try:
                notes = [applier.apply(e) for e in events]
            except (KeyError, ValueError) as e:
                log.error("event not applied: %s", e)
                return self.reply(500, str(e))
            except (OSError, EOFError, BarchError) as e:
                log.warning("barchd: %s; Debezium will send the event again", e)
                applier.barch.close()
                return self.reply(503, "barchd: %s" % e)
            log.debug("%s", ", ".join(notes))
            return self.reply(200, "ok")

        def log_message(self, fmt, *args):
            log.debug(fmt, *args)

    return Handler


def load(path):
    cp = configparser.ConfigParser(interpolation=None)
    with open(path) as f:
        cp.read_file(f)
    b = cp["barch"] if cp.has_section("barch") else {}
    password = b.get("password", "")
    if b.get("password_file"):
        with open(b["password_file"]) as f:
            password = f.read().strip()
    barch = Barch(b.get("host", "127.0.0.1"), int(b.get("port", "14000")),
                  b.get("user", "cdc"), password, float(b.get("timeout", "30")))

    d = cp["defaults"] if cp.has_section("defaults") else {}
    mode, ttl = d.get("mode", "invalidate"), int(d.get("ttl", "0"))
    default = Table("defaults", d["key"], mode, ttl) if d.get("key") else None
    tables = {}
    for sec in cp.sections():
        if sec.startswith("table "):
            name = sec[len("table "):].strip()
            s = cp[sec]
            if not s.get("key") and default is None:
                raise ValueError("%s: no key, and [defaults] has none" % name)
            tables[name] = Table(name, s.get("key", default.key if default else ""),
                                 s.get("mode", mode), int(s.get("ttl", str(ttl))))

    lst = cp["listen"] if cp.has_section("listen") else {}
    listen = (lst.get("address", "127.0.0.1"), int(lst.get("port", "14080")))
    return Applier(barch, tables, default), listen


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--config", default="/etc/barch/cdc.conf")
    ap.add_argument("--verbose", action="store_true")
    args = ap.parse_args()
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO,
                        format="%(name)s: %(levelname)s %(message)s", stream=sys.stderr)
    applier, listen = load(args.config)
    server = http.server.HTTPServer(listen, make_handler(applier))
    log.info("listening on %s:%d, barchd at %s:%d", *listen, *applier.barch.addr)
    server.serve_forever()


if __name__ == "__main__":
    main()
