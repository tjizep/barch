# mail.send() inside stored Luau functions: SMTP over libcurl, parked like
# http.request, with the server and its password read from the configuration
# space and the password hidden from scripts. See TODO 368.
import base64
import email
import email.policy
import http.client
import json
import os
import socketserver
import threading
import time

import scale

import redis
import barch

# barch writes its shards to the cwd, so work somewhere of our own
scale.workdir()

PORT = scale.port(default=14120)
SMTP_PORT = scale.port(1, default=18120)
CROW_PORT = scale.port(2, default=18121)
QDIR = os.path.join(os.getcwd(), "queues")

# a queue file left from a failed run would be delivered as soon as the consumer
# armed, and this test counts what arrives
if os.path.isdir(QDIR):
    for f in os.listdir(QDIR):
        os.remove(os.path.join(QDIR, f))

print("start mail luau test", flush=True)

# --- a small SMTP server -------------------------------------------------------
# Python 3.12 dropped smtpd and aiosmtpd is not something the venv has, and the
# conversation is short enough to carry here. It records what each session did:
# the login, the envelope and the message as it arrived.

received = []
received_lock = threading.Lock()


class SmtpHandler(socketserver.StreamRequestHandler):
    def say(self, line):
        self.wfile.write(line.encode() + b"\r\n")
        self.wfile.flush()

    def line(self):
        raw = self.rfile.readline()
        if not raw:
            raise ConnectionError("client went away")
        return raw.decode("utf-8", "replace").rstrip("\r\n")

    def handle(self):
        session = {"auth": None, "from": None, "rcpt": [], "data": None}
        self.say("220 barch-test ESMTP")
        try:
            while True:
                cmd = self.line()
                up = cmd.upper()
                if up.startswith("EHLO"):
                    self.say("250-barch-test")
                    self.say("250-AUTH PLAIN LOGIN")
                    self.say("250 8BITMIME")
                elif up.startswith("HELO"):
                    self.say("250 barch-test")
                elif up.startswith("AUTH PLAIN"):
                    parts = cmd.split(" ", 2)
                    if len(parts) < 3:
                        self.say("334 ")
                        blob = self.line()
                    else:
                        blob = parts[2]
                    _, user, pw = base64.b64decode(blob).split(b"\0")
                    session["auth"] = (user.decode(), pw.decode())
                    self.say("235 ok")
                elif up.startswith("AUTH LOGIN"):
                    self.say("334 VXNlcm5hbWU6")
                    user = base64.b64decode(self.line()).decode()
                    self.say("334 UGFzc3dvcmQ6")
                    pw = base64.b64decode(self.line()).decode()
                    session["auth"] = (user, pw)
                    self.say("235 ok")
                elif up.startswith("MAIL FROM:"):
                    session["from"] = cmd[10:].split()[0].strip("<>")
                    self.say("250 ok")
                elif up.startswith("RCPT TO:"):
                    who = cmd[8:].split()[0].strip("<>")
                    if who.startswith("reject"):
                        self.say("550 no such user")
                    else:
                        session["rcpt"].append(who)
                        self.say("250 ok")
                elif up == "DATA":
                    self.say("354 go ahead")
                    lines = []
                    while True:
                        l = self.rfile.readline()
                        if not l or l == b".\r\n":
                            break
                        if l.startswith(b".."):
                            l = l[1:]
                        lines.append(l)
                    session["data"] = b"".join(lines)
                    msg = email.message_from_bytes(session["data"], policy=email.policy.default)
                    if "slow" in str(msg["Subject"] or ""):
                        time.sleep(1.0)
                    with received_lock:
                        received.append(dict(session))
                    session = {"auth": session["auth"], "from": None, "rcpt": [], "data": None}
                    self.say("250 queued")
                elif up in ("RSET", "NOOP"):
                    self.say("250 ok")
                elif up == "QUIT":
                    self.say("221 bye")
                    return
                else:
                    self.say("502 not here")
        except (ConnectionError, OSError):
            return


class SmtpServer(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True


smtp = SmtpServer(("127.0.0.1", SMTP_PORT), SmtpHandler)
threading.Thread(target=smtp.serve_forever, daemon=True).start()


def parsed(i=-1):
    with received_lock:
        s = received[i]
    return s, email.message_from_bytes(s["data"], policy=email.policy.default)


def count():
    with received_lock:
        return len(received)


def wait_until(pred, timeout=15.0, step=0.05):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if pred():
            return True
        time.sleep(step)
    return pred()


# --- barch -----------------------------------------------------------------------

barch.start("0.0.0.0", PORT)
barch.ping("127.0.0.1", PORT)
r = redis.Redis(host="127.0.0.1", port=PORT, db=0, protocol=2)
r.execute_command("FLUSHDB")
r.execute_command("configuration:FLUSHDB")
assert r.execute_command("CONFIG", "SET", "queue_dir", QDIR) == b"OK"


def conf(key, value):
    r.execute_command("configuration:SET", key, value)


SERVER = f"smtp://127.0.0.1:{SMTP_PORT}"
conf("mail.server", SERVER)
conf("mail.user", "mailer")
conf("mail.password", "hunter2")
conf("mail.from", "Barch Test <noreply@barch.test>")
conf("mail.starttls", "off")

# the send and the table it answers, flattened to something RESP can carry
SEND = '''
function call(spec)
    local m = simdjson.parse(spec)
    local r = mail.send(m)
    return simdjson.encode({ok = r.ok, code = r.code, message_id = r.message_id,
                            error = r.error or ""})
end
'''

FAST = '''
function call()
    return "fast"
end
'''


def send(**spec):
    return json.loads(r.execute_command("SENDMAIL", json.dumps(spec)))


def error_of(**spec):
    try:
        r.execute_command("SENDMAIL", json.dumps(spec))
    except redis.ResponseError as e:
        return str(e)
    return None


try:
    assert r.execute_command("SETF", "sendmail", SEND) == b"OK"
    assert r.execute_command("SETF", "fast", FAST) == b"OK"

    print("a plain text message", flush=True)
    got = send(to="alice@example.com", cc=["Bob <bob@example.com>"],
               bcc="carol@example.com", subject="Hello", text="line one\nline two\n")
    assert got["ok"] is True and got["code"] == 250, got
    s, msg = parsed()
    assert s["auth"] == ("mailer", "hunter2"), s["auth"]
    assert s["from"] == "noreply@barch.test", s["from"]
    assert s["rcpt"] == ["alice@example.com", "bob@example.com", "carol@example.com"], s["rcpt"]
    assert msg["From"] == "Barch Test <noreply@barch.test>", msg["From"]
    assert msg["To"] == "alice@example.com", msg["To"]
    assert msg["Cc"] == "Bob <bob@example.com>", msg["Cc"]
    assert msg["Bcc"] is None, "Bcc must not be in the headers"
    assert msg["Subject"] == "Hello", msg["Subject"]
    assert msg["Message-ID"] == got["message_id"], (msg["Message-ID"], got)
    assert got["message_id"].endswith("@barch.test>"), got
    assert msg["Date"] is not None
    assert msg.get_content() == "line one\r\nline two\r\n", repr(msg.get_content())

    print("utf-8 subject and name, text and html together", flush=True)
    got = send(to="Zoë Brontë <zoe@example.com>", subject="Ünïcödé — a subject long enough "
               "to need more than one encoded word, which is the point of it",
               text="plain ✓", html="<p>html ✓</p>", reply_to="help@barch.test")
    assert got["ok"], got
    s, msg = parsed()
    assert msg["Subject"] == ("Ünïcödé — a subject long enough to need more than one "
                              "encoded word, which is the point of it"), msg["Subject"]
    assert msg["To"].addresses[0].display_name == "Zoë Brontë", msg["To"]
    assert msg["Reply-To"] == "help@barch.test", msg["Reply-To"]
    assert msg.get_content_type() == "multipart/alternative", msg.get_content_type()
    assert msg.get_body(("plain",)).get_content().strip() == "plain ✓"
    assert msg.get_body(("html",)).get_content().strip() == "<p>html ✓</p>"

    print("a message id from the caller is kept", flush=True)
    got = send(to="alice@example.com", subject="id", text="x", message_id="order-42@barch.test")
    assert got["message_id"] == "<order-42@barch.test>", got
    assert parsed()[1]["Message-ID"] == "<order-42@barch.test>"

    print("a script cannot read a password out of configuration", flush=True)
    conf("sp.foreign_password", "sekrit")
    PEEK = '''
function call()
    local c = barch.space.configuration
    local seen = {}
    for row in c do
        if row.type == "key" then seen[#seen + 1] = row.key end
    end
    table.sort(seen)
    return simdjson.encode({
        server = c["mail.server"] or "nil",
        password = c["mail.password"] or "nil",
        foreign = c["sp.foreign_password"] or "nil",
        walk = table.concat(seen, ","),
    })
end
'''
    assert r.execute_command("SETF", "peek", PEEK) == b"OK"
    peek = json.loads(r.execute_command("PEEK"))
    assert peek["server"] == SERVER, peek
    assert peek["password"] == "nil", peek
    assert peek["foreign"] == "nil", peek
    assert "mail.server" in peek["walk"], peek
    assert "password" not in peek["walk"], peek
    # RESP with the rights to read configuration still can, and the password is
    # still what the send used
    assert r.execute_command("configuration:GET", "mail.password") == b"hunter2"

    print("bad calls are errors, not failed sends", flush=True)
    for spec, want in [
        (dict(subject="s", text="t"), "someone to send to"),
        (dict(to="a@example.com", subject="s"), "text or html"),
        (dict(to="not an address", subject="s", text="t"), "is not an address"),
        (dict(to="a@example.com", subject="s\r\nBcc: evil@example.com", text="t"), "line break"),
        (dict(to="a@example.com\r\nX: y", subject="s", text="t"), "line break"),
        (dict(to="a@example.com", subject="s", text="t", message_id="nope"), "message_id"),
    ]:
        err = error_of(**spec)
        assert err is not None and want in err, (spec, err)

    print("a refused recipient comes back as a failed send", flush=True)
    before = count()
    got = send(to="reject@example.com", subject="s", text="t")
    assert got["ok"] is False and got["code"] == 550, got
    assert got["error"], got
    assert count() == before

    print("a server that is not there is handled, not fatal", flush=True)
    conf("mail.server", "smtp://127.0.0.1:9")
    got = send(to="a@example.com", subject="s", text="t", timeout=2000)
    assert got["ok"] is False and got["error"], got
    assert r.execute_command("FAST") == b"fast"
    conf("mail.server", "http://127.0.0.1:%d" % SMTP_PORT)
    assert "smtp://" in (error_of(to="a@example.com", subject="s", text="t") or "")
    r.execute_command("configuration:DEL", "mail.server")
    assert "mail.server" in (error_of(to="a@example.com", subject="s", text="t") or "")
    conf("mail.server", SERVER)

    print("without the outbound category the send is refused", flush=True)
    r.execute_command("ACL", "SETUSER", "nomail", "on", ">pw", "+read", "+write", "+data",
                      "+keys", "+function", "+connection")
    nomail = redis.Redis(host="127.0.0.1", port=PORT, db=0, protocol=2,
                         username="nomail", password="pw")
    try:
        nomail.execute_command("SENDMAIL", json.dumps(dict(to="a@example.com", subject="s",
                                                           text="t")))
        assert False, "sent without outbound"
    except redis.ResponseError as e:
        assert "outbound category" in str(e), str(e)

    # the point of parking, as for http.request: while the server sits on DATA
    # for a second, the pool keeps running other work
    print("a parked send does not hold its worker", flush=True)
    slow = []

    def slow_send():
        c = redis.Redis(host="127.0.0.1", port=PORT, db=0, protocol=2)
        t0 = time.time()
        out = json.loads(c.execute_command("SENDMAIL", json.dumps(
            dict(to="a@example.com", subject="slow one", text="t"))))
        slow.append((out, time.time() - t0))

    t = threading.Thread(target=slow_send)
    t.start()
    time.sleep(0.25)
    fast_calls = 0
    t1 = time.time()
    while t.is_alive() and time.time() - t1 < 2.0:
        assert r.execute_command("FAST") == b"fast"
        fast_calls += 1
    t.join(timeout=10)
    assert slow and slow[0][0]["ok"], slow
    assert slow[0][1] >= 0.9, slow
    assert fast_calls > 5, fast_calls
    print(f"  {fast_calls} calls ran while the send was parked", flush=True)

    print("a queue consumer sends one", flush=True)
    assert r.execute_command("SETF", "MAILER", '''
        function call(message, sequence, attempts)
            local m = simdjson.parse(message)
            local r = mail.send{to = m.to, subject = m.subject, text = m.text,
                                message_id = "q" .. sequence .. "@barch.test"}
            if not r.ok then error(r.error) end
            return "sent"
        end
    ''') == b"OK"
    assert r.execute_command("configuration:SETF", "queues/mail", '''
        function transport()
            return {
                kind = "queue", name = "mail", space = "default",
                call = "MAILER", user = "default", durability = "each",
                max_attempts = 3, poll = "1h"
            }
        end
    ''') == b"OK"
    before = count()
    seq = r.execute_command("QUEUE", "PUSH", "mail", json.dumps(
        dict(to="queued@example.com", subject="from the queue", text="queued body")))
    assert wait_until(lambda: count() > before), "the queued mail never arrived"
    s, msg = parsed()
    assert s["rcpt"] == ["queued@example.com"], s
    assert msg["Subject"] == "from the queue", msg["Subject"]
    assert msg["Message-ID"] == "<q%d@barch.test>" % seq, msg["Message-ID"]

    # A Crow handler cannot park, so the send waits inline there. Several
    # threads at once is what would show a VM slot being handed back early.
    print("mail.send inside a Crow handler", flush=True)
    ROUTE = """
function call()
    return "mailroute"
end

function post(req, res)
    local got = mail.send{to = "web@example.com", subject = "from a route", text = req.body}
    res.body = tostring(got.ok) .. "|" .. tostring(got.code)
    res.code = 200
end

function transport()
    return {
        kind = "resource",
        route = "/mail",
        methods = {POST = post},
        send = "text/plain",
    }
end
"""
    CONF = """
function call()
    return "mailconf"
end

function transport()
    return {
        kind = "http",
        port = %d,
        bind = "127.0.0.1",
        keys = {"MAILROUTE"},
    }
end
""" % CROW_PORT
    r.execute_command("ACL", "SETUSER", "web", "on", "+outbound")
    assert r.execute_command("SETF", "mailroute", ROUTE) == b"OK"
    assert r.execute_command("SETF", "mailconf", CONF) == b"OK"
    r.execute_command("HTTP", "START", "MAILCONF", str(CROW_PORT), "127.0.0.1")

    def post_once(body):
        conn = http.client.HTTPConnection("127.0.0.1", CROW_PORT, timeout=15)
        try:
            conn.request("POST", "/mail", body=body, headers={"Connection": "close"})
            resp = conn.getresponse()
            return resp.status, resp.read()
        finally:
            conn.close()

    for _ in range(30):
        try:
            if post_once("warmup")[0] == 200:
                break
        except (TimeoutError, ConnectionError, OSError):
            pass
        time.sleep(0.1)

    before = count()
    errs = []

    def hammer(n):
        try:
            for i in range(4):
                status, body = post_once("thread %d message %d" % (n, i))
                assert status == 200 and body == b"true|250", (status, body)
        except Exception as e:
            errs.append(repr(e))

    hs = [threading.Thread(target=hammer, args=(n,)) for n in range(6)]
    for x in hs:
        x.start()
    for x in hs:
        x.join(timeout=60)
    assert not errs, errs
    assert count() - before == 24, count() - before
    print("  24 route sends across 6 threads, all delivered", flush=True)
    r.execute_command("HTTP", "STOP")

    print("complete mail luau test")
finally:
    try:
        r.execute_command("HTTP", "STOP")
    except Exception:
        pass
    try:
        smtp.shutdown()
    except Exception:
        pass
    try:
        barch.stop()
    except Exception:
        pass
