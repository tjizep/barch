import scale

import redis
import barch

# TODO 366: a stored function as a queue consumer. A queue declaration is a
# function key under configuration:queues/<name> whose transport() says
# kind = "queue" - a destination and a handler, no schedule and no commands.
#
# This covers the declaration and its validation. The consumer and the publish
# paths come after it and are tested there.

scale.workdir()

PORT = scale.port(default=14000)

print("start queue test")
barch.start("0.0.0.0", PORT)
barch.ping("127.0.0.1", PORT)
r = redis.Redis(host="127.0.0.1", port=PORT, db=0, protocol=2)
r.execute_command("FLUSHDB")
r.execute_command("configuration:FLUSHDB")

failures = []


def check(ok, what):
    print("  %-62s %s" % (what, "pass" if ok else "FAIL"), flush=True)
    if not ok:
        failures.append(what)


def setf(key, body, space="configuration"):
    """SETF, giving back either b'OK' or the error text as a string"""
    try:
        return r.execute_command("%s:SETF" % space, key, body)
    except redis.ResponseError as e:
        return str(e)


def declaration(**over):
    fields = {
        "kind": '"queue"',
        "name": '"outbound"',
        "space": '"default"',
        "call": '"HANDLE"',
        "user": '"default"',
    }
    fields.update(over)
    body = ",\n".join("                %s = %s" % (k, v) for k, v in fields.items())
    return "\n        function transport()\n            return {\n%s\n            }\n        end\n    " % body


try:
    # the handler: an ordinary function, called with the message
    assert r.execute_command("SETF", "HANDLE", '''
        function call(argv)
            barch.store.set("last", argv[1] or "")
            return "ok"
        end
    ''') == b"OK"

    print("a declaration in the right place")
    check(setf("queues/mail", declaration()) == b"OK",
          "a queue declaration is accepted")
    check(setf("queues/mail", declaration(durability='"none"')) == b"OK",
          "durability none")
    check(setf("queues/mail", declaration(durability='"timer"')) == b"OK",
          "durability timer")
    check(setf("queues/mail", declaration(durability='"512kb"')) == b"OK",
          "durability as a size")
    check(setf("queues/mail", declaration(max_attempts="3")) == b"OK",
          "max_attempts")
    check(setf("queues/mail", declaration(poll='"5m"')) == b"OK",
          "a poll interval")
    check(setf("queues/mail", declaration(enabled="false")) == b"OK",
          "enabled = false is still a declaration")
    check(setf("queues/mail", declaration(dir='"/tmp/barch-queues"')) == b"OK",
          "a directory of its own")
    # a declaration has no call() of its own, the same as a cron entry
    check("no call()" not in str(setf("queues/mail", declaration())),
          "a declaration needs no call() of its own")

    print("where it has to live")
    out = setf("queues/mail", declaration(), space="default")
    check("configuration:queues/" in str(out),
          "outside the configuration space it is refused, and says where it goes")
    out = setf("elsewhere/mail", declaration())
    check("configuration:queues/" in str(out),
          "and under the wrong prefix in the right space")

    print("fields it cannot do without")
    for missing in ("name", "space", "call", "user"):
        out = str(setf("queues/mail", declaration(**{missing: "nil"})))
        check("queue transport()" in out and out != "b'OK'",
              "no %s is refused" % missing)

    print("values that are not values")
    out = str(setf("queues/mail", declaration(durability='"sometimes"')))
    check("durability" in out and "512kb" in out,
          "a durability that is not one of the four, and it lists them")
    out = str(setf("queues/mail", declaration(poll='"next tuesday"')))
    check(out != "b'OK'", "a poll that is not a duration")
    out = str(setf("queues/mail", declaration(max_attempts="0")))
    check("max_attempts" in out, "max_attempts of zero - it would dead letter everything")
    out = str(setf("queues/mail", declaration(max_attempts="2.5")))
    check("max_attempts" in out, "max_attempts that is not a whole number")
    out = str(setf("queues/mail", declaration(max_attempts='"three"')))
    check("max_attempts" in out, "max_attempts that is not a number")
    # a number where a string goes is coerced to its digits, the same as every
    # other transport field - lua_isstring says yes to a number. Recorded as
    # behaviour rather than asserted as a refusal: being strict here and lax in
    # the cron reader would be worse than either on its own
    check(setf("queues/mail", declaration(name="42")) == b"OK",
          "a number for a name becomes its digits, as elsewhere")

    print("a name has to be able to be a file")
    for bad, why in (('"../../etc/passwd"', "a path"),
                     ('"in/box"', "a slash"),
                     ('".."', "the parent directory"),
                     ('"has space"', "a space"),
                     ('""', "nothing at all")):
        out = str(setf("queues/mail", declaration(name=bad)))
        check(out != "b'OK'", "a name that is %s is refused" % why)
    check(setf("queues/mail", declaration(name='"a-b_c.2"')) == b"OK",
          "letters, digits, dash, underscore and dot are fine")

    print("not a queue at all")
    # a cron transport is read as a cron transport wherever it sits, so this is
    # refused for being in the wrong place for a cron entry - not for being a
    # bad queue. The two kinds do not shadow each other
    out = str(setf("queues/cronjob", '''
        function transport()
            return { kind = "cron", space = "default", call = "HANDLE",
                     user = "default", every = "1m" }
        end
    '''))
    check("cron/jobs" in out, "a cron entry under queues/ is judged as a cron entry")
    # a kind no reader claims is not a declaration and not an error either, but
    # it still has to be a runnable function. ("resp" would not do for this: a
    # resp transport needs a methods table, which is its own rule and nothing
    # to do with queues)
    check(setf("queues/plain", '''
        function transport() return { kind = "carrier pigeon" } end
        function call() return "x" end
    ''') == b"OK", "a transport of a kind nobody claims is left alone")
    out = str(setf("queues/nocall", '''
        function transport() return { kind = "carrier pigeon" } end
    '''))
    check("no call()" in out,
          "and without a call() it is refused - only the two declarations are excused")
finally:
    barch.stop()

print()
if failures:
    print("FAILURES: %d" % len(failures))
    for f in failures:
        print("  " + f)
    raise SystemExit(1)
print("all queue declaration checks pass")
