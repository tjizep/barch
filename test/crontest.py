import time

import scale

import redis
import barch

# TODO 249: a stored function on a schedule. A cron entry is a function key
# under configuration:cron/jobs/<name> whose transport() says kind = "cron" -
# a schedule pointing at a target space/call, not the work itself.

scale.workdir()

PORT = scale.port(default=14000)

print("start cron test")
barch.start("0.0.0.0", PORT)
barch.ping("127.0.0.1", PORT)
r = redis.Redis(host="127.0.0.1", port=PORT, db=0, protocol=2)
r.execute_command("FLUSHDB")
r.execute_command("configuration:FLUSHDB")


def wait_until(pred, timeout=10.0, step=0.1):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if pred():
            return True
        time.sleep(step)
    return pred()


try:
    # the target: an ordinary command in the default space, callable on its own
    assert r.execute_command("SETF", "TICK", '''
        function call()
            local n = tonumber(barch.store.get("ticks") or "0") or 0
            barch.store.set("ticks", tostring(n + 1))
            return n + 1
        end
    ''') == b"OK"
    assert r.execute_command("TICK") == 1
    r.execute_command("DEL", "ticks")

    # a job entry: a schedule pointing at TICK, nothing about what TICK does
    assert r.execute_command("configuration:SETF", "cron/jobs/ticker", '''
        function transport()
            return {
                kind = "cron",
                space = "default",
                call = "TICK",
                every = "300ms",
                user = "default",
            }
        end
    ''') == b"OK"

    assert wait_until(lambda: r.get("ticks") is not None and int(r.get("ticks")) >= 2), \
        "TICK should have fired at least twice by now"

    st = r.execute_command("FUNCTIONS", "CRON").decode()
    assert "name=ticker" in st, st
    assert "space=default" in st, st
    assert "call=TICK" in st, st
    assert "enabled=on" in st, st
    assert "runs=" in st, st

    # disabling the job stops it firing again
    assert r.execute_command("configuration:SETF", "cron/jobs/ticker", '''
        function transport()
            return {
                kind = "cron",
                space = "default",
                call = "TICK",
                every = "300ms",
                user = "default",
                enabled = false,
            }
        end
    ''') == b"OK"
    time.sleep(0.5)
    stopped_at = int(r.get("ticks"))
    time.sleep(0.8)
    assert int(r.get("ticks")) == stopped_at, "a disabled job must not fire"

    # removing the job removes it from the listing
    assert r.execute_command("configuration:REMF", "cron/jobs/ticker") == 1
    st = r.execute_command("FUNCTIONS", "CRON").decode()
    assert "name=ticker" not in st, st

    # a bad schedule is refused at write time rather than discovered on a tick
    try:
        r.execute_command("configuration:SETF", "cron/jobs/bad", '''
            function transport()
                return { kind = "cron", space = "default", call = "TICK",
                         every = "banana", user = "default" }
            end
        ''')
        assert False, "an unparseable duration must be refused"
    except redis.exceptions.ResponseError:
        pass

    # a cron transport() has to live under configuration:cron/jobs/
    try:
        r.execute_command("SETF", "notacronjob", '''
            function transport()
                return { kind = "cron", space = "default", call = "TICK",
                         every = "1s", user = "default" }
            end
        ''')
        assert False, "a cron transport() outside cron/jobs/ must be refused"
    except redis.exceptions.ResponseError:
        pass

    # exactly one of every/cron
    try:
        r.execute_command("configuration:SETF", "cron/jobs/both", '''
            function transport()
                return { kind = "cron", space = "default", call = "TICK",
                         every = "1s", cron = "* * * * *", user = "default" }
            end
        ''')
        assert False, "naming both every and cron must be refused"
    except redis.exceptions.ResponseError:
        pass

    # a five field expression, run through the same scheduler with a schedule
    # that matches every minute, checked only for acceptance and listing here -
    # the exhaustive parser cases live beside the parser, not against a live
    # server
    assert r.execute_command("configuration:SETF", "cron/jobs/everym", '''
        function transport()
            return { kind = "cron", space = "default", call = "TICK",
                     cron = "* * * * *", user = "default" }
        end
    ''') == b"OK"
    st = r.execute_command("FUNCTIONS", "CRON").decode()
    assert "name=everym" in st, st
    assert "schedule=* * * * *" in st, st
    r.execute_command("configuration:REMF", "cron/jobs/everym")

    # a user with no rights at all is refused rather than run
    assert r.execute_command("configuration:SETF", "cron/jobs/norights", '''
        function transport()
            return { kind = "cron", space = "default", call = "TICK",
                     every = "300ms", user = "nobody-such-user" }
        end
    ''') == b"OK"
    assert wait_until(
        lambda: "not authorized" in r.execute_command("FUNCTIONS", "CRON").decode(),
        timeout=3.0), r.execute_command("FUNCTIONS", "CRON").decode()
    r.execute_command("configuration:REMF", "cron/jobs/norights")

    print("complete cron test")
finally:
    try:
        barch.stop()
    except Exception:
        pass
