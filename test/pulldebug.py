import sys
import os
import barch
import subprocess
import scale
import atexit
import time

#start the valkey server
launchServer = len(sys.argv) > 2
if len(sys.argv) > 2 :
    barchdir = sys.argv[1]
    srcdir = sys.argv[2]
else :
    barchdir = ""
    srcdir = "."

# Three ports, all out of scale.port(). They were 7777, 14000 and 13000, and two
# of those were written into smallsourcestart.lua as well as in here - which is
# the arrangement where one of them gets moved and the other does not. The lua
# takes both as ARGV now.
#
# The defaults avoid 14000 deliberately: scale.port()'s own fallback is 14000,
# this file is not registered in CMakeLists so nothing hands it BARCH_TEST_PORT,
# and 14000 is what the shop example binds.
VALKEY = scale.port(0, default=17720)    # the valkey that loads the module
SOURCE = scale.port(1, default=17720)    # what the lua B.STARTs, and we pull from
PUBLISH = scale.port(2, default=17720)   # where the lua publishes

print(f"barchdir {barchdir}")
print(f"srcdir {srcdir}")
serverdir = f"{os.getcwd()}/_deps/valkey-src/src/"
print(f"serverdir{serverdir}")
clidir = f"{os.getcwd()}/_deps/valkey-src/src/"
serverProc = None
cliProcess = None
if launchServer :
    # --B.server_port so the barch the module starts for itself and the one the
    # lua asks for with B.START are the same server, rather than two racing for
    # whatever `server_port` defaults to
    serverCmd = [f"{serverdir}valkey-server", "--port", str(VALKEY),
                 "--loadmodule", f"{barchdir}/_barch.so",
                 "--B.server_port", str(SOURCE)]
    serverProc = subprocess.Popen(serverCmd,cwd=barchdir)
    # kill it even when an assertion below fails: without this a failed run leaves
    # valkey-server alive holding its port, and the next run hangs trying to bind
    atexit.register(lambda p=serverProc: p.kill() if p.poll() is None else None)
time.sleep(1)


if launchServer :
    # everything after the comma is ARGV, everything before is KEYS, and there are
    # no keys - hence the bare comma
    cliCmd = [f"{clidir}valkey-cli", "-p", str(VALKEY),
              f"--eval", f"{srcdir}/smallsourcestart.lua", ",", str(SOURCE), str(PUBLISH)]
    cliProcess = subprocess.Popen(cliCmd)
    # kill it even when an assertion below fails: without this a failed run leaves
    # valkey-server alive holding its port, and the next run hangs trying to bind
    atexit.register(lambda p=cliProcess: p.kill() if p.poll() is None else None)

time.sleep(10)
# published keys would be received here
# barch.start("127.0.0.1", str(PUBLISH))
# keys are pulled from the barch the lua started
barch.pull("127.0.0.1", str(SOURCE))
# clear the db we have no keys now
# size is not pulled from the source - keys are on demand only
# and the lru will clear some of them anyway
#barch.clear()
#barch.save()

k = barch.KeyValue()
# A route proxies; it does not cache. `call_route` in rpc_caller.h forwards the
# whole data command to the routed server and hands back its reply, so nothing
# read through a pull is stored here and the local size does not move. These
# asserts used to say `barch.size() == 1` and then `== 2`, which is a cache this
# mechanism has never had - routetest.py found the same thing and has its
# `assert(barch.size() > 900)` commented out for the same reason. Asserting that
# the size does *not* change says what is true and will fail if that ever does.
local_before = barch.size()
# get the key from the source
assert(k.get("1") == "one:test")
assert(barch.size() == local_before), "a pulled key was stored locally - see the note above"
assert(k.get("2") == "two:test")
assert(barch.size() == local_before)
print(f"read through the pull, {barch.size()} keys held locally")
barch.stop()
if serverProc:
    serverProc.kill()
if cliProcess:
    cliProcess.kill()