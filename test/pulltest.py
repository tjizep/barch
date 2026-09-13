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

# Two ports, both out of scale.port(), neither written in here.
#
# They used to be 7777 for the valkey and 14000 for the barch to pull from, and
# 14000 is what the shop example binds by default - so anything left running on
# this machine became the server this pulled from. This one is not registered in
# CMakeLists, so it is run by hand, which is exactly when that is hardest to see:
# no ctest handing it a block, and nothing to say why it read the wrong server.
#
# The source port is not a literal this test chose, either. The module starts a
# barch of its own on `server_port`, which defaults to 14000, so moving it means
# telling the module - `--B.server_port`, the module being registered as "B".
# The defaults matter more here than in a registered test: this one is not in
# CMakeLists, so nothing hands it BARCH_TEST_PORT and the fallback is what it
# actually runs on. scale.port()'s own fallback is 14000, which is the port this
# was trying to get away from.
VALKEY = scale.port(0, default=17710)   # the valkey that loads the module
SOURCE = scale.port(1, default=17710)   # the barch the module starts, and pulls from

print(f"barchdir: {barchdir}")
print(f"srcdir: {srcdir}")
print(f"current dir: {os.getcwd()}")
serverdir = f"{os.getcwd()}/_deps/valkey-src/src/"
print(f"serverdir: {serverdir}")
clidir = f"{os.getcwd()}/_deps/valkey-src/src/"
serverProc = None
if launchServer :
    serverCmd = [f"{serverdir}valkey-server", "--port", str(VALKEY),
                 "--loadmodule", f"{barchdir}/_barch.so",
                 "--B.server_port", str(SOURCE)]
    serverProc = subprocess.Popen(serverCmd,cwd=barchdir)
    # kill it even when an assertion below fails: without this a failed run leaves
    # valkey-server alive holding its port, and the next run hangs trying to bind
    atexit.register(lambda p=serverProc: p.kill() if p.poll() is None else None)
    # wait for it rather than sleeping a fixed second - on a loaded runner the
    # server can take four times that to bind, and the cli below then runs
    # against nothing. See TODO 310.
    scale.wait_for_port(VALKEY, proc=serverProc, what="valkey-server")
    scale.wait_for_port(SOURCE, proc=serverProc, what="the barch the module started")

if launchServer :
    cliCmd = [f"{clidir}valkey-cli", "-p", str(VALKEY),
              f"--eval", f"{srcdir}/pullsourcesstart.lua"]
    # and read the result: this is what creates the keys every assertion below
    # reads, so a cli that could not connect has to fail here rather than be
    # discovered three asserts later
    scale.run_checked(cliCmd, what="pullsourcesstart.lua")
# keys are pulled from the barch the module started
barch.pull("127.0.0.1", str(SOURCE))
# clear the db we have no keys now
# size is not pulled from the source - keys are on demand only
# and the lru will clear some of them anyway
barch.clear()
barch.save()

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
print(k.get("1"))
assert(k.get("1") == "one:test")
assert(barch.size() == local_before), "a pulled key was stored locally - see the note above"
assert(k.get("2") == "two:test")
assert(barch.size() == local_before)
print(k.get("2"))
print(f"read through the pull, {barch.size()} keys held locally")
# barch.stop()
if serverProc:
    serverProc.kill()
# the cli is run to completion by scale.run_checked now, so there is
# nothing left to kill here - see TODO 310