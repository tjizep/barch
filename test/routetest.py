import sys
import os
import barch
import subprocess

import scale  # drops LD_PRELOAD for the valkey it spawns
import atexit
import time
# test a simple cluster by adding publish replication and some routes
# start the valkey server
barchdir = sys.argv[1]
srcdir = sys.argv[2]

# Three ports, all out of scale.port(), none of them written in here.
#
# They used to be literals - 13000 here, 14000 in this file and in
# sourcestart.lua, 7777 for the valkey - and 14000 is the port the shop example
# starts on by default, so anything left running on this machine became the peer
# this test routed to. What came back was `k.get('1')=[]` and an assertion, with
# nothing in it about a port. The lua has to agree about the source port, which
# is why it is passed in as an ARGV rather than set in two places.
# ctest hands this BARCH_TEST_PORT and that wins. The defaults are for running it
# by hand, and they avoid 14000 on purpose - scale.port()'s own fallback is 14000,
# which is what the shop example binds and what this test used to collide with.
HERE = scale.port(0, default=17700)    # where published keys are received
SOURCE = scale.port(1, default=17700)  # the barch sourcestart.lua brings up
VALKEY = scale.port(2, default=17700)  # the valkey that runs the lua

# published keys are received here so start asap
barch.start("127.0.0.1", HERE)

print(f"barchdir {barchdir}")
print(f"srcdir {srcdir}")
serverdir = f"{os.getcwd()}/_deps/valkey-src/src/"
print(f"serverdir{serverdir}")
clidir = f"{os.getcwd()}/_deps/valkey-src/src/"

serverCmd = [f"{serverdir}valkey-server", "--port", str(VALKEY), "--loadmodule", f"{barchdir}/_barch.so"]
serverProc = subprocess.Popen(serverCmd,cwd=serverdir)
# kill it even when an assertion below fails: without this a failed run leaves
# valkey-server alive holding its port, and the next run hangs trying to bind
atexit.register(lambda p=serverProc: p.kill() if p.poll() is None else None)

# wait for it rather than sleeping a fixed second - on a loaded runner the server
# can take four times that to bind and the cli below then runs against nothing.
# See TODO 310.
scale.wait_for_port(VALKEY, proc=serverProc, what="valkey-server")
# sourcestart.lua starts a barch on SOURCE and adds some data. The port reaches it
# as ARGV[1]: everything after the comma in --eval is ARGV, everything before is
# KEYS, and there are no keys here - hence the bare comma.
cliCmd = [f"{clidir}valkey-cli", "-p", str(VALKEY),
          "--eval", f"{srcdir}/sourcestart.lua", ",", str(SOURCE)]
# and read the result: this is what brings up the source and fills it, so a cli
# that could not connect has to fail here rather than three asserts later
scale.run_checked(cliCmd, what="sourcestart.lua")
# the lua B.STARTs the source, which is a server of its own coming up
scale.wait_for_port(SOURCE, proc=serverProc, what="the barch the lua started")
barch.clear()
barch.save()
barch.ping("127.0.0.1", str(SOURCE))
# create a simple cluster by adding some routes to the source
for i in range(0,500) :
    barch.setRoute(i,"127.0.0.1",SOURCE)
# clear the db we have no keys now
# size is not pulled from the source - keys are on demand only
k = barch.KeyValue()
# get the key from the source
print(f"k.get('1')=[{k.get('1')}]")
assert(k.get("1") == "one:test")
print(barch.size())
#assert(barch.size() > 900)
k = barch.KeyValue("127.0.0.1",SOURCE)
for i in range(200,5000):
    assert(k.get(str(i))==f"data{str(i)}")
    if i%100==0:
        print(i)
stats = barch.repl_stats()
print(stats.attempted_routes)
print(stats.routes_succeeded)
assert (stats.attempted_routes == stats.routes_succeeded)
print(stats.routes_succeeded)
barch.stop()
serverProc.kill()
# the cli is run to completion by scale.run_checked now, so there is
# nothing left to kill here - see TODO 310
