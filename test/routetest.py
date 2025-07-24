import sys
import os
import barch
import subprocess
import time
# test a simple cluster by adding publish replication and some routes
# start the valkey server
barchdir = sys.argv[1]
srcdir = sys.argv[2]

# published keys are received here so start asap
barch.start("127.0.0.1",13000)

print(f"barchdir {barchdir}")
print(f"srcdir {srcdir}")
serverdir = f"{os.getcwd()}/_deps/valkey-src/src/"
print(f"serverdir{serverdir}")
clidir = f"{os.getcwd()}/_deps/valkey-src/src/"

serverCmd = [f"{serverdir}valkey-server", f"--loadmodule", f"{barchdir}/_barch.so"]
serverProc = subprocess.Popen(serverCmd,cwd=barchdir)
time.sleep(1)
# sourcestart.lua starts a barch on port 14000 and adds some data while publishing to port 13000
cliCmd = [f"{clidir}valkey-cli", f"--eval", f"{srcdir}/sourcestart.lua"]
cliProcess = subprocess.Popen(cliCmd)
time.sleep(1) # wait for published data to come here

# create a simple cluster by adding some routes to port 14000
barch.setRoute(0,"127.0.0.1",14000)
barch.setRoute(1,"127.0.0.1",14000)
barch.setRoute(2,"127.0.0.1",14000)
# clear the db we have no keys now
# size is not pulled from the source (port 14000) - keys are on demand only

k = barch.KeyValue()
# get the key from the source (port 14000)
assert(k.get("1") == "one:test")
print(barch.size())
assert(barch.size() > 900)
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
cliProcess.kill()

assert(barch.size() > 900)
# the routing resolver should now fall-back on local data while completing the task
for i in range(200,1000):
    assert(k.get(str(i))==f"data{str(i)}")
    if i%100==0:
        print(i)
stats = barch.repl_stats()
assert (stats.attempted_routes > stats.routes_succeeded)
assert (stats.request_errors > 0) # check if there actually where errors
