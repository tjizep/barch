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

serverCmd = [f"{serverdir}valkey-server", "--port", "7777", "--loadmodule", f"{barchdir}/_barch.so"]
serverProc = subprocess.Popen(serverCmd,cwd=serverdir)
time.sleep(1)
# sourcestart.lua starts a barch on port 14000 and adds some data while publishing to port 13000
cliCmd = [f"{clidir}valkey-cli", "-p", "7777", "--eval", f"{srcdir}/sourcestart.lua"]
cliProcess = subprocess.Popen(cliCmd)
time.sleep(1) # wait for published data to come here
barch.clear()
barch.save()
barch.ping("127.0.0.1","14000")
# create a simple cluster by adding some routes to port 14000
for i in range(0,500) :
    barch.setRoute(i,"127.0.0.1",14000)
# clear the db we have no keys now
# size is not pulled from the source (port 14000) - keys are on demand only
k = barch.KeyValue()
# get the key from the source (port 14000)
print(f"k.get('1')=[{k.get('1')}]")
assert(k.get("1") == "one:test")
print(barch.size())
#assert(barch.size() > 900)
k = barch.KeyValue("127.0.0.1",14000)
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
