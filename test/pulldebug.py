import sys
import os
import barch
import subprocess
import time

#start the valkey server
launchServer = len(sys.argv) > 2
if len(sys.argv) > 2 :
    barchdir = sys.argv[1]
    srcdir = sys.argv[2]
else :
    barchdir = ""
    srcdir = "."

print(f"barchdir {barchdir}")
print(f"srcdir {srcdir}")
serverdir = f"{os.getcwd()}/_deps/valkey-src/src/"
print(f"serverdir{serverdir}")
clidir = f"{os.getcwd()}/_deps/valkey-src/src/"
serverProc = None
cliProcess = None
if launchServer :
    serverCmd = [f"{serverdir}valkey-server", "--port","7777",f"--loadmodule", f"{barchdir}/_barch.so"]
    serverProc = subprocess.Popen(serverCmd,cwd=barchdir)
time.sleep(1)


if launchServer :
    cliCmd = [f"{clidir}valkey-cli","-p","7777", f"--eval", f"{srcdir}/smallsourcestart.lua"]
    cliProcess = subprocess.Popen(cliCmd)

time.sleep(10)
# published keys are received here
# barch.start("127.0.0.1","13000")
# keys are pulled from this port
barch.pull("127.0.0.1","14000")
# clear the db we have no keys now
# size is not pulled from the source (port 14000) - keys are on demand only
# and the lru will clear some of them anyway
#barch.clear()
#barch.save()

k = barch.KeyValue()
# get the key from the source (port 14000)
assert(k.get("1") == "one:test")
assert(barch.size() == 1)
assert(k.get("2") == "two:test")
assert(barch.size() == 2)
barch.stop()
if serverProc:
    serverProc.kill()
if cliProcess:
    cliProcess.kill()