import sys
import os
import barch
import subprocess
import time

#start the valkey server
barchdir = sys.argv[1]
srcdir = sys.argv[2]

print(f"barchdir {barchdir}")
print(f"srcdir {srcdir}")
serverdir = f"{os.getcwd()}/_deps/valkey-src/src/"
print(f"serverdir{serverdir}")
clidir = f"{os.getcwd()}/_deps/valkey-src/src/"

# We should not have to do this
df = [f"rm {barchdir}/*.dat"]
print(f"delete dat files {df}")

process = subprocess.run(
    df,
    shell=True,
    capture_output=True,
    text=True
)
time.sleep(1)

serverCmd = [f"{serverdir}valkey-server", f"--loadmodule", f"{barchdir}/_barch.so"]
serverProc = subprocess.Popen(serverCmd,cwd=barchdir)
time.sleep(1)

cliCmd = [f"{clidir}valkey-cli", f"--eval", f"{srcdir}/sourcestart.lua"]
cliProcess = subprocess.Popen(cliCmd)
time.sleep(1)
# published keys are received here
barch.start("127.0.0.1","13000")
# keys are pulled from this port
barch.pull("127.0.0.1","14000")
# clear the db we have no keys now
# size is not pulled from the source (port 14000) - keys are on demand only
# and the lru will clear some of them anyway
barch.clear()
barch.save()

k = barch.KeyValue()
# get the key from the source (port 14000)
assert(k.get("1") == "one:test")
assert(barch.size() == 1)
assert(k.get("2") == "two:test")
assert(barch.size() == 2)
barch.stop()
serverProc.kill()
cliProcess.kill()