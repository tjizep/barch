import barch
import time
tcp = False
if tcp:
    port = 14000
    interface = "0.0.0.0"
else: #if its not tcp it's local unix domain sockets
    port = 0
    interface = "/tmp/lbarch"

barch.start(interface, port)
listen = barch.KeyValue()
print ("Started Barch server waiting for 'exit' on 'message'"
       f" 'SET message exit' (on {interface}:{port}")

msg = "start"
listen.set("message",msg)
print(msg)
while msg != "exit":
    msg = listen.get("message")
    time.sleep(0.1)
barch.saveAll()