import barch
import redis

barch.start("0.0.0.0", 14000)
print ("Started Barch server waiting for 'exit' on 'control.message")
rp = redis.Redis("127.0.0.1", 14000, 0)
rp.lpush("message","start")
msg = (b'message', b'start')
print(msg)
while msg != (b'message', b'exit'):
    msg = rp.blpop(["message"],10.0)
barch.saveAll()