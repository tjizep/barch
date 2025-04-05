local vk
vk = redis

vk.call('B.SET','a',0)

assert(vk.call('B.INCR','a')==1)
assert(vk.call('B.DECR','a')==0)
vk.call('B.SET','b',0)

assert(vk.call('B.INCR','a')==1)
assert(vk.call('B.INCR','b')==1)

assert(vk.call('B.DECR','a')==0)
assert(vk.call('B.DECR','c')~=0)

return {vk.call('B.GET','a')}