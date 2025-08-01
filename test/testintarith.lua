local vk
vk = redis

vk.call('B.SET','a',0)

assert(vk.call('B.INCR','a')==1)
assert(vk.call('B.DECR','a')==0)
vk.call('B.SET','b',-0)
assert(vk.call('B.INCRBY','b',10)==10)

assert(vk.call('B.DECRBY','b',10)==0)

assert(vk.call('B.INCR','a')==1)
assert(vk.call('B.INCR','b')==1)

assert(vk.call('B.DECR','a')==0)
assert(vk.call('B.DECR','c')~=0)

assert(vk.call('B.UINCRBY','a',10)==10)

assert(vk.call('B.UDECRBY','a',10)==0)

vk.call('B.SET','pendant',"ok")
vk.call('B.APPEND','pendant'," ?")
assert(vk.call('B.GET','pendant')=='ok ?')
vk.call('B.PREPEND','pendant',"are you ")
assert(vk.call('B.GET','pendant')=='are you ok ?')
vk.call('B.APPEND','pendant'," ")
assert(vk.call('B.GET','pendant')=='are you ok ? ')

return {vk.call('B.GET','a')}