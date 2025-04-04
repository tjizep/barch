local vk
vk = redis

vk.call('B.MSET','k1','v1','k2','v2')
assert(vk.call('B.MGET','k1','v1')[2]~="v2")
assert(vk.call('B.MGET','k1','k2')[2]=="v2")
assert(vk.call('B.MGET','k1','k2')[1]=="v1")
return vk.call('B.MGET','k1','k2')