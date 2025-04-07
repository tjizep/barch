local vk
vk = redis
vk.call('B.CLEAR')
vk.call('B.HSET','tires','name','p1')
vk.call('B.HSET','tires','pirelli','p2')
assert(vk.call('B.HGET','tires','pirelli')[1] == 'p2')
assert(vk.call('B.HEXPIRE','tires','1000','NX','FIELDS','2','pirelli','rarbg')[1] == 1)
assert(vk.call('B.HEXPIRE','tires','1000','NX','FIELDS','2','pirelli','chips')[1] == 0)
assert(vk.call('B.HEXPIRE','tires','1000','XX','FIELDS','1','pirelli')[1] == 1)
assert(vk.call('B.HEXPIRE','tires','10000','GT','FIELDS','1','pirelli')[1] == 1)
assert(vk.call('B.HEXPIRE','tires','10','GT','FIELDS','1','pirelli')[1] == 0)
assert(vk.call('B.HEXPIRE','tires','50','LT','FIELDS','1','pirelli')[1] == 1)
assert(vk.call('B.HEXPIRE','tires','50000','LT','FIELDS','1','pirelli')[1] == 0)
return {"OK"}