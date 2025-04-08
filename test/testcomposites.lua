local vk
vk = redis
vk.call('B.CLEAR')
vk.call('B.HSET','tires','name','p1')
vk.call('B.HSET','counter','value','0')
vk.call('B.HSET','tires','pirelli','p2')

assert(vk.call('B.HINCRBY','counter','value', 1) == 1)
assert(vk.call('B.HINCRBY','counter','value', 2) == 3)
assert(vk.call('B.HINCRBYFLOAT','counter','value', 0.5) == "3.5")

assert(vk.call('B.HGET','tires','pirelli')[1] == 'p2')
assert(vk.call('B.HEXPIRE','tires','1000','NX','FIELDS','2','pirelli','rarbg')[1] == 1)
assert(vk.call('B.HEXPIRE','tires','1000','NX','FIELDS','2','pirelli','chips')[1] == 0)
assert(vk.call('B.HEXPIRE','tires','1000','XX','FIELDS','1','pirelli')[1] == 1)
assert(vk.call('B.HEXPIRE','tires','10000','GT','FIELDS','1','pirelli')[1] == 1)
assert(vk.call('B.HEXPIRE','tires','10','GT','FIELDS','1','pirelli')[1] == 0)
assert(vk.call('B.HEXPIRE','tires','50','LT','FIELDS','1','pirelli')[1] == 1)
assert(vk.call('B.HEXPIRE','tires','50000','LT','FIELDS','1','pirelli')[1] == 0)
assert(vk.call('B.HGETEX','tires','EX',120,'FIELDS','1','pirelli')[1] == "p2")
assert(vk.call('B.HTTL','tires','FIELDS',1,'pirelli')[1] <= 120)
assert(vk.call('B.HGETEX','tires','PERSIST','FIELDS','2','pirelli','x')[1] == "p2")
assert(vk.call('B.HLEN','tires') == #vk.call('B.HKEYS','tires'))

return {"OK"}