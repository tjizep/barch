local vk
vk = redis

vk.call('B.CLEAR')
local t = vk.call('B.MILLIS')
local count = 10000
for i=1,count do
    vk.call('B.ZADD', 'zrank', i, 'at '..i) --i+2.0, 'second' ,i+3.5, 'third'
end
local toadd = vk.call('B.MILLIS') - t
local min = count*0.01
local max = count*0.9
t = vk.call('B.MILLIS')
local zr = vk.call('B.ZRANK','zrank',min,max)
local torank = vk.call('B.MILLIS') - t
local range = vk.call('B.ZRANGE','zrank',min,max)
assert(#range==zr)
local tr = {"toadd",toadd,"torank",torank,zr,vk.call('B.SIZE')}
vk.call('B.CLEAR')
return tr

