local vk
vk = redis

vk.call('FLUSHALL')
local t = vk.call('B.MILLIS')
for i=1,40000000 do
    vk.call('ZADD', i, i+1.1, 'first') --, i+2.0, 'second' ,i+3.5, 'third'
end
local tr = {vk.call('DBSIZE'),vk.call('B.MILLIS')-t}
vk.call('FLUSHALL')
return tr
