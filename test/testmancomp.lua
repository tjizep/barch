local vk
vk = redis
vk.call('B.CLEAR')
local t = vk.call('B.MILLIS')
for i=1,300000 do
    assert(vk.call('B.ZADD', 'agame', i+1.1, 'first'..i, i+2.0, 'second'..i ,i+3.5, 'third'..i) == 3)
end
return {vk.call('B.SIZE'),vk.call('B.MILLIS')-t}
