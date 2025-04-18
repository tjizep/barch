local vk
vk = redis

vk.call('B.CLEAR')
for i=1,10000 do
    vk.call('B.ZADD', 'zrank', i, 'at '..i) --i+2.0, 'second' ,i+3.5, 'third'
end

local max = 8000
local zr = vk.call('B.ZRANK','zrank',900,max)
local range = vk.call('B.ZRANGE','zrank',900,max)
assert(#range==zr)
local tr = {vk.call('B.SIZE')}
vk.call('B.CLEAR')
return tr

