local vk
vk = redis

local t = vk.call('B.MILLIS')
local count = 1000000
local key = 'z'
if vk.call('B.SIZE') < count then
    vk.call('B.CLEAR')

    for i=1,count do
        vk.call('B.ZADD', key, i, 'at '..i) --i+2.0, 'second' ,i+3.5, 'third'
    end
    vk.call('B.SAVE')

end
--6910,58858;2985,75538,4049;857377
local toadd = vk.call('B.MILLIS') - t
local min = math.floor(count*0.01*math.random())
local max = count*math.random()--math.floor()
t = vk.call('B.MILLIS')
local zr = vk.call('B.ZRANK',key,min,max)
local torank = vk.call('B.MILLIS') - t
local zr2 = vk.call('B.ZFASTRANK',key,min,max)
--local range = vk.call('B.ZRANGE',key,min,max)
assert(zr2==zr)
local tr = {"toadd",toadd,"torank",torank,
    {"min: "..min},
    {"max: "..max},
    {"guess","slow zr : "..zr,"fast zr: "..zr2},
    {"actual 1: "..zr},
    {"max - min: "..(max-min)},
    {"diff",zr2-zr},vk.call('B.SIZE')}
--vk.call('B.CLEAR')
return tr

