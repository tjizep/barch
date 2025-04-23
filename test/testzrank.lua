local vk
vk = redis

vk.call('B.CLEAR')
local t = vk.call('B.MILLIS')
local count = 1000000
local key = 'z'
for i=1,count do
    vk.call('B.ZADD', key, i, 'at '..i) --i+2.0, 'second' ,i+3.5, 'third'
end
--6910,58858;2985,75538,4049;857377
local toadd = vk.call('B.MILLIS') - t
local min = count*0.01*math.random()
local max = count*math.random()
t = vk.call('B.MILLIS')
local zr = vk.call('B.ZRANK',key,min,max)
local torank = vk.call('B.MILLIS') - t
local zr2 = vk.call('B.ZFASTRANK',key,min,max)
local range = vk.call('B.ZRANGE',key,min,max)
--assert(#range==zr)
local tr = {"toadd",toadd,"torank",torank,
    {"min",min},
    {"max",max},
    {"guess","zr :"..zr,"zr2:"..zr2},
    {"actual",#range},
    {"diff",zr-#range},vk.call('B.SIZE')}
vk.call('B.CLEAR')
return tr

