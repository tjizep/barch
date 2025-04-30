local vk
vk = redis

local t = vk.call('B.MILLIS')
local count = 30000
local key = 'z'

vk.call('B.CLEAR')
for i=1,count do
    vk.call('B.ZADD', key, count*math.random(), 'at '..i) 
end
local results = {}
local faults = 0
for i=1,1000 do
    local min = math.floor(count*0.01*math.random())
    local max = count*math.random()--math.floor()
    local zr = vk.call('B.ZRANK',key,min,max)
    local zr2 = vk.call('B.ZFASTRANK',key,min,max)
    --local range = vk.call('B.ZRANGE',key,min,max)
    --assert(zr2==zr)
    if zr ~= zr2 then
        local tr = {
            {"min: "..min},
            {"max: "..max},
            {"guess","slow zr : "..zr,"fast zr: "..zr2},
            {"actual 1: "..zr},
            {"max - min: "..(max-min)},
            {"diff",zr2-zr},vk.call('B.SIZE')}
        results[i] = tr
        faults = faults + 1
    end
end
vk.call('B.CLEAR')
--results[1] = {"it"}
assert(faults > 1)
return results

