local vk
vk = redis
vk.call('B.CLEAR')
local t = vk.call('B.MILLIS')
local count = 120000
local key = 'z'

if vk.call('B.SIZE') < count then

    for i=1,count do
        local s = count*math.random()
        vk.call('B.ZADD', key, s, 'at '..i)
        vk.call('B.ZADD', key, math.floor(s), 'flor '..i)
    end
    vk.call('B.SAVE')
end
local results = {}
local rcnt = 1
local function add(tr)
    results[rcnt] = tr
    rcnt = rcnt + 1
end

local failures = 0
for i=1,1000 do
    local min = math.floor((count-count/10)*math.random())
    local max = min + count/10 --count*math.random()--math.floor()
    local zr = vk.call('B.ZRANK',key,min,max)
    local zr2 = vk.call('B.ZFASTRANK',key,min,max)
    if math.abs(zr-zr2) > 0 then
        local tr = {
            {"min: "..min},
            {"max: "..max},
            {"guess","slow zr : "..zr,"fast zr: "..zr2},
            {"actual 1: "..zr},
            {"max - min: "..(max-min)},
            {"diff",zr2-zr}}
        add(tr)
        failures = failures + 1
    end
end
local sz = vk.call('B.SIZE')
vk.call('B.CLEAR')
--assert(failures == 0)
return {failures,sz,results}

