local vk
vk = redis

local count = 10000000
local result = {}
local i = 1
local index = 0
local convert
local tests = 0
local failures = 0
local successes = 0
local numbers = {}
local mem = {}
local window = 10
local inc = function()
    index = index + 1
    return index
end

local tocharsnum = function(num)
    return num * window
end

local test = function()
    result[inc()] ={'CLEAR B', vk.call('B.CLEAR')}
    tests = tests + 1
    local t
    t = vk.call('B.MILLIS')
    for i = 1, count do
        local k = convert(i-1)
        vk.call('B.SET',k,i)
    end
    result[inc()] = {'TIME', vk.call('B.MILLIS')-t}

    result[inc()] = {'HEAP', vk.call('B.HEAPBYTES')}
    result[inc()] = {'SIZE', vk.call('B.SIZE')}
    result[inc()] = {'CLEAR B', vk.call('B.CLEAR')}

end


result[inc()] = vk.call("B.CONFIG", "SET","max_memory_bytes", "240m")
result[inc()] = vk.call("B.CONFIG", "SET","active_defrag", "on")
result[inc()] = vk.call("B.CONFIG", "SET","compression", "zstd")
result[inc()] = vk.call("B.CONFIG", "SET","save_interval", "1000000")
--[[ Testing ints,doubles and string key types]]
convert = tocharsnum
test()
collectgarbage()
return result