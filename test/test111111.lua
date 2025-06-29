local vk
vk = redis

local count = 1000000
local result = {}
local i = 1
local index = 0
local convert
local tests = 0
local failures = 0
local successes = 0
local mem = {}
local random = math.random
local inc = function()
    index = index + 1
    return index
end

local tocharsnum = function(num)
    return num..'not'
end

local test = function()
    vk.call('B.CLEAR')
    tests = tests + 1
    local t
    t = vk.call('B.MILLIS')
    for i = 1, count do
        local k = convert(i-1)
        vk.call('B.SET',k,i)
    end
    result[inc()] = {'TIME', vk.call('B.MILLIS')-t}
    result[inc()] = {'SIZE', vk.call('B.SIZE')}
    result[inc()] = {'HEAP', vk.call('B.HEAPBYTES')}
    local cnt = vk.call('B.KEYS', '3*n*', 'COUNT')
    result[inc()] = {'B.KEYS 3*n*', cnt}
    --vk.call('B.CLEAR')
    result[inc()] = {'HEAP', vk.call('B.HEAPBYTES')}
    assert(cnt==111111)
end


result[inc()] = vk.call("B.CONFIG", "SET","max_memory_bytes", "128m")
result[inc()] = vk.call("B.CONFIG", "SET","active_defrag", "off")
result[inc()] = vk.call("B.CONFIG", "SET","compression", "off")
result[inc()] = vk.call("B.CONFIG", "SET","save_interval", "1000000")
result[inc()] = vk.call("B.CONFIG", "SET","max_modifications_before_save", "100000000")
--[[ Testing ints,doubles and string key types]]
convert = tocharsnum
test()

return result