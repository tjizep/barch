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
    return num * 10
end

local test = function()
    result[inc()] = {'CLEAR', vk.call('FLUSHALL')}
    tests = tests + 1
    local t
	t = vk.call('B.MILLIS')
	for i = 1, count do
	    local k = convert(i-1)
	    vk.call('SET',k,i)
	end
	result[inc()] = {'TIME', vk.call('B.MILLIS')-t}
    result[inc()] = {'SIZE', vk.call('DBSIZE')}

    --result[inc()] = {'CLEAR', vk.call('FLUSHALL')}

end
--[[ Testing ints,doubles and string key types]]
convert = tocharsnum
test()

return result