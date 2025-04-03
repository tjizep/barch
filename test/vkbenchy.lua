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
local numbers = {}
local mem = {}
local window = 100
local random = math.random
local inc = function()
    index = index + 1
    return index
end
local shuffle = function(array)
    -- Fisher-Yates
    local output = {}
    local random = math.random

    for index = 1, #array do
        local offset = index - 1
        local value = array[index]
        local randomIndex = offset * random()
        local flooredIndex = randomIndex - randomIndex % 1

        if flooredIndex == offset then
            output[#output + 1] = value
        else
            output[#output + 1] = output[flooredIndex + 1]
            output[flooredIndex + 1] = value
        end
    end

    return output
end

for i = 1, count do
    numbers[i] = i
end
numbers = shuffle(numbers)

local tocharsnum = function(num)
    return numbers[num]
end

local test = function()
    result[inc()] = {'CLEAR VALKEY', vk.call('FLUSHALL')}
    tests = tests + 1
    local t
	t = vk.call('B.MILLIS')
	for i = 1, count do
	    local k = convert(i)
	    vk.call('SET',k,i)
	end
    local valids = 0
    for i = 1, count do
	    local k = convert(i)
	    if vk.call('GET',k) == ""..i then
            valids = valids + 1
        end
	end
	result[inc()] = {'TIME', vk.call('B.MILLIS')-t}
	result[inc()] = {'VALIDS', valids}
    result[inc()] = {'SIZE', vk.call('DBSIZE')}
    result[inc()] = {'VK MEM', vk.call('MEMORY', 'STATS')[4]}
    result[inc()] = {'CLEAR VALKEY', vk.call('FLUSHALL')}

end
--[[ Testing ints,doubles and string key types]]
convert = tocharsnum
test()

return result