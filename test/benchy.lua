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
    return numbers[num + 1]
end

local test = function()

    tests = tests + 1
    local t = vk.call('ODMILLIS')
    for i = 1, count do
        local k = convert(i-1)
        vk.call('ODSET',k,i)

    end
    for i = 1, count do
        local k = convert(i-1)
        vk.call('ODGET',k)

    end
    result[inc()] = {'SIZE', vk.call('ODSIZE')}
    result[inc()] = {'TIME', vk.call('ODMILLIS')-t}
    result[inc()] = {'VAC', vk.call('ODHEAPBYTES')}
end


local clear = function()
    for i = 1, count do
        local k = convert(i-1)
        local v = i
        if vk.call('ODREM',k) == v then
            successes = successes + 1
        else
            result[inc()] = {"Failed remove result ",k, v, vk.call('ODGET',k)}
        end

    end
    result[inc()] = {'FAILURES', failures}
    result[inc()] = {'SUCCESSES', successes}
end

--[[ Testing ints,doubles and string key types]]
convert = tocharsnum
test()

return result