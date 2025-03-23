local vk
vk = redis
local chars= {'a','b','c','d','e','f','g','h','i','j','k','l','m','n','o','p','q','r','s','t','u','v','w','x','y','z'}
local count = 10
local result = {}
local index = 0
local convert
local tests = 0
local successes = 0
local inc = function()
    index = index + 1
    return index
end

local tochars123 = function(num)
    return chars[num+1]
end

local test = function()

    tests = tests + 1
    result[inc()] = {"running test "..tests}
    local call = 1
    local res = vk.call('B.SET','a',1,'px',10)
    if res then
        successes = successes + 1
        result[inc()] = {call, res, successes, 'ok'}
    else
        result[inc()] = {call, res, successes, 'faile'}
    end
    call = call + 1
    res = vk.call('B.SET','j',1,'get','keepttl')

    if res ~= 'j'  then
        successes = successes + 1
        result[inc()] = {call, res, successes, 'ok'}
    else
        result[inc()] = {call, res, successes,'fail'}
    end
    call = call + 1
    res = vk.call('B.SET','j',1,'get','px',10)
    if  res == 'j' then
        successes = successes + 1
        result[inc()] = {call, res, successes, 'ok'}
    else
        result[inc()] = {call, res, successes, 'fail'}
    end
    call = call + 1
    res = vk.call('B.SET','j',1,'get','px',10,"keepttl")
    if res == 'j' then
        successes = successes + 1
        result[inc()] = {call, res, successes, 'ok'}
    else
        result[inc()] = {call, res, successes, 'fail'}
    end
    call = call + 1
    res = vk.call('B.MIN')
    if  res == 'a' then
        successes = successes + 1
        result[inc()] = {call, res, successes, 'ok'}
    else
        result[inc()] = {call, res, successes, 'fail'}
    end
    call = call + 1
    res = vk.call('B.MAX')
    if res == 'j' then
        successes = successes + 1
        result[inc()] = {call, res, successes, 'ok'}
    else
        result[inc()] = {call, res, successes, 'fail'}
    end

    result[inc()] = {"succeses for test "..tests..": "..successes}
    end



--[[ Testing num hash string key types]]

convert = tochars123
test()
assert(successes==6, "test failures")
--assert(failures==0, "test failures")

return result