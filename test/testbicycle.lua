local vk
vk = redis

vk.call('B.SET', 'bicycle:yours', 1)
vk.call('B.SET', 'bicycle:mine', 1)
vk.call('B.SET', 'bicycle:theirs', 1)

vk.call('B.SET', 'car:yours', 1)
vk.call('B.SET', 'car:mine', 1)
vk.call('B.SET', 'car:theirs', 1)

vk.call('B.SET', 'cat:yours', 1)
vk.call('B.SET', 'cat:mine', 1)
vk.call('B.SET', 'cat:theirs', 1)

vk.call('B.SET', 'eyes:yours', 1)
vk.call('B.SET', 'eyes:mine', 1)
vk.call('B.SET', 'eyes:theirs', 1)

assert(vk.call('B.LB', 'bic') == "bicycle:yours")
assert(#vk.call('B.RANGE', 'bicycle:', 'bicycle:~', 1000000) == 3)
assert(#vk.call('B.RANGE', 'cat:', 'cat:~', 1000000) == 3)
assert(#vk.call('B.KEYS', 'cat:*') == 3)
assert(#vk.call('B.KEYS', 'eyes:*') == 3)

return vk.call('B.LB', 'bic')