local vk
vk = redis
vk.call('B.START','127.0.0.1','14000')
vk.call('B.SET','1','one:test')
vk.call('B.SET','2','two:test')
vk.call('B.SET','3','three:test')
vk.call('B.SAVE')
return vk.call('B.SIZE')