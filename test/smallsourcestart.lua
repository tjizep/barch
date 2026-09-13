local vk
vk = redis
vk.call('B.CLEAR')
vk.call('B.SAVE')
-- both ports come from pulldebug.py as ARGV rather than being written here:
-- python pulls from the first and the second is where this publishes, so a
-- literal in two files is one of them being wrong the first time either moves.
-- See TODO 298.
vk.call('B.START','127.0.0.1',ARGV[1])
vk.call('B.PUBLISH','127.0.0.1',ARGV[2])

vk.call('B.SET','1','one:test')
vk.call('B.SET','2','two:test')
vk.call('B.SET','3','three:test')
vk.call('B.SAVE')
return vk.call('B.SIZE')