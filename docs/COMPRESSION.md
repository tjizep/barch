# ZSTD Dictionary Compression

- Use `SET CONFIG compression zstd` (port 14000) or `SET CONFIG B.compression zstd` on the valkey port
- You can use `valkey-server --port 7777 --loadmodule _barch.so` which will put valkey on port 7777 and barch on port 14000
- Use `TRAIN "SOME DATA"` to add samples for training the dictionary, should be at least five (5)
- See test/test_data.py for generating some training data - training isn't required though you can just enable compression
- Use `TRAIN` (no parameters) to complete training and save the trained dictionary - this is used to compress all data
- If training data does not exist barch will use the values as training data and save a dictionary automatically
