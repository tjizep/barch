To run the python examples try the following (after building barch in `cmake-build-release` for instance)
- This specific command will import overture data into a collection of key spaces for querying using the h3 gepositioning system  
```
cd ../examples/flask
mkdir data
cd data
python3 -m venv ./venv
./venv/bin/pip install ../../../cmake-build-release/
./venv/bin/pip install overturemaps
./venv/bin/pip install h3
./venv/bin/pip install gpd
./venv/bin/pip install geopandas
./venv/bin/python3 ../overture.py
```
- After running the last command you may wait for a while until the canadian maps are loaded 
- connect a redis-client to port 14000 while the import is running
  ```
  redis-cli -p 14000
  ```
- run the `SPACES` command to see the data importing in real time
- once the data is imported the server will stop - there are round about 16 million records to process
- your system needs about 7 GiB of Free memory to import all the data

- to start the server again (more permanently) if you want
  ```
  ./venv/bin/python3 ../server.py
  ```
- after the server is running you can use the redis (or valkey client) to query for instance all the addresses in MAIN street like so
 ```
 spatial_data:VALUES "*MAIN ST*"
 ```
- the above command can also be run while importing