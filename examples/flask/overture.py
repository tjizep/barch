import overturemaps
from overturemaps import core
import json
import barch
import h3
import numpy as np

class OvertureEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.ndarray):
            return obj.tolist() # Convert ndarray to list
        if hasattr(obj, 'wkt'): # Handle Shapely geometry
            return obj.wkt
        return super().default(obj)
h3_res = 12
# 1. Fetch the data
barch.setConfiguration("compression","zstd")

conf = barch.KeyValue("configuration")
conf.set("text_data.shards","1")
conf.set("spatial_data.shards","1")
conf.set("counters.ordered","0")
conf.set("counters.shards","1")

txt = barch.KeyValue("text_data")
spc = barch.KeyValue("spatial_data")
assert spc.getShards() == 1
assert txt.getShards() == 1
cnt = barch.KeyValue("counters")
cnt.set("records","0")
counter = cnt.get("records")

bbox = (-74.001, 40.710, -73.990, 40.720) # Small slice of NYC
# bbox = (-141.0, 41.7, -52.6, 83.1) # Canada
gdf = core.geodataframe("address", bbox=bbox)

print(gdf.head())

# 2. Iterate using itertuples
for row in gdf.itertuples(index=False):
    # Access columns by name: row.street, row.house_number, etc.
    # Geometry is usually accessed via row.geometry

    lon = row.geometry.x
    lat = row.geometry.y
    prov = row.address_levels[1]['value']
    h3_index = int(h3.latlng_to_cell(lat, lon, h3_res), 16)
    row_dict = row._asdict()

    # Handle the geometry (Shapely objects aren't natively JSON serializable)
    if 'geometry' in row_dict:
        row_dict['geometry'] = row_dict['geometry'].wkt # Convert to text (WKT)

    # Now, convert to JSON

    json_val = json.dumps(row_dict, cls=OvertureEncoder)
    print(json_val)
    key = f"{h3_index} {counter}"
    spc.set(key , json_val) # add spatial data
    txt.set(f"{row.street} {counter}",key) # add street text index
    txt.set(f"{row.number}_{counter}",key) # add street number text index
    cnt.incr("records", 1)

print(cnt.get("records"),"rows loaded")
barch.saveAll()