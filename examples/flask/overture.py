import overturemaps
from overturemaps import core
import json
import barch
import h3
import numpy as np
from shapely import wkb
import geopandas as gpd
import unicodedata

class OvertureEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.ndarray):
            return obj.tolist() # Convert ndarray to list
        if hasattr(obj, 'wkt'): # Handle Shapely geometry
            return obj.wkt
        return super().default(obj)
def remove_accents(input_str):
    # Normalize the string to a decomposed form (NFKD)
    # This separates base characters from their diacritics
    nfkd_form = unicodedata.normalize('NFKD', input_str)

    # Filter out all combining characters (the diacritics)
    # unicodedata.combining(c) returns True if the character is a diacritic
    only_base_chars = "".join([c for c in nfkd_form if not unicodedata.combining(c)])

    return only_base_chars
def filter_street(sn):
    s = f"{sn}"
    if s.find("_") != -1:
        return True
    if s.find("_") != -1:
        return True
    if s.startswith("-"):
        return True
    return False

h3_res = 12
# 1. Fetch the data
barch.setConfiguration("compression","zstd")
barch.start("0.0.0.0", 14000)
conf = barch.KeyValue("configuration")
conf.set("text_data.shards","1")
conf.set("overflow.ordered","0")
conf.set("spatial_data.shards","1")
conf.set("counters.ordered","0")
conf.set("counters.shards","1")
conf.set("postcodes.shards","1")
conf.set("provinces.shards","1")
conf.set("cities.shards","1")
conf.set("tokey.ordered", "0")
txt = barch.KeyValue("text_data")
spc = barch.KeyValue("spatial_data")
overflows = barch.KeyValue("overflows")
postcodes = barch.KeyValue("postcodes")
provinces = barch.KeyValue("provinces")
cities =  barch.KeyValue("cities")
tokey = barch.KeyValue("tokey")

assert spc.getShards() == 1
assert txt.getShards() == 1
cnt = barch.KeyValue("counters")
cnt.set("records","0")
counter = cnt.get("records")

#bbox = (-74.001, 40.710, -73.990, 40.720) # Small slice of NYC
bbox = (-141.0, 41.7, -52.6, 83.1) # Canada
#gdf = core.geodataframe("address", bbox=bbox, connect_timeout=60,  request_timeout=120)
reader = overturemaps.record_batch_reader("address", bbox, connect_timeout=60,  request_timeout=120)
ignored = 0
# Process in chunks (batches) to stay under RAM limits
for batch in reader:
    current_batch_size = batch.num_rows
    print(f"Processing a batch of {current_batch_size} rows")
    gdf = batch.to_pandas()
    df = gpd.GeoDataFrame(
        gdf,
        geometry=gdf['geometry'].apply(wkb.loads),
        crs="EPSG:4326"
    )
    print(df.head())

    # 2. Iterate using itertuples
    for row in df.itertuples(index=False):
        # Access columns by name: row.street, row.house_number, etc.
        # Geometry is usually accessed via row.geometry
        row_dict = row._asdict()

        lon = row.geometry.x
        lat = row.geometry.y
        if filter_street(row.street):
            ignored =ignored+1
        else:
            prov = 'NONE'
            if len(row.address_levels) > 1:
                prov = row.address_levels[1]['value']

            h3_index = int(h3.latlng_to_cell(lat, lon, h3_res), 16)
            street = remove_accents(f"{row.street}").upper()
            # Handle the geometry (Shapely objects aren't natively JSON serializable)
            if 'geometry' in row_dict:
                row_dict['geometry'] = row_dict['geometry'].wkt # Convert to text (WKT)

            # Now, convert to JSON

            json_val = json.dumps(row_dict, cls=OvertureEncoder)
            #print(json_val)
            counter = cnt.get("records")
            key = f"{h3_index} {counter}"
            tokey.set(f"{counter}", key)
            spc.set(key , json_val) # add spatial data
            # add street text index

            overflow = overflows.get(f"{street}")
            if not overflow:
                overflow = 0
                overflows.set(f"{street}","0")

            if not txt.append(f"{street} {overflow}",f",{counter}"):
                print("Street overflow",street)
                overflow = overflows.incr(f"{street}", 1)
                txt.append(f"{street} {overflow}",f",{counter}")
            # txt.set(f"{row.number}_{street}{counter}",key) # add street number text index
            # postcodes.set(f"{row.postcode} {counter}",f"{counter}") # postcode index
            # provinces.set(f"{prov} {counter}",f"{counter}") # province index
            # cities.set(f"{row.city} {counter}",f"{counter}") # city index
            cnt.incr("records", 1)
            if int(cnt.get("records")) % 2000000 == 0:
                print("saving...")
                barch.saveAll()
                print(f"saved {cnt.get('records')} records")
    print(cnt.get("records"),"rows loaded",ignored,"rows ignored")
