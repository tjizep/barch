# requirements
# python -m venc venv
# venv/bin/pip install overturemaps
# venv/bin/pip install h3
# venv/bin/pip install geopandas

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
            #return obj.tolist() # Convert ndarray to list
            return "[]"
        if hasattr(obj, 'wkt'): # Handle Shapely geometry
            return obj.wkt
        return super().default(obj)
def remove_accents(input_str):
    """
    Removes accent characters from a string and converts them to their base characters.

    Args:
        input_str: The string to process.

    Returns:
        The string with accents removed.
    """
    # Normalize the string to the 'NFD' form (Normalization Form D), which decomposes
    # accented characters into their base character and a separate accent mark.
    nfd_str = unicodedata.normalize('NFD', input_str)

    # Encode to 'ascii' and ignore characters it can't encode (the accent marks).
    # Then decode back to a string.
    base_chars_str = nfd_str.encode('ascii', 'ignore').decode('utf-8')
    return base_chars_str.replace("\0","")
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
# Configure barch for memory use + performance
barch.setConfiguration("compression","zstd")
barch.setConfiguration("maintenance_poll_delay","500")
barch.start("0.0.0.0", 14000)
conf = barch.KeyValue("configuration")
conf.set("streets.shards","1")
conf.set("overflows.ordered","0")
conf.set("overflows.shards","1")
conf.set("spatial_data.shards","16")
conf.set("counters.ordered","0")
conf.set("counters.shards","1")
conf.set("postcodes.shards","1")
conf.set("postcode_street.shards","1")
conf.set("provinces.shards","1")
conf.set("street_province","1")
conf.set("cities.shards","1")
conf.set("tokey.ordered", "0")
conf.set("tokey.shards", "16")
streets = barch.KeyValue("streets")
spc = barch.KeyValue("spatial_data")
overflows = barch.KeyValue("overflows")
postcodes = barch.KeyValue("postcodes")
pcode_street = barch.KeyValue("postcode_street")
street_province = barch.KeyValue("street_province")
provinces = barch.KeyValue("provinces")
cities =  barch.KeyValue("cities")
tokey = barch.KeyValue("tokey")


assert spc.getShards() == 16
assert streets.getShards() == 1
cnt = barch.KeyValue("counters")
cnt.set("records","0")
counter = cnt.get("records")

#bbox = (-74.001, 40.710, -73.990, 40.720) # Small slice of NYC
bbox = (-141.0, 41.7, -52.6, 83.1) # All of Canada
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
                prov = f"{row.address_levels[1]['value']}"
            if prov is None:
                prov = 'UNSPECIFIED'
            h3_index = int(h3.latlng_to_cell(lat, lon, h3_res), 16)
            street = remove_accents(f"{row.street}").upper()
            # Handle the geometry (Shapely objects aren't natively JSON serializable)
            if 'geometry' in row_dict:
                row_dict['geometry'] = row_dict['geometry'].wkt # Convert to text (WKT)

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

            streetkey = f"{street} {overflow}"

            if not streets.append(streetkey, f",{counter}"):
                print(f"Street overflow {street} overflow before:{overflow} at {counter}")
                overflow = overflows.incr(f"{street}", 1).s()
                print(f"Street overflow {street} overflow now:{overflow} at {counter}")
                streets.append(f"{street} {overflow}", f",{counter}")

            postcodes.incr(f"{row.postcode}",1) # postcode index forward
            pcode_street.incr(f"{row.postcode} {street}",1) # postcode index street
            street_province.incr(f"{street} *{prov.upper()}")
            # provinces.incr(f"{prov}",1) # province index
            # cities.incr(f"{row.city}",1) # city index
            cnt.incr("records", 1)
            if int(cnt.get("records")) % 4000000 == 0:
                print(json_val)
                print("saving...")
                barch.saveAll()
                print(f"saved {cnt.get('records')} records")

    print(cnt.get("records"),"rows loaded",ignored,"rows ignored")

print("saving...")
barch.saveAll()
print(f"saved {cnt.get('records')} records")


