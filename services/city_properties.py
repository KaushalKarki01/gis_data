import requests
import sqlite3
import json  

url = 'https://services5.arcgis.com/inY93B27l4TSbT7h/ArcGIS/rest/services/Properties_Owned_by_City_of_SSF_WFL1/FeatureServer/0/query'
params = {
    'where': '1=1',
    'outFields': '*',
    'f': 'json',
    'returnGeometry': 'true'
}

response = requests.get(url, params=params)


if response.status_code != 200:
    print("Error fetching data:", response.status_code)
    exit()

data = response.json()
features = data.get('features', [])
fields_metadata = data.get('fields', [])

# Extract all possible column names and their data types
attribute_keys = {}
geometry_keys = {"geometry": "TEXT"}  # Store geometry as TEXT

# Mapping field types to SQLite types
field_type_mapping = {
    "esriFieldTypeInteger": "INTEGER",
    "esriFieldTypeSmallInteger": "INTEGER",
    "esriFieldTypeDouble": "REAL",
    "esriFieldTypeSingle": "REAL",
    "esriFieldTypeString": "TEXT",
    "esriFieldTypeDate": "TEXT",  
    "esriFieldTypeGUID": "TEXT"
}

# Processing attribute fields
for field in fields_metadata:
    field_name = field["name"]
    field_type = field_type_mapping.get(field["type"], "TEXT")  # Default to TEXT if type is unknown
    attribute_keys[field_name] = field_type

# Combine attribute and geometry columns
all_columns = {**attribute_keys, **geometry_keys}

# Connecting to SQLite database
conn = sqlite3.connect('cityProperties.db')
cursor = conn.cursor()

# Creatiing table dynamically
columns_def = ', '.join([f'"{col}" {dtype}' for col, dtype in all_columns.items()])
create_table_query = f'CREATE TABLE IF NOT EXISTS cityProperties ({columns_def})'
cursor.execute(create_table_query)

# Inserting data into the table
insert_query = f'INSERT OR REPLACE INTO cityProperties ({", ".join(all_columns.keys())}) VALUES ({", ".join(["?" for _ in all_columns])})'

data_to_insert = []
for feature in features:
    attrs = feature.get('attributes', {})
    geometry = feature.get('geometry', {})
    
    # Convert the rings to a JSON string
    rings = geometry.get('rings', [])
    geometry_str = json.dumps(rings)  # Serialize the rings to a JSON string
    
    row = [
        attrs.get(col, None) if col in attrs else geometry_str if col == "geometry" else None
        for col in all_columns.keys()
    ]
    data_to_insert.append(row)

cursor.executemany(insert_query, data_to_insert)


conn.commit()
conn.close()
print('City data successfully stored.')