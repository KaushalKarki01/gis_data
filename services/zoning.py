import requests
import sqlite3
import json  

# Fetch data from ArcGIS API
url = 'https://services5.arcgis.com/inY93B27l4TSbT7h/ArcGIS/rest/services/Zoning/FeatureServer/0/query'
params = {
    'where': '1=1',
    'outFields': '*',
    'f': 'json',
    'returnGeometry': 'true'
}

response = requests.get(url, params=params)
data = response.json()
features = data.get('features', [])
fields_metadata = data.get('fields', [])  

# Extract all possible column names and their data types
attribute_keys = {}
geometry_keys = {"geometry_x": "REAL", "geometry_y": "REAL", "rings": "TEXT"}  

# Map ArcGIS field types to SQLite types
field_type_mapping = {
    "esriFieldTypeInteger": "INTEGER",
    "esriFieldTypeSmallInteger": "INTEGER",
    "esriFieldTypeDouble": "REAL",
    "esriFieldTypeSingle": "REAL",
    "esriFieldTypeString": "TEXT",
    "esriFieldTypeDate": "TEXT",  
    "esriFieldTypeGUID": "TEXT"
}

# Process attribute fields
for field in fields_metadata:
    field_name = field["name"]
    field_type = field_type_mapping.get(field["type"], "TEXT")  
    attribute_keys[field_name] = field_type

# Combine attribute and geometry columns
all_columns = {**attribute_keys, **geometry_keys}

# Connect to SQLite
conn = sqlite3.connect('zoning_data.db')
cursor = conn.cursor()

# Create table dynamically
columns_def = ', '.join([f'"{col}" {dtype}' for col, dtype in all_columns.items()])
create_table_query = f'CREATE TABLE IF NOT EXISTS zoning ({columns_def})'
cursor.execute(create_table_query)

# Insert data into the table
insert_query = f'INSERT OR REPLACE INTO zoning ({", ".join(all_columns.keys())}) VALUES ({", ".join(["?" for _ in all_columns])})'

data_to_insert = []
for feature in features:
    attrs = feature.get('attributes', {})
    geometry = feature.get('geometry', {})

    # Extract geometry values
    geometry_x = geometry.get("x")
    geometry_y = geometry.get("y")
    rings = json.dumps(geometry.get("rings", []))  

    # Construct row properly
    row = []
    for col in all_columns.keys():
        if col == "rings":
            row.append(rings)  
        elif col == "geometry_x":
            row.append(geometry_x)
        elif col == "geometry_y":
            row.append(geometry_y)
        else:
            row.append(attrs.get(col, None))  

    data_to_insert.append(row)

cursor.executemany(insert_query, data_to_insert)

# Commit and close connection
conn.commit()
conn.close()
print('Zoning data successfully stored in SQLite, including ring geometry.')
