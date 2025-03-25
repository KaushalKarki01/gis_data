import requests
import sqlite3

# Fetch data from ArcGIS API
url = 'https://services5.arcgis.com/inY93B27l4TSbT7h/ArcGIS/rest/services/SSF_Parcels_Open_Data/FeatureServer/0/query'
params = {
    'where': '1=1',
    'outFields': '*',
    'f': 'json'
}

response = requests.get(url, params=params)
data = response.json()
features = data.get('features', [])

# Extracting all possible column names from attributes and geometry fields
attribute_keys = set()
geometry_keys = set(['geometry_x', 'geometry_y'])  # Default geometry keys

for feature in features:
    attribute_keys.update(feature.get('attributes', {}).keys())
    if 'geometry' in feature and feature['geometry']:
        geometry_keys.update(feature['geometry'].keys())

all_columns = sorted(attribute_keys) + sorted(geometry_keys)

# Connecting to SQLite database
conn = sqlite3.connect('parcels.db')
cursor = conn.cursor()

# Create table dynamically
columns_def = ', '.join([f'"{col}" TEXT' for col in all_columns])  # Store all as TEXT for flexibility
create_table_query = f'CREATE TABLE IF NOT EXISTS parcels ({columns_def})'
cursor.execute(create_table_query)

# Insert data into the table
insert_query = f'INSERT OR REPLACE INTO parcels ({", ".join(all_columns)}) VALUES ({", ".join(["?" for _ in all_columns])})'

data_to_insert = []
for feature in features:
    attrs = feature.get('attributes', {})
    geometry = feature.get('geometry', {})
    row = [str(attrs.get(col, '')) if col in attrs else str(geometry.get(col, '')) if col in geometry else '' for col in all_columns]
    data_to_insert.append(row)

cursor.executemany(insert_query, data_to_insert)

# Commit and close connection
conn.commit()
conn.close()
print('Parcel data added to the database')
