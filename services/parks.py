import requests
import sqlite3

url = 'https://services5.arcgis.com/inY93B27l4TSbT7h/ArcGIS/rest/services/Parks/FeatureServer/0/query'
params = {
    'where': '1=1',
    'outFields': '*',
    'f': 'json'
}

response = requests.get(url, params=params)
data = response.json()

parks = data.get('features', [])

#connecting to the database
conn = sqlite3.connect('parks.db')
cursor = conn.cursor()

cursor.execute('''
        CREATE TABLE IF NOT EXISTS parks (
            fid INTEGER PRIMARY KEY,
            objectId REAL,
            subtype INTEGER,
            facId TEXT,
            address TEXT,
            shape_Area REAL,
            shape_Length REAL,
            orig_Fid INTEGER,
            number INTEGER,
            title TEXT,
            d2 TEXT,
            artist TEXT,
            url TEXT,
            latitude REAL,
            longitude REAL,
            location1 TEXT,
            acres REAL,
            facilities TEXT,
            creationDate DATETIME,
            creator TEXT,
            editDate DATETIME,
            editor TEXT,
            geometry_x REAL,
            geometry_y REAL
            )

 ''')

# inserting into the table

for park in parks:
    attrs = park['attributes']
    geometry = park.get('geometry',{})
    cursor.execute('''
        INSERT OR REPLACE INTO parks(fid, objectId, subtype, facId, address, shape_Area, shape_Length, orig_Fid, number, title, d2, artist, url, latitude, longitude, location1, acres, facilities, creationDate, creator, editDate, editor, geometry_x, geometry_y)
        VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
 ''', (
    attrs.get('FID'),
    attrs.get('OBJECTID'),
    attrs.get('SUBTYPE'),
    attrs.get('FACID'),
    attrs.get('ADDRESS'),
    attrs.get('SHAPE_area'),
    attrs.get('SHAPE_length'),
    attrs.get('ORIG_FID'),
    attrs.get('Number'),
    attrs.get('Title'),
    attrs.get('D2'),
    attrs.get('Artist'),
    attrs.get('URL'),
    attrs.get('LAT'),
    attrs.get('LONG'),
    attrs.get('Location1'),
    attrs.get('Acres'),
    attrs.get('Facilities'),
    attrs.get('CreationDate'),
    attrs.get('Creator'),
    attrs.get('EditDate'),
    attrs.get('Editor'),
    geometry.get('x'),
    geometry.get('y')
 ))
    
conn.commit()
conn.close()
print('Parks data added to the database')