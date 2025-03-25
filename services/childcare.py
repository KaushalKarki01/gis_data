import requests
import sqlite3

url = 'https://services5.arcgis.com/inY93B27l4TSbT7h/ArcGIS/rest/services/Childcare_Facilities_Map_WFL1/FeatureServer/0/query'
params = {
    'where': '1=1',
    'outFields': '*',
    'f': 'json'
}

response = requests.get(url, params=params)
data = response.json()

childcares = data.get('features', [])

#connecting to the database
conn = sqlite3.connect('childcare.db')
cursor = conn.cursor()

cursor.execute('''
        CREATE TABLE IF NOT EXISTS childcare (
               fid INTEGER PRIMARY KEY, 
               name TEXT,
               pid INTEGER,
               pid_1 INTEGER,
               program TEXT,
               address TEXT,
               capacity REAL,
               subsidy TEXT,
               authority TEXT,
               geometry_x REAL,
                geometry_y REAL
            )
 ''')

# inserting into the table

for childcare in childcares:
    attrs = childcare['attributes']
    geometry = childcare.get('geometry', {})
    cursor.execute('''
        INSERT OR REPLACE INTO childcare(fid, name, pid, pid_1, program, address, capacity, subsidy, authority, geometry_x, geometry_y)
        VALUES(?,?,?,?,?,?,?,?,?,?,?)
 ''', (
        attrs.get('FID'),
        attrs.get('Name'),
        attrs.get('PID'),
        attrs.get('PID_1'),
        attrs.get('Program'),
        attrs.get('Address'),
        attrs.get('Capacity'),
        attrs.get('Subsidy'),
        attrs.get('Authority'),
        geometry.get('x'),
        geometry.get('y')
))

conn.commit()
conn.close()
print('Childcare data successfully saved on database.')