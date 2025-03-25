import sqlite3
import requests

url = 'https://services3.arcgis.com/6CawrotsIAWp4yUX/ArcGIS/rest/services/Hospitals/FeatureServer/0/query'
params = {
    'where': '1=1',
    'outFields': '*',
    'f': 'json'
}

response = requests.get(url, params=params)
data = response.json()

#extracting the features list
hospitals = data.get('features', [])

#connecting to the database
conn = sqlite3.connect('hospitals.db')
cursor = conn.cursor()

cursor.execute('''
        CREATE TABLE IF NOT EXISTS hospitals (
            id INTEGER PRIMARY KEY,
            name TEXT,
            address TEXT,
            city TEXT,
            state TEXT,
            zip TEXT,
            zip4 TEXT,
            telephone TEXT,
            type TEXT,
            status TEXT,
            population NUMBER,
            county TEXT,
            countyFips TEXT,
            country TEXT,
            latitude NUMBER,
            longitude NUMBER,
            naics_Code TEXT,
            naics_Description TEXT,
            source TEXT,
            sourceDate DATETIME,
            val_Method TEXT,
            val_Date DATETIME,
            website TEXT,
            state_Id TEXT,
            alt_Name TEXT,
            st_Fips TEXT,
            owner TEXT,
            ttl_Staff NUMBER,
            beds NUMBER,
            trauma TEXT,
            helipad TEXT,
            geometry_x DECIMAL,
            geometry_y DECIMAL
        )

 ''')

# inserting into the table

for hospital in hospitals:
    attrs = hospital['attributes']
    geometry = hospital['geometry']
    cursor.execute('''
        INSERT OR IGNORE INTO hospitals(id, name, address, city, state, zip, zip4, telephone, type, status, population, county, countyFips, country, latitude, longitude, naics_Code, naics_Description, source, sourceDate, val_Method, val_Date, website, state_Id, alt_Name, st_Fips, owner, ttl_Staff, beds, trauma, helipad, geometry_x, geometry_y)
        VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
 ''', (
    attrs.get('ID'),
    attrs.get('NAME'),
    attrs.get('ADDRESS'),
    attrs.get('CITY'),
    attrs.get('STATE'),
    attrs.get('ZIP'),
    attrs.get('ZIP4'),
    attrs.get('TELEPHONE'),
    attrs.get('TYPE'),
    attrs.get('STATUS'),
    attrs.get('POPULATION'),
    attrs.get('COUNTY'),
    attrs.get('COUNTYFIPS'),
    attrs.get('COUNTRY'),
    attrs.get('LATITUDE'),
    attrs.get('LONGITUDE'),
    attrs.get('NAICS_CODE'),
    attrs.get('NAICS_DESC'),
    attrs.get('SOURCE'),
    attrs.get('SOURCEDATE'),
    attrs.get('VAL_METHOD'),
    attrs.get('VAL_DATE'),
    attrs.get('WEBSITE'),
    attrs.get('STATE_ID'),
    attrs.get('ALT_NAME'),
    attrs.get('ST_FIPS'),
    attrs.get('OWNER'),
    attrs.get('TTL_STAFF'),
    attrs.get('BEDS'),
    attrs.get('TRAUMA'),
    attrs.get('HELIPAD'),
    geometry.get('x'),
    geometry.get('y')
 ))
    
conn.commit()
conn.close()
print('Hospitals data saved to database')
