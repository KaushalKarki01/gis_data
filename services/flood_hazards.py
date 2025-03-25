import requests
import sqlite3
import json

url = 'https://services5.arcgis.com/inY93B27l4TSbT7h/ArcGIS/rest/services/SSF_FinalFloodHazard/FeatureServer/1/query'
params = {
    'where': '1=1',
    'outFields': '*',
    'f': 'json'
}

response = requests.get(url, params=params)
data = response.json()


features = data.get('features', [])

#connecting to the database
conn = sqlite3.connect('floodHazards.db')
cursor = conn.cursor()

# creating the table
cursor.execute('''
        CREATE TABLE IF NOT EXISTS floodHazards ( 
                objectid INTEGER PRIMARY KEY,
                dfirm_id TEXT,
                version_id TEXT,
                fld_ar_id TEXT,
                study_type TEXT,
                fld_zone TEXT,
                zone_subty TEXT,
                sfha_tf TEXT,
                static_bfe REAL,
                v_datum TEXT,
                depth REAL,
                len_unit TEXT,
                velocity REAL,
                vel_unit TEXT,
                ar_revert TEXT,
                ar_subtrv TEXT,
                bfe_revert REAL,
                dep_revert REAL,
                dual_zone TEXT,
                source_cit TEXT,
                display TEXT,
                shape_area REAL,
                shape_length REAL,
                geometry TEXT  -- Store geometry as JSON string
        )
 ''')

# inserting into the table

for feature in features:
    attrs = feature.get('attributes', {})
    geometry = feature.get('geometry', {})

    #converting geometry rings to json string
    geometry_rings_json = json.dumps(geometry.get("rings", []))

    cursor.execute('''
        INSERT OR REPLACE INTO floodHazards(
              objectid, dfirm_id, version_id, fld_ar_id, study_type, fld_zone, zone_subty, sfha_tf, static_bfe,
            v_datum, depth, len_unit, velocity, vel_unit, ar_revert, ar_subtrv, bfe_revert, dep_revert, dual_zone,
            source_cit, display, shape_area, shape_length, geometry)
        VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            attrs.get("OBJECTID"),
        attrs.get("DFIRM_ID"),
        attrs.get("VERSION_ID"),
        attrs.get("FLD_AR_ID"),
        attrs.get("STUDY_TYP"),
        attrs.get("FLD_ZONE"),
        attrs.get("ZONE_SUBTY"),
        attrs.get("SFHA_TF"),
        attrs.get("STATIC_BFE"),
        attrs.get("V_DATUM"),
        attrs.get("DEPTH"),
        attrs.get("LEN_UNIT"),
        attrs.get("VELOCITY"),
        attrs.get("VEL_UNIT"),
        attrs.get("AR_REVERT"),
        attrs.get("AR_SUBTRV"),
        attrs.get("BFE_REVERT"),
        attrs.get("DEP_REVERT"),
        attrs.get("DUAL_ZONE"),
        attrs.get("SOURCE_CIT"),
        attrs.get("display"),
        attrs.get("Shape__Area"),
        attrs.get("Shape__Length"),
        geometry_rings_json  # Store geometry as a JSON string
     
        ))
    
conn.commit()
conn.close()
print('Flood Hazards data successfully saved on database.')
