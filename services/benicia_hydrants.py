import requests
import sqlite3

url = "https://services3.arcgis.com/6CawrotsIAWp4yUX/ArcGIS/rest/services/BeniciaHydrants/FeatureServer/0/query"
params = {
    'where': '1=1',
    'outFields': '*',
    'f': 'json',
    'returnGeometry': 'true'
}

response = requests.get(url, params=params)
data = response.json()

hydrants = data.get('features', [])

#connecting to the database
conn = sqlite3.connect('benicia_hydrants.db')
cursor = conn.cursor()

cursor.execute('''
    CREATE TABLE IF NOT EXISTS hydrants (
        objectId INTEGER PRIMARY KEY,
        facilityId TEXT,
        installDate TEXT,
        locDesc TEXT,
        rotation TEXT,
        manufacturer TEXT,
        operable INTEGER,
        lastService TEXT,
        enabled INTEGER,
        activeFlag INTEGER,
        ownedBy INTEGER,
        maintBy INTEGER,
        lastUpdate TEXT,
        lastEditor TEXT,
        flow REAL,
        created_user TEXT,
        created_date TEXT,
        last_edited_user TEXT,
        last_edited_date TEXT,
        hydrantid TEXT,
        firedistid TEXT,
        gridnum TEXT,
        inspectdt TEXT,
        flowed TEXT,
        noflow TEXT,
        brokestem TEXT,
        hydcap TEXT,
        hyddrain TEXT,
        defthread TEXT,
        leaking TEXT,
        chatter TEXT,
        criticalfac TEXT,
        hardopen TEXT,
        hardclose TEXT,
        pressure REAL,
        geometry_x REAL,
        geometry_y REAL
    )
''')

for hydrant in hydrants:
    attrs = hydrant.get("attributes", {})
    geometry = hydrant.get("geometry", {})

    cursor.execute('''
        INSERT OR REPLACE INTO hydrants (
            objectId,
            facilityId,
            installDate,
            locDesc,
            rotation,
            manufacturer,
            operable,
            lastService,
            enabled,
            activeFlag,
            ownedBy,
            maintBy,
            lastUpdate,
            lastEditor,
            flow,
            created_user,
            created_date,
            last_edited_user,
            last_edited_date,
            hydrantid,
            firedistid,
            gridnum,
            inspectdt,
            flowed,
            noflow,
            brokestem,
            hydcap,
            hyddrain,
            defthread,
            leaking,
            chatter,
            criticalfac,
            hardopen,
            hardclose,
            pressure,
            geometry_x,
            geometry_y
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', (
        attrs.get("OBJECTID"),
        attrs.get("FACILITYID"),
        attrs.get("INSTALLDATE"),
        attrs.get("LOCDESC"),
        attrs.get("ROTATION"),
        attrs.get("MANUFACTURER"),
        attrs.get("OPERABLE"),
        attrs.get("LASTSERVICE"),
        attrs.get("ENABLED"),
        attrs.get("ACTIVEFLAG"),
        attrs.get("OWNEDBY"),
        attrs.get("MAINTBY"),
        attrs.get("LASTUPDATE"),
        attrs.get("LASTEDITOR"),
        attrs.get("FLOW"),
        attrs.get("created_user"),
        attrs.get("created_date"),
        attrs.get("last_edited_user"),
        attrs.get("last_edited_date"),
        attrs.get("hydrantid"),
        attrs.get("firedistid"),
        attrs.get("gridnum"),
        attrs.get("inspectdt"),
        attrs.get("flowed"),
        attrs.get("noflow"),
        attrs.get("brokestem"),
        attrs.get("hydcap"),
        attrs.get("hyddrain"),
        attrs.get("defthread"),
        attrs.get("leaking"),
        attrs.get("chatter"),
        attrs.get("criticalfac"),
        attrs.get("hardopen"),
        attrs.get("hardclose"),
        attrs.get("pressure"),
        geometry.get("x"),
        geometry.get("y")
    ))

conn.commit()
conn.close()
print('Benicia hydrants data successfully saved on database.')