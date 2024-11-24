"""
CREATE TABLE "NO2" (
   [@MeasurementDateGMT] TEXT,
   [@Value] FLOAT,
   [@SiteCode] TEXT,
   PRIMARY KEY ([@MeasurementDateGMT], [@SiteCode])
);
CREATE TABLE "sites" (
   [@LocalAuthorityCode] TEXT,
   [@LocalAuthorityName] TEXT,
   [@SiteCode] TEXT PRIMARY KEY,
   [@SiteName] TEXT,
   [@SiteType] TEXT,
   [@DateClosed] TEXT,
   [@DateOpened] TEXT,
   [Latitude] FLOAT,
   [Longitude] FLOAT,
   [@DataOwner] TEXT,
   [@DataManager] TEXT,
   [@SiteLink] TEXT,
   [Species] TEXT
);
"""
import requests
from datetime import date, timedelta
import sqlite_utils
import time

# SETUP DATABASE, TABLE AND SCHEMA
db = sqlite_utils.Database("air-sensors.db", recreate=False)

# Whether to check certificate
verify=True

# EXTRACT THE SITES IN TOWER HAMLETS
db["sites"].drop(ignore=True)
req = requests.get(
    "https://api.erg.ic.ac.uk/AirQuality/Information/MonitoringSiteSpecies/GroupName=towerhamlets/Json", verify=verify
)
js = req.json()
sites = js["Sites"]["Site"]
db["sites"].upsert_all(sites, pk=("@SiteCode"))
db["sites"].transform(drop={"@LongitudeWGS84", "@LatitudeWGS84"})
db["sites"].transform(rename={"@Longitude": "Longitude"})
db["sites"].transform(rename={"@Latitude": "Latitude"})
db["sites"].transform(types={"Longitude": float, "Latitude": float})

# WE ARE ONLY COLLECTING NO2 AS THIS IS THE ONLY PARTICLE THAT IS MEASURED AT ALL SITES
tablename = "NO2"
table = db.table(
    tablename,
    pk=("@MeasurementDateGMT", "@SiteCode"),
    not_null={"@MeasurementDateGMT", "@Value", "@SiteCode"},
    column_order=("@MeasurementDateGMT", "@Value", "@SiteCode"),
)
db[tablename].transform(types={"@Value": float})

# Set the current date as the start point
current_date = date.today()

# Number of days to step back
days_to_step_back = 1

for i in range(days_to_step_back, -1, -1):
    # Calculate the date that is one day ago
    previous_date = current_date - timedelta(days=1)
    
    # PREPARE TO SCAN DATA FOR THE LAST 1 EPOCH
    EndDate = previous_date + timedelta(days=1)
    EndEpochDate = EndDate
    StartEpochDate = EndDate - timedelta(days=1)
    StartDate = StartEpochDate - timedelta(days=1)

    # GET THE JSON DATA, UPSERT INTO THE FULL HISTORY DATABASE
    while StartEpochDate > StartDate:
        for el in sites:

            def convert(l):
                l["@Value"] = float(l["@Value"])
                l["@SiteCode"] = el["@SiteCode"]
                return l

            url = f'https://api.erg.ic.ac.uk/AirQuality/Data/SiteSpecies/SiteCode={el["@SiteCode"]}/SpeciesCode={tablename}/StartDate={StartEpochDate.strftime("%d %b %Y")}/EndDate={EndEpochDate.strftime("%d %b %Y")}/Json'

            req = requests.get(url, headers={"Connection": "close"}, verify=verify)
            j = req.json()
            # CLEAN SITES WITH NO DATA OR ZERO VALUE OR NOT NO2 (ONLY MEASURE AVAILABLE AT ALL SITES)
            filtered = [
                a
                for a in j["RawAQData"]["Data"]
                if a["@Value"] != "" and a["@Value"] != "0"
            ]
            if len(filtered) != 0:
                filtered = map(convert, filtered)
                filteredList = list(filtered)
                db[tablename].upsert_all(filteredList, pk=("@MeasurementDateGMT", "@SiteCode"))
        EndEpochDate = StartEpochDate
        StartEpochDate = EndEpochDate - timedelta(days=1)

    # at end of each for loop iteration
    # move current_date back a step preparing next iteration
    current_date = previous_date
    # give the api a break
    time.sleep(.5)

db.vacuum()
