import requests
from datetime import date, timedelta
import sqlite_utils
import time

# SETUP DATABASE, TABLE AND SCHEMA
db = sqlite_utils.Database("air-sensors.db", recreate=False)

# EXTRACT THE SITES IN TOWER HAMLETS
req = requests.get(
    "https://api.erg.ic.ac.uk/AirQuality/Information/MonitoringSiteSpecies/GroupName=towerhamlets/Json"
)
js = req.json()
sites = js["Sites"]["Site"]
db["sites"].upsert_all(sites, pk=("@SiteCode"))
db["sites"].transform(rename={"@Longitude": "Longitude"})
db["sites"].transform(rename={"@Latitude": "Latitude"})

# WE ARE ONLY COLLECTING NO2 AS THIS IS THE ONLY PARTICLE THAT IS MEASURED AT ALL SITES
tablename = "NO2"
table = db.table(
    tablename,
    pk=("@MeasurementDateGMT", "@SiteCode"),
    not_null={"@MeasurementDateGMT", "@Value", "@SiteCode"},
    column_order=("@MeasurementDateGMT", "@Value", "@SiteCode"),
)

# Set the current date as the start point
current_date = date.today()

# Number of days to step back
days_to_step_back = 1

for i in range(days_to_step_back, -1, -1):
    # Calculate the date that is one day ago
    previous_date = current_date - timedelta(days=1)
    
    # PREPARE TO SCAN DATA FOR THE LAST 1 WEEK
    EndDate = previous_date + timedelta(days=1)
    EndWeekDate = EndDate
    StartWeekDate = EndDate - timedelta(days=1)
    StartDate = StartWeekDate - timedelta(days=1)

    # GET THE JSON DATA, UPSERT INTO THE FULL HISTORY DATABASE
    while StartWeekDate > StartDate:
        for el in sites:

            def convert(l):
                l["@Value"] = float(l["@Value"])
                l["@SiteCode"] = el["@SiteCode"]
                return l

            url = f'https://api.erg.ic.ac.uk/AirQuality/Data/SiteSpecies/SiteCode={el["@SiteCode"]}/SpeciesCode=NO2/StartDate={StartWeekDate.strftime("%d %b %Y")}/EndDate={EndWeekDate.strftime("%d %b %Y")}/Json'
            print(url)
            req = requests.get(url, headers={"Connection": "close"})
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
        EndWeekDate = StartWeekDate
        StartWeekDate = EndWeekDate - timedelta(weeks=1)

    # at end of each for loop iteration
    # move current_date back a step preparing next iteration
    current_date = previous_date
    # give the api a break
    time.sleep(.5)
