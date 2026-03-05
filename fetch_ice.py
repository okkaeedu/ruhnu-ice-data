"""
Fetches Baltic Sea ice data from Copernicus Marine Service (CMEMS)
Product: BALTICSEA_ANALYSISFORECAST_PHY_003_006
Variables: siconc (ice concentration %), sithick (ice thickness m)
Location: Ruhnu / Pöörupoi area (57.78°N, 23.28°E)

Result saved to: ruhnu_ice_cmems.json
"""

import copernicusmarine
import json
import os
from datetime import datetime, timezone, timedelta

# ── Koordinaadid (Ruhnu lähiümbrus) ──────────────────────────
LAT = 57.78
LON = 23.28
LAT_MARGIN = 0.1   # ±0.1° = ~11 km ruut keskmistamiseks
LON_MARGIN = 0.15

# ── CMEMS toote konfig ────────────────────────────────────────
DATASET_ID  = "cmems_mod_bal_phy_anfc_P1D-m"   # päevased keskmised
VARIABLES   = ["siconc", "sithick"]

# ── Kuupäevavahemik: täna + 10 päeva ─────────────────────────
today    = datetime.now(timezone.utc).date()
end_date = today + timedelta(days=10)

print(f"Fetching CMEMS ice data for {today} .. {end_date}")
print(f"Location: {LAT}°N {LON}°E ±{LAT_MARGIN}°")

# ── Andmete laadimine ─────────────────────────────────────────
ds = copernicusmarine.open_dataset(
    dataset_id  = DATASET_ID,
    variables   = VARIABLES,
    minimum_latitude  = LAT - LAT_MARGIN,
    maximum_latitude  = LAT + LAT_MARGIN,
    minimum_longitude = LON - LON_MARGIN,
    maximum_longitude = LON + LON_MARGIN,
    start_datetime    = f"{today}T00:00:00",
    end_datetime      = f"{end_date}T23:59:59",
    username = os.environ["CMEMS_USER"],
    password = os.environ["CMEMS_PASS"],
)

# ── Ruumiline keskmistamine punkti kohta ──────────────────────
ds_mean = ds.mean(dim=["latitude", "longitude"])

# ── JSON ehitamine ────────────────────────────────────────────
days = []
for t in ds_mean.time.values:
    dt = str(t)[:10]  # 'YYYY-MM-DD'
    row = {"date": dt}

    if "siconc" in ds_mean:
        val = float(ds_mean["siconc"].sel(time=t).values)
        row["siconc"] = round(val, 3) if not __import__('math').isnan(val) else None

    if "sithick" in ds_mean:
        val = float(ds_mean["sithick"].sel(time=t).values)
        row["sithick"] = round(val, 3) if not __import__('math').isnan(val) else None

    days.append(row)
    print(f"  {dt}: siconc={row.get('siconc')} sithick={row.get('sithick')}")

output = {
    "updated":    datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%MZ"),
    "product":    DATASET_ID,
    "lat":        LAT,
    "lon":        LON,
    "source":     "Copernicus Marine Service — BALTICSEA_ANALYSISFORECAST_PHY_003_006",
    "days":       days,
}

with open("ruhnu_ice_cmems.json", "w") as f:
    json.dump(output, f, indent=2)

print(f"\nSaved {len(days)} days to ruhnu_ice_cmems.json")
