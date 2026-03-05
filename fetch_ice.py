"""
Fetches Baltic Sea ice data from Copernicus Marine Service (CMEMS)
Product: BALTICSEA_ANALYSISFORECAST_PHY_003_006
Variables: siconc (ice concentration %), sithick (ice thickness m)

Outputs two things per day:
  - center point (57.78°N 23.28°E) → for table display
  - sparse grid (Liivi laht area, every 5th CMEMS point) → for map heatmap

Result saved to: ruhnu_ice_cmems.json
"""

import copernicusmarine
import json
import os
import math
import numpy as np
from datetime import datetime, timezone, timedelta

# ── Grid-ala (Liivi laht + Irbe väin + Sõrve + Kolka) ────────
# Lõuna: Läti rannik + Kolka neem ~56.3°N
# Lääs:  Irbe väina läänesuue + avameri ~20.0°E
# Põhi:  Hiiumaa põhjatipp + Soome lahe suue ~59.2°N
GRID_LAT_MIN = 56.3
GRID_LAT_MAX = 60.0
GRID_LON_MIN = 20.0
GRID_LON_MAX = 25.0
GRID_STEP    = 2       # iga 5. CMEMS ruudu punkt (~9 km vahe)

# ── Ruhnu center-punkt tabelile ───────────────────────────────
CENTER_LAT = 57.78
CENTER_LON = 23.28

# ── CMEMS toote konfig ────────────────────────────────────────
DATASET_ID = "cmems_mod_bal_phy_anfc_P1D-m"
VARIABLES  = ["siconc", "sithick"]

# ── Kuupäevavahemik: täna + 10 päeva ─────────────────────────
today    = datetime.now(timezone.utc).date()
end_date = today + timedelta(days=10)

print(f"Fetching CMEMS ice grid for {today} .. {end_date}")
print(f"Grid: {GRID_LAT_MIN}-{GRID_LAT_MAX}°N, {GRID_LON_MIN}-{GRID_LON_MAX}°E, step={GRID_STEP}")

# ── Andmete laadimine (suurem ala) ────────────────────────────
ds = copernicusmarine.open_dataset(
    dataset_id        = DATASET_ID,
    variables         = VARIABLES,
    minimum_latitude  = GRID_LAT_MIN,
    maximum_latitude  = GRID_LAT_MAX,
    minimum_longitude = GRID_LON_MIN,
    maximum_longitude = GRID_LON_MAX,
    start_datetime    = f"{today}T00:00:00",
    end_datetime      = f"{end_date}T23:59:59",
    username = os.environ["CMEMS_USER"],
    password = os.environ["CMEMS_PASS"],
)

# ── Center-punkti indeks ──────────────────────────────────────
lats_all = ds.latitude.values
lons_all = ds.longitude.values
ci = int(np.abs(lats_all - CENTER_LAT).argmin())
cj = int(np.abs(lons_all - CENTER_LON).argmin())

# ── Subsampled grid koordinaadid ──────────────────────────────
lats_sub = lats_all[::GRID_STEP]
lons_sub = lons_all[::GRID_STEP]
print(f"Grid size after subsampling: {len(lats_sub)} x {len(lons_sub)} = {len(lats_sub)*len(lons_sub)} points/day")

# ── JSON ehitamine ────────────────────────────────────────────
days = []
for t in ds.time.values:
    dt = str(t)[:10]

    # Center-punkt (tabelile)
    sc_c = float(ds["siconc"].values[list(ds.time.values).index(t), ci, cj])
    st_c = float(ds["sithick"].values[list(ds.time.values).index(t), ci, cj])
    siconc_center  = round(sc_c, 3) if not math.isnan(sc_c) else None
    sithick_center = round(st_c, 3) if not math.isnan(st_c) else None

    # Sparse grid (kaardile) — ainult mitte-NaN merepunktid
    tidx = list(ds.time.values).index(t)
    sc_grid = ds["siconc"].values[tidx, ::GRID_STEP, ::GRID_STEP]
    st_grid = ds["sithick"].values[tidx, ::GRID_STEP, ::GRID_STEP]

    pts = []
    for i, la in enumerate(lats_sub):
        for j, lo in enumerate(lons_sub):
            sc = sc_grid[i, j]
            if math.isnan(sc):
                continue   # maa või andmepuudus
            st = st_grid[i, j]
            pts.append([
                round(float(la), 3),
                round(float(lo), 3),
                round(float(sc), 2),
                round(float(st), 2) if not math.isnan(float(st)) else 0
            ])

    days.append({
        "date":    dt,
        "siconc":  siconc_center,
        "sithick": sithick_center,
        "pts":     pts,          # [[lat, lon, siconc, sithick], ...]
    })
    print(f"  {dt}: center siconc={siconc_center} sithick={sithick_center} | grid pts={len(pts)}")

output = {
    "updated": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%MZ"),
    "product": DATASET_ID,
    "lat":     CENTER_LAT,
    "lon":     CENTER_LON,
    "source":  "Copernicus Marine Service — BALTICSEA_ANALYSISFORECAST_PHY_003_006",
    "grid_step_deg": round(float(lats_sub[1] - lats_sub[0]), 4) if len(lats_sub) > 1 else 0.083,
    "days":    days,
}

with open("ruhnu_ice_cmems.json", "w") as f:
    json.dump(output, f, separators=(',', ':'))   # kompaktne, väiksem fail

print(f"\nSaved {len(days)} days, ~{len(days[0]['pts']) if days else 0} grid pts/day")
print(f"JSON size estimate: ~{sum(len(json.dumps(d['pts'])) for d in days)//1024} KB for grid data")


