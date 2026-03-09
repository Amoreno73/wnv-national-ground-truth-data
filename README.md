# gee_county_summer_metrics.py

`gee_county_summer_metrics.py` builds county-level summer environmental metrics in Google Earth Engine and exports them to CSV in Google Drive.

## What it does
- Loads U.S. county polygons from a GEE FeatureCollection.
- For each year (default `2017-2024`), uses the window `July 1 - September 30`.
- Computes summer NDVI from Landsat 8/9 + Sentinel-2, then summarizes NDVI over MODIS land-cover masks:
  - `ndvi_agri_mean`
  - `ndvi_forest_mean`
  - `ndvi_deciduous_mean`
  - `ndvi_evergreen_mean`
  - `ndvi_mixed_mean`
  - `ndvi_rangeland_mean`
- Computes water metrics:
  - `water_lst_day_c` and `water_lst_night_c` from MODIS LST, masked to JRC surface-water occurrence threshold.
  - `water_chlorophyll_a` as Sentinel-2 NDCI, where `NDCI = (B5 - B4) / (B5 + B4)`, masked to JRC surface-water occurrence threshold.
- Computes DAYMET summer climate metrics:
  - `daymet_temp_c` from daily `(tmin + tmax) / 2`, aggregated over Jul-Sep.
  - `daymet_prcp_mm` from daily `prcp` (mean daily precipitation over Jul-Sep).
- Computes ERA5-Land monthly aggregated climate metrics (Jul-Sep):
  - Relative humidity from `temperature_2m` and `dewpoint_temperature_2m`:
    - `era5_rh_mean_pct`, `era5_rh_min_pct`, `era5_rh_max_pct`
  - 10m wind components:
    - `era5_u10_mean_m_s`, `era5_u10_min_m_s`, `era5_u10_max_m_s`
    - `era5_v10_mean_m_s`, `era5_v10_min_m_s`, `era5_v10_max_m_s`
- Reduces each metric to county statistics (`mean`, `min`, `max`) and exports one row per county-year.
- Fills missing metric values with `-9999` so all expected metric columns are always present.

## Output
- Google Drive CSV with county attributes, metrics, and `year`.
- Test mode is available through `--test-fips` or `--test-limit` to run smaller subsets.
- Example, Cook and Champaign County test (2021 & 2022 only): `python .\gee_county_summer_metrics.py --test-fips 17019,17031 --start-year 2021 --end-year 2022 --export-prefix test_summer_metrics_2021_2022`
