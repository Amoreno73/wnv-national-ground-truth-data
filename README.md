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
  - `water_chlorophyll_a` from MODIS-Aqua chlorophyll-a.
- Reduces each metric to county statistics (`mean`, `min`, `max`) and exports one row per county-year.
- Fills missing metric values with `-9999` so all expected metric columns are always present.

## Output
- Google Drive CSV with county attributes, metrics, and `year`.
- Test mode is available through `--test-fips` or `--test-limit` to run smaller subsets.
