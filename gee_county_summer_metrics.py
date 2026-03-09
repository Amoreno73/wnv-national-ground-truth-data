from __future__ import annotations

import argparse
from typing import Iterable, Optional

# run this command in the terminal to connect google account --> earthengine authenticate
import ee 

# Context: parameters relevant to the propagation of the West Nile Virus.
# Years: 2017 to 2024
# Months: July to September
# Aggregation: yearly mean (July to September only for each year).
# Region: County-level in the United States.
# Parameters: 
# NDVI (Agricultural, Forest, Rangeland), NDCI, LST day/night, Temp, Precipitation, Humidity, Wind speeds

# this is under "users/angel314/"
GEE_PROJECT="wnv-embeddings"

# Satellite/image datasets used by this workflow.
LANDSAT_8_L2 = "LANDSAT/LC08/C02/T1_L2" # ndvi
LANDSAT_9_L2 = "LANDSAT/LC09/C02/T1_L2" # ndvi
SENTINEL2_SR = "COPERNICUS/S2_SR_HARMONIZED" # ndvi and ndci
MODIS_LST = "MODIS/061/MOD11A2" # LST day and night 1km
MODIS_LANDCOVER = "MODIS/061/MCD12Q1" # ag/forest/rangeland masks
# ^ yearly resets last updated 2024

JRC_SURFACE_WATER = "JRC/GSW1_4/GlobalSurfaceWater" # static water mask

# new for climate data - mean/min/max 
DAYMET_CLIMATE = "NASA/ORNL/DAYMET_V4" # temperatures and precipitation
ERA5_LAND_MONTHLY = "ECMWF/ERA5_LAND/MONTHLY_AGGR" # rel. humidity (from temp & pressure), v & u wind speeds 

# Base metric columns (mean values). These names are preserved for backward compatibility.
BASE_METRIC_COLUMNS = [
    "ndvi_agri_mean",
    "ndvi_forest_mean",
    "ndvi_deciduous_mean",
    "ndvi_evergreen_mean",
    "ndvi_mixed_mean",
    "ndvi_rangeland_mean",
    "water_lst_day_c",
    "water_lst_night_c",
    "water_chlorophyll_a",
    "daymet_temp_c",
    "daymet_prcp_mm",
    "era5_rh_mean_pct",
    "era5_rh_min_pct",
    "era5_rh_max_pct",
    "era5_u10_mean_m_s",
    "era5_u10_min_m_s",
    "era5_u10_max_m_s",
    "era5_v10_mean_m_s",
    "era5_v10_min_m_s",
    "era5_v10_max_m_s",
]

# Metrics we always want present in the output schema (saved CSV output in Google Drive):
# each base metric plus explicit min/max companions.
REQUIRED_METRIC_COLUMNS = [
    column
    for base_name in BASE_METRIC_COLUMNS
    for column in (base_name, f"{base_name}_min", f"{base_name}_max")
]

# Explicit export column order to keep schema stable in Drive CSV outputs.
EXPORT_BASE_COLUMNS = [
    "system:index",
    "ALAND",
    "AWATER",
    "CBSAFP",
    "CLASSFP",
    "COUNTYFP",
    "COUNTYNS",
    "CSAFP",
    "FUNCSTAT",
    "GEOID",
    "GEOIDFQ",
    "INTPTLAT",
    "INTPTLON",
    "LSAD",
    "METDIVFP",
    "MTFCC",
    "NAME",
    "NAMELSAD",
    "STATEFP",
]

def validate_project_id(project: Optional[str]) -> Optional[str]:
    """Validate a GCP project id for Earth Engine Cloud API initialization."""
    if project is None:
        return None

    project = project.strip()
    if not project:
        return None

    if "/" in project:
        raise ValueError(
            "Invalid --project value. Use only the GCP project id (for example: my-ee-project), "
            "not a path like users/angel314 or projects/my-ee-project."
        )
    return project


def parse_args() -> argparse.Namespace:
    # Command-line interface for the script.
    # These arguments let you:
    # 1) choose data/time settings (asset, years, water mask threshold),
    # 2) control export destination and file naming,
    # 3) run small county subsets for quick validation before national runs.
    # so an example is:
    # python gee_county_summer_metrics.py --project users/angel314/wnv_embeddings --test-fips 17031 --start-year 2019
    parser = argparse.ArgumentParser(
        description=(
            "Build county-level summer (Jul 1-Sep 30) metrics in Google Earth Engine "
            "for 2017-2024 and export to CSV."
        )
    )
    parser.add_argument(
        "--county-asset-id",
        default="projects/wnv-embeddings/assets/tl_2025_us_county", # this is the same asset used for the 2025 county boundary embedding calculations
        help="GEE FeatureCollection asset with county boundaries and FIPS property.",
    )
    parser.add_argument("--fips-property", default="GEOID", help="FIPS property name in county asset.")
    parser.add_argument("--start-year", type=int, default=2017) # can also change this for quick testing
    parser.add_argument("--end-year", type=int, default=2024)
    parser.add_argument(
        "--water-occurrence-threshold",
        type=float,
        default=10.0, # 10% captures seasonal water layers, 90% would capture permanent ones
        help="JRC water occurrence percent threshold used to define county water mask.",
    )
    parser.add_argument(
        "--export-folder",
        default="gee_exports",
        help="GEE folder name for CSV export.",
    )
    parser.add_argument(
        "--export-prefix",
        default="county_summer_metrics_2017_2024",
        help="Output CSV prefix in Google Drive.",
    )
    parser.add_argument(
        "--project",
        default=GEE_PROJECT,
        help="project for ee.Initialize(project=...).",
    )
    parser.add_argument(
        "--test-fips",
        default=None,
        help="Comma-separated county FIPS list to run a small test subset.",
    )
    parser.add_argument(
        "--test-limit",
        type=int,
        default=None,
        help="If set, limit counties to the first N features (useful for quick tests).",
    )
    parser.add_argument(
        "--scale",
        type=int,
        default=1000,
        help="Reduction scale in meters for county means.",
    )
    parser.add_argument(
        "--tile-scale",
        type=int,
        default=4,
        help="tileScale used in reduceRegions to avoid memory issues.",
    )
    return parser.parse_args()


def initialize_ee(project: Optional[str] = None) -> None:
    """Initialize Earth Engine; tries direct init, then interactive auth fallback."""
    project = validate_project_id(project)

    try:
        if project:
            ee.Initialize(project=project)
        else:
            ee.Initialize()
    except Exception:
        ee.Authenticate()
        try:
            if project:
                ee.Initialize(project=project)
            else:
                ee.Initialize()
        except TypeError as exc:
            error_text = str(exc)
            if "projects/users/" in error_text or 'pattern "^projects/[^/]+$"' in error_text:
                raise RuntimeError(
                    "Earth Engine is configured with an invalid project id (looks like 'users/...'). "
                    "Run with --project YOUR_GCP_PROJECT_ID, or re-authenticate with:\n"
                    "  earthengine authenticate --project YOUR_GCP_PROJECT_ID"
                ) from exc
            raise


def split_csv_values(csv_values: Optional[str]) -> list[str]:
    if not csv_values:
        return []
    return [value.strip() for value in csv_values.split(",") if value.strip()]


def load_counties(asset_id: str, fips_property: str, test_fips: Iterable[str], test_limit: Optional[int]) -> ee.FeatureCollection:
    """Load counties and optionally filter down to a test subset."""
    counties = ee.FeatureCollection(asset_id)

    fips_list = list(test_fips)
    if fips_list:
        counties = counties.filter(ee.Filter.inList(fips_property, fips_list))

    if test_limit is not None and test_limit > 0:
        counties = ee.FeatureCollection(counties.toList(test_limit))

    return counties


def summer_window(year: int) -> tuple[ee.Date, ee.Date]:
    """Return [start, end) dates for July 1 through September 30 of one year."""
    start = ee.Date.fromYMD(year, 7, 1)
    end = ee.Date.fromYMD(year, 9, 30).advance(1, "day")
    return start, end


def empty_masked_band(name: str) -> ee.Image:
    """Return a fully masked single-band image for safe fallbacks."""
    return ee.Image.constant(0).rename(name).updateMask(ee.Image(0))


def safe_mean_single_band(collection: ee.ImageCollection, output_name: str) -> ee.Image:
    """Return collection mean for one-band collections, or a masked fallback when empty."""
    return ee.Image(
        ee.Algorithms.If(
            collection.size().gt(0),
            collection.mean().rename(output_name),
            empty_masked_band(output_name),
        )
    )


def mask_landsat_l2_sr(image: ee.Image) -> ee.Image:
    """Mask clouds/shadows/snow for Landsat Collection 2 Level 2 SR images."""
    qa_pixel = image.select("QA_PIXEL")
    clear_mask = (
        qa_pixel.bitwiseAnd(1 << 3).eq(0)  # cloud
        .And(qa_pixel.bitwiseAnd(1 << 4).eq(0))  # cloud shadow
        .And(qa_pixel.bitwiseAnd(1 << 5).eq(0))  # snow
        .And(qa_pixel.bitwiseAnd(1 << 1).eq(0))  # dilated cloud
    )

    ndvi = image.normalizedDifference(["SR_B5", "SR_B4"]).rename("ndvi")
    return ndvi.updateMask(clear_mask).copyProperties(image, ["system:time_start"])


def mask_sentinel2_sr(image: ee.Image) -> ee.Image:
    """Mask clouds and cirrus for Sentinel-2 SR harmonized images."""
    qa60 = image.select("QA60")
    cloud_mask = qa60.bitwiseAnd(1 << 10).eq(0).And(qa60.bitwiseAnd(1 << 11).eq(0))

    scl = image.select("SCL")
    scl_mask = (
        scl.neq(3)  # cloud shadow
        .And(scl.neq(8))  # cloud medium probability
        .And(scl.neq(9))  # cloud high probability
        .And(scl.neq(10))  # cirrus
        .And(scl.neq(11))  # snow/ice
    )

    ndvi = image.normalizedDifference(["B8", "B4"]).rename("ndvi")
    return ndvi.updateMask(cloud_mask).updateMask(scl_mask).copyProperties(image, ["system:time_start"])


def mask_sentinel2_ndci(image: ee.Image) -> ee.Image:
    """Mask clouds/cirrus and compute NDCI from Sentinel-2: (B5 - B4) / (B5 + B4)."""
    qa60 = image.select("QA60")
    cloud_mask = qa60.bitwiseAnd(1 << 10).eq(0).And(qa60.bitwiseAnd(1 << 11).eq(0))

    scl = image.select("SCL")
    scl_mask = (
        scl.neq(3)  # cloud shadow
        .And(scl.neq(8))  # cloud medium probability
        .And(scl.neq(9))  # cloud high probability
        .And(scl.neq(10))  # cirrus
        .And(scl.neq(11))  # snow/ice
    )

    ndci = image.normalizedDifference(["B5", "B4"]).rename("water_chlorophyll_a")
    return ndci.updateMask(cloud_mask).updateMask(scl_mask).copyProperties(image, ["system:time_start"])


def get_summer_ndvi_mean(county_geometry: ee.Geometry, start: ee.Date, end: ee.Date) -> ee.Image:
    """Create one summer NDVI mean image from Landsat 8/9 and Sentinel-2."""
    l8_ndvi = (
        ee.ImageCollection(LANDSAT_8_L2)
        .filterBounds(county_geometry)
        .filterDate(start, end)
        .map(mask_landsat_l2_sr)
    )
    l9_ndvi = (
        ee.ImageCollection(LANDSAT_9_L2)
        .filterBounds(county_geometry)
        .filterDate(start, end)
        .map(mask_landsat_l2_sr)
    )
    s2_ndvi = (
        ee.ImageCollection(SENTINEL2_SR)
        .filterBounds(county_geometry)
        .filterDate(start, end)
        .map(mask_sentinel2_sr)
    )

    ndvi_collection = l8_ndvi.merge(l9_ndvi).merge(s2_ndvi)
    return safe_mean_single_band(ndvi_collection, "ndvi")


def daymet_daily_mean_temp(image: ee.Image) -> ee.Image:
    """Build one daily mean temperature band in Celsius from DAYMET tmin/tmax."""
    tmean = image.select("tmin").add(image.select("tmax")).divide(2).rename("daymet_temp_c")
    return tmean.copyProperties(image, ["system:time_start"])


def build_daymet_metrics(county_geometry: ee.Geometry, start: ee.Date, end: ee.Date) -> ee.Image:
    """Build summer DAYMET metrics: mean daily temperature and mean daily precipitation."""
    daymet_collection = (
        ee.ImageCollection(DAYMET_CLIMATE)
        .filterBounds(county_geometry)
        .filterDate(start, end)
    )

    # Temperature in DAYMET is already in Celsius. We compute daily mean temp as (tmin + tmax) / 2,
    # then take the July-Sep mean image for county reduction.
    temp_collection = daymet_collection.map(daymet_daily_mean_temp)
    daymet_temp_c = safe_mean_single_band(temp_collection, "daymet_temp_c")

    # DAYMET prcp is daily precipitation (mm/day). We use the July-Sep mean image.
    daymet_prcp_mm = safe_mean_single_band(daymet_collection.select("prcp"), "daymet_prcp_mm")

    return ee.Image.cat([daymet_temp_c, daymet_prcp_mm]).clip(county_geometry)


def relative_humidity_from_t_and_td(temp_k: ee.Image, dewpoint_k: ee.Image, output_name: str) -> ee.Image:
    """Compute RH (%) from air temperature and dewpoint temperature, both in Kelvin."""
    temp_c = temp_k.subtract(273.15)
    dewpoint_c = dewpoint_k.subtract(273.15)

    # Magnus approximation; RH = 100 * exp(gamma(Td) - gamma(T))
    gamma_td = dewpoint_c.multiply(17.625).divide(dewpoint_c.add(243.04))
    gamma_t = temp_c.multiply(17.625).divide(temp_c.add(243.04))
    return gamma_td.subtract(gamma_t).exp().multiply(100).clamp(0, 100).rename(output_name)


def build_era5_metrics(county_geometry: ee.Geometry, start: ee.Date, end: ee.Date) -> ee.Image:
    """Build Jul-Sep ERA5-Land metrics: RH and 10m u/v wind components (mean/min/max bands)."""
    era5_collection = (
        ee.ImageCollection(ERA5_LAND_MONTHLY)
        .filterBounds(county_geometry)
        .filterDate(start, end)
    )

    rh_mean_collection = era5_collection.map(
        lambda image: relative_humidity_from_t_and_td(
            image.select("temperature_2m"),
            image.select("dewpoint_temperature_2m"),
            "era5_rh_mean_pct",
        ).copyProperties(image, ["system:time_start"])
    )
    rh_min_collection = era5_collection.map(
        lambda image: relative_humidity_from_t_and_td(
            image.select("temperature_2m_min"),
            image.select("dewpoint_temperature_2m_min"),
            "era5_rh_min_pct",
        ).copyProperties(image, ["system:time_start"])
    )
    rh_max_collection = era5_collection.map(
        lambda image: relative_humidity_from_t_and_td(
            image.select("temperature_2m_max"),
            image.select("dewpoint_temperature_2m_max"),
            "era5_rh_max_pct",
        ).copyProperties(image, ["system:time_start"])
    )

    era5_rh_mean = safe_mean_single_band(rh_mean_collection, "era5_rh_mean_pct")
    era5_rh_min = safe_mean_single_band(rh_min_collection, "era5_rh_min_pct")
    era5_rh_max = safe_mean_single_band(rh_max_collection, "era5_rh_max_pct")

    # Monthly mean/min/max wind component bands are provided directly in ERA5-Land monthly_aggr.
    era5_u10_mean = safe_mean_single_band(era5_collection.select("u_component_of_wind_10m"), "era5_u10_mean_m_s")
    era5_u10_min = safe_mean_single_band(era5_collection.select("u_component_of_wind_10m_min"), "era5_u10_min_m_s")
    era5_u10_max = safe_mean_single_band(era5_collection.select("u_component_of_wind_10m_max"), "era5_u10_max_m_s")
    era5_v10_mean = safe_mean_single_band(era5_collection.select("v_component_of_wind_10m"), "era5_v10_mean_m_s")
    era5_v10_min = safe_mean_single_band(era5_collection.select("v_component_of_wind_10m_min"), "era5_v10_min_m_s")
    era5_v10_max = safe_mean_single_band(era5_collection.select("v_component_of_wind_10m_max"), "era5_v10_max_m_s")

    return ee.Image.cat(
        [
            era5_rh_mean,
            era5_rh_min,
            era5_rh_max,
            era5_u10_mean,
            era5_u10_min,
            era5_u10_max,
            era5_v10_mean,
            era5_v10_min,
            era5_v10_max,
        ]
    ).clip(county_geometry)


def get_landcover_image(year: int) -> ee.Image:
    """Get MODIS IGBP landcover for a year, fallback to latest if missing."""
    # LC_Type1 corresponds to:
    # Annual International Geosphere-Biosphere Programme (IGBP) classification

    collection = ee.ImageCollection(MODIS_LANDCOVER).select("LC_Type1")
    by_year = collection.filter(ee.Filter.calendarRange(year, year, "year")).first()
    latest = collection.sort("system:time_start", False).first()
    return ee.Image(ee.Algorithms.If(by_year, by_year, latest))


def build_ndvi_cover_metrics(year: int, county_geometry: ee.Geometry, ndvi_mean: ee.Image) -> ee.Image:
    """Mask NDVI by cover classes and return one multi-band image for reduceRegions."""
    lc = get_landcover_image(year)

    # MODIS MCD12Q1 LC_Type1 uses IGBP classes:
    # see https://developers.google.com/earth-engine/datasets/catalog/MODIS_061_MCD12Q1
    # 12 = Croplands, 14 = Cropland/Natural Vegetation Mosaic.
    agri = lc.eq(12).Or(lc.eq(14)).selfMask()

    # Forest subclasses:
    # 1 = Evergreen Needleleaf Forest, 2 = Evergreen Broadleaf Forest,
    # 3 = Deciduous Needleleaf Forest, 4 = Deciduous Broadleaf Forest,
    # 5 = Mixed Forests.
    evergreen = lc.eq(1).Or(lc.eq(2)).selfMask()
    deciduous = lc.eq(3).Or(lc.eq(4)).selfMask()
    mixed = lc.eq(5).selfMask()
    forest = evergreen.Or(deciduous).Or(mixed).selfMask()

    # Rangeland proxy from IGBP classes:
    # 6 = Closed Shrublands, 7 = Open Shrublands, 8 = Woody Savannas,
    # 9 = Savannas, 10 = Grasslands.
    rangeland = (
        lc.eq(6)  # shrubland
        .Or(lc.eq(7))
        .Or(lc.eq(8))
        .Or(lc.eq(9))
        .Or(lc.eq(10))  # grassland
        .selfMask()
    )

    # Reproject masks to NDVI projection so masking is spatially consistent.
    ndvi_projection = ndvi_mean.projection()
    agri = agri.reproject(ndvi_projection)
    evergreen = evergreen.reproject(ndvi_projection)
    deciduous = deciduous.reproject(ndvi_projection)
    mixed = mixed.reproject(ndvi_projection)
    forest = forest.reproject(ndvi_projection)
    rangeland = rangeland.reproject(ndvi_projection)

    ndvi_agri = ndvi_mean.updateMask(agri).rename("ndvi_agri_mean")
    ndvi_forest = ndvi_mean.updateMask(forest).rename("ndvi_forest_mean")
    ndvi_decid = ndvi_mean.updateMask(deciduous).rename("ndvi_deciduous_mean")
    ndvi_evergreen = ndvi_mean.updateMask(evergreen).rename("ndvi_evergreen_mean")
    ndvi_mixed = ndvi_mean.updateMask(mixed).rename("ndvi_mixed_mean")
    ndvi_range = ndvi_mean.updateMask(rangeland).rename("ndvi_rangeland_mean")

    return ee.Image.cat(
        [
            ndvi_agri,
            ndvi_forest,
            ndvi_decid,
            ndvi_evergreen,
            ndvi_mixed,
            ndvi_range,
        ]
    ).clip(county_geometry)


def build_water_metrics(
    county_geometry: ee.Geometry,
    start: ee.Date,
    end: ee.Date,
    water_occurrence_threshold: float,
) -> ee.Image:
    """Build summer water-related metrics: LST day/night and chlorophyll proxy from Sentinel-2 NDCI."""
    # JRC occurrence is used as an inland/open-surface-water mask in county polygons.
    # binary water-only mask to differentiate between water and non-water pixels
    # thus, no land will be included in LST day and night columns
    # a lower threshold corresponds to more inclusivity and higher leans towards permanent water bodies
    water_mask = ee.Image(JRC_SURFACE_WATER).select("occurrence").gte(water_occurrence_threshold).selfMask()

    lst_collection = (
        ee.ImageCollection(MODIS_LST)
        .filterBounds(county_geometry)
        .filterDate(start, end)
        .select(["LST_Day_1km", "LST_Night_1km"])
    )
    # MOD11A2 scale factor is 0.02 Kelvin; convert to Celsius for export.
    lst_day_raw = safe_mean_single_band(lst_collection.select("LST_Day_1km"), "LST_Day_1km")
    lst_night_raw = safe_mean_single_band(lst_collection.select("LST_Night_1km"), "LST_Night_1km")
    lst_day_c = lst_day_raw.multiply(0.02).subtract(273.15).rename("water_lst_day_c")
    lst_night_c = lst_night_raw.multiply(0.02).subtract(273.15).rename("water_lst_night_c")

    # Chlorophyll estimation from Sentinel-2 NDCI:
    # NDCI = (B5 - B4) / (B5 + B4), computed per image then summer-averaged.
    ndci_collection = (
        ee.ImageCollection(SENTINEL2_SR)
        .filterBounds(county_geometry)
        .filterDate(start, end)
        .map(mask_sentinel2_ndci)
    )
    ndci_mean = safe_mean_single_band(ndci_collection, "water_chlorophyll_a")

    return ee.Image.cat(
        [
            lst_day_c.updateMask(water_mask),
            lst_night_c.updateMask(water_mask),
            ndci_mean.updateMask(water_mask),
        ]
    ).clip(county_geometry)


def build_year_metrics(
    year: int,
    counties: ee.FeatureCollection,
    fips_property: str,
    water_occurrence_threshold: float,
    scale: int,
    tile_scale: int,
) -> ee.FeatureCollection:
    """Compute county-level means for all requested metrics in one year."""
    start, end = summer_window(year)
    county_geometry = counties.geometry()

    ndvi_mean = get_summer_ndvi_mean(county_geometry, start, end)
    ndvi_cover_metrics = build_ndvi_cover_metrics(year, county_geometry, ndvi_mean)
    water_metrics = build_water_metrics(county_geometry, start, end, water_occurrence_threshold)
    daymet_metrics = build_daymet_metrics(county_geometry, start, end)
    era5_metrics = build_era5_metrics(county_geometry, start, end)

    metrics_image = ee.Image.cat([ndvi_cover_metrics, water_metrics, daymet_metrics, era5_metrics])

    # Compute mean/min/max for each band in one pass.
    reducer = ee.Reducer.mean().combine( 
        reducer2=ee.Reducer.minMax(), # returns "min" and "max"
        sharedInputs=True,
    )

    reduced = metrics_image.reduceRegions(
        collection=counties,
        reducer=reducer,
        scale=scale,
        tileScale=tile_scale,
    )

    def _append_year_and_fill_missing(feature: ee.Feature) -> ee.Feature:
        # Force all metric columns to exist in every row.
        # Missing or null values are filled with -9999 (sentinel) so columns are not dropped.
        # A value can be missing when a county has no valid pixels for a given metric
        # (for example no land-cover class pixels, no inland water pixels, no NDCI coverage,
        # no DAYMET pixels after masking/filtering, or no ERA5 monthly coverage).
        property_names = feature.propertyNames()

        def _filled_value_from_candidates(candidates: list[str]):
            value = -9999
            for candidate in candidates:
                has_property = property_names.contains(candidate)
                raw_value = feature.get(candidate)
                value = ee.Algorithms.If(
                    has_property,
                    ee.Algorithms.If(ee.Algorithms.IsEqual(raw_value, None), -9999, raw_value),
                    value,
                )
            return value

        filled_metrics = {}
        for base_name in BASE_METRIC_COLUMNS:
            # Combined reducers usually emit "<band>_mean", "<band>_min", "<band>_max".
            # Keep backward-compatible mean column names while also writing min/max columns.
            filled_metrics[base_name] = _filled_value_from_candidates(
                [base_name, f"{base_name}_mean"]
            )
            filled_metrics[f"{base_name}_min"] = _filled_value_from_candidates(
                [f"{base_name}_min"]
            )
            filled_metrics[f"{base_name}_max"] = _filled_value_from_candidates(
                [f"{base_name}_max"]
            )

        geoid = ee.String(feature.get(fips_property))
        return feature.set(filled_metrics).set({"GEOID": geoid, "year": year})

    return reduced.map(_append_year_and_fill_missing)


def build_all_year_metrics(
    counties: ee.FeatureCollection,
    fips_property: str,
    start_year: int,
    end_year: int,
    water_occurrence_threshold: float,
    scale: int,
    tile_scale: int,
) -> ee.FeatureCollection:
    """Merge yearly county metrics into one FeatureCollection."""
    merged = ee.FeatureCollection([])
    for year in range(start_year, end_year + 1):
        yearly_fc = build_year_metrics(
            year=year,
            counties=counties,
            fips_property=fips_property,
            water_occurrence_threshold=water_occurrence_threshold,
            scale=scale,
            tile_scale=tile_scale,
        )
        merged = merged.merge(yearly_fc)
    return merged

# rather than converting to CSV later, I can just save a CSV directly onto Google Drive.
def start_drive_export(collection: ee.FeatureCollection, folder: str, prefix: str) -> ee.batch.Task:
    """Start a CSV export task to Google Drive."""
    selectors = EXPORT_BASE_COLUMNS + REQUIRED_METRIC_COLUMNS + ["year", ".geo"]
    task = ee.batch.Export.table.toDrive(
        collection=collection,
        description=prefix,
        folder=folder,
        fileNamePrefix=prefix,
        fileFormat="CSV",
        selectors=selectors,
    )
    task.start()
    return task


def main() -> None:
    args = parse_args()
    initialize_ee(project=args.project)

    test_fips = split_csv_values(args.test_fips)

    counties = load_counties(
        asset_id=args.county_asset_id,
        fips_property=args.fips_property,
        test_fips=test_fips,
        test_limit=args.test_limit,
    )

    metrics_fc = build_all_year_metrics(
        counties=counties,
        fips_property=args.fips_property,
        start_year=args.start_year,
        end_year=args.end_year,
        water_occurrence_threshold=args.water_occurrence_threshold,
        scale=args.scale,
        tile_scale=args.tile_scale,
    )

    # if args include any testing then the Google Drive save path suffix is _test
    if test_fips or args.test_limit:
        suffix = "_test"
    else:
        suffix = ""

    export_prefix = f"{args.export_prefix}{suffix}"
    task = start_drive_export(metrics_fc, folder=args.export_folder, prefix=export_prefix)

    print("Started Earth Engine export task")
    print(f"Task ID: {task.id}")
    print(f"Description: {export_prefix}")
    print(f"Drive folder: {args.export_folder}")

# example usage - in terminal (powershell):
# python .\gee_county_summer_metrics.py --test-fips 17031, 17019 --start-year 2022 --end-year 2022
if __name__ == "__main__":
    main()
