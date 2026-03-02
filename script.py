from __future__ import annotations

import argparse
import os
import random
import threading
import time
from collections import deque
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Deque, Optional

import pandas as pd
import requests
from tqdm import tqdm

API_URL = "https://archive-api.open-meteo.com/v1/archive"


@dataclass
class RateLimitConfig:
    max_calls_per_minute: int = 450
    max_calls_per_hour: int = 4500


class SlidingWindowRateLimiter:
    """Thread-safe limiter with independent 60s and 3600s windows."""

    def __init__(self, config: RateLimitConfig) -> None:
        self.config = config
        self._lock = threading.Lock()
        self._calls_1m: Deque[float] = deque()
        self._calls_1h: Deque[float] = deque()

    def acquire(self) -> None:
        while True:
            sleep_for = 0.0
            now = time.monotonic()

            with self._lock:
                self._evict(now)

                over_1m = len(self._calls_1m) >= self.config.max_calls_per_minute
                over_1h = len(self._calls_1h) >= self.config.max_calls_per_hour

                if not over_1m and not over_1h:
                    self._calls_1m.append(now)
                    self._calls_1h.append(now)
                    return

                if over_1m:
                    next_1m = 60.0 - (now - self._calls_1m[0])
                    sleep_for = max(sleep_for, next_1m)

                if over_1h:
                    next_1h = 3600.0 - (now - self._calls_1h[0])
                    sleep_for = max(sleep_for, next_1h)

            time.sleep(max(sleep_for, 0.01))

    def _evict(self, now: float) -> None:
        while self._calls_1m and now - self._calls_1m[0] >= 60.0:
            self._calls_1m.popleft()
        while self._calls_1h and now - self._calls_1h[0] >= 3600.0:
            self._calls_1h.popleft()


_tls = threading.local()


def get_session() -> requests.Session:
    session = getattr(_tls, "session", None)
    if session is None:
        session = requests.Session()
        adapter = requests.adapters.HTTPAdapter(pool_connections=64, pool_maxsize=64)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        _tls.session = session
    return session


def parse_retry_after_seconds(response: requests.Response) -> Optional[float]:
    value = response.headers.get("Retry-After")
    if not value:
        return None
    try:
        return float(value)
    except ValueError:
        return None


def request_with_backoff(
    params: dict,
    limiter: SlidingWindowRateLimiter,
    timeout_seconds: int = 60,
    max_retries: int = 8,
    base_wait_seconds: float = 3.0,
) -> dict:
    """Rate-limited request with 429-aware exponential backoff + jitter."""
    for attempt in range(max_retries):
        limiter.acquire()

        session = get_session()
        response = session.get(API_URL, params=params, timeout=timeout_seconds)

        if response.status_code == 429:
            retry_after = parse_retry_after_seconds(response)
            exp_wait = min(120.0, base_wait_seconds * (2**attempt))
            wait = (retry_after if retry_after is not None else exp_wait) + random.uniform(0.0, 1.5)
            print(f"Rate limited (429). Waiting {wait:.1f}s then retrying...")
            time.sleep(wait)
            continue

        response.raise_for_status()
        payload = response.json()

        if isinstance(payload, dict) and payload.get("error"):
            reason = str(payload.get("reason", "")).lower()
            if "limit" in reason and "exceed" in reason:
                exp_wait = min(120.0, base_wait_seconds * (2**attempt))
                wait = exp_wait + random.uniform(0.0, 1.5)
                print(f"Rate limited ({payload.get('reason')}). Waiting {wait:.1f}s then retrying...")
                time.sleep(wait)
                continue

        return payload

    raise RuntimeError(f"Rate limit retries exhausted after {max_retries} attempts")


def filter_and_aggregate_by_year(data: dict, year: int, county_id: str, lat: float, lon: float) -> pd.DataFrame:
    start_date = f"{year}-07-01"
    end_date = f"{year}-09-30"

    aggregated = {
        "GEOID": county_id,
        "year": year,
        "latitude": lat,
        "longitude": lon,
    }

    daily = data.get("daily")
    if daily:
        daily_df = pd.DataFrame(daily)
        daily_df["time"] = pd.to_datetime(daily_df["time"])
        mask = (daily_df["time"] >= start_date) & (daily_df["time"] <= end_date)
        filtered = daily_df.loc[mask]

        aggregated["mean_temp_2m_max"] = filtered["temperature_2m_max"].mean() if not filtered.empty else None
        aggregated["mean_temp_2m_min"] = filtered["temperature_2m_min"].mean() if not filtered.empty else None
    else:
        aggregated["mean_temp_2m_max"] = None
        aggregated["mean_temp_2m_min"] = None

    hourly = data.get("hourly")
    if hourly:
        hourly_df = pd.DataFrame(hourly)
        hourly_df["time"] = pd.to_datetime(hourly_df["time"])
        mask = (hourly_df["time"] >= start_date) & (hourly_df["time"] <= end_date)
        filtered = hourly_df.loc[mask].copy()

        if not filtered.empty:
            filtered["date"] = filtered["time"].dt.date
            daily_agg = (
                filtered.groupby("date", as_index=False)
                .agg(
                    {
                        "relative_humidity_2m": "mean",
                        "wind_speed_10m": "mean",
                        "precipitation": "sum",
                    }
                )
            )
            aggregated["mean_humidity_2m"] = daily_agg["relative_humidity_2m"].mean()
            aggregated["mean_wind_speed_10m"] = daily_agg["wind_speed_10m"].mean()
            aggregated["mean_precipitation"] = daily_agg["precipitation"].mean()
        else:
            aggregated["mean_humidity_2m"] = None
            aggregated["mean_wind_speed_10m"] = None
            aggregated["mean_precipitation"] = None
    else:
        aggregated["mean_humidity_2m"] = None
        aggregated["mean_wind_speed_10m"] = None
        aggregated["mean_precipitation"] = None

    return pd.DataFrame([aggregated])


def get_missing_counties(counties_df: pd.DataFrame, years: range, save_dir: str) -> pd.DataFrame:
    missing_rows = []
    for _, row in counties_df.iterrows():
        county_id = str(row["GEOID"])
        missing = False
        for year in years:
            filename = os.path.join(save_dir, f"county_{county_id}_year_{year}.parquet")
            if not os.path.exists(filename):
                missing = True
                break
        if missing:
            missing_rows.append(row)

    return pd.DataFrame(missing_rows) if missing_rows else pd.DataFrame(columns=counties_df.columns)


def fetch_county_all_years(
    row: pd.Series,
    years: range,
    save_dir: str,
    limiter: SlidingWindowRateLimiter,
) -> Optional[pd.DataFrame]:
    county_id = str(row["GEOID"])
    lat = float(row["latitude"])
    lon = float(row["longitude"])

    if not (-90.0 <= lat <= 90.0) or not (-180.0 <= lon <= 180.0):
        print(f"Skipping county {county_id}: invalid lat/lon ({lat}, {lon})")
        return None

    cached_results = []
    fully_cached = True

    for year in years:
        filename = os.path.join(save_dir, f"county_{county_id}_year_{year}.parquet")
        if os.path.exists(filename):
            try:
                cached_results.append(pd.read_parquet(filename))
            except Exception:
                fully_cached = False
                break
        else:
            fully_cached = False
            break

    if fully_cached and cached_results:
        return pd.concat(cached_results, ignore_index=True)

    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": f"{min(years)}-07-01",
        "end_date": f"{max(years)}-09-30",
        "daily": ["temperature_2m_max", "temperature_2m_min"],
        "hourly": ["relative_humidity_2m", "wind_speed_10m", "precipitation"],
        "timezone": "auto",
    }

    try:
        payload = request_with_backoff(params=params, limiter=limiter)
        if "hourly" not in payload and "daily" not in payload:
            raise ValueError(f"No weather data returned for county {county_id}")

        os.makedirs(save_dir, exist_ok=True)
        out = []

        for year in years:
            year_df = filter_and_aggregate_by_year(payload, year, county_id, lat, lon)
            filename = os.path.join(save_dir, f"county_{county_id}_year_{year}.parquet")
            year_df.to_parquet(filename, index=False)
            out.append(year_df)

        return pd.concat(out, ignore_index=True)

    except requests.exceptions.Timeout:
        print(f"Timeout for county {county_id} at ({lat}, {lon})")
        return None
    except requests.exceptions.RequestException as exc:
        print(f"Request failed for county {county_id}: {exc}")
        return None
    except Exception as exc:
        print(f"Unexpected error for county {county_id}: {exc}")
        return None


def load_all_cached(counties_df: pd.DataFrame, years: range, save_dir: str) -> pd.DataFrame:
    all_results = []

    for _, row in tqdm(counties_df.iterrows(), total=len(counties_df), desc="Loading cached data"):
        county_id = str(row["GEOID"])
        yearly_rows = []
        for year in years:
            filename = os.path.join(save_dir, f"county_{county_id}_year_{year}.parquet")
            if os.path.exists(filename):
                try:
                    yearly_rows.append(pd.read_parquet(filename))
                except Exception as exc:
                    print(f"Failed to load cache for county {county_id}, year {year}: {exc}")

        if yearly_rows:
            all_results.append(pd.concat(yearly_rows, ignore_index=True))

    if not all_results:
        return pd.DataFrame()

    final_df = pd.concat(all_results, ignore_index=True)
    final_df = final_df.sort_values(["GEOID", "year"]).reset_index(drop=True)
    return final_df[
        [
            "GEOID",
            "mean_temp_2m_max",
            "mean_temp_2m_min",
            "mean_humidity_2m",
            "mean_wind_speed_10m",
            "mean_precipitation",
            "year",
        ]
    ]


def fetch_all_weather_data(
    counties_df: pd.DataFrame,
    years: range = range(2017, 2025),
    max_workers: int = 2,
    save_dir: str = "weather_data",
    max_calls_per_minute: int = 450,
    max_calls_per_hour: int = 4500,
) -> pd.DataFrame:
    counties_df = counties_df[["GEOID", "latitude", "longitude"]].drop_duplicates().reset_index(drop=True)

    print(f"Total counties: {len(counties_df)}")
    print(f"Years: {list(years)}")
    print(f"Workers: {max_workers}")
    print(f"Rate limit target: {max_calls_per_minute}/min, {max_calls_per_hour}/hour")

    os.makedirs(save_dir, exist_ok=True)
    missing = get_missing_counties(counties_df, years, save_dir)
    print(f"Counties needing fetch: {len(missing)}/{len(counties_df)}")

    limiter = SlidingWindowRateLimiter(
        RateLimitConfig(
            max_calls_per_minute=max_calls_per_minute,
            max_calls_per_hour=max_calls_per_hour,
        )
    )

    failed = []

    if not missing.empty:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(fetch_county_all_years, row, years, save_dir, limiter): str(row["GEOID"])
                for _, row in missing.iterrows()
            }

            for future in tqdm(as_completed(futures), total=len(futures), desc="Fetching counties"):
                county_id = futures[future]
                try:
                    result = future.result()
                    if result is None or result.empty:
                        failed.append(county_id)
                except Exception as exc:
                    print(f"Task failed for county {county_id}: {exc}")
                    failed.append(county_id)

    weather_df = load_all_cached(counties_df, years, save_dir)

    if weather_df.empty:
        print("No data retrieved.")
        return weather_df

    print("\nSummary")
    print(f"Rows: {len(weather_df)}")
    print(f"Unique counties: {weather_df['GEOID'].nunique()}")
    print(f"Years covered: {sorted(weather_df['year'].unique())}")
    print(f"Failed counties: {len(set(failed))}")

    if failed:
        failed_unique = sorted(set(failed))
        print(f"Failed county GEOIDs (sample): {failed_unique[:10]}{'...' if len(failed_unique) > 10 else ''}")

    missing_data = weather_df.isnull().sum()
    missing_data = missing_data[missing_data > 0]
    if not missing_data.empty:
        print("\nMissing values per column:")
        print(missing_data)

    return weather_df


def save_weather_summary(weather_df: pd.DataFrame, output_path: str = "county_weather_2017_2024.csv") -> None:
    weather_df.to_csv(output_path, index=False)
    print(f"Weather data saved to: {output_path}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch and cache county weather from Open-Meteo archive API.")
    parser.add_argument("--counties-csv", required=True, help="CSV with GEOID, latitude, longitude columns")
    parser.add_argument("--save-dir", default="weather_data")
    parser.add_argument("--output-csv", default="county_weather_2017_2024.csv")
    parser.add_argument("--start-year", type=int, default=2017)
    parser.add_argument("--end-year", type=int, default=2024)
    parser.add_argument("--max-workers", type=int, default=2)
    parser.add_argument("--max-calls-per-minute", type=int, default=450)
    parser.add_argument("--max-calls-per-hour", type=int, default=4500)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    years = range(args.start_year, args.end_year + 1)

    counties_df = pd.read_csv(args.counties_csv)
    missing_cols = {"GEOID", "latitude", "longitude"} - set(counties_df.columns)
    if missing_cols:
        raise ValueError(f"Missing required columns in counties CSV: {sorted(missing_cols)}")

    weather_df = fetch_all_weather_data(
        counties_df=counties_df,
        years=years,
        max_workers=args.max_workers,
        save_dir=args.save_dir,
        max_calls_per_minute=args.max_calls_per_minute,
        max_calls_per_hour=args.max_calls_per_hour,
    )

    if not weather_df.empty:
        save_weather_summary(weather_df, output_path=args.output_csv)


if __name__ == "__main__":
    main()
