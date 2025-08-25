from pathlib import Path
import pandas as pd
import os, time, requests
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[2]
load_dotenv()

OWM_BASE = "https://api.openweathermap.org"
API_KEY = os.getenv("OPENWEATHER_API_KEY")

_GEOCACHE: dict[str, tuple[float, float]] = {}

def fetch_from_csv(path: str | None = None) -> pd.DataFrame:
    csv_path = Path(path) if path else ROOT / "data" / "sample.csv"
    df = pd.read_csv(csv_path)
    return df

def save_raw_local(df: pd.DataFrame, date_str:str, outdir: str = "data/raw"):
    out_dir = ROOT / outdir / f"date={date_str}"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "weather_raw.csv"
    df.to_csv(out_path, index=False)
    return str(out_path)

def _geocode(city_country: str) -> tuple[float, float]:
    if city_country in _GEOCACHE:
        return _GEOCACHE[city_country]
    
    url = f"{OWM_BASE}/geo/1.0/direct"
    parameter = {"q": city_country,
                 "limit":1,
                 "appid": API_KEY
                }
    r = requests.get(url, params=parameter, timeout=10)
    r.raise_for_status() # requests.get 오류 발생시 HTTPerror출력, 성공시 그냥 넘어감

    arr = r.json()
    if not arr: # 내용은 json으로 파싱되었으나, 내용이 없는 경우
        raise ValueError(f"Geocoding not found: {city_country}")
    
    lat, lon = float(arr[0]["lat"]), float(arr[0]["lon"])
    _GEOCACHE[city_country] = (lat, lon)
    return lat, lon

def _fetch_weather_by_coords(lat: float, lon: float) -> dict:
    url = f"{OWM_BASE}/data/2.5/weather"
    parameter = {"lat": lat, 
                 "lon": lon,
                 'units': 'metric',
                 "appid": API_KEY
                }
    r = requests.get(url, params=parameter, timeout=10)
    r.raise_for_status()
    return r.json() # api응답을 파싱한 결과를 dict형태로 반환할 것


def fetch_openweather(cities: list[str], api_key: str | None=None) -> pd.DataFrame:
    key = api_key or API_KEY
    assert key, "OPENWEATHER_API_KEY not set"

    rows = []
    for city in cities:
        lat, lon = _geocode(city)

        j = _fetch_weather_by_coords(lat, lon)

        # extract only what we want
        temp_c      = j.get("main",{}).get("temp")
        humidity    = j.get("main",{}).get("humidity")
        rain        = j.get("rain", {}) or {}
        precip      = rain.get("1h") or rain.get("3h") or 0.0

        date_str = pd.Timestamp.utcnow().strftime("%Y-%m-%d")

        rows.append({
            "date": date_str,
            "city": city,
            "temp_c": temp_c,
            "precip_mm": float(precip),
            "humidity": humidity
        })

        time.sleep(1) # free plan rate guard
    
    return pd.DataFrame(rows)

def fetch_openweather_from_env() -> pd.DataFrame:
    cities = os.getenv("DEFAULT_CITIES", "Tokyo,JP,Osaka,JP").split(',')
    cities = [c.strip() for c in cities if c.strip()]
    return fetch_openweather(cities)


# ============================================================================
# AWS S3
# ============================================================================

import io
import boto3

def save_raw_s3(df: pd.DataFrame, date_str: str, bucket: str, prefix: str = "raw"):
    csv_buf = io.StringIO()
    df.to_csv(csv_buf, index=False)
    key = f"{prefix}/date={date_str}/weather_raw.csv"
    s3 = boto3.client("s3")
    s3.put_object(Bucket=bucket, Key=key, Body=csv_buf.getvalue().encode())
    return f"s3://{bucket}/{key}"


