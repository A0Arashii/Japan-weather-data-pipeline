import os, io, csv, time, json
from datetime import datetime, timezone, timedelta
import requests
import boto3

S3_BUCKET = os.getenv("RAW_BUCKET", "iseunggi-weather-pipeline")
S3_PREFIX = os.getenv("RAW_PREFIX", "raw")
API_KEY   = os.getenv("OPENWEATHER_API_KEY")  # 콘솔의 환경변수에서 설정
CITIES    = [c.strip() for c in os.getenv("DEFAULT_CITIES", "tokyo,osaka,sapporo").split(",") if c.strip()]

s3 = boto3.client("s3")

def _jst_today_str():
    jst = timezone(timedelta(hours=9))
    return datetime.now(jst).date().isoformat()

def fetch_openweather(cities):
    assert API_KEY, "OPENWEATHER_API_KEY not set"
    rows = []
    for city in cities:
        url = "https://api.openweathermap.org/data/2.5/weather"
        params = {"q": city, "appid": API_KEY, "units": "metric"}
        r = requests.get(url, params=params, timeout=10)
        r.raise_for_status()
        j = r.json()
        temp_c   = j.get("main", {}).get("temp")
        humidity = j.get("main", {}).get("humidity")
        precip   = 0.0
        if "rain" in j:
            precip = j["rain"].get("1h") or j["rain"].get("3h") or 0.0
        rows.append({"city": city, "temp_c": temp_c, "precip_mm": precip, "humidity": humidity})
        time.sleep(0.5)  # 무료 플랜 속도 제한 대비
    return rows

def lambda_handler(event, context):
    date_str = _jst_today_str()
    rows = fetch_openweather(CITIES)

    # CSV in-memory
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=["date","city","temp_c","precip_mm","humidity"])
    writer.writeheader()
    for r in rows:
        writer.writerow({"date": date_str, **r})

    key = f"{S3_PREFIX}/date={date_str}/weather_raw.csv"
    s3.put_object(Bucket=S3_BUCKET, Key=key, Body=buf.getvalue().encode("utf-8"))

    return {
        "statusCode": 200,
        "body": json.dumps({"saved": f"s3://{S3_BUCKET}/{key}", "rows": len(rows)})
    }


