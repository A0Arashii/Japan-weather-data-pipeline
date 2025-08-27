from etl.extract import fetch_from_csv, save_raw_local, fetch_openweather_from_env
from etl.transform import validate_weather, normalize_weather
from etl.load import save_processed_local_parquet, save_processed_s3_parquet
import sys, os

if __name__ == "__main__":
    mode = (sys.argv[1] if len(sys.argv) > 1 else "csv").lower()

    # 1) Extract
    if mode == "api":
        df = fetch_openweather_from_env()
        date_str = str(df.iloc[0]['date'])
    else:
        df = fetch_from_csv()
        date_str = "2025-08-20"

    out = save_raw_local(df, date_str=date_str)
    print("saved to:", out, "| rows:", len(df))

    # 2) Transform
    df_t = normalize_weather(df)
    rep = validate_weather(df_t)
    print("transform report:", rep)

    # 3) Load (Local Parquet)
    pq_local = save_processed_local_parquet(df_t, date_str=date_str)
    print("saved processed local:", pq_local)


    # 4) Load(S3 Parquet)
    if os.getenv("PROCESSED_BUCKET"):
        uri = save_processed_s3_parquet(
            df_t,
            date_str=date_str,
            bucket=os.getenv("PROCESSED_BUCKET"),
            prefix=os.getenv("PROCESSED_PREFIX", "processed")
        )
        print("uploaded processed:", uri)
        
'''
# S3 upload
if os.getenv("RAW_BUCKET"):
    from etl.extract import save_raw_s3
    uri = save_raw_s3(df, date_str, bucket=os.getenv("RAW_BUCKET"), prefix=os.getenv("RAW_PREFIX", "raw"))
    print("uproaded:", uri)
'''

    