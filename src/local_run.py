import sys, os
import pandas as pd

from etl.extract import fetch_from_csv, save_raw_local, fetch_openweather_from_env
from etl.transform import normalize_weather, validate_weather
from etl.load import save_processed_local_parquet, save_processed_s3_parquet


if __name__ == "__main__":
    mode = (sys.argv[1] if len(sys.argv) > 1 else "csv").lower()

    if mode == "api":
        # 1) Extract (보통 하루치)
        df = fetch_openweather_from_env()
        date_str = str(df.iloc[0]["date"])

        # raw 저장 (원본 그대로 하루치)
        raw_path = save_raw_local(df, date_str=date_str)
        print("saved raw:", raw_path, "| rows:", len(df))

        # 2) Transform
        df_t = normalize_weather(df)
        rep = validate_weather(df_t)
        print("transform report:", rep)

        # 3) Load processed (하루치 단일 파티션)
        pq_local = save_processed_local_parquet(df_t, date_str=date_str)
        print("saved processed local:", pq_local)

        # 4) S3 upload
        if os.getenv("PROCESSED_BUCKET"):
            uri = save_processed_s3_parquet(
                df_t,
                date_str=date_str,
                bucket=os.getenv("PROCESSED_BUCKET"),
                prefix=os.getenv("PROCESSED_PREFIX", "processed"),
            )
            print("uploaded processed:", uri)

    else:
        # 1) Extract (여러 날짜 포함 가능)
        df = fetch_from_csv()

        # 2) Transform
        df_t = normalize_weather(df)
        rep = validate_weather(df_t)
        print("transform report:", rep)

        # 3) Load processed (날짜별 파티션으로 분할 저장)
        for d, df_day in df_t.groupby("date"):
            date_str = pd.to_datetime(d).strftime("%Y-%m-%d")

            pq_local = save_processed_local_parquet(df_day, date_str=date_str)
            print("saved processed local:", pq_local)

            # 4) S3 업로드
            if os.getenv("PROCESSED_BUCKET"):
                uri = save_processed_s3_parquet(
                    df_day,
                    date_str=date_str,
                    bucket=os.getenv("PROCESSED_BUCKET"),
                    prefix=os.getenv("PROCESSED_PREFIX", "processed"),
                )
                print("uploaded processed:", uri)

        
'''
# S3 upload
if os.getenv("RAW_BUCKET"):
    from etl.extract import save_raw_s3
    uri = save_raw_s3(df, date_str, bucket=os.getenv("RAW_BUCKET"), prefix=os.getenv("RAW_PREFIX", "raw"))
    print("uproaded:", uri)
'''

    