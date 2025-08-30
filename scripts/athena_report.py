import os, time, io
import boto3
import pandas as pd
import matplotlib.pyplot as plt

ATHENA_DB = os.getenv("ATHENA_DB", "weather")
OUTPUT = os.getenv("ATHENA_OUTPUT", "s3://iseunggi-weather-pipeline/athena-results/")
S3_OUTPUT_BUCKET = OUTPUT.split("/")[2]
S3_OUTPUT_PREFIX = "/".join(OUTPUT.split("/")[3:]).rstrip("/")  

REGION = os.getenv("AWS_REGION", "ap-northeast-1")
athena = boto3.client("athena", region_name=REGION)
s3 = boto3.client("s3")

def run_athena(sql: str) -> pd.DataFrame:
    qid = athena.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={"Database": ATHENA_DB},
        ResultConfiguration={"OutputLocation": OUTPUT},
    )["QueryExecutionId"]
    while True:
        st = athena.get_query_execution(QueryExecutionId=qid)["QueryExecution"]["Status"]["State"]
        if st in ("SUCCEEDED", "FAILED", "CANCELLED"):
            if st != "SUCCEEDED":
                raise RuntimeError(f"Athena status={st}")
            break
        time.sleep(2)
    key = f"{S3_OUTPUT_PREFIX}/{qid}.csv"
    obj = s3.get_object(Bucket=S3_OUTPUT_BUCKET, Key=key)
    return pd.read_csv(io.BytesIO(obj["Body"].read()))

def ensure_dirs():
    os.makedirs("docs/images", exist_ok=True)

def main():
    ensure_dirs()
    # 날짜별 평균 기온
    q1 = """
    SELECT date, AVG(temp_c) AS avg_temp
    FROM processed_weather
    GROUP BY date
    ORDER BY date
    """
    df1 = run_athena(q1)
    # 도시별 평균 기온(최근 7일)
    q2 = """
    SELECT city, AVG(temp_c) AS avg_temp
    FROM processed_weather
    WHERE date >= date_add('day', -7, current_date)
    GROUP BY city
    ORDER BY city
    """
    df2 = run_athena(q2)

    # plot 1
    plt.figure()
    try:
        df1["date"] = pd.to_datetime(df1["date"])
    except: pass
    df1.sort_values("date").plot(x="date", y="avg_temp", marker='o', legend=False)
    plt.title("Average Temperature by Date")
    plt.xlabel("Date"); plt.ylabel("Temperature (°C)")
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig("docs/images/avg_temp_by_date.png")
    plt.close()

    # plot 2
    plt.figure()
    df2.sort_values("city").plot(x="city", y="avg_temp", kind="bar", legend=False)
    plt.title("Average Temperature by City (Last 7 days)")
    plt.ylabel("Temperature (°C)")
    plt.tight_layout()
    plt.savefig("docs/images/avg_temp_by_city.png")
    plt.close()

if __name__ == "__main__":
    main()
