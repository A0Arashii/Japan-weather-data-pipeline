# scripts/athena_report.py
import os, time, io
import boto3
import pandas as pd
import matplotlib
matplotlib.use("Agg")  # CI(헤드리스)에서 안전하게
import matplotlib.pyplot as plt

REGION = os.getenv("AWS_REGION", "ap-northeast-1")
ATHENA_DB = os.getenv("ATHENA_DB", "weather")
WORKGROUP = os.getenv("ATHENA_WORKGROUP", "primary")
OUTPUT = os.getenv("ATHENA_OUTPUT", "s3://iseunggi-weather-pipeline/athena-results/")

# S3 경로 파싱 (항상 끝에 / 유지)
if not OUTPUT.endswith("/"):
    OUTPUT += "/"
S3_OUTPUT_BUCKET = OUTPUT.split("/")[2]
S3_OUTPUT_PREFIX = "/".join(OUTPUT.split("/")[3:]).rstrip("/")

athena = boto3.client("athena", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)

def run_athena(sql: str) -> pd.DataFrame:
    kwargs = {
        "QueryString": sql,
        "QueryExecutionContext": {"Database": ATHENA_DB},
        "WorkGroup": WORKGROUP,
        "ResultConfiguration": {"OutputLocation": OUTPUT},
    }
    qid = athena.start_query_execution(**kwargs)["QueryExecutionId"]

    # 완료까지 폴링
    while True:
        exec_info = athena.get_query_execution(QueryExecutionId=qid)["QueryExecution"]
        st = exec_info["Status"]["State"]
        if st in ("SUCCEEDED", "FAILED", "CANCELLED"):
            if st != "SUCCEEDED":
                reason = exec_info["Status"].get("StateChangeReason", "(no reason)")
                print(f"[Athena FAILED] QueryExecutionId={qid} reason={reason}")
                raise RuntimeError(f"Athena status={st}: {reason}")
            break
        time.sleep(2)

    # 결과 csv 다운로드
    key = f"{S3_OUTPUT_PREFIX}/{qid}.csv"
    obj = s3.get_object(Bucket=S3_OUTPUT_BUCKET, Key=key)
    return pd.read_csv(io.BytesIO(obj["Body"].read()))

def ensure_dirs():
    os.makedirs("docs/images", exist_ok=True)

def main():
    ensure_dirs()

    # 1) 날짜별 평균 기온 (타입 안전 캐스팅)
    q1 = """
    SELECT
      CAST(date AS DATE) AS d,
      AVG(CAST(temp_c AS DOUBLE)) AS avg_temp
    FROM processed_weather
    GROUP BY CAST(date AS DATE)
    ORDER BY d
    """
    df1 = run_athena(q1)

    # 2) 최근 7일 도시별 평균 (실패해도 리포트는 계속)
    df2 = None
    try:
        q2 = """
        SELECT
          city,
          AVG(CAST(temp_c AS DOUBLE)) AS avg_temp
        FROM processed_weather
        WHERE CAST(date AS DATE) >= date_add('day', -7, current_date)
        GROUP BY city
        ORDER BY city
        """
        df2 = run_athena(q2)
    except Exception as e:
        print("[WARN] city 7d chart skipped:", e)

    # Plot 1: 날짜별 평균
    plt.figure()
    # 쿼리에서 별칭을 d로 줬으니 그걸 사용
    df1 = df1.rename(columns={"d": "date"})
    df1["date"] = pd.to_datetime(df1["date"])
    df1.sort_values("date").plot(x="date", y="avg_temp", marker="o", legend=False)
    plt.title("Average Temperature by Date")
    plt.xlabel("Date"); plt.ylabel("Temperature (°C)")
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig("docs/images/avg_temp_by_date.png")
    plt.close()

    # Plot 2: 도시별 평균(최근 7일)
    if df2 is not None and not df2.empty:
        plt.figure()
        df2.sort_values("city").plot(x="city", y="avg_temp", kind="bar", legend=False)
        plt.title("Average Temperature by City (Last 7 days)")
        plt.ylabel("Temperature (°C)")
        plt.tight_layout()
        plt.savefig("docs/images/avg_temp_by_city.png")
        plt.close()
    else:
        print("[INFO] skip city chart: no data or previous step failed")

if __name__ == "__main__":
    main()

