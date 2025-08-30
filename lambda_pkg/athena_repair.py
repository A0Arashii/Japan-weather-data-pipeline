import boto3
import os
import time

athena = boto3.client("athena")

def lambda_handler(event, context):
    db = os.getenv("ATHENA_DB", "weather")
    table = os.getenv("ATHENA_TABLE", "processed_weather")
    output = os.getenv("ATHENA_OUTPUT", "s3://iseunggi-weather-pipeline/athena-results/")

    query = f"MSCK REPAIR TABLE {db}.{table}"

    response = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={"Database": db},
        ResultConfiguration={"OutputLocation": output}
    )

    qid = response["QueryExecutionId"]
    print("Started Athena Repair Query:", qid)

    # 간단한 상태 확인 (60초 대기)
    for _ in range(12):  # 5초 × 12 = 60초
        status = athena.get_query_execution(QueryExecutionId=qid)["QueryExecution"]["Status"]["State"]
        if status in ("SUCCEEDED", "FAILED", "CANCELLED"):
            print("Final status:", status)
            return {"statusCode": 200, "state": status, "query_id": qid}
        time.sleep(5)

    return {"statusCode": 200, "state": "RUNNING", "query_id": qid}
