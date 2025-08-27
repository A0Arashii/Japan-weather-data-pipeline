from __future__ import annotations
import os, io, boto3
from pathlib import Path
import pandas  as pd


def save_processed_local_parquet(df: pd.DataFrame, date_str: str, outdir: str = "data/processed") -> str:
    '''
    processed data -> local save with Parquet (Hive-style partition directory)
    ex: data/processed/date=2025-08-25/part-0000.parquet
    '''
    out_dir = Path(outdir) / f"date={date_str}"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "part-0000.parquet"
    df.to_parquet(out_path, index=False, engine="pyarrow") # 압축 기본 snappy
    return str(out_path)

def save_processed_s3_parquet(df: pd.DataFrame, date_str: str, bucket: str, prefix: str = "processed") -> str:
    """
    write processed data with parquet in memory and upload to s3
    ex: s3://<bucket>/processed/date=2025-08-25/part-0000.parquet
    """
    import io
    buf = io.BytesIO()
    df.to_parquet(buf, index=False, engine="pyarrow")
    key = f"{prefix}/date={date_str}/part-0000.parquet"

    s3 = boto3.client("s3")
    s3.put_object(Bucket=bucket, Key=key, Body=buf.getvalue())
    return f"s3://{bucket}/{key}"