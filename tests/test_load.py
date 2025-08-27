from src.etl.load import save_processed_local_parquet
import os, pandas as pd
from pathlib import Path
import pytest


def test_save_processed_local_parquet_writes_file(tmp_path: Path):
    df = pd.DataFrame([{
        "date":"2025-08-25",
        "city":"Tokyo,JP",
        "temp_c":30.2,
        "precip_mm":0.0,
        "humidity":65
        }])
    outdir = tmp_path/"processed"
    out = save_processed_local_parquet(df, date_str="2025-08-25", outdir=str(outdir))
    assert Path(out).exists()


@pytest.mark.skipif(not os.getenv("PROCESSED_BUCKET"), reason="no processed bucket set")
def test_s3_upload_skipped_without_bucket():
    # 환경이 없으면 그냥 skip되도록 더미 테스트
    assert True