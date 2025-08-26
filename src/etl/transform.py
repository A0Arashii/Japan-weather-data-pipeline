from __future__ import annotations
import pandas as pd

CANONICAL_COLS = ["date", "city","temp_c","precip_mm","humidity"]

def normalize_weather(df: pd.DataFrame) -> pd.DataFrame:
    '''
    raw -> canonical schema
    - 칼럼 존재 보장
    - 타입 변환: date -> datetime, 수치 -> float
    - 결측, 이상치 처리
    - city 표준화 (양 끝 공백 제거)
    '''
    out = df.copy()

    # make column if there is not what i want
    for col in CANONICAL_COLS:
        if col not in out.columns:
            out[col] = pd.NA 
    
    # type nomalize
    out["date"] = pd.to_datetime(out["date"], errors="coerce")
    out["city"] = out["city"].astype("string").str.strip()

    for num_col in ["temp_c","precip_mm","humidity"]:
        out[num_col] = pd.to_numeric(out[num_col], errors="coerce")

    # missing value/outlier processing
    out["precip_mm"] = out["precip_mm"].fillna(0.0)
    out.loc[(out["humidity"] < 0) | (out["humidity"] > 100), "humidity"] = pd.NA

    out = out[CANONICAL_COLS]

    return out

def validate_weather(df: pd.DataFrame) -> dict:
    report = {}
    report["rows"] = len(df)
    report["null_counts"] = df.isna().sum().to_dict()
    report["dtypes"] = {c: str(df[c].dtype) for c in df.columns}
    return report

