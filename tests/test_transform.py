import pandas as pd
from src.etl.transform import normalize_weather, validate_weather

def test_normalize_weather_types_and_columns():
    df = pd.DataFrame([{
        "date": "2025-08-25",
        "city": " Tokyo,JP ",
        "temp_c": "30.2",
        "humidity": "65",
        "precip_mm": None,
    }])

    out = normalize_weather(df)

    # 칼럼 존재/순서
    assert list(out.columns) == ["date","city","temp_c","precip_mm","humidity"]

    # type
    assert pd.api.types.is_datetime64_any_dtype(out["date"])
    assert pd.api.types.is_float_dtype(out["temp_c"])
    assert pd.api.types.is_float_dtype(out["precip_mm"]) or pd.api.types.is_integer_dtype(out["precip_mm"])

    # value
    assert out.loc[0, "city"] == "Tokyo,JP"
    assert out.loc[0, "precip_mm"] == 0.0

def test_validate_weather_basic():
    df = pd.DataFrame([{
        "date": "2025-08-25",
        "city": "Tokyo,JP",
        "temp_c": 30.0,
        "precip_mm":0.0,
        "humidity": 60
    }])
    rep = validate_weather(df)
    assert rep["rows"] == 1
    assert set(rep["null_counts"].keys()) == {
        "date",
        "city",
        "temp_c",
        "precip_mm",
        "humidity"
    }