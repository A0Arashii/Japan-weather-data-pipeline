import os
import pytest
from src.etl.extract import fetch_openweather

@pytest.mark.skipif(not os.getenv("OPENWEATHER_API_KEY"), reason="no api key")

def test_fetch_openweather_geocoded():
    df = fetch_openweather(["Tokyo,JP"])
    assert not df.empty
    assert {"date","city","temp_c"}.issubset(set(map(str.lower, df.columns)))