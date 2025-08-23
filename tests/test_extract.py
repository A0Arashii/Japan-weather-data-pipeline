from src.etl.extract import fetch_from_csv

def test_fetch_from_csv_returns_rows():
    df = fetch_from_csv()
    assert not df.empty()
    assert {"data", "city", "temp_c"}.issubset(set(map(str.lower, df.columns)))

    