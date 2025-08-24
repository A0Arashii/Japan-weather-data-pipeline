from src.etl.extract import fetch_from_csv

def test_fetch_from_csv_returns_rows():
    df = fetch_from_csv()
    assert not df.empty   # verification point 1
    assert {"date", "city", "temp_c"}.issubset(set(map(str.lower, df.columns))) # verification point 2

# .issubset : 왼쪽 집합이 오른쪽 집합의 부분집합인가?