from pathlib import Path
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]

def fetch_from_csv(path: str | None = None) -> pd.DataFrame:
    csv_path = Path(path) if path else ROOT / "data" / "sample.csv"
    df = pd.read_csv(csv_path)
    return df

def save_raw_local(df: pd.DataFrame, date_str:str, outdir: str = "data/raw"):
    out_dir = ROOT / outdir / f"date={date_str}"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "weather_raw.csv"
    df.to_csv(out_path, index=False)
    return str(out_path)

