from etl.extract import fetch_from_csv, save_raw_local

if __name__ == "__main__":
    df = fetch_from_csv()
    out = save_raw_local(df, date_str="2025-08-20")
    print("saved to: ", out, "| rows:", len(df))
    