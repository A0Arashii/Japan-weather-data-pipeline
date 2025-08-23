# Japan-weather-data-pipeline (AWS S3 + Lambda + Athena)

**Goal**: Public weather data (JAPAN) -> S3 -> Lambda ETL -> Athena query -> simple analysis/plot.

## Architecture (MVP)
- S3: `s3://<bucket>/raw/`, `.../processed/` (partitioned by `date=YYYY-MM-DD/`)
- Lambda (Python): extract → transform (pandas) → load (Parquet)
- Athena (+ Glue Crawler optional): query processed table
- (Local) Matplotlib or console output for avg temperature

## Repo Structure
japan-weather-data-pipeline/
├─ README.md                # Project overview and setup
├─ LICENSE                 # MIT License
├─ .gitignore              # Python-specific ignores
├─ REQUIREMENTS.txt         # Python dependencies
├─ infrastructure/          # (Optional) IaC for SAM/CloudFormation/Terraform
│  └─ README.md
├─ src/                    # Core ETL logic
│  ├─ etl/
│  │  ├─ extract.py       # Fetch weather data
│  │  ├─ transform.py     # Process data with Pandas
│  │  └─ load.py          # Save to S3 as Parquet
│  ├─ lambda_handler.py   # Lambda entry point
│  └─ local_run.py        # Local testing script
├─ tests/                  # Unit tests
│  ├─ test_extract.py
│  ├─ test_transform.py
│  ├─ test_load.py
│  └─ test_smoke.py       # Smoke test for CI
├─ data/                   # Sample/temporary data (no large files)
│  └─ sample.csv
├─ queries/                # Athena SQL queries
│  └─ athena_example.sql
├─ notebooks/              # Exploratory data analysis
│  └─ EDA.ipynb
├─ .github/workflows/      # GitHub Actions CI
│  └─ ci.yml

## Roadmap (9-day plan → GitHub issues/PRs)

CI + README (this PR)

Extract: get sample JP weather (CSV/API) → save to data/ and S3/raw

Transform: clean & normalize → Parquet partition by date/city

Load: write to s3://.../processed/date=.../

Athena: create external table, run avg temperature query

Results: notebook/console chart + screenshots in README

Tests: unit tests for transform, simple integration smoke

Release: tag v0.1.0, add demo screenshots

## Tech

Python 3.11, pandas, boto3, pyarrow, AWS (S3, Lambda, Athena).
License: MIT