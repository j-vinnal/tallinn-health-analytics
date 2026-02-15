"""
config.py
"""
from pathlib import Path

PROJECT_DIR = Path(__file__).resolve().parent
RAW_DIR = PROJECT_DIR / "data" / "raw"
PROCESSED_DIR = PROJECT_DIR / "data" / "processed"

# Snowflake connection (DEV / ETL)
SNOWFLAKE_DEV_ETL_ACCOUNT = "sf29869.north-europe.azure"
SNOWFLAKE_DEV_ETL_USER = "STRAT_DEV_USER_ETL"
SNOWFLAKE_DEV_ETL_PASSWORD = "Stratusertallinn35"
SNOWFLAKE_DEV_ETL_ROLE = "STRAT_DEV_ROLE_ETL"
SNOWFLAKE_DEV_ETL_WAREHOUSE = "STRAT_DEV_WH_ETL"

SNOWFLAKE_DEV_DATABASE = "STRAT_DEV_DB"
SNOWFLAKE_DEV_SCHEMA_STAGING = "STAGING"

# TAI PxWeb API sources
TAI_SOURCES = {
    "pkh2": {
        "url": "https://statistika.tai.ee/api/v1/et/Andmebaas/02Haigestumus/05Psyyhikahaired/PKH2.px",
        "query": [
            {
                "code": "Aasta",
                "selection": {
                    "filter": "item",
                    "values": ["2023", "2024"],
                },
            }
        ],
        "response_format": "csv",
        "dml_file": "sql/dml/staging/load_staging_tai_pkh2_st1.sql",
    },
    "tht001": {
        "url": "https://statistika.tai.ee/api/v1/et/Andmebaas/04THressursid/05Tootajad/THT001.px",
        "query": [
            {
                "code": "Aasta",
                "selection": {
                    "filter": "item",
                    "values": ["2023", "2024"],
                },
            }
        ],
        "response_format": "csv",
        "dml_file": "sql/dml/staging/load_staging_tai_tht001_st1.sql",
    },
}

ODS_LOADS = {
    "ods_pkh2":   "sql/dml/ods/load_ods_tai_pkh2.sql",
    "ods_tht001": "sql/dml/ods/load_ods_tai_tht001.sql",
}
