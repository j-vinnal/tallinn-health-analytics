USE ROLE SYSADMIN;
USE WAREHOUSE COMPUTE_WH;
USE DATABASE STRAT_DEV_DB;
USE SCHEMA ODS;

CREATE TABLE IF NOT EXISTS ods.tai_sr57_avg_age (
    year INTEGER NOT NULL,
    education_level_code VARCHAR(2) NOT NULL,
    education_level_label VARCHAR(200),
    avg_father_age DECIMAL(4, 1),
    record_hash VARCHAR(32) NOT NULL,
    extract_id INTEGER NOT NULL,
    loaded_ts TIMESTAMP_NTZ(9) DEFAULT CAST(CURRENT_TIMESTAMP() AS TIMESTAMP_NTZ(9)),
    CONSTRAINT uk_ods_tai_sr57_avg_age UNIQUE (year, education_level_code, extract_id)
);
