USE ROLE SYSADMIN;
USE WAREHOUSE COMPUTE_WH;
USE DATABASE STRAT_DEV_DB;
USE SCHEMA ODS;

CREATE TABLE IF NOT EXISTS ods.tai_sr57_counts (
    year INTEGER NOT NULL,
    education_level_code VARCHAR(2) NOT NULL,
    education_level_label VARCHAR(200),
    age_group_code VARCHAR(2) NOT NULL,
    age_group_label VARCHAR(200),
    father_count INTEGER,
    record_hash VARCHAR(32) NOT NULL,
    extract_id INTEGER NOT NULL,
    loaded_ts TIMESTAMP_NTZ(9) DEFAULT CAST(CURRENT_TIMESTAMP() AS TIMESTAMP_NTZ(9)),
    CONSTRAINT uk_ods_tai_sr57_counts UNIQUE (year, education_level_code, age_group_code, extract_id)
);
