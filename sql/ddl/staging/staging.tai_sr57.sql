USE ROLE SYSADMIN;
USE WAREHOUSE COMPUTE_WH;
USE DATABASE STRAT_DEV_DB;
USE SCHEMA STAGING;

CREATE TABLE IF NOT EXISTS staging.tai_sr57_st1 (
    source_file VARCHAR(500) NOT NULL,
    year_code VARCHAR NOT NULL,
    year_label VARCHAR,
    education_level_code VARCHAR NOT NULL,
    education_level_label VARCHAR,
    age_group_code VARCHAR NOT NULL,
    age_group_label VARCHAR,
    metric_label VARCHAR,
    value DOUBLE,
    natural_key_hash VARCHAR(32) NOT NULL,
    record_hash VARCHAR(32) NOT NULL,
    extract_id INTEGER NOT NULL,
    loaded_ts TIMESTAMP_NTZ(9) DEFAULT CAST(CURRENT_TIMESTAMP() AS TIMESTAMP_NTZ(9)),
    UNIQUE (extract_id, natural_key_hash)
);
