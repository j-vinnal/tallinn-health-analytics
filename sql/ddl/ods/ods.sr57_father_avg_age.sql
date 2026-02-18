USE ROLE SYSADMIN;
USE WAREHOUSE COMPUTE_WH;
USE DATABASE STRAT_DEV_DB;
USE SCHEMA ODS;

CREATE TABLE IF NOT EXISTS ods.sr57_father_avg_age (
    year NUMBER(4,0) NOT NULL,
    educational_level VARCHAR(255) NOT NULL,
    avg_father_age NUMBER(10,1),
    valid_from TIMESTAMP_NTZ(9) NOT NULL,
    valid_to TIMESTAMP_NTZ(9),
    is_current BOOLEAN NOT NULL,
    record_hash VARCHAR(32) NOT NULL,
    extract_id NUMBER(38,0) NOT NULL,
    loaded_ts TIMESTAMP_NTZ(9) DEFAULT CAST(CURRENT_TIMESTAMP() AS TIMESTAMP_NTZ(9)),
    CONSTRAINT uk_ods_sr57_father_avg_age UNIQUE (year, educational_level, valid_from)
);
