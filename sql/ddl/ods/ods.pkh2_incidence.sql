USE ROLE SYSADMIN;
USE WAREHOUSE COMPUTE_WH;
USE DATABASE STRAT_DEV_DB;
USE SCHEMA ODS;

CREATE TABLE IF NOT EXISTS ods.pkh2_incidence (
    year NUMBER(4,0) NOT NULL,
    diagnosis_id NUMBER(38,0) NOT NULL,
    sex_id NUMBER(38,0) NOT NULL,
    age_group_id NUMBER(38,0) NOT NULL,
    incidence_count NUMBER(10,0),
    valid_from TIMESTAMP_NTZ(9) NOT NULL,
    valid_to TIMESTAMP_NTZ(9),
    is_current BOOLEAN NOT NULL,
    record_hash VARCHAR(32) NOT NULL,
    extract_id NUMBER(38,0) NOT NULL,
    loaded_ts TIMESTAMP_NTZ(9) DEFAULT CAST(CURRENT_TIMESTAMP() AS TIMESTAMP_NTZ(9)),
    CONSTRAINT uk_ods_pkh2_incidence UNIQUE (year, diagnosis_id, sex_id, age_group_id, valid_from)
);
