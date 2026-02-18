USE ROLE SYSADMIN;
USE WAREHOUSE COMPUTE_WH;
USE DATABASE STRAT_DEV_DB;
USE SCHEMA ODS;

CREATE TABLE IF NOT EXISTS ods.lk_sr57_education_level (
    education_level_id NUMBER(38,0) AUTOINCREMENT,
    education_level_label VARCHAR(200) NOT NULL,
    sort_order NUMBER(3,0) NOT NULL,
    loaded_ts TIMESTAMP_NTZ(9) DEFAULT CAST(CURRENT_TIMESTAMP() AS TIMESTAMP_NTZ(9)),
    CONSTRAINT pk_ods_lk_sr57_education_level PRIMARY KEY (education_level_id),
    CONSTRAINT uk_ods_lk_sr57_education_level UNIQUE (education_level_label)
);
