USE ROLE SYSADMIN;
USE WAREHOUSE COMPUTE_WH;
USE DATABASE STRAT_DEV_DB;
USE SCHEMA ODS;

CREATE TABLE IF NOT EXISTS ods.lk_pkh2_age_group (
    age_group_id NUMBER(38,0) AUTOINCREMENT,
    age_group_label VARCHAR(100) NOT NULL,
    sort_order NUMBER(3,0) NOT NULL,
    loaded_ts TIMESTAMP_NTZ(9) DEFAULT CAST(CURRENT_TIMESTAMP() AS TIMESTAMP_NTZ(9)),
    CONSTRAINT pk_ods_lk_pkh2_age_group PRIMARY KEY (age_group_id),
    CONSTRAINT uk_ods_lk_pkh2_age_group UNIQUE (age_group_label)
);
