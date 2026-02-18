USE ROLE SYSADMIN;
USE WAREHOUSE COMPUTE_WH;
USE DATABASE STRAT_DEV_DB;
USE SCHEMA ODS;

CREATE TABLE IF NOT EXISTS ods.lk_pkh2_diagnosis (
    diagnosis_id NUMBER(38,0) AUTOINCREMENT,
    diagnosis_label VARCHAR(255) NOT NULL,
    loaded_ts TIMESTAMP_NTZ(9) DEFAULT CAST(CURRENT_TIMESTAMP() AS TIMESTAMP_NTZ(9)),
    CONSTRAINT pk_ods_lk_pkh2_diagnosis PRIMARY KEY (diagnosis_id),
    CONSTRAINT uk_ods_lk_pkh2_diagnosis UNIQUE (diagnosis_label)
);
