USE ROLE SYSADMIN;
USE WAREHOUSE COMPUTE_WH;
USE DATABASE STRAT_DEV_DB;
USE SCHEMA STAGING;

CREATE TRANSIENT TABLE IF NOT EXISTS staging.tai_pkh2_st1 (
    source_file                        VARCHAR(500) NOT NULL,
    diagnosis_icd10                    VARCHAR(255),
    sex                                VARCHAR(100),
    age_group                          VARCHAR(100),
    year                               NUMBER(4,0),
    incidence_of_psychiatric_disorders NUMBER(10,0),
    natural_key_hash                   VARCHAR(32) NOT NULL,
    record_hash                        VARCHAR(32) NOT NULL,
    extract_id                         NUMBER(38,0) NOT NULL,
    loaded_ts                          TIMESTAMP_NTZ(9) DEFAULT CAST(CURRENT_TIMESTAMP() AS TIMESTAMP_NTZ(9)),
    UNIQUE (extract_id, natural_key_hash)
);
