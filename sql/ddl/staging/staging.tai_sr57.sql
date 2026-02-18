USE ROLE SYSADMIN;
USE WAREHOUSE COMPUTE_WH;
USE DATABASE STRAT_DEV_DB;
USE SCHEMA STAGING;

CREATE TRANSIENT TABLE IF NOT EXISTS staging.tai_sr57_st1 (
    educational_level VARCHAR(255),
    age               VARCHAR(100),
    year              NUMBER(4,0),
    fathers_age       NUMBER(10,1),
    record_hash       VARCHAR(32) NOT NULL,
    extract_id        NUMBER(38,0) NOT NULL,
    loaded_ts         TIMESTAMP_NTZ(9) DEFAULT CAST(CURRENT_TIMESTAMP() AS TIMESTAMP_NTZ(9)),
    UNIQUE (educational_level, age, year)
);
