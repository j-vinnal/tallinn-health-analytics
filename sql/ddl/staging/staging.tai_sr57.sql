USE ROLE SYSADMIN;
USE WAREHOUSE COMPUTE_WH;
USE DATABASE STRAT_DEV_DB;
USE SCHEMA STAGING;

CREATE TRANSIENT TABLE IF NOT EXISTS staging.tai_sr57_st1 (
    source_file        VARCHAR(500) NOT NULL,
    aasta              INTEGER,
    haridustase        VARCHAR(255),
    vanuseruhmad_kokku DOUBLE,
    age_10_14          DOUBLE,
    age_15_19          DOUBLE,
    age_20_24          DOUBLE,
    age_25_29          DOUBLE,
    age_30_34          DOUBLE,
    age_35_39          DOUBLE,
    age_40_44          DOUBLE,
    age_45_49          DOUBLE,
    age_50_54          DOUBLE,
    age_55_59          DOUBLE,
    age_60_64          DOUBLE,
    age_65_plus        DOUBLE,
    age_unknown        DOUBLE,
    average_age        DOUBLE,
    natural_key_hash   VARCHAR(32) NOT NULL,
    record_hash        VARCHAR(32) NOT NULL,
    extract_id         INTEGER NOT NULL,
    loaded_ts          TIMESTAMP_NTZ(9) DEFAULT CAST(CURRENT_TIMESTAMP() AS TIMESTAMP_NTZ(9)),
    UNIQUE (extract_id, natural_key_hash)
);
