USE ROLE SYSADMIN;
USE WAREHOUSE COMPUTE_WH;
USE DATABASE STRAT_DEV_DB;
USE SCHEMA STAGING;

CREATE TRANSIENT TABLE IF NOT EXISTS staging.tai_sr57_st1 (
    source_file        VARCHAR(500) NOT NULL,
    aasta              NUMBER(4,0),
    haridustase        VARCHAR(255),
    vanuseruhmad_kokku NUMBER(10,0),
    age_10_14          NUMBER(10,0),
    age_15_19          NUMBER(10,0),
    age_20_24          NUMBER(10,0),
    age_25_29          NUMBER(10,0),
    age_30_34          NUMBER(10,0),
    age_35_39          NUMBER(10,0),
    age_40_44          NUMBER(10,0),
    age_45_49          NUMBER(10,0),
    age_50_54          NUMBER(10,0),
    age_55_59          NUMBER(10,0),
    age_60_64          NUMBER(10,0),
    age_65_plus        NUMBER(10,0),
    age_unknown        NUMBER(10,0),
    average_age        NUMBER(5,1),
    natural_key_hash   VARCHAR(32) NOT NULL,
    record_hash        VARCHAR(32) NOT NULL,
    extract_id         NUMBER(14,0) NOT NULL,
    loaded_ts          TIMESTAMP_NTZ(9) DEFAULT CAST(CURRENT_TIMESTAMP() AS TIMESTAMP_NTZ(9)),
    UNIQUE (extract_id, natural_key_hash)
);
