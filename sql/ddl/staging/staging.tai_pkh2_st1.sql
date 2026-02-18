USE ROLE SYSADMIN;
USE WAREHOUSE COMPUTE_WH;
USE DATABASE STRAT_DEV_DB;
USE SCHEMA STAGING;

CREATE TRANSIENT TABLE IF NOT EXISTS staging.tai_pkh2_st1
(
    source_file        VARCHAR(500) NOT NULL,
    aasta              NUMBER(4,0),
    diagnoos_rhk10     VARCHAR(255),
    sugu               VARCHAR(100),
    vanuseruhmad_kokku NUMBER(10,0),
    age_0              NUMBER(10,0),
    age_1_4            NUMBER(10,0),
    age_5_9            NUMBER(10,0),
    age_10_14          NUMBER(10,0),
    age_15_19          NUMBER(10,0),
    age_20_24          NUMBER(10,0),
    age_25_34          NUMBER(10,0),
    age_35_44          NUMBER(10,0),
    age_45_54          NUMBER(10,0),
    age_55_64          NUMBER(10,0),
    age_65_74          NUMBER(10,0),
    age_75_plus        NUMBER(10,0),
    age_75_84          NUMBER(10,0),
    age_85_plus        NUMBER(10,0),
    natural_key_hash   VARCHAR(32) NOT NULL,
    record_hash        VARCHAR(32) NOT NULL,
    extract_id         NUMBER(14,0) NOT NULL,
    loaded_ts          TIMESTAMP_NTZ(9) DEFAULT CAST(CURRENT_TIMESTAMP() AS TIMESTAMP_NTZ(9)),
    UNIQUE (extract_id, natural_key_hash)
);
