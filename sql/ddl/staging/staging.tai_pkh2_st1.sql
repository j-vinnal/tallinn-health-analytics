USE ROLE SYSADMIN;
USE WAREHOUSE COMPUTE_WH;
USE DATABASE STRAT_DEV_DB;
USE SCHEMA STAGING;

CREATE TRANSIENT TABLE IF NOT EXISTS staging.tai_pkh2_st1
(
    source_file        VARCHAR(500) NOT NULL,
    aasta              INTEGER,
    diagnoos_rhk10     VARCHAR(255),
    sugu               VARCHAR(100),
    vanuseruhmad_kokku INTEGER,
    age_0              INTEGER,
    age_1_4            INTEGER,
    age_5_9            INTEGER,
    age_10_14          INTEGER,
    age_15_19          INTEGER,
    age_20_24          INTEGER,
    age_25_34          INTEGER,
    age_35_44          INTEGER,
    age_45_54          INTEGER,
    age_55_64          INTEGER,
    age_65_74          INTEGER,
    age_75_plus        INTEGER,
    age_75_84          INTEGER,
    age_85_plus        INTEGER,
    natural_key_hash   VARCHAR(32) NOT NULL,
    record_hash        VARCHAR(32) NOT NULL,
    extract_id         INTEGER NOT NULL,
    loaded_ts          TIMESTAMP_NTZ(9) DEFAULT CAST(CURRENT_TIMESTAMP() AS TIMESTAMP_NTZ(9)),
    UNIQUE (extract_id, natural_key_hash)
);
