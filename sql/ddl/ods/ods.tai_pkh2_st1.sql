USE ROLE SYSADMIN;
USE WAREHOUSE COMPUTE_WH;
USE DATABASE STRAT_DEV_DB;
USE SCHEMA ODS;

CREATE TABLE IF NOT EXISTS ods.tai_pkh2
(
    aasta              INTEGER       NOT NULL,
    diagnoos_rhk10     VARCHAR(255)  NOT NULL,
    sugu               VARCHAR(100)  NOT NULL,
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
    ods_insert_ts      TIMESTAMP_NTZ(9) DEFAULT CAST(CURRENT_TIMESTAMP() AS TIMESTAMP_NTZ(9)),

    CONSTRAINT uk_ods_pkh2 UNIQUE (aasta, diagnoos_rhk10, sugu)
);