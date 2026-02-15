USE ROLE SYSADMIN;
USE WAREHOUSE COMPUTE_WH;
USE DATABASE STRAT_DEV_DB;
USE SCHEMA STAGING;

CREATE TRANSIENT TABLE IF NOT EXISTS staging.tai_tht001_st1
(
    aasta                      INTEGER,
    naitaja                    VARCHAR(255),
    amet                       VARCHAR(500),
    tootavad_isikud            VARCHAR(50),
    taidetud_ametikohad_leping VARCHAR(50),
    tegelikult_taidetud        VARCHAR(50),
    uletunnid_taiendav         VARCHAR(50),
    staging_insert_ts          TIMESTAMP_NTZ(9) DEFAULT CAST(CURRENT_TIMESTAMP() AS TIMESTAMP_NTZ(9))
);