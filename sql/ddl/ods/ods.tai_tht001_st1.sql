USE ROLE SYSADMIN;
USE WAREHOUSE COMPUTE_WH;
USE DATABASE STRAT_DEV_DB;
USE SCHEMA ODS;

CREATE TABLE IF NOT EXISTS ods.tai_tht001
(
    aasta                      INTEGER      NOT NULL,
    naitaja                    VARCHAR(255) NOT NULL,
    amet                       VARCHAR(500) NOT NULL,
    tootavad_isikud            FLOAT,
    taidetud_ametikohad_leping FLOAT,
    tegelikult_taidetud        FLOAT,
    uletunnid_taiendav         FLOAT,
    ods_insert_ts              TIMESTAMP_NTZ(9) DEFAULT CAST(CURRENT_TIMESTAMP() AS TIMESTAMP_NTZ(9)),

    CONSTRAINT uk_ods_tht001 UNIQUE (aasta, naitaja, amet)
);