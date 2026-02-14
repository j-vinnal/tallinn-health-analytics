-- Plain SQL intended to be executed via Snowflake connector.
-- Placeholders are replaced by Python `run_sql_file()`.

-- 1) Upload to internal stage (auto-compress -> .gz)
PUT {local_file_uri} @STAGING.MY_STAGE AUTO_COMPRESS=TRUE OVERWRITE=TRUE;

-- 2) Reload table
TRUNCATE TABLE STAGING.TAI_PKH2_ST1;

-- 3) Copy into (explicit column list so staging_insert_ts uses DEFAULT)
COPY INTO STAGING.TAI_PKH2_ST1 (
  aasta,
  diagnoos_rhk10,
  sugu,
  vanuseruhmad_kokku,
  age_0,
  age_1_4,
  age_5_9,
  age_10_14,
  age_15_19,
  age_20_24,
  age_25_34,
  age_35_44,
  age_45_54,
  age_55_64,
  age_65_74,
  age_75_plus,
  age_75_84,
  age_85_plus
)
FROM @STAGING.MY_STAGE/{gz_name}
FILE_FORMAT = (FORMAT_NAME = 'STAGING.file_format_csv_comma_doublequote_enclosure')
ON_ERROR = 'ABORT_STATEMENT';

-- 4) Optional cleanup
REMOVE @STAGING.MY_STAGE/{gz_name};
