-- Plain SQL intended to be executed via Snowflake connector.
-- Placeholders are replaced by Python `run_sql_file()`.

-- 1) Upload to internal stage (auto-compress -> .gz)
PUT {local_file_uri} @STAGING.MY_STAGE AUTO_COMPRESS=TRUE OVERWRITE=TRUE;

-- 2) Reload table
TRUNCATE TABLE STAGING.TAI_SR57_ST1;

-- 3) Copy into
COPY INTO STAGING.TAI_SR57_ST1 (
  source_file,
  educational_level,
  age,
  year,
  fathers_age,
  natural_key_hash,
  record_hash,
  extract_id
)
FROM (
  SELECT
    '{source_file}'::VARCHAR AS source_file,
    $1::VARCHAR AS educational_level,
    $2::VARCHAR AS age,
    TRY_TO_NUMBER($3)::NUMBER(4,0) AS year,
    TRY_TO_NUMBER($4)::NUMBER(10,1) AS fathers_age,
    MD5(
      CONCAT(
        COALESCE($1::VARCHAR, ''), '|',
        COALESCE($2::VARCHAR, ''), '|',
        COALESCE($3::VARCHAR, '')
      )
    ) AS natural_key_hash,
    MD5(
      CONCAT(
        COALESCE($1::VARCHAR, ''), '|',
        COALESCE($2::VARCHAR, ''), '|',
        COALESCE($3::VARCHAR, ''), '|',
        COALESCE($4::VARCHAR, '')
      )
    ) AS record_hash,
    '{extract_id}'::NUMBER(38,0) AS extract_id
  FROM @STAGING.MY_STAGE/{source_file}.gz
)
FILE_FORMAT = (FORMAT_NAME = 'STAGING.file_format_csv_comma_doublequote_enclosure')
ON_ERROR = 'ABORT_STATEMENT';

-- 4) Optional cleanup
REMOVE @STAGING.MY_STAGE/{source_file}.gz;
