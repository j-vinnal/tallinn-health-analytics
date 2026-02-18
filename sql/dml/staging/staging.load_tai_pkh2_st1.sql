-- Plain SQL intended to be executed via Snowflake connector.
-- Placeholders are replaced by Python `run_sql_file()`.

-- 1) Upload to internal stage (auto-compress -> .gz)
PUT {local_file_uri} @STAGING.MY_STAGE AUTO_COMPRESS=TRUE OVERWRITE=TRUE;

-- 2) Reload table
TRUNCATE TABLE STAGING.TAI_PKH2_ST1;

-- 3) Copy into
COPY INTO STAGING.TAI_PKH2_ST1 (
  source_file,
  diagnosis_icd10,
  sex,
  age_group,
  year,
  incidence_of_psychiatric_disorders,
  natural_key_hash,
  record_hash,
  extract_id
)
FROM (
  SELECT
    METADATA$FILENAME::VARCHAR AS source_file,
    $1::VARCHAR AS diagnosis_icd10,
    $2::VARCHAR AS sex,
    $3::VARCHAR AS age_group,
    TRY_TO_NUMBER($4)::NUMBER(4,0) AS year,
    TRY_TO_NUMBER($5)::NUMBER(10,0) AS incidence_of_psychiatric_disorders,
    MD5(
      CONCAT(
        COALESCE($1::VARCHAR, ''), '|',
        COALESCE($2::VARCHAR, ''), '|',
        COALESCE($3::VARCHAR, ''), '|',
        COALESCE($4::VARCHAR, '')
      )
    ) AS natural_key_hash,
    MD5(
      CONCAT(
        COALESCE($1::VARCHAR, ''), '|',
        COALESCE($2::VARCHAR, ''), '|',
        COALESCE($3::VARCHAR, ''), '|',
        COALESCE($4::VARCHAR, ''), '|',
        COALESCE($5::VARCHAR, '')
      )
    ) AS record_hash,
    TRY_TO_NUMBER(REPLACE(REGEXP_SUBSTR(METADATA$FILENAME, '\\d{{8}}_\\d{{6}}'), '_', ''))::NUMBER(14,0) AS extract_id
  FROM @STAGING.MY_STAGE/{gz_name}
)
FILE_FORMAT = (FORMAT_NAME = 'STAGING.file_format_csv_comma_doublequote_enclosure')
ON_ERROR = 'ABORT_STATEMENT';

-- 4) Optional cleanup
REMOVE @STAGING.MY_STAGE/{gz_name};
