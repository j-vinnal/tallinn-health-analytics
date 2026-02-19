USE ROLE SYSADMIN;
USE WAREHOUSE COMPUTE_WH;
USE DATABASE STRAT_DEV_DB;
USE SCHEMA ODS;

SET v_run_ts = CAST(CURRENT_TIMESTAMP() AS TIMESTAMP_NTZ(9));
SET v_src_count = (SELECT COUNT(*) FROM STAGING.TAI_PKH2_ST1);
SET v_extract_id = (SELECT MAX(extract_id) FROM STAGING.TAI_PKH2_ST1);

UPDATE ODS.PKH2_INCIDENCE tgt
SET
    valid_to = CAST($v_run_ts AS TIMESTAMP_NTZ(9)),
    is_current = FALSE
FROM (
    WITH src AS (
        SELECT
            st.year,
            TRIM(st.diagnosis_icd10) AS diagnosis_icd10,
            TRIM(st.sex) AS sex,
            TRIM(st.age_group) AS age_group,
            st.incidence_of_psychiatric_disorders AS incidence_count,
            st.record_hash AS record_hash
        FROM STAGING.TAI_PKH2_ST1 st
        WHERE st.year IS NOT NULL
          AND st.diagnosis_icd10 IS NOT NULL
          AND st.sex IS NOT NULL
          AND st.age_group IS NOT NULL
    )
    SELECT *
    FROM src
) src
WHERE $v_src_count > 0
  AND tgt.is_current = TRUE
  AND tgt.year = src.year
  AND tgt.diagnosis_icd10 = src.diagnosis_icd10
  AND tgt.sex = src.sex
  AND tgt.age_group = src.age_group
  AND tgt.record_hash <> src.record_hash;

UPDATE ODS.PKH2_INCIDENCE tgt
SET
    valid_to = CAST($v_run_ts AS TIMESTAMP_NTZ(9)),
    is_current = FALSE
WHERE $v_src_count > 0
  AND tgt.is_current = TRUE
  AND NOT EXISTS (
      SELECT 1
      FROM STAGING.TAI_PKH2_ST1 st
      WHERE st.year = tgt.year
        AND TRIM(st.diagnosis_icd10) = tgt.diagnosis_icd10
        AND TRIM(st.sex) = tgt.sex
        AND TRIM(st.age_group) = tgt.age_group
  );

INSERT INTO ODS.PKH2_INCIDENCE (
    year,
    diagnosis_icd10,
    sex,
    age_group,
    incidence_count,
    valid_from,
    valid_to,
    is_current,
    record_hash,
    extract_id
)
WITH src AS (
    SELECT
        st.year,
        TRIM(st.diagnosis_icd10) AS diagnosis_icd10,
        TRIM(st.sex) AS sex,
        TRIM(st.age_group) AS age_group,
        st.incidence_of_psychiatric_disorders AS incidence_count,
        st.record_hash AS record_hash
    FROM STAGING.TAI_PKH2_ST1 st
    WHERE st.year IS NOT NULL
      AND st.diagnosis_icd10 IS NOT NULL
      AND st.sex IS NOT NULL
      AND st.age_group IS NOT NULL
)
SELECT
    src.year,
    src.diagnosis_icd10,
    src.sex,
    src.age_group,
    src.incidence_count,
    CAST($v_run_ts AS TIMESTAMP_NTZ(9)) AS valid_from,
    NULL AS valid_to,
    TRUE AS is_current,
    src.record_hash,
    CAST($v_extract_id AS NUMBER(38,0)) AS extract_id
FROM src
LEFT JOIN ODS.PKH2_INCIDENCE cur
    ON cur.is_current = TRUE
   AND cur.year = src.year
   AND cur.diagnosis_icd10 = src.diagnosis_icd10
   AND cur.sex = src.sex
   AND cur.age_group = src.age_group
WHERE $v_src_count > 0
  AND (cur.year IS NULL OR cur.record_hash <> src.record_hash);
