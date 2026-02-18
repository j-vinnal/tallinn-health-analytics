USE ROLE SYSADMIN;
USE WAREHOUSE COMPUTE_WH;
USE DATABASE STRAT_DEV_DB;
USE SCHEMA ODS;

SET v_run_ts = CAST(CURRENT_TIMESTAMP() AS TIMESTAMP_NTZ(9));
SET v_src_count = (
    SELECT COUNT(*)
    FROM STAGING.TAI_SR57_ST1
    WHERE TRIM(age) = 'Average age'
);
SET v_extract_id = (
    SELECT MAX(extract_id)
    FROM STAGING.TAI_SR57_ST1
    WHERE TRIM(age) = 'Average age'
);

MERGE INTO ODS.LK_SR57_EDUCATION_LEVEL tgt
USING (
    SELECT DISTINCT
        TRIM(educational_level) AS education_level_label,
        CASE TRIM(educational_level)
            WHEN 'All levels of education' THEN 0
            WHEN 'Basic education or lower' THEN 1
            WHEN 'Secondary education' THEN 2
            WHEN 'Vocational education' THEN 3
            WHEN 'Higher education' THEN 4
            WHEN 'Educational level unknown' THEN 5
            ELSE 999
        END AS sort_order
    FROM STAGING.TAI_SR57_ST1
    WHERE educational_level IS NOT NULL
      AND TRIM(age) = 'Average age'
      AND $v_src_count > 0
) src
ON tgt.education_level_label = src.education_level_label
WHEN NOT MATCHED THEN
    INSERT (education_level_label, sort_order)
    VALUES (src.education_level_label, src.sort_order);

UPDATE ODS.SR57_FATHER_AVG_AGE tgt
SET
    valid_to = CAST($v_run_ts AS TIMESTAMP_NTZ(9)),
    is_current = FALSE
FROM (
    WITH src AS (
        SELECT
            st.year,
            el.education_level_id,
            st.fathers_age::NUMBER(10,1) AS avg_father_age,
            MD5(
                CONCAT(
                    COALESCE(TO_VARCHAR(st.year), ''), '|',
                    COALESCE(TO_VARCHAR(el.education_level_id), ''), '|',
                    COALESCE(TO_VARCHAR(st.fathers_age::NUMBER(10,1)), '')
                )
            ) AS record_hash
        FROM STAGING.TAI_SR57_ST1 st
        JOIN ODS.LK_SR57_EDUCATION_LEVEL el
            ON el.education_level_label = TRIM(st.educational_level)
        WHERE st.year IS NOT NULL
          AND TRIM(st.age) = 'Average age'
    )
    SELECT *
    FROM src
) src
WHERE $v_src_count > 0
  AND tgt.is_current = TRUE
  AND tgt.year = src.year
  AND tgt.education_level_id = src.education_level_id
  AND tgt.record_hash <> src.record_hash;

UPDATE ODS.SR57_FATHER_AVG_AGE tgt
SET
    valid_to = CAST($v_run_ts AS TIMESTAMP_NTZ(9)),
    is_current = FALSE
WHERE $v_src_count > 0
  AND tgt.is_current = TRUE
  AND NOT EXISTS (
      SELECT 1
      FROM STAGING.TAI_SR57_ST1 st
      JOIN ODS.LK_SR57_EDUCATION_LEVEL el
          ON el.education_level_label = TRIM(st.educational_level)
      WHERE st.year = tgt.year
        AND el.education_level_id = tgt.education_level_id
        AND TRIM(st.age) = 'Average age'
  );

INSERT INTO ODS.SR57_FATHER_AVG_AGE (
    year,
    education_level_id,
    avg_father_age,
    valid_from,
    valid_to,
    is_current,
    record_hash,
    extract_id
)
WITH src AS (
    SELECT
        st.year,
        el.education_level_id,
        st.fathers_age::NUMBER(10,1) AS avg_father_age,
        MD5(
            CONCAT(
                COALESCE(TO_VARCHAR(st.year), ''), '|',
                COALESCE(TO_VARCHAR(el.education_level_id), ''), '|',
                COALESCE(TO_VARCHAR(st.fathers_age::NUMBER(10,1)), '')
            )
        ) AS record_hash
    FROM STAGING.TAI_SR57_ST1 st
    JOIN ODS.LK_SR57_EDUCATION_LEVEL el
        ON el.education_level_label = TRIM(st.educational_level)
    WHERE st.year IS NOT NULL
      AND TRIM(st.age) = 'Average age'
)
SELECT
    src.year,
    src.education_level_id,
    src.avg_father_age,
    CAST($v_run_ts AS TIMESTAMP_NTZ(9)) AS valid_from,
    NULL AS valid_to,
    TRUE AS is_current,
    src.record_hash,
    CAST($v_extract_id AS NUMBER(38,0)) AS extract_id
FROM src
LEFT JOIN ODS.SR57_FATHER_AVG_AGE cur
    ON cur.is_current = TRUE
   AND cur.year = src.year
   AND cur.education_level_id = src.education_level_id
WHERE $v_src_count > 0
  AND (cur.year IS NULL OR cur.record_hash <> src.record_hash);
