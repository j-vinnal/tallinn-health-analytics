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

UPDATE ODS.SR57_FATHER_AVG_AGE tgt
SET
    valid_to = CAST($v_run_ts AS TIMESTAMP_NTZ(9)),
    is_current = FALSE
FROM (
    WITH src AS (
        SELECT
            st.year,
            TRIM(st.educational_level) AS educational_level,
            st.fathers_age::NUMBER(10,1) AS avg_father_age,
            st.record_hash AS record_hash
        FROM STAGING.TAI_SR57_ST1 st
        WHERE st.year IS NOT NULL
          AND st.educational_level IS NOT NULL
          AND TRIM(st.age) = 'Average age'
    )
    SELECT *
    FROM src
) src
WHERE $v_src_count > 0
  AND tgt.is_current = TRUE
  AND tgt.year = src.year
  AND tgt.educational_level = src.educational_level
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
      WHERE st.year = tgt.year
        AND TRIM(st.educational_level) = tgt.educational_level
        AND TRIM(st.age) = 'Average age'
  );

INSERT INTO ODS.SR57_FATHER_AVG_AGE (
    year,
    educational_level,
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
        TRIM(st.educational_level) AS educational_level,
        st.fathers_age::NUMBER(10,1) AS avg_father_age,
        st.record_hash AS record_hash
    FROM STAGING.TAI_SR57_ST1 st
    WHERE st.year IS NOT NULL
      AND st.educational_level IS NOT NULL
      AND TRIM(st.age) = 'Average age'
)
SELECT
    src.year,
    src.educational_level,
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
   AND cur.educational_level = src.educational_level
WHERE $v_src_count > 0
  AND (cur.year IS NULL OR cur.record_hash <> src.record_hash);
