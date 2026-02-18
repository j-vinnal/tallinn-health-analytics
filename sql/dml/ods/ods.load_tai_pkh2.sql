USE ROLE SYSADMIN;
USE WAREHOUSE COMPUTE_WH;
USE DATABASE STRAT_DEV_DB;
USE SCHEMA ODS;

SET v_run_ts = CAST(CURRENT_TIMESTAMP() AS TIMESTAMP_NTZ(9));
SET v_src_count = (SELECT COUNT(*) FROM STAGING.TAI_PKH2_ST1);
SET v_extract_id = (SELECT MAX(extract_id) FROM STAGING.TAI_PKH2_ST1);

MERGE INTO ODS.LK_PKH2_DIAGNOSIS tgt
USING (
    SELECT DISTINCT TRIM(diagnosis_icd10) AS diagnosis_label
    FROM STAGING.TAI_PKH2_ST1
    WHERE diagnosis_icd10 IS NOT NULL
      AND $v_src_count > 0
) src
ON tgt.diagnosis_label = src.diagnosis_label
WHEN NOT MATCHED THEN
    INSERT (diagnosis_label)
    VALUES (src.diagnosis_label);

MERGE INTO ODS.LK_PKH2_SEX tgt
USING (
    SELECT DISTINCT TRIM(sex) AS sex_label
    FROM STAGING.TAI_PKH2_ST1
    WHERE sex IS NOT NULL
      AND $v_src_count > 0
) src
ON tgt.sex_label = src.sex_label
WHEN NOT MATCHED THEN
    INSERT (sex_label)
    VALUES (src.sex_label);

MERGE INTO ODS.LK_PKH2_AGE_GROUP tgt
USING (
    SELECT DISTINCT
        TRIM(age_group) AS age_group_label,
        CASE TRIM(age_group)
            WHEN 'All age groups' THEN 0
            WHEN '0' THEN 1
            WHEN '1-4' THEN 2
            WHEN '5-9' THEN 3
            WHEN '10-14' THEN 4
            WHEN '15-19' THEN 5
            WHEN '20-24' THEN 6
            WHEN '25-34' THEN 7
            WHEN '35-44' THEN 8
            WHEN '45-54' THEN 9
            WHEN '55-64' THEN 10
            WHEN '65-74' THEN 11
            WHEN '75 and older' THEN 12
            WHEN '75-84' THEN 13
            WHEN '85 and older' THEN 14
            ELSE 999
        END AS sort_order
    FROM STAGING.TAI_PKH2_ST1
    WHERE age_group IS NOT NULL
      AND $v_src_count > 0
) src
ON tgt.age_group_label = src.age_group_label
WHEN NOT MATCHED THEN
    INSERT (age_group_label, sort_order)
    VALUES (src.age_group_label, src.sort_order);

UPDATE ODS.PKH2_INCIDENCE tgt
SET
    valid_to = CAST($v_run_ts AS TIMESTAMP_NTZ(9)),
    is_current = FALSE
FROM (
    WITH src AS (
        SELECT
            st.year,
            d.diagnosis_id,
            sx.sex_id,
            ag.age_group_id,
            st.incidence_of_psychiatric_disorders AS incidence_count,
            MD5(
                CONCAT(
                    COALESCE(TO_VARCHAR(st.year), ''), '|',
                    COALESCE(TO_VARCHAR(d.diagnosis_id), ''), '|',
                    COALESCE(TO_VARCHAR(sx.sex_id), ''), '|',
                    COALESCE(TO_VARCHAR(ag.age_group_id), ''), '|',
                    COALESCE(TO_VARCHAR(st.incidence_of_psychiatric_disorders), '')
                )
            ) AS record_hash
        FROM STAGING.TAI_PKH2_ST1 st
        JOIN ODS.LK_PKH2_DIAGNOSIS d
            ON d.diagnosis_label = TRIM(st.diagnosis_icd10)
        JOIN ODS.LK_PKH2_SEX sx
            ON sx.sex_label = TRIM(st.sex)
        JOIN ODS.LK_PKH2_AGE_GROUP ag
            ON ag.age_group_label = TRIM(st.age_group)
        WHERE st.year IS NOT NULL
    )
    SELECT *
    FROM src
) src
WHERE $v_src_count > 0
  AND tgt.is_current = TRUE
  AND tgt.year = src.year
  AND tgt.diagnosis_id = src.diagnosis_id
  AND tgt.sex_id = src.sex_id
  AND tgt.age_group_id = src.age_group_id
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
      JOIN ODS.LK_PKH2_DIAGNOSIS d
          ON d.diagnosis_label = TRIM(st.diagnosis_icd10)
      JOIN ODS.LK_PKH2_SEX sx
          ON sx.sex_label = TRIM(st.sex)
      JOIN ODS.LK_PKH2_AGE_GROUP ag
          ON ag.age_group_label = TRIM(st.age_group)
      WHERE st.year = tgt.year
        AND d.diagnosis_id = tgt.diagnosis_id
        AND sx.sex_id = tgt.sex_id
        AND ag.age_group_id = tgt.age_group_id
  );

INSERT INTO ODS.PKH2_INCIDENCE (
    year,
    diagnosis_id,
    sex_id,
    age_group_id,
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
        d.diagnosis_id,
        sx.sex_id,
        ag.age_group_id,
        st.incidence_of_psychiatric_disorders AS incidence_count,
        MD5(
            CONCAT(
                COALESCE(TO_VARCHAR(st.year), ''), '|',
                COALESCE(TO_VARCHAR(d.diagnosis_id), ''), '|',
                COALESCE(TO_VARCHAR(sx.sex_id), ''), '|',
                COALESCE(TO_VARCHAR(ag.age_group_id), ''), '|',
                COALESCE(TO_VARCHAR(st.incidence_of_psychiatric_disorders), '')
            )
        ) AS record_hash
    FROM STAGING.TAI_PKH2_ST1 st
    JOIN ODS.LK_PKH2_DIAGNOSIS d
        ON d.diagnosis_label = TRIM(st.diagnosis_icd10)
    JOIN ODS.LK_PKH2_SEX sx
        ON sx.sex_label = TRIM(st.sex)
    JOIN ODS.LK_PKH2_AGE_GROUP ag
        ON ag.age_group_label = TRIM(st.age_group)
    WHERE st.year IS NOT NULL
)
SELECT
    src.year,
    src.diagnosis_id,
    src.sex_id,
    src.age_group_id,
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
   AND cur.diagnosis_id = src.diagnosis_id
   AND cur.sex_id = src.sex_id
   AND cur.age_group_id = src.age_group_id
WHERE $v_src_count > 0
  AND (cur.year IS NULL OR cur.record_hash <> src.record_hash);
