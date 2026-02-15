DELETE FROM ods.tai_pkh2 t
WHERE EXISTS (
    SELECT 1
    FROM staging.tai_pkh2_st1 st
    WHERE st.aasta = t.aasta
      AND TRIM(st.diagnoos_rhk10) = t.diagnoos_rhk10
      AND TRIM(st.sugu) = t.sugu
);

INSERT INTO ods.tai_pkh2 (
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
SELECT
    s.aasta,
    TRIM(s.diagnoos_rhk10)                   AS diagnoos_rhk10,
    TRIM(s.sugu)                             AS sugu,
    s.vanuseruhmad_kokku,
    s.age_0,
    s.age_1_4,
    s.age_5_9,
    s.age_10_14,
    s.age_15_19,
    s.age_20_24,
    s.age_25_34,
    s.age_35_44,
    s.age_45_54,
    s.age_55_64,
    s.age_65_74,
    s.age_75_plus,
    s.age_75_84,
    s.age_85_plus
FROM STAGING.TAI_PKH2_ST1 s
WHERE s.aasta IS NOT NULL
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY s.aasta, TRIM(s.diagnoos_rhk10), TRIM(s.sugu)
    ORDER BY s.staging_insert_ts DESC
) = 1;