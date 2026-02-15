DELETE FROM ods.tai_tht001 t
WHERE EXISTS (
    SELECT 1
    FROM staging.tai_tht001_st1 st
    WHERE st.aasta = t.aasta
      AND TRIM(st.naitaja) = t.naitaja
      AND TRIM(st.amet) = t.amet
);

INSERT INTO ods.tai_tht001 (
    aasta,
    naitaja,
    amet,
    tootavad_isikud,
    taidetud_ametikohad_leping,
    tegelikult_taidetud,
    uletunnid_taiendav
)
SELECT
    s.aasta,
    TRIM(s.naitaja)                                                    AS naitaja,
    TRIM(s.amet)                                                       AS amet,
    TRY_CAST(NULLIF(s.tootavad_isikud, '..')             AS FLOAT)     AS tootavad_isikud,
    TRY_CAST(NULLIF(s.taidetud_ametikohad_leping, '..')  AS FLOAT)     AS taidetud_ametikohad_leping,
    TRY_CAST(NULLIF(s.tegelikult_taidetud, '..')         AS FLOAT)     AS tegelikult_taidetud,
    TRY_CAST(NULLIF(s.uletunnid_taiendav, '..')          AS FLOAT)     AS uletunnid_taiendav
FROM staging.tai_tht001_st1 s
WHERE s.aasta IS NOT NULL
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY s.aasta, TRIM(s.naitaja), TRIM(s.amet)
    ORDER BY s.staging_insert_ts DESC
) = 1;