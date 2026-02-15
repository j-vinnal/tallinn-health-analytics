PUT {local_file_uri} @STAGING.MY_STAGE AUTO_COMPRESS=TRUE OVERWRITE=TRUE;

TRUNCATE TABLE STAGING.TAI_THT001_ST1;

COPY INTO STAGING.TAI_THT001_ST1 (
    aasta,
    naitaja,
    amet,
    tootavad_isikud,
    taidetud_ametikohad_leping,
    tegelikult_taidetud,
    uletunnid_taiendav
)
FROM @STAGING.MY_STAGE/{gz_name}
FILE_FORMAT = (FORMAT_NAME = 'STAGING.file_format_csv_comma_doublequote_enclosure')
ON_ERROR = 'ABORT_STATEMENT';

REMOVE @STAGING.MY_STAGE/{gz_name};