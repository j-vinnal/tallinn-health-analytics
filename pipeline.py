from src.data_extraction.pxweb_client import download_pkh2_csv
from src.data_loading.snowflake_loader import run_sql_file
import config


def main() -> None:
    csv_path = download_pkh2_csv()

    run_sql_file(
        config.PROJECT_DIR / "sql" / "dml" / "staging" / "load_tai_pkh2_st1.sql",
        local_file_uri=csv_path.resolve().as_uri(),
        gz_name=f"{csv_path.name}.gz",
    )


if __name__ == "__main__":
    main()
