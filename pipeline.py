"""
pipeline.py
"""
from src.data_extraction.pxweb_client import download
from src.data_loading.snowflake_loader import run_sql_file
import config


def extract_and_stage(source_name: str) -> None:
    """Extract data from TAI API and load into Snowflake staging."""
    source = config.TAI_SOURCES[source_name]

    raw_file = download(
        source_name=source_name,
        url=source["url"],
        query=source["query"],
        response_format=source["response_format"],
    )

    run_sql_file(
        config.PROJECT_DIR / source["dml_file"],
        local_file_uri=raw_file.resolve().as_uri(),
        gz_name=f"{raw_file.name}.gz",
    )

    print(f"Loaded {source_name} → staging")


def load_ods() -> None:
    """Transform staging → ODS layer."""
    for ods_name, dml_path in config.ODS_LOADS.items():
        run_sql_file(config.PROJECT_DIR / dml_path)
        print(f"Loaded {ods_name}")


def main() -> None:
    """Main function."""
    # Step 1: Extract → Staging
    for source_name in config.TAI_SOURCES:
        extract_and_stage(source_name)

    # Step 2: Staging → ODS
    load_ods()


if __name__ == "__main__":
    main()
