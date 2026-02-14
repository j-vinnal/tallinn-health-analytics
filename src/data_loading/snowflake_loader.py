from __future__ import annotations

from pathlib import Path
from io import StringIO

import config
import snowflake.connector
from snowflake.connector.util_text import split_statements


def connect_etl_dev():
    """Return a Snowflake connection using DEV/ETL credentials from config."""
    return snowflake.connector.connect(
        account=config.SNOWFLAKE_DEV_ETL_ACCOUNT,
        user=config.SNOWFLAKE_DEV_ETL_USER,
        password=config.SNOWFLAKE_DEV_ETL_PASSWORD,
        role=config.SNOWFLAKE_DEV_ETL_ROLE,
        warehouse=config.SNOWFLAKE_DEV_ETL_WAREHOUSE,
        database=config.SNOWFLAKE_DEV_DATABASE,
        schema=config.SNOWFLAKE_DEV_SCHEMA_STAGING,
    )


def run_sql_file(sql_path: Path, **params: str) -> None:
    """Execute a .sql file with simple `{placeholder}` substitution.

    Keep scripts to plain SQL.
    """

    sql_text = Path(sql_path).read_text(encoding="utf-8")
    if params:
        sql_text = sql_text.format(**params)

    with connect_etl_dev() as conn:
        with conn.cursor() as cur:
            for statement, _ in split_statements(
                StringIO(sql_text), remove_comments=True
            ):
                stmt = statement.strip()
                if not stmt:
                    continue
                cur.execute(stmt)
