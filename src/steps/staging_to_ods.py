from __future__ import annotations
from pathlib import Path
from src.logger import log

STEP = "staging_to_ods"


def run(ctx) -> None:
    for source_id in ctx.sources:
        ods_tables = ctx.sources_config[source_id].get("ods_tables", [])
        ods_dmls = ctx.sources_config[source_id].get("ods_dmls", [])
        if not ods_dmls:
            ctx.logger.warning("No ODS DML configured for: %s", source_id)
            continue
        _load_source(ctx, source_id, ods_dmls, ods_tables)


def _load_source(ctx, source_id: str, ods_dmls: list[str], ods_tables: list[str]) -> None:
    for dml_path_str, target_table in zip(ods_dmls, ods_tables):
        try:
            sql = Path(dml_path_str).read_text(encoding="utf-8")
            for cur in ctx.conn.execute_string(sql):
                cur.close()
            log(ctx.conn, ctx.logger, ctx.extract_id, STEP, source_id, "SUCCESS",
                target_table=target_table)
        except Exception as exc:
            log(ctx.conn, ctx.logger, ctx.extract_id, STEP, source_id, "FAILED",
                target_table=target_table, error_message=str(exc)[:4000])
            raise
