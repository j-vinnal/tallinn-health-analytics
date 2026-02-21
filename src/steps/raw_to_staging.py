from __future__ import annotations

import shutil
from pathlib import Path
from src.logger import log

STEP = "raw_to_staging"


def run(ctx) -> None:
    api_cfg = ctx.api_config["tai_api"]
    raw_root = Path(api_cfg["output_path"])
    processed_root = Path(api_cfg["processed_path"])

    for source_id in ctx.sources:
        _load_source(ctx, source_id, raw_root, processed_root)


def _load_source(ctx, source_id: str, raw_root: Path, processed_root: Path) -> None:
    src_cfg = ctx.sources_config[source_id]
    target_table = src_cfg["staging_table"]
    sql_path = Path(src_cfg["staging_dml"])
    source_file = None

    try:
        files = sorted((raw_root / source_id).glob("*.csv"))
        if not files:
            ctx.logger.warning("No CSV found for: %s", source_id)
            log(ctx.conn, ctx.logger, ctx.extract_id, STEP, source_id, "SKIPPED",
                target_table=target_table)
            return

        file_path = files[0]
        source_file = file_path.name

        _execute_sql_template(ctx.conn, sql_path, {
            "local_file_uri": f"'{file_path.resolve().as_uri()}'",
            "gz_name": f"{file_path.name}.gz",
            "extract_id": str(ctx.extract_id),
        })

        dest_dir = processed_root / source_id
        dest_dir.mkdir(parents=True, exist_ok=True)
        shutil.move(str(file_path), str(dest_dir / file_path.name))

        log(ctx.conn, ctx.logger, ctx.extract_id, STEP, source_id, "SUCCESS",
            source_file=source_file, target_table=target_table)

    except Exception as exc:
        log(ctx.conn, ctx.logger, ctx.extract_id, STEP, source_id, "FAILED",
            source_file=source_file, target_table=target_table, error_message=str(exc)[:4000])
        raise


def _execute_sql_template(conn, sql_path: Path, params: dict) -> None:
    sql = sql_path.read_text(encoding="utf-8").format(**params)
    for cur in conn.execute_string(sql):
        cur.close()
