from __future__ import annotations

from datetime import datetime
from pathlib import Path

import requests


def run(ctx) -> None:
    api_cfg = ctx.api_config["tai_api"]
    output_root = Path(api_cfg["output_path"])

    for source_id in ctx.sources:
        api_path = ctx.sources_config[source_id]["api_path"]
        file_path = _fetch(source_id, api_path, api_cfg, output_root)
        ctx.logger.info("Saved: %s", file_path)


def _fetch(source_id: str, api_path: str, api_cfg: dict, output_root: Path) -> Path:
    output_dir = output_root / source_id
    output_dir.mkdir(parents=True, exist_ok=True)

# Peab mõtlema, kas see on vajalik
#    for f in output_dir.glob("*"):
#        if f.is_file():
#            f.unlink()

    url = f"{api_cfg['api_root']}/{api_path}"
    resp = requests.post(
        url,
        json={"query": [], "response": {"format": api_cfg.get("response_format", "csv2")}},
        timeout=api_cfg.get("timeout_seconds", 30),
    )
    resp.raise_for_status()

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_path = output_dir / f"{source_id}_{ts}.json"
    file_path.write_bytes(resp.content)
    return file_path
