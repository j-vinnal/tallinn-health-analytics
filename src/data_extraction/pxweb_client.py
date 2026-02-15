"""
pxweb_client.py
"""

import json
from pathlib import Path
from datetime import date
from urllib.request import Request, urlopen

from config import RAW_DIR


def download(source_name: str, url: str, query: list, response_format: str = "csv") -> Path:
    """Download data from TAI PxWeb API."""
    body = {
        "query": query,
        "response": {"format": response_format},
    }

    RAW_DIR.mkdir(parents=True, exist_ok=True)

    payload = json.dumps(body).encode()
    request = Request(
        url,
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    out_file = RAW_DIR / f"{source_name}_{date.today().isoformat()}.{response_format}"
    with urlopen(request, timeout=60) as response:
        out_file.write_bytes(response.read())

    print(f"Saved: {out_file}")
    return out_file
