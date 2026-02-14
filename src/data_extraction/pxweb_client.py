"""
pxweb_client.py
"""

import json
from pathlib import Path
from datetime import date
from urllib.request import Request, urlopen

from config import RAW_DIR

URL = "https://statistika.tai.ee/api/v1/et/Andmebaas/02Haigestumus/05Psyyhikahaired/PKH2.px"
BODY = {
    "query": [
        {
            "code": "Aasta",
            "selection": {
                "filter": "item",
                "values": ["2023", "2024"],
            },
        }
    ],
    "response": {"format": "csv"},
}


def download_pkh2_csv() -> Path:
    """

    :return:
    """
    RAW_DIR.mkdir(parents=True, exist_ok=True)

    payload = json.dumps(BODY).encode()
    request = Request(
        URL, data=payload, headers={"Content-Type": "application/json"}, method="POST"
    )

    out_file = RAW_DIR / f"pkh2_{date.today().isoformat()}.csv"
    with urlopen(request, timeout=60) as response:
        out_file.write_bytes(response.read())

    print(f"Saved: {out_file}")
    return out_file


if __name__ == "__main__":
    download_pkh2_csv()
