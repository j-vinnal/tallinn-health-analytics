import json
import os
from datetime import date
from pathlib import Path
from urllib.request import Request, urlopen

from dotenv import load_dotenv


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


def main() -> None:
    load_dotenv()

    project_dir = Path(os.getenv("PROJECT_DIR", Path.cwd()))
    raw_dir = Path(project_dir, os.getenv("RAW_DIR"))

    raw_dir.mkdir(parents=True, exist_ok=True)

    payload = json.dumps(BODY).encode("utf-8")
    request = Request(
        URL, data=payload, headers={"Content-Type": "application/json"}, method="POST"
    )

    with urlopen(request, timeout=60) as response:
        data = response.read().decode("utf-8")

    out_file = raw_dir / f"pkh2_{date.today().isoformat()}.csv"
    if out_file.exists():
        out_file.unlink()

    out_file.write_text(data, encoding="utf-8")

    print(f"Saved: {out_file}")


if __name__ == "__main__":
    main()
