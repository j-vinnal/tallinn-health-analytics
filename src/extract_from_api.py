from datetime import datetime
from pathlib import Path
import requests


def fetch_data_to_file(source_id: str, config: dict, source_config: dict) -> Path:
    api_config = config["tai_api"]
    output_path = Path(api_config.get("output_path", "data/raw"))
    output_path.mkdir(parents=True, exist_ok=True)

    url = f"{api_config['api_root']}/{source_config['path']}"
    payload = {
        "query": source_config.get("query", []),
        "response": {"format": api_config.get("response_format", "json-stat2")},
    }
    response = requests.post(
        url, json=payload, timeout=api_config.get("timeout_seconds", 30)
    )
    response.raise_for_status()

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_path = output_path / f"{source_id}_{timestamp}.csv"
    file_path.write_bytes(response.content)
    return file_path
