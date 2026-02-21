# tallinn-health-analytics
Simple CLI data pipeline

## Quick Start

1. Python version:
- Use Python `3.11` or `3.12`.
- Python `3.14` is not supported for this project (`snowflake-connector-python` install may fail).

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Check configuration:
- API sources: `conf/api_sources.toml`
- Snowflake connection: `conf/snowflake.toml`

4. Run API -> raw:
- Without `--sources` , all configured sources are downloaded.
- With `--sources`, only the provided source IDs are downloaded.

```bash
# All configured sources
python pipeline.py run --step api_to_raw

# Only SR57
python pipeline.py run --step api_to_raw --sources SR57

# Only PKH2
python pipeline.py run --step api_to_raw --sources PKH2
```

Result: files are saved in the `data/raw/` folder.
