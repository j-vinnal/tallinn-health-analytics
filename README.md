# tallinn-health-analytics
Simple CLI data pipeline

## Quick Start

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Check configuration:
- API sources: `conf/api_sources.toml`
- Snowflake connection: `conf/snowflake.toml`

3. Run API -> raw:
- Without `--sources`, all configured sources are downloaded.
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
