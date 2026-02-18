from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from src.utils import PROJECT_ROOT


@dataclass(frozen=True)
class SourceSpec:
    staging_table: str
    staging_dml_sql: Path
    ods_dml_sql: tuple[Path, ...]
    ods_target_tables: str


SOURCE_SPECS: dict[str, SourceSpec] = {
    "PKH2": SourceSpec(
        staging_table="STAGING.TAI_PKH2_ST1",
        staging_dml_sql=PROJECT_ROOT / "sql" / "dml" / "staging" / "staging.load_tai_pkh2_st1.sql",
        ods_dml_sql=(PROJECT_ROOT / "sql" / "dml" / "ods" / "ods.load_tai_pkh2.sql",),
        ods_target_tables="ODS.PKH2_INCIDENCE",
    ),
    "SR57": SourceSpec(
        staging_table="STAGING.TAI_SR57_ST1",
        staging_dml_sql=PROJECT_ROOT / "sql" / "dml" / "staging" / "staging.load_tai_sr57_st1.sql",
        ods_dml_sql=(
            PROJECT_ROOT / "sql" / "dml" / "ods" / "ods.load_tai_sr57_counts.sql",
            PROJECT_ROOT / "sql" / "dml" / "ods" / "ods.load_tai_sr57_avg_age.sql",
        ),
        ods_target_tables="ODS.SR57_FATHER_COUNTS, ODS.SR57_FATHER_AVG_AGE",
    ),
}


def get_source_spec(source_id: str) -> SourceSpec:
    spec = SOURCE_SPECS.get(source_id)
    if spec is None:
        known = ", ".join(sorted(SOURCE_SPECS.keys()))
        raise ValueError(f"No source spec configured for {source_id}. Known: {known}")
    return spec
