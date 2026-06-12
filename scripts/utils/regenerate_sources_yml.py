"""
Regenerate models/sources.yml column lists from a live Snowflake schema dump.

Preserves source-level metadata (name/database/schema/description) and table-level
identifiers; replaces only the per-table `columns:` blocks with Snowflake reality.

Inputs:
  - models/sources.yml             (existing, used for source metadata + non-OLIDS sources)
  - /tmp/olids-schema/schema_clean.json   (output of `snow sql -c secondees` for
                                          Data_Store_OLIDS INFORMATION_SCHEMA.COLUMNS)

Outputs:
  - models/sources.yml             (rewritten)
"""

import json
from collections import defaultdict
from pathlib import Path

import yaml

import os
SCHEMA_DUMP = Path(os.environ.get(
    "OLIDS_SCHEMA_DUMP",
    Path(os.environ.get("TEMP", "/tmp")) / "olids-schema" / "schema_clean.json",
))
SOURCES_YML = Path("models/sources.yml")

# Snowflake schema name -> dbt source name in sources.yml
SCHEMA_TO_SOURCE = {
    "OLIDS_COMMON": "olids_common",
    "OLIDS_MASKED": "olids_masked",
    "OLIDS_TERMINOLOGY": "olids_terminology",
    "REFERENCE": ("olids_reference", "emis_reference"),  # both consume from REFERENCE
    "NDOO_MASKED": "ndoo_masked",
}

# Tables to keep under olids_reference (vs emis_reference)
OLIDS_REFERENCE_TABLES = {"POSTCODE_HASH"}

# Map Snowflake data types to the dbt sources.yml short forms
TYPE_MAP = {
    "TEXT": "TEXT",
    "VARCHAR": "TEXT",
    "CHAR": "TEXT",
    "NUMBER": "NUMBER",
    "FLOAT": "FLOAT",
    "DOUBLE": "FLOAT",
    "BOOLEAN": "BOOLEAN",
    "DATE": "DATE",
    "TIMESTAMP_NTZ": "TIMESTAMP_NTZ",
    "TIMESTAMP_LTZ": "TIMESTAMP_LTZ",
    "TIMESTAMP_TZ": "TIMESTAMP_TZ",
    "BINARY": "BINARY",
    "VARIANT": "VARIANT",
    "OBJECT": "OBJECT",
    "ARRAY": "ARRAY",
    "UUID": "UUID",
}


def normalise_type(sf_type: str) -> str:
    base = sf_type.split("(")[0].upper().strip()
    return TYPE_MAP.get(base, base)


def load_dump() -> dict:
    """Returns {source_name: {table_name: [{name, data_type}, ...]}}"""
    rows = json.loads(SCHEMA_DUMP.read_text())
    grouped: dict[str, dict[str, list[dict]]] = defaultdict(lambda: defaultdict(list))

    for row in rows:
        schema = row["TABLE_SCHEMA"]
        table = row["TABLE_NAME"]
        col = {
            "name": row["COLUMN_NAME"].lower(),
            "data_type": normalise_type(row["DATA_TYPE"]),
        }

        target = SCHEMA_TO_SOURCE.get(schema)
        if target is None:
            continue

        if isinstance(target, tuple):
            # REFERENCE schema split between olids_reference and emis_reference
            if table in OLIDS_REFERENCE_TABLES:
                grouped["olids_reference"][table].append(col)
            else:
                grouped["emis_reference"][table].append(col)
        else:
            grouped[target][table].append(col)

    return grouped


def build_tables(source_name: str, tables_map: dict[str, list[dict]]) -> list[dict]:
    """Build the tables list for a given source, sorted by table name."""
    tables = []
    for table_name in sorted(tables_map.keys()):
        tables.append({
            "name": table_name,
            "identifier": f'"{table_name}"',
            "columns": tables_map[table_name],
        })
    return tables


def main():
    dump = load_dump()

    with SOURCES_YML.open() as f:
        sources_doc = yaml.safe_load(f)

    for source in sources_doc["sources"]:
        source_name = source["name"]
        if source_name in dump:
            source["tables"] = build_tables(source_name, dump[source_name])
            print(f"  Regenerated {source_name}: {len(source['tables'])} tables")
        else:
            print(f"  Skipped {source_name} (not in Snowflake dump)")

    with SOURCES_YML.open("w", encoding="utf-8") as f:
        yaml.dump(sources_doc, f, default_flow_style=False, sort_keys=False, width=200)

    print(f"\nWrote {SOURCES_YML}")


if __name__ == "__main__":
    main()
