#!/usr/bin/env python3
"""Introspect a Postgres schema (default: ``mtc_aat_cohort``) and optionally
write a Century-format dictionary workbook straight out of the database.

One file, end-to-end:

    1. Connect to the warehouse using the standard PG* env vars.
    2. Walk every table in the target schema.
    3. For every column, collect data type, row count, NULL count,
       completeness %, and the top-N frequent values.
    4. Print a tree to stdout and, if requested, write the draft to CSV
       and/or an XLSX workbook with Summary / Tables / Variables sheets
       that ``validate_dictionary.py`` can read directly.

You still fill in ``category``, ``description``, ``criteria``, ``extraction_type``
by hand (the schema does not know those). Everything else is pre-populated.

Required env vars (or the equivalent ``--`` flags):
    PGHOST, PGPORT, PGDATABASE, PGUSER, PGPASSWORD

Optional:
    PGSSLMODE   (default: require - typical for RDS/Aurora)

Typical usage::

    # Configure via a local .env (copy .env.example) so credentials don't
    # live in shell history.

    python introspect_cohort.py                                         # tree only
    python introspect_cohort.py --out-xlsx mtc_aat_cohort.xlsx          # curated
    python introspect_cohort.py --out-xlsx-raw mtc_aat_cohort_raw.xlsx  # raw QA

    # The curated workbook has one row per business variable (fact tables
    # collapsed by concept_name). The raw workbook has one row per source
    # column - useful only for QA. Descriptions on the curated workbook
    # are intentionally blank; fill them in before circulating.

    python validate_dictionary.py --input mtc_aat_cohort.xlsx

Dependencies::

    pip install 'psycopg[binary]' pandas openpyxl
"""

from __future__ import annotations

import argparse
import csv
import os
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

try:
    import psycopg  # psycopg 3.x
except ImportError:  # pragma: no cover
    sys.stderr.write("psycopg is not installed. Run: pip install 'psycopg[binary]'\n")
    raise SystemExit(1)


# --------------------------------------------------------------------------- #
# .env loading (no external dependency)
# --------------------------------------------------------------------------- #


def load_dotenv(path: Path) -> int:
    """Parse KEY=VALUE lines from ``path`` and set them on ``os.environ``.

    Values already present in the environment (e.g. from the shell) win so
    ``PGPASSWORD=... python introspect_cohort.py`` still overrides the file.
    Returns the number of keys loaded. Silently no-ops if the file is absent.
    """
    if not path.is_file():
        return 0

    loaded = 0
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        # Support "export KEY=VALUE" form too.
        if line.startswith("export "):
            line = line[len("export "):].lstrip()
        key, _, value = line.partition("=")
        key = key.strip()
        value = value.strip()
        # Strip matching surrounding quotes.
        if len(value) >= 2 and value[0] == value[-1] and value[0] in ("'", '"'):
            value = value[1:-1]
        if key and key not in os.environ:
            os.environ[key] = value
            loaded += 1
    return loaded


# Auto-load ``.env`` from the script's directory on import. Running from a
# different cwd still works.
_DOTENV_PATH = Path(__file__).resolve().parent / ".env"
_loaded = load_dotenv(_DOTENV_PATH)
if _loaded:
    print(f"Loaded {_loaded} value(s) from {_DOTENV_PATH}", file=sys.stderr)


# --------------------------------------------------------------------------- #
# Connection
# --------------------------------------------------------------------------- #


def build_conn_kwargs(args: argparse.Namespace) -> dict[str, str]:
    kwargs = {
        "host": args.host or os.environ.get("PGHOST"),
        "port": str(args.port or os.environ.get("PGPORT") or 5432),
        "dbname": args.database or os.environ.get("PGDATABASE"),
        "user": args.user or os.environ.get("PGUSER"),
        "password": args.password or os.environ.get("PGPASSWORD"),
        "sslmode": args.sslmode or os.environ.get("PGSSLMODE") or "require",
    }
    missing = [k for k in ("host", "dbname", "user", "password") if not kwargs.get(k)]
    if missing:
        sys.stderr.write(
            "Missing required DB settings. Provide via env vars (PGHOST, "
            "PGDATABASE, PGUSER, PGPASSWORD) or CLI flags. Missing: "
            + ", ".join(missing)
            + "\n"
        )
        raise SystemExit(2)
    return kwargs


# --------------------------------------------------------------------------- #
# Queries
# --------------------------------------------------------------------------- #

LIST_TABLES_SQL = """
SELECT table_name, table_type
FROM information_schema.tables
WHERE table_schema = %s
  AND table_type IN ('BASE TABLE', 'VIEW')
ORDER BY table_name;
"""

LIST_SCHEMAS_SQL = """
SELECT n.nspname AS schema_name,
       COUNT(c.oid) FILTER (
         WHERE c.relkind IN ('r', 'v', 'm', 'f', 'p')
       ) AS object_count
FROM pg_namespace n
LEFT JOIN pg_class c ON c.relnamespace = n.oid
WHERE n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
  AND n.nspname NOT LIKE 'pg_temp_%'
  AND n.nspname NOT LIKE 'pg_toast_temp_%'
GROUP BY n.nspname
ORDER BY n.nspname;
"""

LIST_COLUMNS_SQL = """
SELECT column_name, data_type, is_nullable, character_maximum_length, numeric_precision
FROM information_schema.columns
WHERE table_schema = %s AND table_name = %s
ORDER BY ordinal_position;
"""

ROW_COUNT_SQL_TEMPLATE = 'SELECT COUNT(*) FROM "{schema}"."{table}";'

NULL_COUNT_SQL_TEMPLATE = (
    'SELECT COUNT(*) FROM "{schema}"."{table}" WHERE "{column}" IS NULL;'
)

TOP_VALUES_SQL_TEMPLATE = """
SELECT "{column}"::text AS value, COUNT(*) AS n
FROM "{schema}"."{table}"
WHERE "{column}" IS NOT NULL
GROUP BY "{column}"
ORDER BY n DESC
LIMIT %s;
"""

PERSON_COUNT_SQL_TEMPLATE = 'SELECT COUNT(*) FROM "{schema}".person;'


# --------------------------------------------------------------------------- #
# Dataclasses
# --------------------------------------------------------------------------- #


@dataclass
class ColumnInfo:
    schema: str
    table: str
    column: str
    data_type: str
    is_nullable: bool
    row_count: int
    null_count: int
    completeness_pct: float  # 0-100
    top_values: list[tuple[str, int]]

    def distribution_cell(self) -> str:
        if not self.top_values:
            return ""
        parts = []
        for value, count in self.top_values:
            display = value if len(value) <= 60 else value[:57] + "..."
            parts.append(f"{display}: {count}")
        return "; ".join(parts)


@dataclass
class TableInfo:
    name: str
    row_count: int
    column_count: int


# --------------------------------------------------------------------------- #
# Introspection
# --------------------------------------------------------------------------- #


# Types we compute top-N distributions for. Long free text gets skipped.
SAMPLEABLE_TYPES = {
    "character varying",
    "varchar",
    "text",
    "character",
    "name",
    "integer",
    "bigint",
    "smallint",
    "numeric",
    "real",
    "double precision",
    "boolean",
    "date",
}


def introspect(
    conn: psycopg.Connection,
    schema: str,
    sample_values: int,
    include_top_for_types: set[str],
) -> tuple[list[ColumnInfo], list[TableInfo]]:
    columns_out: list[ColumnInfo] = []
    tables_out: list[TableInfo] = []

    with conn.cursor() as cur:
        cur.execute(LIST_TABLES_SQL, (schema,))
        table_rows = cur.fetchall()
        tables = [row[0] for row in table_rows]
        view_count = sum(1 for _, t in table_rows if t == "VIEW")
        if view_count:
            print(
                f"  (found {view_count} VIEW(s) in '{schema}' - included in output)",
                file=sys.stderr,
            )

    if not tables:
        sys.stderr.write(
            f"No tables or views found in schema '{schema}'. "
            "Run with --list-schemas to see what the DB user can actually read.\n"
        )
        return columns_out, tables_out

    for table in tables:
        with conn.cursor() as cur:
            cur.execute(ROW_COUNT_SQL_TEMPLATE.format(schema=schema, table=table))
            row_count = cur.fetchone()[0]

            cur.execute(LIST_COLUMNS_SQL, (schema, table))
            columns = cur.fetchall()

        tables_out.append(TableInfo(name=table, row_count=row_count, column_count=len(columns)))
        print(
            f"  {schema}.{table}  ({row_count:,} rows, {len(columns)} cols)",
            file=sys.stderr,
        )

        for col_row in columns:
            column_name, data_type, is_nullable_str, _max_len, _prec = col_row
            is_nullable = is_nullable_str == "YES"

            null_count = 0
            top_values: list[tuple[str, int]] = []

            if row_count > 0:
                with conn.cursor() as cur:
                    cur.execute(
                        NULL_COUNT_SQL_TEMPLATE.format(
                            schema=schema, table=table, column=column_name
                        )
                    )
                    null_count = cur.fetchone()[0]

                    if sample_values > 0 and data_type in include_top_for_types:
                        cur.execute(
                            TOP_VALUES_SQL_TEMPLATE.format(
                                schema=schema, table=table, column=column_name
                            ),
                            (sample_values,),
                        )
                        top_values = [(str(v), int(n)) for v, n in cur.fetchall()]

            completeness = (1 - null_count / row_count) * 100 if row_count > 0 else 0.0

            columns_out.append(
                ColumnInfo(
                    schema=schema,
                    table=table,
                    column=column_name,
                    data_type=data_type,
                    is_nullable=is_nullable,
                    row_count=row_count,
                    null_count=null_count,
                    completeness_pct=completeness,
                    top_values=top_values,
                )
            )

    return columns_out, tables_out


def fetch_person_count(conn: psycopg.Connection, schema: str) -> int | None:
    """Best-effort patient count from ``{schema}.person``; None if absent."""
    try:
        with conn.cursor() as cur:
            cur.execute(PERSON_COUNT_SQL_TEMPLATE.format(schema=schema))
            row = cur.fetchone()
        return int(row[0]) if row else None
    except Exception:
        conn.rollback()
        return None


# --------------------------------------------------------------------------- #
# Output: stdout tree
# --------------------------------------------------------------------------- #


def print_tree(columns: list[ColumnInfo]) -> None:
    current_table = ""
    for col in columns:
        if col.table != current_table:
            print(f"\n{col.schema}.{col.table}  ({col.row_count:,} rows)")
            current_table = col.table
        sample = ""
        if col.top_values:
            sample = "  [" + ", ".join(f"{v}:{n}" for v, n in col.top_values[:3]) + "]"
        print(
            f"  - {col.column:<32} {col.data_type:<22} "
            f"completeness={col.completeness_pct:5.1f}%{sample}"
        )


# --------------------------------------------------------------------------- #
# Output: CSV (raw introspection)
# --------------------------------------------------------------------------- #


def write_raw_csv(columns: list[ColumnInfo], out_path: Path) -> None:
    fieldnames = [
        "schema",
        "table",
        "column",
        "data_type",
        "is_nullable",
        "row_count",
        "null_count",
        "completeness_pct",
        "top_values",
    ]
    with out_path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        for col in columns:
            writer.writerow({
                "schema": col.schema,
                "table": col.table,
                "column": col.column,
                "data_type": col.data_type,
                "is_nullable": "yes" if col.is_nullable else "no",
                "row_count": col.row_count,
                "null_count": col.null_count,
                "completeness_pct": f"{col.completeness_pct:.1f}",
                "top_values": col.distribution_cell(),
            })
    print(f"\nWrote raw CSV -> {out_path}", file=sys.stderr)


# --------------------------------------------------------------------------- #
# Output: Century-format XLSX dictionary draft
# --------------------------------------------------------------------------- #


def write_dictionary_xlsx(
    columns: list[ColumnInfo],
    tables: list[TableInfo],
    out_path: Path,
    cohort: str,
    person_count: int | None,
) -> None:
    """Write a Summary / Tables / Variables workbook the validator can read.

    ``category``, ``description``, ``criteria``, ``extraction_type`` and
    ``notes`` are left blank on purpose - fill them in after reviewing.
    """
    try:
        import pandas as pd
    except ImportError as exc:
        sys.stderr.write(
            "pandas is not installed; install it to emit XLSX: "
            "pip install pandas openpyxl\n"
        )
        raise SystemExit(3) from exc

    summary_rows = [
        {"metric": "cohort", "value": cohort},
        {"metric": "table_count", "value": len(tables)},
        {"metric": "column_count", "value": len(columns)},
    ]
    if person_count is not None:
        summary_rows.insert(1, {"metric": "patient_count", "value": person_count})

    summary_df = pd.DataFrame(summary_rows, columns=["metric", "value"])

    tables_df = pd.DataFrame(
        [
            {
                "table": t.name,
                "row_count": t.row_count,
                "column_count": t.column_count,
                "description": "",
            }
            for t in tables
        ]
    )

    variables_rows = []
    for col in columns:
        variables_rows.append({
            # Blank columns for the user to fill in:
            "Category": "",
            # Prefill Variable as "<table>.<column>" so rows stay unique even
            # when the same column (value_as_number, concept_id, ...) appears
            # in several tables. Rename to the display label ("AAT level",
            # "Heart rate", ...) during review.
            "Variable": f"{col.table}.{col.column}",
            "Description": "",
            # Auto-populated from the warehouse:
            "Schema": col.table,
            "Column(s)": col.column,
            "Criteria": "",
            "Values": col.top_values[0][0] if col.top_values else "",
            "Distribution": col.distribution_cell(),
            "Completeness": f"{col.completeness_pct:.1f}%",
            "Extraction Type": "Structured",
            "Notes": "",
        })
    variables_df = pd.DataFrame(variables_rows)

    with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
        summary_df.to_excel(writer, sheet_name="Summary", index=False)
        tables_df.to_excel(writer, sheet_name="Tables", index=False)
        variables_df.to_excel(writer, sheet_name="Variables", index=False)

    print(
        f"\nWrote dictionary workbook -> {out_path}\n"
        "  Fill in Category / Description / Criteria / Extraction Type\n"
        f"  Then validate: python validate_dictionary.py --input {out_path}",
        file=sys.stderr,
    )


# --------------------------------------------------------------------------- #
# Curation rules (how to collapse the raw inventory to Century-format rows)
# --------------------------------------------------------------------------- #

# Tables dropped entirely - PII plumbing / vendor-specific tokens.
TABLES_TO_SKIP: set[str] = {
    "dv_tokenized_profile_data",
}

# Specific column names to drop even if they live on an otherwise-kept table
# (PHI pushed down into the warehouse but not intended as analytic variables).
SENSITIVE_COLUMNS: set[str] = {
    "ssn",
    "first_name",
    "last_name",
    "middle_name",
    "email",
    "phone",
    "cellphone",
    "address1",
    "address2",
    "street",
    "record_id_tokenized",
    "patient_id_tokenized",
    "encryption_key",
    "file_name",
    "s3_uri",
    "document_text",
    "document_s3_uri",
}

# Regex patterns applied to every column - things that are plumbing, not data.
DROP_COLUMN_REGEXES = [
    re.compile(p) for p in (
        r".*_source_concept_id$",
        r".*_source_concept_name$",
        r".*_source_value$",
        r".*_type_concept_id$",
        r".*_type_source_value$",
        r"^airflow_.*",
        r".*_event_id$",
        # IDs and FKs - keep concept_names, drop the numeric join keys.
        r".*_concept_id$",
        r"^provider_id$",
        r"^visit_detail_id$",
        r"^visit_occurrence_id$",
        r"^care_site_id$",
        r"^payer_plan_period_id$",
        r"^location_id$",
    )
]

# Per-table curation recipes. Keys are unqualified table names. Each recipe
# emits zero or more business-variable rows (one row per concept for fact
# tables, or one row per listed column for demographic-style tables).
#
#   ``mode`` values:
#     "per_concept"          - one row per distinct ``group_by`` value
#                              (observations, measurements)
#     "single_row_with_list" - one row; distinct group values go in Values
#                              (condition_occurrence, visit_occurrence)
#     "split_by_type"        - one row per value of ``group_by`` using the
#                              per-value override in ``splits``
#                              (drug_exposure: Prescriptions vs
#                              Administrations)
#     "keep_columns"         - one row per named column (person / location /
#                              payer_plan_period demographics)
#     "static"               - emit the hard-coded rows verbatim
#                              (note / document unstructured rows)
CURATION_RULES: dict[str, dict[str, Any]] = {
    "observation": {
        "category": "Observation",
        "mode": "per_concept",
        "group_by": "observation_concept_name",
        "value_column": "value_as_number",
        "criteria_template": "observation_concept_name = '{name}'",
        "description_template": "Observation '{name}' captured at the office visit.",
        "extraction_type": "Structured",
        "max_concepts": 30,
    },
    "measurement": {
        "category": "Labs",
        "mode": "per_concept",
        "group_by": "measurement_concept_name",
        "value_column": "value_as_number",
        "criteria_template": "measurement_concept_name = '{name}'",
        "description_template": "Laboratory measurement '{name}' recorded for the patient.",
        "extraction_type": "Structured",
        "max_concepts": 30,
    },
    "condition_occurrence": {
        "category": "Diagnosis",
        "mode": "single_row_with_list",
        "variable": "Diagnosis",
        "description": "Medical diagnoses and conditions recorded for the patient.",
        "group_by": "condition_concept_name",
        "value_column": "condition_concept_name",
        "extraction_type": "Structured",
        "max_values": 50,
    },
    "drug_exposure": {
        "category": "Prescriptions",
        "mode": "split_by_type",
        "group_by": "drug_type_concept_name",
        "value_column": "drug_concept_name",
        "criteria_template": "drug_type_concept_name = '{name}'",
        "extraction_type": "Structured",
        "splits": {
            "EHR prescription": {
                "variable": "Prescriptions",
                "category": "Prescriptions",
                "description": "Medications prescribed to the patient.",
            },
            "EHR administration record": {
                "variable": "Administrations",
                "category": "Administrations",
                "description": "Administrations recorded for the patient.",
            },
        },
    },
    "procedure_occurrence": {
        "category": "Procedures",
        "mode": "single_row_with_list",
        "variable": "Procedures",
        "description": "Procedures recorded for the patient.",
        "group_by": "procedure_concept_name",
        "value_column": "procedure_concept_name",
        "extraction_type": "Structured",
        "max_values": 50,
    },
    "visit_occurrence": {
        "category": "Visits",
        "mode": "single_row_with_list",
        "variable": "Visit Type and Date",
        "description": "The type of healthcare visit or encounter.",
        "group_by": "visit_concept_name",
        "value_column": "visit_concept_name",
        "extraction_type": "Structured",
        "max_values": 20,
    },
    "person": {
        "category": "Demographics",
        "mode": "keep_columns",
        "columns": {
            "gender_concept_name": {
                "variable": "Sex",
                "description": "Biological sex at birth.",
            },
            "race_concept_name": {
                "variable": "Race",
                "description": "Self-identified racial category.",
            },
            "ethnicity_concept_name": {
                "variable": "Ethnicity",
                "description": "Self-identified ethnicity.",
            },
            "year_of_birth": {
                "variable": "BirthYear",
                "description": "Year the patient was born.",
            },
        },
    },
    "location": {
        "category": "Demographics",
        "mode": "keep_columns",
        "columns": {
            "zip": {
                "variable": "ZIP Code",
                "description": "Patient ZIP code (full or first 3 digits).",
            },
            "state": {
                "variable": "State",
                "description": "Patient state of residence.",
            },
        },
    },
    "payer_plan_period": {
        "category": "Demographics",
        "mode": "keep_columns",
        "columns": {
            "payer_concept_name": {
                "variable": "Payer",
                "description": "Insurance payer responsible for the visit.",
            },
            "plan_type_concept_name": {
                "variable": "Plan Type",
                "description": "Insurance plan type.",
            },
        },
    },
    "note": {
        "category": "Documents",
        "mode": "static",
        "rows": [
            {
                "variable": "Clinical Note",
                "description": "Free text of the office visit clinical note.",
                "column": "note_text",
                "criteria": "",
                "extraction_type": "Unstructured",
            }
        ],
    },
    "document": {
        "category": "Documents",
        "mode": "static",
        "rows": [
            {
                "variable": "Document",
                "description": "Scanned PDFs (MRI, EKG, PET scan, infusion notes, etc.).",
                "column": "document_type_concept_name",
                "criteria": "",
                "extraction_type": "Unstructured",
            }
        ],
    },
    "infusion": {
        "category": "Administrations",
        "mode": "static",
        "rows": [
            {
                "variable": "Infusion Note",
                "description": "Free text or PDF of the infusion note.",
                "column": "note_text",
                "criteria": "",
                "extraction_type": "Unstructured",
            }
        ],
    },
}


DISTINCT_CONCEPTS_SQL_TEMPLATE = """
SELECT "{column}"::text AS name, COUNT(*) AS n
FROM "{schema}"."{table}"
WHERE "{column}" IS NOT NULL
GROUP BY "{column}"
ORDER BY n DESC
LIMIT %s;
"""


def _column_is_dropped(table: str, column: str) -> bool:
    if table in TABLES_TO_SKIP:
        return True
    if column in SENSITIVE_COLUMNS:
        return True
    for pattern in DROP_COLUMN_REGEXES:
        if pattern.match(column):
            return True
    return False


def _fetch_distinct_concepts(
    conn: psycopg.Connection,
    schema: str,
    table: str,
    column: str,
    limit: int,
) -> list[tuple[str, int]]:
    """Top-N concept names and their counts for a fact table's group column."""
    try:
        with conn.cursor() as cur:
            cur.execute(
                DISTINCT_CONCEPTS_SQL_TEMPLATE.format(
                    schema=schema, table=table, column=column
                ),
                (limit,),
            )
            return [(str(row[0]), int(row[1])) for row in cur.fetchall()]
    except Exception as exc:
        sys.stderr.write(
            f"  skipping concept enumeration on {table}.{column}: {exc}\n"
        )
        conn.rollback()
        return []


def _column_lookup(columns: list[ColumnInfo]) -> dict[tuple[str, str], ColumnInfo]:
    return {(c.table, c.column): c for c in columns}


def build_curated_variables(
    conn: psycopg.Connection,
    schema: str,
    columns: list[ColumnInfo],
) -> list[dict[str, str]]:
    """Apply ``CURATION_RULES`` to the raw inventory and return Century-format
    Variables-sheet rows ready for ``pd.DataFrame``.

    Rows for fact tables (observation, measurement, etc.) are produced by
    querying the distinct concept_name values and emitting one row per concept.
    Demographic tables produce one row per curated column. Documents emit
    hard-coded Unstructured rows.
    """
    lookup = _column_lookup(columns)
    rows: list[dict[str, str]] = []

    for table_name, recipe in CURATION_RULES.items():
        mode = recipe["mode"]

        # --- per_concept: one dictionary row per distinct concept_name value
        if mode == "per_concept":
            group_col = recipe["group_by"]
            info = lookup.get((table_name, group_col))
            if info is None:
                continue
            concepts = _fetch_distinct_concepts(
                conn, schema, table_name, group_col, recipe.get("max_concepts", 30)
            )
            for name, count in concepts:
                variable = name
                if len(variable) > 64:
                    variable = variable[:61] + "..."
                description = recipe.get(
                    "description_template", "Concept '{name}' captured for the patient."
                ).format(name=name)
                rows.append({
                    "Category": recipe["category"],
                    "Variable": variable,
                    "Description": description,
                    "Schema": table_name,
                    "Column(s)": recipe["value_column"],
                    "Criteria": recipe["criteria_template"].format(name=name),
                    "Values": "",
                    "Distribution": f"{name}: {count}",
                    "Completeness": f"{info.completeness_pct:.1f}%",
                    "Extraction Type": recipe["extraction_type"],
                    "Notes": "",
                })
            continue

        # --- single_row_with_list: one row summarising the whole fact table
        if mode == "single_row_with_list":
            group_col = recipe["group_by"]
            info = lookup.get((table_name, group_col))
            if info is None:
                continue
            concepts = _fetch_distinct_concepts(
                conn, schema, table_name, group_col, recipe.get("max_values", 50)
            )
            values_cell = ", ".join(name for name, _ in concepts[:20])
            distribution_cell = "; ".join(f"{n}: {c}" for n, c in concepts[:5])
            rows.append({
                "Category": recipe["category"],
                "Variable": recipe["variable"],
                "Description": recipe["description"],
                "Schema": table_name,
                "Column(s)": recipe["value_column"],
                "Criteria": "",
                "Values": values_cell,
                "Distribution": distribution_cell,
                "Completeness": f"{info.completeness_pct:.1f}%",
                "Extraction Type": recipe["extraction_type"],
                "Notes": "",
            })
            continue

        # --- split_by_type: one row per pre-declared drug type
        if mode == "split_by_type":
            group_col = recipe["group_by"]
            info = lookup.get((table_name, group_col))
            if info is None:
                continue
            for drug_type, split_cfg in recipe["splits"].items():
                rows.append({
                    "Category": split_cfg["category"],
                    "Variable": split_cfg["variable"],
                    "Description": split_cfg["description"],
                    "Schema": table_name,
                    "Column(s)": recipe["value_column"],
                    "Criteria": recipe["criteria_template"].format(name=drug_type),
                    "Values": "",
                    "Distribution": "",
                    "Completeness": f"{info.completeness_pct:.1f}%",
                    "Extraction Type": recipe["extraction_type"],
                    "Notes": "",
                })
            continue

        # --- keep_columns: one row per named demographic column
        if mode == "keep_columns":
            for col_name, cfg in recipe["columns"].items():
                info = lookup.get((table_name, col_name))
                if info is None:
                    continue
                rows.append({
                    "Category": recipe["category"],
                    "Variable": cfg["variable"],
                    "Description": cfg["description"],
                    "Schema": table_name,
                    "Column(s)": col_name,
                    "Criteria": "",
                    "Values": info.top_values[0][0] if info.top_values else "",
                    "Distribution": info.distribution_cell(),
                    "Completeness": f"{info.completeness_pct:.1f}%",
                    "Extraction Type": "Structured",
                    "Notes": "",
                })
            continue

        # --- static: hard-coded Unstructured rows for notes / documents.
        # Only emit rows when the table actually exists in the inventory -
        # skip gracefully when the schema doesn't carry this optional table.
        if mode == "static":
            present_tables = {c.table for c in columns}
            if table_name not in present_tables:
                continue
            for spec in recipe["rows"]:
                info = lookup.get((table_name, spec["column"]))
                completeness = (
                    f"{info.completeness_pct:.1f}%" if info else "100%"
                )
                rows.append({
                    "Category": recipe["category"],
                    "Variable": spec["variable"],
                    "Description": spec["description"],
                    "Schema": table_name,
                    "Column(s)": spec["column"],
                    "Criteria": spec.get("criteria", ""),
                    "Values": "",
                    "Distribution": "",
                    "Completeness": completeness,
                    "Extraction Type": spec["extraction_type"],
                    "Notes": "",
                })

    return rows


def write_curated_xlsx(
    conn: psycopg.Connection,
    schema: str,
    columns: list[ColumnInfo],
    tables: list[TableInfo],
    out_path: Path,
    cohort: str,
    person_count: int | None,
) -> None:
    """Write a curated Century-format workbook: one row per business variable."""
    try:
        import pandas as pd
    except ImportError as exc:
        sys.stderr.write(
            "pandas is not installed; install it to emit XLSX: "
            "pip install pandas openpyxl\n"
        )
        raise SystemExit(3) from exc

    variables_rows = build_curated_variables(conn, schema, columns)

    summary_rows = [
        {"metric": "cohort", "value": cohort},
        {"metric": "output_kind", "value": "curated"},
        {"metric": "variable_count", "value": len(variables_rows)},
        {"metric": "source_table_count", "value": len(tables)},
        {"metric": "source_column_count", "value": len(columns)},
    ]
    if person_count is not None:
        summary_rows.insert(1, {"metric": "patient_count", "value": person_count})
    summary_df = pd.DataFrame(summary_rows, columns=["metric", "value"])

    curated_tables = sorted({row["Schema"] for row in variables_rows})
    tables_df = pd.DataFrame(
        [
            {
                "table": t.name,
                "row_count": t.row_count,
                "column_count": t.column_count,
                "included_in_dictionary": "yes" if t.name in curated_tables else "no",
                "description": "",
            }
            for t in tables
        ]
    )

    variables_df = pd.DataFrame(
        variables_rows,
        columns=[
            "Category",
            "Variable",
            "Description",
            "Schema",
            "Column(s)",
            "Criteria",
            "Values",
            "Distribution",
            "Completeness",
            "Extraction Type",
            "Notes",
        ],
    )

    with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
        summary_df.to_excel(writer, sheet_name="Summary", index=False)
        tables_df.to_excel(writer, sheet_name="Tables", index=False)
        variables_df.to_excel(writer, sheet_name="Variables", index=False)

    print(
        f"\nWrote curated dictionary -> {out_path}\n"
        f"  {len(variables_rows)} variable(s) covering {len(curated_tables)} source table(s)\n"
        f"  Fill in Description (and review Category) where blank\n"
        f"  Then validate: python validate_dictionary.py --input {out_path}",
        file=sys.stderr,
    )


# --------------------------------------------------------------------------- #
# CLI
# --------------------------------------------------------------------------- #


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Introspect a Postgres schema and (optionally) emit a Century-format "
            "dictionary workbook drafted straight from the database."
        )
    )
    parser.add_argument(
        "--schema",
        default="mtc_aat_cohort",
        help="Schema to introspect (default: mtc_aat_cohort).",
    )
    parser.add_argument(
        "--out-csv",
        type=Path,
        default=None,
        help="Write raw introspection to this CSV (one row per column).",
    )
    parser.add_argument(
        "--out-xlsx",
        type=Path,
        default=None,
        help=(
            "Write a curated Century-style dictionary workbook "
            "(one row per business variable; fact tables collapsed by "
            "concept_name). This is usually what you want."
        ),
    )
    parser.add_argument(
        "--out-xlsx-raw",
        type=Path,
        default=None,
        help=(
            "Write the raw inventory as a workbook (one row per source "
            "column; includes every plumbing field). Useful for QA only."
        ),
    )
    parser.add_argument(
        "--sample-values",
        type=int,
        default=5,
        help="Top-N frequent values per column (0 to disable).",
    )
    parser.add_argument(
        "--list-schemas",
        action="store_true",
        help="Connect, list every accessible schema + object count, then exit.",
    )

    # Optional overrides for env-var credentials.
    parser.add_argument("--host")
    parser.add_argument("--port", type=int)
    parser.add_argument("--database")
    parser.add_argument("--user")
    parser.add_argument("--password")
    parser.add_argument("--sslmode")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    conn_kwargs = build_conn_kwargs(args)

    print(
        f"Connecting to {conn_kwargs['host']}:{conn_kwargs['port']}/"
        f"{conn_kwargs['dbname']} as {conn_kwargs['user']}...",
        file=sys.stderr,
    )

    with psycopg.connect(**conn_kwargs) as conn:
        if args.list_schemas:
            with conn.cursor() as cur:
                cur.execute(LIST_SCHEMAS_SQL)
                rows = cur.fetchall()
            print(f"{'schema':<45} objects")
            print("-" * 55)
            for name, count in rows:
                print(f"{name:<45} {count}")
            return 0

        columns, tables = introspect(
            conn,
            schema=args.schema,
            sample_values=args.sample_values,
            include_top_for_types=SAMPLEABLE_TYPES,
        )
        person_count = fetch_person_count(conn, args.schema)

        # The curated writer needs an open connection to enumerate concepts,
        # so run it inside the ``with`` block.
        if args.out_xlsx:
            write_curated_xlsx(
                conn=conn,
                schema=args.schema,
                columns=columns,
                tables=tables,
                out_path=args.out_xlsx,
                cohort=args.schema,
                person_count=person_count,
            )

    print_tree(columns)

    if args.out_csv:
        write_raw_csv(columns, args.out_csv)

    if args.out_xlsx_raw:
        write_dictionary_xlsx(
            columns=columns,
            tables=tables,
            out_path=args.out_xlsx_raw,
            cohort=args.schema,
            person_count=person_count,
        )

    return 0 if columns else 1


if __name__ == "__main__":
    raise SystemExit(main())
