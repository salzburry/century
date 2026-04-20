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

# NOTE: psycopg is imported lazily inside ``_require_psycopg()`` so the
# module can be imported for offline tasks (``--list-cohorts``, unit tests
# that stub the connection) without the DB driver installed. Type hints use
# string forward references via ``from __future__ import annotations``, so
# referencing ``psycopg.Connection`` in a signature does not trigger the
# import.
def _require_psycopg():
    """Lazy import the ``psycopg`` driver, or exit with a clear install hint."""
    try:
        import psycopg  # noqa: F401 - returned for local use
        return psycopg
    except ImportError as exc:
        raise SystemExit(
            "psycopg is not installed. Run: pip install 'psycopg[binary]'"
        ) from exc


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

# Sampleable types are supplied via the core pack (``sampleable_types``);
# see ``Pack`` in the pack-loader section below.


def introspect(
    conn: psycopg.Connection,
    schema: str,
    sample_values: int,
    pack: "Pack",
) -> tuple[list[ColumnInfo], list[TableInfo]]:
    """Walk the target schema and return ``(columns, tables)``.

    The ``pack`` drives two filters applied *before* any SQL runs:

    * tables named in ``pack.tables_to_skip`` are skipped wholesale - no
      row count, no column listing, no null-count queries. Keeps PII-style
      linkage tables (``dv_tokenized_profile_data`` and similar) entirely
      out of the raw inventory and out of any downstream report.
    * individual columns that match ``pack.sensitive_columns`` (exact name)
      or ``pack.drop_column_patterns`` (regex) are filtered out as soon as
      ``information_schema.columns`` returns them, again before any
      null-count or top-value query hits them.

    Top-value sampling is additionally gated by ``pack.sampleable_types``
    so long free-text columns never have their contents selected.
    """
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

    skipped_tables = [t for t in tables if t in pack.tables_to_skip]
    tables = [t for t in tables if t not in pack.tables_to_skip]
    if skipped_tables:
        print(
            f"  (skipped {len(skipped_tables)} table(s) per pack.tables_to_skip: "
            f"{', '.join(sorted(skipped_tables))})",
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
            raw_columns = cur.fetchall()

        # Filter out sensitive / plumbing columns before we run any further
        # queries against them. We still record the table's full column
        # count for Summary accuracy.
        visible_columns = [
            row for row in raw_columns
            if not _column_is_dropped(table, row[0], pack)
        ]
        dropped = len(raw_columns) - len(visible_columns)

        tables_out.append(
            TableInfo(name=table, row_count=row_count, column_count=len(raw_columns))
        )
        print(
            f"  {schema}.{table}  ({row_count:,} rows, {len(raw_columns)} cols"
            + (f", {dropped} filtered" if dropped else "")
            + ")",
            file=sys.stderr,
        )

        for col_row in visible_columns:
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

                    if (
                        sample_values > 0
                        and data_type in pack.sampleable_types
                    ):
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
# Pack loader (core + cohort rules from YAML)
# --------------------------------------------------------------------------- #


try:
    import yaml  # PyYAML — imported lazily in _require_yaml()
except ImportError:  # pragma: no cover - handled at call time
    yaml = None  # type: ignore[assignment]


def _require_yaml():
    """Lazy getter for PyYAML. Exits with a clear install hint if missing.

    Module-level ``import yaml`` would break offline tests and
    ``--list-cohorts`` for users who haven't pip-installed the package,
    same anti-pattern as psycopg earlier.
    """
    if yaml is None:
        raise SystemExit(
            "PyYAML is not installed. Run: pip install pyyaml"
        )
    return yaml


PACKS_DIR = Path(__file__).resolve().parent / "packs"


@dataclass
class Pack:
    """Merged runtime view of ``packs/core.yaml`` + ``packs/cohorts/<x>.yaml``."""

    cohort_name: str
    schema_name: str
    tables_to_skip: set[str]
    sensitive_columns: set[str]
    drop_column_patterns: list[re.Pattern[str]]
    sampleable_types: set[str]
    curation_rules: dict[str, dict[str, Any]]


def _deep_merge(base: Any, overlay: Any) -> Any:
    """Merge ``overlay`` onto ``base`` with the rules we agreed on:

    * dicts → deep-merge (recurse per-key)
    * lists → append (overlay extends base; stable order)
    * scalars → replace

    Neither side is mutated.
    """
    if isinstance(base, dict) and isinstance(overlay, dict):
        merged: dict[str, Any] = {**base}
        for key, value in overlay.items():
            merged[key] = _deep_merge(base.get(key), value) if key in base else value
        return merged
    if isinstance(base, list) and isinstance(overlay, list):
        return [*base, *overlay]
    return overlay


def load_pack(cohort: str, packs_dir: Path = PACKS_DIR) -> Pack:
    """Load ``packs/core.yaml`` then overlay ``packs/cohorts/<cohort>.yaml``."""
    core_path = packs_dir / "core.yaml"
    cohort_path = packs_dir / "cohorts" / f"{cohort}.yaml"
    if not core_path.is_file():
        raise FileNotFoundError(f"core pack missing: {core_path}")
    if not cohort_path.is_file():
        raise FileNotFoundError(
            f"cohort pack missing: {cohort_path}. Available: "
            + ", ".join(
                sorted(p.stem for p in (packs_dir / "cohorts").glob("*.yaml"))
            )
        )

    y = _require_yaml()
    core_data = y.safe_load(core_path.read_text(encoding="utf-8")) or {}
    cohort_data = y.safe_load(cohort_path.read_text(encoding="utf-8")) or {}
    merged = _deep_merge(core_data, cohort_data)

    cohort_name = merged.get("cohort_name")
    schema_name = merged.get("schema_name")
    if not cohort_name or not schema_name:
        raise ValueError(
            f"cohort pack {cohort_path} must define cohort_name and schema_name"
        )

    return Pack(
        cohort_name=str(cohort_name),
        schema_name=str(schema_name),
        tables_to_skip=set(merged.get("tables_to_skip", [])),
        sensitive_columns=set(merged.get("sensitive_columns", [])),
        drop_column_patterns=[
            re.compile(p) for p in merged.get("drop_column_patterns", [])
        ],
        sampleable_types=set(merged.get("sampleable_types", [])),
        curation_rules=dict(merged.get("curation_rules", {})),
    )


def available_cohorts(packs_dir: Path = PACKS_DIR) -> list[str]:
    return sorted(p.stem for p in (packs_dir / "cohorts").glob("*.yaml"))


DISTINCT_CONCEPTS_SQL_TEMPLATE = """
SELECT "{column}"::text AS name, COUNT(*) AS n
FROM "{schema}"."{table}"
WHERE "{column}" IS NOT NULL
GROUP BY "{column}"
ORDER BY n DESC
LIMIT %s;
"""


# For per-concept rows on fact tables: one query returns, per concept,
# how many rows populate value_as_number / value_as_string /
# value_as_concept_name. Used to pick the right source column (and
# implicitly, the variable_type) per concept.
CONCEPT_VALUE_SHAPE_SQL_TEMPLATE = """
SELECT
  "{group_col}"::text AS name,
  COUNT(*) AS total,
  COUNT("value_as_number") AS n_num,
  COUNT(NULLIF(TRIM("value_as_string"::text), '')) AS n_str,
  COUNT("value_as_concept_name") AS n_concept
FROM "{schema}"."{table}"
WHERE "{group_col}" IS NOT NULL
GROUP BY "{group_col}"
ORDER BY total DESC
LIMIT %s;
"""


# Continuous summary for a single column (optionally scoped to a concept).
CONTINUOUS_SUMMARY_SQL_TEMPLATE = """
SELECT
  MIN("{column}")::text AS min_val,
  PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY "{column}")::text AS q1,
  PERCENTILE_CONT(0.5)  WITHIN GROUP (ORDER BY "{column}")::text AS median,
  PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY "{column}")::text AS q3,
  MAX("{column}")::text AS max_val
FROM "{schema}"."{table}"
WHERE "{column}" IS NOT NULL {concept_filter};
"""


# Date summary (min / max) for a single column.
DATE_SUMMARY_SQL_TEMPLATE = """
SELECT
  MIN("{column}")::text AS min_val,
  MAX("{column}")::text AS max_val
FROM "{schema}"."{table}"
WHERE "{column}" IS NOT NULL;
"""


# Categorical top-N with percentages for a single column.
CATEGORICAL_SUMMARY_SQL_TEMPLATE = """
WITH total AS (
    SELECT COUNT(*)::numeric AS n FROM "{schema}"."{table}"
    WHERE "{column}" IS NOT NULL
)
SELECT "{column}"::text AS val, COUNT(*) AS n,
       ROUND(100.0 * COUNT(*) / NULLIF((SELECT n FROM total), 0), 1) AS pct
FROM "{schema}"."{table}"
WHERE "{column}" IS NOT NULL
GROUP BY "{column}"
ORDER BY COUNT(*) DESC
LIMIT %s;
"""


def _fetch_concept_value_shape(
    conn: psycopg.Connection,
    schema: str,
    table: str,
    group_col: str,
    limit: int,
) -> list[tuple[str, int, int, int, int]]:
    """Return ``[(concept, total, n_num, n_str, n_concept), ...]`` for a fact
    table, ranked by total count. Used to auto-pick the value column per
    concept in per_concept mode.
    """
    try:
        with conn.cursor() as cur:
            cur.execute(
                CONCEPT_VALUE_SHAPE_SQL_TEMPLATE.format(
                    schema=schema, table=table, group_col=group_col
                ),
                (limit,),
            )
            return [
                (str(row[0]), int(row[1]), int(row[2]), int(row[3]), int(row[4]))
                for row in cur.fetchall()
            ]
    except Exception as exc:
        sys.stderr.write(
            f"  concept value-shape query failed on {table}.{group_col}: {exc}\n"
        )
        conn.rollback()
        return []


def _pick_value_column(n_num: int, n_str: int, n_concept: int) -> tuple[str, str]:
    """Return ``(column_name, variable_type)`` for the most-populated
    value_as_* on an OMOP fact row. Ties break toward value_as_number."""
    best = max(n_num, n_str, n_concept)
    if best == 0:
        return "value_as_number", "continuous"
    if n_num == best:
        return "value_as_number", "continuous"
    if n_str == best:
        return "value_as_string", "categorical"
    return "value_as_concept_name", "categorical"


def _summarize_continuous(
    conn: psycopg.Connection,
    schema: str,
    table: str,
    column: str,
    concept_col: str | None = None,
    concept_value: str | None = None,
) -> str:
    """Return a ``Min: ..., Q1: ..., Median: ..., Q3: ..., Max: ...`` string."""
    params: list[Any] = []
    concept_filter = ""
    if concept_col and concept_value is not None:
        concept_filter = f'AND "{concept_col}" = %s'
        params.append(concept_value)
    sql = CONTINUOUS_SUMMARY_SQL_TEMPLATE.format(
        schema=schema, table=table, column=column, concept_filter=concept_filter
    )
    try:
        with conn.cursor() as cur:
            cur.execute(sql, tuple(params))
            row = cur.fetchone()
        if not row or row[0] is None:
            return ""
        mn, q1, median, q3, mx = (str(v) if v is not None else "—" for v in row)
        return f"Min: {mn}, Q1: {q1}, Median: {median}, Q3: {q3}, Max: {mx}"
    except Exception as exc:
        sys.stderr.write(
            f"  continuous summary failed on {table}.{column}: {exc}\n"
        )
        conn.rollback()
        return ""


def _summarize_date(
    conn: psycopg.Connection, schema: str, table: str, column: str
) -> str:
    """Return ``Min: YYYY-MM-DD, Max: YYYY-MM-DD`` for a date/timestamp column."""
    try:
        with conn.cursor() as cur:
            cur.execute(
                DATE_SUMMARY_SQL_TEMPLATE.format(
                    schema=schema, table=table, column=column
                )
            )
            row = cur.fetchone()
        if not row or row[0] is None:
            return ""
        return f"Min: {row[0]}, Max: {row[1]}"
    except Exception as exc:
        sys.stderr.write(f"  date summary failed on {table}.{column}: {exc}\n")
        conn.rollback()
        return ""


def _summarize_categorical_for_concept(
    conn: psycopg.Connection,
    schema: str,
    table: str,
    column: str,
    concept_col: str,
    concept_value: str,
    limit: int = 5,
) -> str:
    """Top-N values of ``column`` scoped to rows where ``concept_col =
    concept_value``. Returns ``val: n (pct%); ...``.
    """
    sql = f"""
        WITH scoped AS (
            SELECT "{column}"::text AS val
            FROM "{schema}"."{table}"
            WHERE "{concept_col}" = %s AND "{column}" IS NOT NULL
        )
        SELECT val, COUNT(*) AS n,
               ROUND(100.0 * COUNT(*) / NULLIF((SELECT COUNT(*) FROM scoped), 0), 1) AS pct
        FROM scoped
        GROUP BY val
        ORDER BY COUNT(*) DESC
        LIMIT %s;
    """
    try:
        with conn.cursor() as cur:
            cur.execute(sql, (concept_value, limit))
            fetched = cur.fetchall()
    except Exception as exc:
        sys.stderr.write(
            f"  categorical-for-concept failed on {table}.{column}: {exc}\n"
        )
        conn.rollback()
        return ""
    parts = []
    for val, n, pct in fetched:
        display = str(val) if len(str(val)) <= 60 else str(val)[:57] + "..."
        pct_str = f"{pct:.1f}" if pct is not None else "—"
        parts.append(f"{display}: {n} ({pct_str}%)")
    return "; ".join(parts)


def _summarize_categorical(
    conn: psycopg.Connection,
    schema: str,
    table: str,
    column: str,
    limit: int = 5,
) -> str:
    """Return ``val1: n (pct%), val2: n (pct%), ...`` for a categorical column."""
    try:
        with conn.cursor() as cur:
            cur.execute(
                CATEGORICAL_SUMMARY_SQL_TEMPLATE.format(
                    schema=schema, table=table, column=column
                ),
                (limit,),
            )
            rows_out = cur.fetchall()
        parts = []
        for val, n, pct in rows_out:
            display = str(val) if len(str(val)) <= 60 else str(val)[:57] + "..."
            pct_str = f"{pct:.1f}" if pct is not None else "—"
            parts.append(f"{display}: {n} ({pct_str}%)")
        return "; ".join(parts)
    except Exception as exc:
        sys.stderr.write(
            f"  categorical summary failed on {table}.{column}: {exc}\n"
        )
        conn.rollback()
        return ""


def _column_is_dropped(table: str, column: str, pack: Pack) -> bool:
    if table in pack.tables_to_skip:
        return True
    if column in pack.sensitive_columns:
        return True
    for pattern in pack.drop_column_patterns:
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
    pack: Pack,
) -> list[dict[str, str]]:
    """Apply ``pack.curation_rules`` to the raw inventory and return
    Century-format Variables-sheet rows ready for ``pd.DataFrame``.

    Rows for fact tables (observation, measurement, etc.) are produced by
    querying the distinct concept_name values and emitting one row per concept.
    Demographic tables produce one row per curated column. Documents emit
    hard-coded Unstructured rows.
    """
    lookup = _column_lookup(columns)
    rows: list[dict[str, str]] = []

    for table_name, recipe in pack.curation_rules.items():
        mode = recipe["mode"]

        # --- per_concept: one dictionary row per distinct concept_name value.
        # Per concept, we look at the shape of value_as_number /
        # value_as_string / value_as_concept_name to pick the right source
        # column and variable type, then fetch a typed summary (continuous
        # stats for numbers, top-N with pct for categoricals).
        if mode == "per_concept":
            group_col = recipe["group_by"]
            info = lookup.get((table_name, group_col))
            if info is None:
                continue
            shapes = _fetch_concept_value_shape(
                conn, schema, table_name, group_col,
                recipe.get("max_concepts", 30),
            )
            if not shapes:
                # Fall back to simple concept count if the shape query failed.
                for name, count in _fetch_distinct_concepts(
                    conn, schema, table_name, group_col,
                    recipe.get("max_concepts", 30),
                ):
                    shapes.append((name, count, 0, 0, 0))

            fallback_col = recipe.get("value_column", "value_as_number")
            for name, total, n_num, n_str, n_concept in shapes:
                value_column, variable_type = _pick_value_column(
                    n_num, n_str, n_concept
                )
                # Honour the pack's declared value_column if the data is
                # ambiguous (total = 0 rows populated in any of them).
                if n_num + n_str + n_concept == 0:
                    value_column = fallback_col

                # Per-concept completeness for the column we actually show.
                # The ``info`` completeness (looked up from the grouping
                # column) would describe concept_name coverage, not the
                # value column, and would overstate sparse concepts. Using
                # the shape counts avoids another round-trip.
                column_populated = {
                    "value_as_number": n_num,
                    "value_as_string": n_str,
                    "value_as_concept_name": n_concept,
                }.get(value_column, 0)
                row_completeness = (
                    f"{(100 * column_populated / total):.1f}%"
                    if total > 0
                    else "0.0%"
                )

                if variable_type == "continuous":
                    distribution = _summarize_continuous(
                        conn, schema, table_name, value_column,
                        concept_col=group_col, concept_value=name,
                    )
                else:
                    # Categorical-within-concept: query distinct values of
                    # the picked column, filtered to this concept.
                    distribution = _summarize_categorical_for_concept(
                        conn, schema, table_name, value_column,
                        concept_col=group_col, concept_value=name,
                    )

                if not distribution:
                    distribution = f"{name}: {total}"  # fall back to concept count

                variable = name if len(name) <= 64 else name[:61] + "..."
                description = recipe.get(
                    "description_template",
                    "Concept '{name}' captured for the patient.",
                ).format(name=name)
                rows.append({
                    "Category": recipe["category"],
                    "Variable": variable,
                    "Description": description,
                    "Schema": table_name,
                    "Column(s)": value_column,
                    "Criteria": recipe["criteria_template"].format(name=name),
                    "Values": "",
                    "Distribution": distribution,
                    "Completeness": row_completeness,
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
                # Scope the top-N of ``value_column`` to rows where
                # ``group_by = drug_type``. Fills Values + Distribution so
                # the validator does not warn missing_value_context.
                distribution = _summarize_categorical_for_concept(
                    conn,
                    schema,
                    table_name,
                    recipe["value_column"],
                    concept_col=group_col,
                    concept_value=drug_type,
                )
                # Derive a compact Values cell from the same top-N (drug
                # names only, no counts/pct) so reviewers see a quick
                # example without re-running a query.
                values_cell = ", ".join(
                    seg.split(":", 1)[0].strip()
                    for seg in distribution.split(";")
                    if seg.strip()
                )[:400]  # keep the cell manageable in Excel

                # Fallback when the drug type is declared in the pack but
                # absent from (or silent in) the data: avoid blanks so the
                # validator does not fire missing_value_context. The
                # placeholder still tells a reviewer which split this row
                # belongs to.
                if not distribution:
                    distribution = f"(no rows with {group_col} = '{drug_type}')"
                if not values_cell:
                    values_cell = drug_type
                rows.append({
                    "Category": split_cfg["category"],
                    "Variable": split_cfg["variable"],
                    "Description": split_cfg["description"],
                    "Schema": table_name,
                    "Column(s)": recipe["value_column"],
                    "Criteria": recipe["criteria_template"].format(name=drug_type),
                    "Values": values_cell,
                    "Distribution": distribution,
                    "Completeness": f"{info.completeness_pct:.1f}%",
                    "Extraction Type": recipe["extraction_type"],
                    "Notes": "",
                })
            continue

        # --- keep_columns: one row per named demographic column.
        # Each column spec may carry ``variable_type`` to drive the summary:
        #   categorical - top values with pct
        #   continuous  - Min/Q1/Median/Q3/Max
        #   date        - Min/Max
        #   identifier  - row skipped entirely
        #   free_text   - row emitted with Unstructured extraction, no stats
        if mode == "keep_columns":
            for col_name, cfg in recipe["columns"].items():
                info = lookup.get((table_name, col_name))
                if info is None:
                    continue
                variable_type = cfg.get("variable_type", "categorical")
                if variable_type == "identifier":
                    continue

                extraction_type = "Structured"
                values_cell = ""
                distribution_cell = ""

                if variable_type == "continuous":
                    distribution_cell = _summarize_continuous(
                        conn, schema, table_name, col_name
                    )
                elif variable_type == "date":
                    distribution_cell = _summarize_date(
                        conn, schema, table_name, col_name
                    )
                elif variable_type == "free_text":
                    extraction_type = "Unstructured"
                else:  # categorical (default)
                    distribution_cell = _summarize_categorical(
                        conn, schema, table_name, col_name
                    ) or info.distribution_cell()
                    values_cell = info.top_values[0][0] if info.top_values else ""

                rows.append({
                    "Category": recipe["category"],
                    "Variable": cfg["variable"],
                    "Description": cfg["description"],
                    "Schema": table_name,
                    "Column(s)": col_name,
                    "Criteria": "",
                    "Values": values_cell,
                    "Distribution": distribution_cell,
                    "Completeness": f"{info.completeness_pct:.1f}%",
                    "Extraction Type": extraction_type,
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
                # Give the validator something in Values so the unstructured
                # row does not trip missing_value_context. The point of the
                # cell for unstructured rows is to tell a reviewer what
                # kind of content lives in the source field, not to
                # enumerate values.
                values_cell = spec.get(
                    "values", f"Unstructured content - see {spec['column']}."
                )
                distribution_cell = spec.get(
                    "distribution",
                    f"{info.row_count:,} records" if info else "",
                )
                rows.append({
                    "Category": recipe["category"],
                    "Variable": spec["variable"],
                    "Description": spec["description"],
                    "Schema": table_name,
                    "Column(s)": spec["column"],
                    "Criteria": spec.get("criteria", ""),
                    "Values": values_cell,
                    "Distribution": distribution_cell,
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
    pack: Pack,
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

    variables_rows = build_curated_variables(conn, schema, columns, pack)

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
        "--cohort",
        default="mtc_aat",
        help=(
            "Cohort pack to load from packs/cohorts/<cohort>.yaml "
            "(default: mtc_aat). Use --list-cohorts to see available packs."
        ),
    )
    parser.add_argument(
        "--schema",
        default=None,
        help=(
            "Override the pack's schema_name. Default: the schema_name "
            "declared in the cohort pack."
        ),
    )
    parser.add_argument(
        "--list-cohorts",
        action="store_true",
        help="List available cohort packs and exit.",
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

    if args.list_cohorts:
        for name in available_cohorts():
            print(name)
        return 0

    pack = load_pack(args.cohort)
    schema_name = args.schema or pack.schema_name
    print(f"Loaded pack '{args.cohort}' (schema={schema_name})", file=sys.stderr)

    conn_kwargs = build_conn_kwargs(args)

    print(
        f"Connecting to {conn_kwargs['host']}:{conn_kwargs['port']}/"
        f"{conn_kwargs['dbname']} as {conn_kwargs['user']}...",
        file=sys.stderr,
    )

    psycopg = _require_psycopg()
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
            schema=schema_name,
            sample_values=args.sample_values,
            pack=pack,
        )
        person_count = fetch_person_count(conn, schema_name)

        # The curated writer needs an open connection to enumerate concepts,
        # so run it inside the ``with`` block.
        if args.out_xlsx:
            write_curated_xlsx(
                conn=conn,
                schema=schema_name,
                columns=columns,
                tables=tables,
                out_path=args.out_xlsx,
                cohort=pack.cohort_name,
                person_count=person_count,
                pack=pack,
            )

    print_tree(columns)

    if args.out_csv:
        write_raw_csv(columns, args.out_csv)

    if args.out_xlsx_raw:
        write_dictionary_xlsx(
            columns=columns,
            tables=tables,
            out_path=args.out_xlsx_raw,
            cohort=pack.cohort_name,
            person_count=person_count,
        )

    return 0 if columns else 1


if __name__ == "__main__":
    raise SystemExit(main())
