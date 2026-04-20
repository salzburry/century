#!/usr/bin/env python3
"""Introspect a Postgres cohort schema and write a data dictionary.

One file, end-to-end:

    1. Connect to the warehouse using PG* env vars (or a local ``.env``).
    2. Walk every table in the cohort schema.
    3. For every column, record data type, row count, null count,
       completeness %, and a typed summary:
         * continuous  -> Min / Max / Mean (std) and Median (IQR)
         * date        -> Min / Max
         * categorical -> top-N value counts with percentages
         * text        -> no summary (marked Unstructured)
    4. Emit the dictionary as Excel and/or HTML. Same content, same
       column order as the Century reference PDF:

         Category | Variable | Description | Table(s) | Column(s) |
         Criteria | Values | Distribution | Median (IQR) |
         Completeness | Extraction Type | Notes

Typical usage::

    # Copy .env.example to .env and fill in real credentials.
    python introspect_cohort.py --cohort mtc_aat \\
        --out-xlsx mtc_aat_cohort.xlsx \\
        --out-html mtc_aat_cohort.html

Dependencies (see ``requirements.txt``)::

    pip install 'psycopg[binary]' pandas openpyxl pyyaml
"""

from __future__ import annotations

import argparse
import os
import re
import sys
from dataclasses import dataclass, field
from pathlib import Path

# NOTE: psycopg is imported lazily inside ``_require_psycopg()`` so the
# module can be imported for offline tasks (``--list-cohorts``, unit tests
# that stub the connection) without the DB driver installed. Type hints use
# string forward references via ``from __future__ import annotations``, so
# referencing ``psycopg.Connection`` in a signature does not trigger the
# import.
class MissingDependencyError(RuntimeError):
    """Raised when a lazily-imported dependency is unavailable at call time.

    Using a regular exception (not ``SystemExit``) means the test runner
    can report the single failing test cleanly, and the CLI ``main()`` can
    translate it into a non-zero exit with a user-friendly stderr line.
    """


def _require_psycopg():
    """Lazy import the ``psycopg`` driver, or raise a clear install hint."""
    try:
        import psycopg  # noqa: F401 - returned for local use
        return psycopg
    except ImportError as exc:
        raise MissingDependencyError(
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
# different cwd still works. We only log the count when the module is being
# run as the CLI entry point - when imported for tests, keep silent so the
# test output stays clean.
_DOTENV_PATH = Path(__file__).resolve().parent / ".env"
_loaded = load_dotenv(_DOTENV_PATH)
if _loaded and __name__ == "__main__":
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


# ------------------------------------------------------------------- #
# Metric-kind classification. Drives what summary we try to compute
# for each column in ``introspect()`` and which cell gets filled on
# the ``All Columns`` sheet.
# ------------------------------------------------------------------- #

_NUMERIC_TYPES = frozenset({
    "integer", "bigint", "smallint",
    "numeric", "real", "double precision",
})

_DATE_TYPES = frozenset({
    "date",
    "timestamp without time zone",
    "timestamp with time zone",
    "time without time zone",
    "time with time zone",
})

# Long free-form content: no summary. Everything else that isn't
# numeric/date is treated as categorical (top-N with counts).
_UNSTRUCTURED_TYPES = frozenset({"text"})


def _classify_metric_kind(data_type: str) -> str:
    """Return 'continuous' | 'date' | 'unstructured' | 'categorical'."""
    if data_type in _NUMERIC_TYPES:
        return "continuous"
    if data_type in _DATE_TYPES:
        return "date"
    if data_type in _UNSTRUCTURED_TYPES:
        return "unstructured"
    return "categorical"


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
    # Pre-formatted summary cells. At most one of these three is
    # populated for any given column, based on the column's
    # metric kind (see ``_classify_metric_kind``):
    #   categorical  -> value_distribution
    #   continuous   -> numeric_summary + median_iqr
    #   date         -> numeric_summary (min/max only)
    #   unstructured -> all three blank
    value_distribution: str = ""   # "Female: 620 (58.1%); Male: 440 (41.3%)"
    numeric_summary: str = ""       # "Min: 1924, Max: 2002, Mean: 1958 (std: 12.3)"
    median_iqr: str = ""            # "Median: 1958 (IQR: 1948-1968)"

    # Categorical top-N (legacy field retained for keep_columns recipes).
    top_values: list[tuple[str, int]] = field(default_factory=list)

    def distribution_cell(self) -> str:
        if self.value_distribution:
            return self.value_distribution
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
    quiet: bool = False,
) -> tuple[list[ColumnInfo], list[TableInfo]]:
    """Walk the target schema and return ``(columns, tables)``.

    Dictionary-as-documentation policy: run typed metrics on **every
    column of every accessible table**. No sampling gates.

    For each column the generator computes:

      null_count + completeness   always
      value_distribution          if metric kind == categorical
      numeric_summary             if metric kind == continuous or date
      median_iqr                  if metric kind == continuous

    Metric kind is inferred from the Postgres data type
    (``_classify_metric_kind``):

      numeric types   -> continuous   (Min, Max, Mean (std), Median IQR)
      date/timestamp  -> date         (Min, Max only)
      text            -> unstructured (all three cells blank)
      everything else -> categorical  (value counts with %)

    If a column cannot be read (e.g. column-level permission denial),
    each summary query is wrapped in its own try/except so the row
    stays listed in the inventory with blank stats.
    """
    columns_out: list[ColumnInfo] = []
    tables_out: list[TableInfo] = []

    with conn.cursor() as cur:
        cur.execute(LIST_TABLES_SQL, (schema,))
        table_rows = cur.fetchall()
        tables = [row[0] for row in table_rows]
        view_count = sum(1 for _, t in table_rows if t == "VIEW")
        if view_count and not quiet:
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
            raw_columns = cur.fetchall()

        tables_out.append(
            TableInfo(name=table, row_count=row_count, column_count=len(raw_columns))
        )
        if not quiet:
            print(
                f"  {schema}.{table}  ({row_count:,} rows, {len(raw_columns)} cols)",
                file=sys.stderr,
            )

        for col_row in raw_columns:
            column_name, data_type, is_nullable_str, _max_len, _prec = col_row
            is_nullable = is_nullable_str == "YES"
            kind = _classify_metric_kind(data_type)

            null_count = 0
            value_distribution = ""
            numeric_summary = ""
            median_iqr = ""
            top_values: list[tuple[str, int]] = []

            if row_count > 0:
                null_count = _safe_null_count(
                    conn, schema, table, column_name
                )

                if kind == "continuous":
                    numeric_summary, median_iqr = _compile_continuous(
                        conn, schema, table, column_name
                    )
                elif kind == "date":
                    numeric_summary = _compile_date_range(
                        conn, schema, table, column_name
                    )
                elif kind == "categorical":
                    top_values = _compile_top_values(
                        conn, schema, table, column_name,
                        limit=sample_values if sample_values > 0 else 5,
                    )
                    value_distribution = _format_value_distribution(
                        top_values, null_count, row_count
                    )
                # kind == "unstructured": all three summary cells stay blank.

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
                    value_distribution=value_distribution,
                    numeric_summary=numeric_summary,
                    median_iqr=median_iqr,
                    top_values=top_values,
                )
            )

    return columns_out, tables_out


# --------------------------------------------------------------------------- #
# Per-column summary helpers used during introspection
# --------------------------------------------------------------------------- #


def _safe_null_count(
    conn: psycopg.Connection, schema: str, table: str, column: str
) -> int:
    """``COUNT(*) WHERE col IS NULL`` with a try/except so one locked-down
    column doesn't break the rest of the run."""
    try:
        with conn.cursor() as cur:
            cur.execute(
                NULL_COUNT_SQL_TEMPLATE.format(
                    schema=schema, table=table, column=column
                )
            )
            return int(cur.fetchone()[0])
    except Exception as exc:
        sys.stderr.write(
            f"  null-count failed on {table}.{column}: {exc}\n"
        )
        conn.rollback()
        return 0


_CONTINUOUS_SUMMARY_SQL = """
SELECT
  MIN("{column}")::text,
  MAX("{column}")::text,
  AVG("{column}")::text,
  STDDEV_SAMP("{column}")::text,
  PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY "{column}")::text,
  PERCENTILE_CONT(0.5)  WITHIN GROUP (ORDER BY "{column}")::text,
  PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY "{column}")::text
FROM "{schema}"."{table}"
WHERE "{column}" IS NOT NULL;
"""


def _fmt_num(value: str | None) -> str:
    """Format a numeric string to at most 3 decimals; pass through if
    it's already short or non-numeric."""
    if value is None:
        return "—"
    try:
        f = float(value)
    except (TypeError, ValueError):
        return str(value)
    if f == int(f) and abs(f) < 1e15:
        return str(int(f))
    return f"{f:.3g}"


def _compile_continuous(
    conn: psycopg.Connection, schema: str, table: str, column: str
) -> tuple[str, str]:
    """Return ``(numeric_summary, median_iqr)`` for a numeric column.

    numeric_summary  -> 'Min: X, Max: Y, Mean: M (std: S)'
    median_iqr       -> 'Median: M (IQR: Q1–Q3)'

    Empty strings when the query fails or the column has no non-null rows.
    """
    try:
        with conn.cursor() as cur:
            cur.execute(
                _CONTINUOUS_SUMMARY_SQL.format(
                    schema=schema, table=table, column=column
                )
            )
            row = cur.fetchone()
    except Exception as exc:
        sys.stderr.write(
            f"  continuous summary failed on {table}.{column}: {exc}\n"
        )
        conn.rollback()
        return "", ""

    if not row or row[0] is None:
        return "", ""

    mn, mx, mean, std, q1, median, q3 = row
    numeric_summary = (
        f"Min: {_fmt_num(mn)}, Max: {_fmt_num(mx)}, "
        f"Mean: {_fmt_num(mean)} (std: {_fmt_num(std)})"
    )
    median_iqr = (
        f"Median: {_fmt_num(median)} (IQR: {_fmt_num(q1)}–{_fmt_num(q3)})"
    )
    return numeric_summary, median_iqr


_DATE_RANGE_SQL = """
SELECT MIN("{column}")::text, MAX("{column}")::text
FROM "{schema}"."{table}"
WHERE "{column}" IS NOT NULL;
"""


def _compile_date_range(
    conn: psycopg.Connection, schema: str, table: str, column: str
) -> str:
    """Min / Max of a date/timestamp column, or empty on failure / no data."""
    try:
        with conn.cursor() as cur:
            cur.execute(
                _DATE_RANGE_SQL.format(schema=schema, table=table, column=column)
            )
            row = cur.fetchone()
    except Exception as exc:
        sys.stderr.write(
            f"  date range failed on {table}.{column}: {exc}\n"
        )
        conn.rollback()
        return ""
    if not row or row[0] is None:
        return ""
    return f"Min: {row[0]}, Max: {row[1]}"


def _compile_top_values(
    conn: psycopg.Connection,
    schema: str,
    table: str,
    column: str,
    limit: int = 5,
) -> list[tuple[str, int]]:
    """Top-N value counts for a categorical column."""
    try:
        with conn.cursor() as cur:
            cur.execute(
                TOP_VALUES_SQL_TEMPLATE.format(
                    schema=schema, table=table, column=column
                ),
                (limit,),
            )
            return [(str(v), int(n)) for v, n in cur.fetchall()]
    except Exception as exc:
        sys.stderr.write(
            f"  top-values failed on {table}.{column}: {exc}\n"
        )
        conn.rollback()
        return []


def _format_value_distribution(
    top_values: list[tuple[str, int]],
    null_count: int,
    row_count: int,
) -> str:
    """Render ``[(value, n), ...]`` as ``"Female: 620 (58.1%); Male: 440 (41.3%)"``.

    Percentages are over ``row_count`` (including nulls) so they match
    the Completeness figure the reviewer sees on the same row.
    """
    if not top_values or row_count <= 0:
        return ""
    parts = []
    for value, count in top_values:
        display = value if len(value) <= 60 else value[:57] + "..."
        pct = 100.0 * count / row_count
        parts.append(f"{display}: {count} ({pct:.1f}%)")
    return "; ".join(parts)


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


try:
    import yaml  # PyYAML — imported lazily in _require_yaml()
except ImportError:  # pragma: no cover - handled at call time
    yaml = None  # type: ignore[assignment]


def _require_yaml():
    """Lazy getter for PyYAML. Raises ``MissingDependencyError`` if missing.

    Module-level ``import yaml`` would break offline tests and
    ``--list-cohorts`` for users who haven't pip-installed the package.
    """
    if yaml is None:
        raise MissingDependencyError(
            "PyYAML is not installed. Run: pip install pyyaml"
        )
    return yaml


PACKS_DIR = Path(__file__).resolve().parent / "packs"


@dataclass
class Pack:
    """Runtime view of ``packs/cohorts/<x>.yaml``.

    Declares the display name and warehouse schema for one cohort. The
    pack file may also carry an optional ``validator:`` section read by
    ``validate_dictionary.py``, but the generator itself only needs
    these three fields.
    """

    slug: str           # pack filename stem, e.g. "mtc_aat"
    cohort_name: str    # human-readable cohort label
    schema_name: str    # actual Postgres schema (e.g. "mtc__aat_cohort")


def load_pack(cohort: str, packs_dir: Path = PACKS_DIR) -> Pack:
    """Load ``packs/cohorts/<cohort>.yaml`` and return a ``Pack``."""
    cohort_path = packs_dir / "cohorts" / f"{cohort}.yaml"
    if not cohort_path.is_file():
        raise FileNotFoundError(
            f"cohort pack missing: {cohort_path}. Available: "
            + ", ".join(
                sorted(p.stem for p in (packs_dir / "cohorts").glob("*.yaml"))
            )
        )

    y = _require_yaml()
    data = y.safe_load(cohort_path.read_text(encoding="utf-8")) or {}

    cohort_name = data.get("cohort_name")
    schema_name = data.get("schema_name")
    if not cohort_name or not schema_name:
        raise ValueError(
            f"cohort pack {cohort_path} must define cohort_name and schema_name"
        )

    return Pack(
        slug=cohort,
        cohort_name=str(cohort_name),
        schema_name=str(schema_name),
    )


def available_cohorts(packs_dir: Path = PACKS_DIR) -> list[str]:
    return sorted(p.stem for p in (packs_dir / "cohorts").glob("*.yaml"))



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
    """Write a simple data dictionary workbook.

    Two sheets, nothing clever:

      Summary     cohort / patient_count / table_count / column_count
      Variables   one row per column of every accessible table, in the
                  column layout of the Century reference PDF:
                    Category | Variable | Description | Table(s) |
                    Column(s) | Criteria | Values | Distribution |
                    Median (IQR) | Completeness | Extraction Type |
                    Notes

    Distribution cell rules (by Postgres data type):
      categorical types  ``Female: 620 (58.1%); Male: 440 (41.3%); ...``
      numeric types      ``Min: X, Max: Y, Mean: M (std: S)``
      date/timestamp     ``Min: YYYY-MM-DD, Max: YYYY-MM-DD``
      text (unstructured)  blank

    Median (IQR) cell is populated only for numeric types.
    Category / Description / Criteria / Notes are left blank for the
    reviewer to fill in.
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

    variables_rows: list[dict[str, str]] = []
    for col in columns:
        kind = _classify_metric_kind(col.data_type)
        extraction_type = "Unstructured" if kind == "unstructured" else "Structured"
        # ``Distribution`` carries whichever typed summary was computed
        # for this column. Continuous rows get the Min/Max/Mean(std) form;
        # date rows get Min/Max; categorical rows get the value-count list.
        distribution_cell = col.value_distribution or col.numeric_summary
        # ``Values`` is a compact comma-joined list of the top categorical
        # values (no counts), mirroring the Alzheimer's reference.
        values_cell = ", ".join(v for v, _ in col.top_values[:10])

        variables_rows.append({
            "Category": "",
            "Variable": col.column,
            "Description": "",
            "Table(s)": col.table,
            "Column(s)": col.column,
            "Criteria": "",
            "Values": values_cell,
            "Distribution": distribution_cell,
            "Median (IQR)": col.median_iqr,
            "Completeness": f"{col.completeness_pct:.1f}%",
            "Extraction Type": extraction_type,
            "Notes": "",
        })

    variables_df = pd.DataFrame(
        variables_rows,
        columns=[
            "Category",
            "Variable",
            "Description",
            "Table(s)",
            "Column(s)",
            "Criteria",
            "Values",
            "Distribution",
            "Median (IQR)",
            "Completeness",
            "Extraction Type",
            "Notes",
        ],
    )

    with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
        summary_df.to_excel(writer, sheet_name="Summary", index=False)
        variables_df.to_excel(writer, sheet_name="Variables", index=False)

    # Print the full validate command including --cohort so the user
    print(
        f"\nWrote dictionary -> {out_path}\n"
        f"  {len(variables_rows)} column(s) across {len(tables)} source table(s)\n"
        f"  Fill in Category / Description / Criteria where blank",
        file=sys.stderr,
    )


def write_curated_html(
    columns: list[ColumnInfo],
    tables: list[TableInfo],
    out_path: Path,
    cohort: str,
    person_count: int | None,
) -> None:
    """Write the same data as ``write_curated_xlsx`` to an HTML file.

    Two tables - Summary and Variables - in the column layout of the
    Century reference PDF. Styled so it's readable straight in a browser
    without a stylesheet, but light enough to paste into an email.
    """
    try:
        import pandas as pd
    except ImportError as exc:
        sys.stderr.write(
            "pandas is not installed; install it to emit HTML: "
            "pip install pandas\n"
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

    variables_rows: list[dict[str, str]] = []
    for col in columns:
        kind = _classify_metric_kind(col.data_type)
        extraction_type = "Unstructured" if kind == "unstructured" else "Structured"
        distribution_cell = col.value_distribution or col.numeric_summary
        values_cell = ", ".join(v for v, _ in col.top_values[:10])
        variables_rows.append({
            "Category": "",
            "Variable": col.column,
            "Description": "",
            "Table(s)": col.table,
            "Column(s)": col.column,
            "Criteria": "",
            "Values": values_cell,
            "Distribution": distribution_cell,
            "Median (IQR)": col.median_iqr,
            "Completeness": f"{col.completeness_pct:.1f}%",
            "Extraction Type": extraction_type,
            "Notes": "",
        })
    variables_df = pd.DataFrame(
        variables_rows,
        columns=[
            "Category", "Variable", "Description",
            "Table(s)", "Column(s)", "Criteria",
            "Values", "Distribution", "Median (IQR)",
            "Completeness", "Extraction Type", "Notes",
        ],
    )

    summary_html = summary_df.to_html(index=False, escape=True, border=0)
    variables_html = variables_df.to_html(index=False, escape=True, border=0)

    page = f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>Data Dictionary — {cohort}</title>
<style>
  body {{ font-family: -apple-system, Segoe UI, Roboto, Arial, sans-serif;
          margin: 24px; color: #222; }}
  h1 {{ font-size: 1.4rem; margin-bottom: 4px; }}
  h2 {{ font-size: 1.1rem; margin-top: 28px; color: #444; }}
  table {{ border-collapse: collapse; font-size: 0.88rem;
           margin-top: 8px; }}
  th, td {{ border: 1px solid #ccc; padding: 6px 10px;
            vertical-align: top; text-align: left; }}
  th {{ background: #f2f4f7; font-weight: 600; }}
  tr:nth-child(even) td {{ background: #fafbfc; }}
  caption {{ caption-side: top; padding: 4px 0; font-weight: 600; }}
</style>
</head>
<body>
<h1>Data Dictionary</h1>
<div>Cohort: <code>{cohort}</code></div>

<h2>Summary</h2>
{summary_html}

<h2>Variables</h2>
{variables_html}
</body>
</html>
"""
    out_path.write_text(page, encoding="utf-8")
    print(
        f"\nWrote dictionary -> {out_path}\n"
        f"  {len(variables_rows)} column(s) across {len(tables)} source table(s)",
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
        default=None,
        help=(
            "Cohort pack to load from packs/cohorts/<cohort>.yaml. "
            "If omitted, --schema is required and no pack is needed. "
            "Use --list-cohorts to see available packs."
        ),
    )
    parser.add_argument(
        "--schema",
        default=None,
        help=(
            "Postgres schema to introspect. Required if --cohort is not "
            "given, and otherwise overrides the pack's schema_name."
        ),
    )
    parser.add_argument(
        "--list-cohorts",
        action="store_true",
        help="List available cohort packs and exit.",
    )
    parser.add_argument(
        "--out-xlsx",
        type=Path,
        default=None,
        help=(
            "Write the dictionary as an Excel workbook (Summary + "
            "Variables, one row per column of every table)."
        ),
    )
    parser.add_argument(
        "--out-html",
        type=Path,
        default=None,
        help=(
            "Write the dictionary as a single-page HTML file. Can be "
            "combined with --out-xlsx."
        ),
    )
    parser.add_argument(
        "--sample-values",
        type=int,
        default=5,
        help="Top-N frequent values per categorical column (0 to disable).",
    )
    parser.add_argument(
        "--list-schemas",
        action="store_true",
        help="Connect, list every accessible schema + object count, then exit.",
    )
    parser.add_argument(
        "--quiet",
        action="store_true",
        help=(
            "Suppress operational stderr chatter (connection line, "
            "per-table progress, column tree). Errors and the "
            "'Wrote ...' summary still print so the caller knows the "
            "result. Safer for piping into reports."
        ),
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
    """CLI entry point with friendly handling of the expected failure modes.

    Expected exceptions that should *not* surface as a traceback:

      MissingDependencyError  - pyyaml / psycopg not installed
      FileNotFoundError       - wrong --cohort name, no .env credentials,
                                missing input workbook
      ValueError              - malformed pack YAML, unsupported source
                                type on --input

    Everything else is a genuine crash and is allowed to propagate so
    the stacktrace lands in the log where someone can diagnose it.
    """
    try:
        return _main(argv)
    except MissingDependencyError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    except FileNotFoundError as exc:
        print(f"error: {exc}", file=sys.stderr)
        if "cohort pack missing" in str(exc):
            print(
                "hint: run 'python introspect_cohort.py --list-cohorts' "
                "to see available packs.",
                file=sys.stderr,
            )
        return 1
    except ValueError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1


def _main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    quiet = bool(getattr(args, "quiet", False))

    def chatter(msg: str) -> None:
        """Operational log-line helper. Suppressed under --quiet so the
        host / user / per-table chatter doesn't end up in exported
        reports or shoulder-surfed screenshots."""
        if not quiet:
            print(msg, file=sys.stderr)

    if args.list_cohorts:
        for name in available_cohorts():
            print(name)
        return 0

    # --list-schemas is diagnostic and shouldn't require a working
    # cohort pack. Handle it before we try to load one.
    if args.list_schemas:
        conn_kwargs = build_conn_kwargs(args)
        chatter(
            f"Connecting to {conn_kwargs['host']}:{conn_kwargs['port']}/"
            f"{conn_kwargs['dbname']} as {conn_kwargs['user']}..."
        )
        psycopg = _require_psycopg()
        with psycopg.connect(**conn_kwargs) as conn:
            with conn.cursor() as cur:
                cur.execute(LIST_SCHEMAS_SQL)
                rows = cur.fetchall()
        print(f"{'schema':<45} objects")
        print("-" * 55)
        for name, count in rows:
            print(f"{name:<45} {count}")
        return 0

    # Two ways to identify the cohort:
    #   --cohort NAME        read packs/cohorts/NAME.yaml (handy when you
    #                        reuse the same schema repeatedly and want the
    #                        display name and schema pinned in version
    #                        control)
    #   --schema NAME        skip the pack entirely; cohort_name is
    #                        synthesised from the schema name
    # If both are given, --cohort provides the display name and --schema
    # overrides the schema lookup.
    if args.cohort:
        pack = load_pack(args.cohort)
        schema_name = args.schema or pack.schema_name
        chatter(f"Loaded pack '{args.cohort}' (schema={schema_name})")
    elif args.schema:
        schema_name = args.schema
        pack = Pack(slug=schema_name, cohort_name=schema_name, schema_name=schema_name)
        chatter(f"Using schema '{schema_name}' (no pack)")
    else:
        print(
            "error: one of --cohort or --schema is required.",
            file=sys.stderr,
        )
        return 2

    conn_kwargs = build_conn_kwargs(args)

    chatter(
        f"Connecting to {conn_kwargs['host']}:{conn_kwargs['port']}/"
        f"{conn_kwargs['dbname']} as {conn_kwargs['user']}..."
    )

    psycopg = _require_psycopg()
    with psycopg.connect(**conn_kwargs) as conn:

        columns, tables = introspect(
            conn,
            schema=schema_name,
            sample_values=args.sample_values,
            pack=pack,
            quiet=quiet,
        )
        person_count = fetch_person_count(conn, schema_name)

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
        if args.out_html:
            write_curated_html(
                columns=columns,
                tables=tables,
                out_path=args.out_html,
                cohort=pack.cohort_name,
                person_count=person_count,
            )

    if not quiet:
        print_tree(columns)

    return 0 if columns else 1


if __name__ == "__main__":
    raise SystemExit(main())
