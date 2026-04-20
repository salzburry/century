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

    export PGHOST=chealth-stage-db.ct4u6ogcouw8.us-east-1.rds.amazonaws.com
    export PGDATABASE=clinical
    export PGUSER=onkar
    export PGPASSWORD='***'

    python introspect_cohort.py                                    # tree only
    python introspect_cohort.py --out-csv fields.csv               # + CSV
    python introspect_cohort.py --out-xlsx mtc_aat_cohort.xlsx     # + workbook

    # then edit the XLSX (category/description/criteria/extraction_type)
    python validate_dictionary.py --input mtc_aat_cohort.xlsx

Dependencies::

    pip install 'psycopg[binary]' pandas openpyxl
"""

from __future__ import annotations

import argparse
import csv
import os
import sys
from dataclasses import dataclass
from pathlib import Path

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
SELECT table_name
FROM information_schema.tables
WHERE table_schema = %s
  AND table_type = 'BASE TABLE'
ORDER BY table_name;
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
        tables = [row[0] for row in cur.fetchall()]

    if not tables:
        sys.stderr.write(f"No tables found in schema '{schema}'.\n")
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
        help="Write a Century-format dictionary workbook (Summary/Tables/Variables).",
    )
    parser.add_argument(
        "--sample-values",
        type=int,
        default=5,
        help="Top-N frequent values per column (0 to disable).",
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
        columns, tables = introspect(
            conn,
            schema=args.schema,
            sample_values=args.sample_values,
            include_top_for_types=SAMPLEABLE_TYPES,
        )
        person_count = fetch_person_count(conn, args.schema)

    print_tree(columns)

    if args.out_csv:
        write_raw_csv(columns, args.out_csv)

    if args.out_xlsx:
        write_dictionary_xlsx(
            columns=columns,
            tables=tables,
            out_path=args.out_xlsx,
            cohort=args.schema,
            person_count=person_count,
        )

    return 0 if columns else 1


if __name__ == "__main__":
    raise SystemExit(main())
