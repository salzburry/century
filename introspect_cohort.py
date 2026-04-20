#!/usr/bin/env python3
"""Introspect a Postgres schema (default: ``mtc_aat_cohort``) and dump its
tables, columns, data types, row counts, and NULL completeness so the output
can seed a clinical coding dictionary draft.

Run it locally where you have DB access (my sandbox doesn't). The warehouse
connection is read from environment variables so nothing sensitive is
hard-coded.

Required env vars (or pass the equivalent ``--`` flags):
    PGHOST, PGPORT, PGDATABASE, PGUSER, PGPASSWORD

Optional:
    PGSSLMODE   (default: require - typical for RDS/Aurora)

Example usage::

    export PGHOST=chealth-stage-db.ct4u6ogcouw8.us-east-1.rds.amazonaws.com
    export PGPORT=5432
    export PGDATABASE=clinical
    export PGUSER=onkar
    export PGPASSWORD='***'

    python introspect_cohort.py                         # prints to stdout
    python introspect_cohort.py --schema mtc_aat_cohort --out cohort.csv
    python introspect_cohort.py --schema mtc_aat_cohort --sample-values 5

Dependencies::

    pip install psycopg[binary]
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
    sys.stderr.write(
        "psycopg is not installed. Run: pip install 'psycopg[binary]'\n"
    )
    raise SystemExit(1)


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

    def to_row(self) -> dict[str, str]:
        return {
            "schema": self.schema,
            "table": self.table,
            "column": self.column,
            "data_type": self.data_type,
            "is_nullable": "yes" if self.is_nullable else "no",
            "row_count": str(self.row_count),
            "null_count": str(self.null_count),
            "completeness_pct": f"{self.completeness_pct:.1f}",
            "top_values": "; ".join(f"{v}: {n}" for v, n in self.top_values),
        }


# --------------------------------------------------------------------------- #
# Main flow
# --------------------------------------------------------------------------- #


def introspect(
    conn: psycopg.Connection,
    schema: str,
    sample_values: int,
    include_top_for_types: set[str],
) -> list[ColumnInfo]:
    """Walk every table/column in ``schema`` and collect structural + QC info.

    ``include_top_for_types`` controls which data types we compute top-N values
    for (skip them on long free-text columns to keep the script fast).
    """
    results: list[ColumnInfo] = []

    with conn.cursor() as cur:
        cur.execute(LIST_TABLES_SQL, (schema,))
        tables = [row[0] for row in cur.fetchall()]

    if not tables:
        sys.stderr.write(f"No tables found in schema '{schema}'.\n")
        return results

    for table in tables:
        with conn.cursor() as cur:
            cur.execute(ROW_COUNT_SQL_TEMPLATE.format(schema=schema, table=table))
            row_count = cur.fetchone()[0]

            cur.execute(LIST_COLUMNS_SQL, (schema, table))
            columns = cur.fetchall()

        print(f"  {schema}.{table}  ({row_count:,} rows, {len(columns)} cols)", file=sys.stderr)

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

            completeness = 0.0
            if row_count > 0:
                completeness = (1 - null_count / row_count) * 100

            results.append(
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

    return results


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


def write_csv(columns: list[ColumnInfo], out_path: Path) -> None:
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
            writer.writerow(col.to_row())
    print(f"\nWrote {len(columns)} rows -> {out_path}", file=sys.stderr)


# --------------------------------------------------------------------------- #
# CLI
# --------------------------------------------------------------------------- #


# Types worth computing TOP-N distributions for. Skip long/unstructured types.
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


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Introspect a Postgres schema to seed a dictionary draft."
    )
    parser.add_argument("--schema", default="mtc_aat_cohort",
                        help="Schema to introspect (default: mtc_aat_cohort).")
    parser.add_argument("--out", type=Path, default=None,
                        help="Write results to this CSV path as well as stdout.")
    parser.add_argument("--sample-values", type=int, default=5,
                        help="Top-N frequent values per column (0 to disable).")

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

    print(f"Connecting to {conn_kwargs['host']}:{conn_kwargs['port']}/"
          f"{conn_kwargs['dbname']} as {conn_kwargs['user']}...", file=sys.stderr)

    with psycopg.connect(**conn_kwargs) as conn:
        columns = introspect(
            conn,
            schema=args.schema,
            sample_values=args.sample_values,
            include_top_for_types=SAMPLEABLE_TYPES,
        )

    print_tree(columns)

    if args.out:
        write_csv(columns, args.out)

    return 0 if columns else 1


if __name__ == "__main__":
    raise SystemExit(main())
