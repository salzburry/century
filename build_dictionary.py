#!/usr/bin/env python3
"""Build a four-page data dictionary for one cohort.

Reads:
    packs/cohorts/<cohort>.yaml        (provider, disease, schema, etc.)
    packs/categories.yaml              (table -> Category map)
    packs/table_descriptions.yaml      (table -> purpose text)
    packs/column_descriptions.yaml     (table.column -> description)
    packs/pii.yaml                     (PII column allowlist + regex)
    packs/variables/<disease>.yaml     (Page-4 clinical variables)

Walks the cohort's Postgres schema (via introspect_cohort.introspect)
and emits:
    Output/<schema>_dictionary.xlsx
    Output/<schema>_dictionary.html

Four sheets / sections (matches century/Data dictionary.pdf):
    Summary  — provider, disease, patients, years_of_data, tables, cols
    Tables   — one row per warehouse table (row_count, column_count, purpose)
    Columns  — one row per physical column (existing inventory w/ enrichment)
    Variables — one row per clinical concept, driven by the disease pack

Usage:
    python build_dictionary.py --cohort mtc_aat
    python build_dictionary.py --cohort mtc_alzheimers
    python build_dictionary.py --cohort mtc_aat --audience sales
    python build_dictionary.py --cohort mtc_aat --dry-run    # no DB required

See README.md for the canonical model and the nine-PR shipping plan.
This script implements PR 1-4 worth of the plan for the MTC cohorts
end-to-end; PR 5-9 (audience filters, PDF renderer, validation, batch
runner, combined views) layer on top of the same CohortModel.
"""
from __future__ import annotations

import argparse
import dataclasses
import datetime as _dt
import hashlib
import json
import re
import subprocess
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

# Re-use the existing introspection backbone — don't duplicate the
# schema-walking code.
from introspect_cohort import (
    ColumnInfo,
    TableInfo,
    _classify_metric_kind,
    _compile_continuous,
    _compile_date_range,
    _compile_top_values,
    _format_value_distribution,
    _require_psycopg,
    _require_yaml,
    _safe_null_count,
    build_conn_kwargs,
    fetch_person_count,
    load_dotenv,
    LIST_COLUMNS_SQL,
    LIST_TABLES_SQL,
    ROW_COUNT_SQL_TEMPLATE,
)


# --------------------------------------------------------------------------- #
# Paths / version
# --------------------------------------------------------------------------- #

REPO_ROOT = Path(__file__).resolve().parent
PACKS_DIR = REPO_ROOT / "packs"
OUTPUT_DIR = REPO_ROOT / "Output"
INTROSPECT_VERSION = "0.5.0-dictionary"

# Auto-load .env the same way introspect_cohort does.
load_dotenv(REPO_ROOT / ".env")


# --------------------------------------------------------------------------- #
# Dataclasses — the canonical CohortModel
# --------------------------------------------------------------------------- #


@dataclass
class DateCoverage:
    min_date: str | None = None
    max_date: str | None = None
    years_of_data: float | None = None
    contributing_columns: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return dataclasses.asdict(self)


@dataclass
class CohortSummary:
    patient_count: int | None
    table_count: int
    column_count: int
    date_coverage: DateCoverage


@dataclass
class TableRow:
    """One row on Page 2 (Tables)."""
    table_name: str
    category: str
    row_count: int
    column_count: int
    patient_count_in_table: int | None
    purpose: str


@dataclass
class ColumnRow:
    """One row on Page 3 (Columns). Extends introspect_cohort.ColumnInfo with
    enrichment (category, description, pii flag, patient-level completeness)."""
    category: str
    table: str
    column: str
    description: str
    data_type: str
    values: str
    distribution: str
    median_iqr: str
    completeness_pct: float
    patient_pct: float | None
    extraction_type: str
    pii: bool
    notes: str


@dataclass
class VariableRow:
    """One row on Page 4 (Variables). Driven by packs/variables/<disease>.yaml.

    Column order matches the earlier Century workbook:

        Category | Variable | Description | Table | Column(s) | Criteria |
        Values | Distribution | Median (IQR) | Completeness |
        Implemented | % Patient | Extraction Type | Notes

    Median (IQR) and Completeness are populated for numeric columns
    within the criteria-filtered subset; Implemented / % Patient remain
    the page's coverage metrics.
    """
    category: str
    variable: str
    description: str
    table: str
    column: str
    criteria: str
    values: str
    distribution: str
    median_iqr: str
    completeness_pct: float | None
    implemented: str        # "Yes" / "No"
    patient_pct: float | None
    extraction_type: str
    notes: str
    pii: bool = False       # Audience filter drops these for sales/pharma.


@dataclass
class CohortModel:
    cohort: str
    provider: str
    disease: str
    schema_name: str
    variant: str
    display_name: str
    description: str
    status: str
    generated_at: str
    git_sha: str
    introspect_version: str
    schema_snapshot_digest: str
    summary: CohortSummary
    tables: list[TableRow]
    columns: list[ColumnRow]
    variables: list[VariableRow]

    def to_dict(self) -> dict[str, Any]:
        def _conv(v: Any) -> Any:
            if dataclasses.is_dataclass(v):
                return _conv(dataclasses.asdict(v))
            if isinstance(v, dict):
                return {k: _conv(val) for k, val in v.items()}
            if isinstance(v, list):
                return [_conv(x) for x in v]
            return v
        return _conv(dataclasses.asdict(self))


# --------------------------------------------------------------------------- #
# Pack loaders
# --------------------------------------------------------------------------- #


def _yaml_load(path: Path) -> dict[str, Any]:
    yaml = _require_yaml()
    if not path.is_file():
        return {}
    return yaml.safe_load(path.read_text(encoding="utf-8")) or {}


def load_cohort_pack(cohort: str) -> dict[str, Any]:
    path = PACKS_DIR / "cohorts" / f"{cohort}.yaml"
    if not path.is_file():
        raise FileNotFoundError(
            f"cohort pack missing: {path}. Available: "
            + ", ".join(sorted(p.stem for p in (PACKS_DIR / "cohorts").glob("*.yaml")))
        )
    data = _yaml_load(path)
    required = ("provider", "disease", "schema_name", "cohort_name")
    missing = [k for k in required if not data.get(k)]
    if missing:
        raise ValueError(f"{path} missing required fields: {', '.join(missing)}")
    return data


def load_variables_pack(disease_slug: str) -> list[dict[str, Any]]:
    """Load packs/variables/<slug>.yaml and resolve any `include:` list."""
    if not disease_slug:
        return []
    path = PACKS_DIR / "variables" / f"{disease_slug}.yaml"
    if not path.is_file():
        sys.stderr.write(f"[warn] variables pack not found: {path} -> Page 4 will be empty\n")
        return []
    data = _yaml_load(path)
    result: list[dict[str, Any]] = []
    for inc in data.get("include") or []:
        result.extend(load_variables_pack(inc))
    result.extend(data.get("variables") or [])
    return result


def load_categories_map() -> dict[str, str]:
    """Invert packs/categories.yaml into table_name -> Category."""
    raw = _yaml_load(PACKS_DIR / "categories.yaml").get("categories", {}) or {}
    out: dict[str, str] = {}
    for category, payload in raw.items():
        for table in (payload or {}).get("tables", []) or []:
            out[table] = category
    return out


def load_table_descriptions() -> dict[str, str]:
    return _yaml_load(PACKS_DIR / "table_descriptions.yaml").get("tables", {}) or {}


def load_column_descriptions() -> dict[str, str]:
    return _yaml_load(PACKS_DIR / "column_descriptions.yaml").get("columns", {}) or {}


def load_pii_pack() -> tuple[set[tuple[str, str]], list[re.Pattern[str]]]:
    raw = _yaml_load(PACKS_DIR / "pii.yaml")
    pairs: set[tuple[str, str]] = set()
    for table, cols in (raw.get("pii_columns") or {}).items():
        for c in cols or []:
            pairs.add((table, c))
    patterns = [re.compile(p) for p in (raw.get("pii_name_patterns") or [])]
    return pairs, patterns


def is_pii(table: str, column: str,
           pii_pairs: set[tuple[str, str]],
           pii_patterns: list[re.Pattern[str]]) -> bool:
    if (table, column) in pii_pairs:
        return True
    return any(p.search(column) for p in pii_patterns)


# --------------------------------------------------------------------------- #
# Enrichment heuristics / excludes
# --------------------------------------------------------------------------- #


_SURROGATE_KEY_SUFFIXES = ("_id", "_concept_id")
_SURROGATE_KEY_EXACT = {"person_id", "visit_occurrence_id", "visit_detail_id"}


def is_surrogate_key(column: str) -> bool:
    """IDs we don't want to summarize as continuous measurements."""
    if column in _SURROGATE_KEY_EXACT:
        return True
    return any(column.endswith(suf) for suf in _SURROGATE_KEY_SUFFIXES)


_DATE_COVERAGE_CANDIDATES = [
    ("visit_occurrence", "visit_start_date"),
    ("condition_occurrence", "condition_start_date"),
    ("drug_exposure", "drug_exposure_start_date"),
    ("measurement", "measurement_date"),
    ("observation", "observation_date"),
    ("procedure_occurrence", "procedure_date"),
]


# --------------------------------------------------------------------------- #
# Introspection — extends introspect_cohort.introspect() with PR-1..PR-4 logic
# --------------------------------------------------------------------------- #


def _patient_completeness_sql(schema: str, table: str, column: str) -> str:
    return (
        f'SELECT COUNT(DISTINCT "person_id") '
        f'FROM "{schema}"."{table}" '
        f'WHERE "{column}" IS NOT NULL;'
    )


def _distinct_patients_in_table_sql(schema: str, table: str) -> str:
    return f'SELECT COUNT(DISTINCT "person_id") FROM "{schema}"."{table}";'


def introspect_cohort(
    conn,
    schema: str,
    sample_values_default: int = 5,
    sample_values_concept: int = 20,
) -> tuple[list[ColumnInfo], list[TableInfo], dict[str, int | None], set[str]]:
    """Walk the cohort schema with PR-1 / PR-3 rules baked in.

    PR 1: skip surrogate keys from continuous summary, bump sample depth
          for *_concept_name columns, collapse empty tables.
    PR 3: patient-level completeness for tables with person_id.

    Returns (columns, tables, patient_count_per_table, tables_with_person_id).
    The last set is a separate signal from the patient_count dict because
    "no person_id column" and "query failed" would otherwise both show up
    as None.
    """
    psycopg = _require_psycopg()  # noqa: F841 — import-time validation only
    columns_out: list[ColumnInfo] = []
    tables_out: list[TableInfo] = []
    patients_per_table: dict[str, int | None] = {}
    tables_with_person_id: set[str] = set()

    with conn.cursor() as cur:
        cur.execute(LIST_TABLES_SQL, (schema,))
        table_rows = cur.fetchall()
    tables = [t for (t, _) in table_rows]

    for table in tables:
        with conn.cursor() as cur:
            cur.execute(ROW_COUNT_SQL_TEMPLATE.format(schema=schema, table=table))
            row_count = cur.fetchone()[0]
            cur.execute(LIST_COLUMNS_SQL, (schema, table))
            raw_columns = cur.fetchall()

        tables_out.append(TableInfo(
            name=table, row_count=row_count, column_count=len(raw_columns)
        ))

        # patient count in this table (if person_id present)
        col_names = {c[0] for c in raw_columns}
        has_person_id = "person_id" in col_names
        if has_person_id:
            tables_with_person_id.add(table)
        if has_person_id and row_count > 0:
            try:
                with conn.cursor() as cur:
                    cur.execute(_distinct_patients_in_table_sql(schema, table))
                    patients_per_table[table] = int(cur.fetchone()[0])
            except Exception:
                conn.rollback()
                patients_per_table[table] = None
        else:
            patients_per_table[table] = None

        # Empty-table collapse: list columns but skip per-column summaries.
        if row_count == 0:
            for column_name, data_type, is_nullable_str, _ml, _p in raw_columns:
                columns_out.append(ColumnInfo(
                    schema=schema, table=table, column=column_name,
                    data_type=data_type,
                    is_nullable=(is_nullable_str == "YES"),
                    row_count=0, null_count=0, completeness_pct=0.0,
                ))
            continue

        for column_name, data_type, is_nullable_str, _ml, _p in raw_columns:
            is_nullable = is_nullable_str == "YES"
            kind = _classify_metric_kind(data_type)
            null_count = _safe_null_count(conn, schema, table, column_name)

            value_distribution = ""
            numeric_summary = ""
            median_iqr = ""
            top_values: list[tuple[str, int]] = []

            # PR 1: exclude surrogate keys from continuous summary
            if kind == "continuous" and not is_surrogate_key(column_name):
                numeric_summary, median_iqr = _compile_continuous(
                    conn, schema, table, column_name
                )
            elif kind == "date":
                numeric_summary = _compile_date_range(
                    conn, schema, table, column_name
                )
            elif kind == "categorical":
                # PR 1: deeper sample for *_concept_name columns
                limit = sample_values_concept if column_name.endswith("_concept_name") \
                    else sample_values_default
                top_values = _compile_top_values(
                    conn, schema, table, column_name, limit=limit,
                )
                value_distribution = _format_value_distribution(
                    top_values, null_count, row_count
                )

            completeness = (1 - null_count / row_count) * 100 if row_count else 0.0
            columns_out.append(ColumnInfo(
                schema=schema, table=table, column=column_name,
                data_type=data_type, is_nullable=is_nullable,
                row_count=row_count, null_count=null_count,
                completeness_pct=completeness,
                value_distribution=value_distribution,
                numeric_summary=numeric_summary,
                median_iqr=median_iqr,
                top_values=top_values,
            ))

    return columns_out, tables_out, patients_per_table, tables_with_person_id


def compute_patient_completeness(
    conn, schema: str, column: ColumnInfo, total_patients: int | None,
) -> float | None:
    """% Patient = distinct person_ids with col IS NOT NULL / total patients."""
    if total_patients is None or total_patients <= 0:
        return None
    if column.row_count == 0:
        return 0.0
    try:
        with conn.cursor() as cur:
            cur.execute(
                _patient_completeness_sql(schema, column.table, column.column)
            )
            n = int(cur.fetchone()[0])
    except Exception:
        conn.rollback()
        return None
    return 100.0 * n / total_patients


# --------------------------------------------------------------------------- #
# Date coverage rollup (PR 4)
# --------------------------------------------------------------------------- #


_DATE_RE = re.compile(r"Min: (\S+), Max: (\S+)")


def compute_date_coverage(columns: list[ColumnInfo]) -> DateCoverage:
    """Scan the per-column date-range summaries and roll up the earliest /
    latest date across the approved clinical-date candidates."""
    min_seen: str | None = None
    max_seen: str | None = None
    contributing: list[str] = []

    by_key = {(c.table, c.column): c for c in columns}
    for table, col in _DATE_COVERAGE_CANDIDATES:
        info = by_key.get((table, col))
        if info is None or not info.numeric_summary:
            continue
        m = _DATE_RE.search(info.numeric_summary)
        if not m:
            continue
        mn, mx = m.group(1), m.group(2)
        contributing.append(f"{table}.{col}")
        if min_seen is None or mn < min_seen:
            min_seen = mn
        if max_seen is None or mx > max_seen:
            max_seen = mx

    years = None
    if min_seen and max_seen:
        try:
            d_min = _dt.date.fromisoformat(min_seen[:10])
            d_max = _dt.date.fromisoformat(max_seen[:10])
            years = round((d_max - d_min).days / 365.25, 2)
        except ValueError:
            years = None

    return DateCoverage(
        min_date=min_seen,
        max_date=max_seen,
        years_of_data=years,
        contributing_columns=contributing,
    )


# --------------------------------------------------------------------------- #
# Variable (Page 4) resolution
# --------------------------------------------------------------------------- #


def _format_top_values_from_rows(rows: list[tuple[str, int]], total: int) -> tuple[str, str]:
    """(values_cell, distribution_cell) from SQL top-N rows."""
    if not rows or total <= 0:
        return "", ""
    values = ", ".join(r[0] if r[0] else "(null)" for r in rows[:10])
    distribution = "; ".join(
        f"{r[0] if r[0] else '(null)'}: {r[1]} ({100.0 * r[1] / total:.1f}%)"
        for r in rows
    )
    return values, distribution


_TEXT_COLUMN_PATTERNS = (
    re.compile(r"_text$"),
    re.compile(r"_note$"),
    re.compile(r"^note_text$"),
)


def _is_freetext_column(column: str) -> bool:
    return any(p.search(column) for p in _TEXT_COLUMN_PATTERNS)


def resolve_variables(
    conn, schema: str,
    variables_pack: list[dict[str, Any]],
    total_patients: int | None,
    pii_pairs: set[tuple[str, str]] | None = None,
    pii_patterns: list[re.Pattern[str]] | None = None,
    tables_with_person_id: set[str] | None = None,
    column_types: dict[tuple[str, str], str] | None = None,
) -> list[VariableRow]:
    """For each entry in the disease pack, query the cohort and populate
    Values / Distribution / Median (IQR) / Completeness / Implemented /
    % Patient.

    Pack fields consumed:
        table           (required)
        column          (required — drives the "Column(s)" display cell)
        expression      (optional — SQL expression used in place of
                         a bare `"<column>"` reference; lets the pack
                         say `LEFT("zip", 3)` without losing the `zip`
                         label for display)
        criteria        (optional — SQL WHERE fragment)
        extraction_type (Structured / Abstracted / Unstructured)
        category / variable / description / notes

    Skips:
      - GROUP BY "{column}" for `extraction_type: Unstructured` rows or
        column names matching `*_text` / `*_note` — unique-per-row
        values + expensive query on note tables.
      - COUNT(DISTINCT person_id) for tables not in
        `tables_with_person_id` (location and other dimension tables
        don't carry a patient FK, so the query would fail + roll back
        every column).
    """
    pii_pairs = pii_pairs or set()
    pii_patterns = pii_patterns or []
    tables_with_person_id = tables_with_person_id or set()
    column_types = column_types or {}

    out: list[VariableRow] = []
    for v in variables_pack:
        table = v.get("table") or ""
        column = v.get("column") or ""
        expression = (v.get("expression") or "").strip() or f'"{column}"'
        criteria = (v.get("criteria") or "").strip()
        category = v.get("category") or ""
        variable_name = v.get("variable") or column
        description = v.get("description") or ""
        extraction = v.get("extraction_type") or "Structured"
        notes = v.get("notes") or ""

        values_cell = ""
        distribution_cell = ""
        median_iqr_cell = ""
        completeness_pct: float | None = None
        implemented = "No"
        patient_pct: float | None = None

        # Two WHERE forms:
        #   `where_criteria` scopes to rows matching the variable's
        #                    criteria (null or not) — Completeness
        #                    denominator.
        #   `where_nonnull`  adds the is-not-null guard — the rows
        #                    that actually contribute data.
        where_criteria = f"({criteria})" if criteria else "TRUE"
        where_nonnull = f"{expression} IS NOT NULL"
        if criteria:
            where_nonnull += f" AND ({criteria})"

        total_with_criteria = 0
        try:
            with conn.cursor() as cur:
                cur.execute(
                    f'SELECT COUNT(*) FROM "{schema}"."{table}" '
                    f'WHERE {where_criteria};'
                )
                total_with_criteria = int(cur.fetchone()[0])
        except Exception as exc:
            sys.stderr.write(
                f"[warn] {category}/{variable_name}: criteria count failed "
                f"({table}.{column}): {exc}\n"
            )
            conn.rollback()

        total_nonnull = 0
        try:
            with conn.cursor() as cur:
                cur.execute(
                    f'SELECT COUNT(*) FROM "{schema}"."{table}" '
                    f'WHERE {where_nonnull};'
                )
                total_nonnull = int(cur.fetchone()[0])
        except Exception as exc:
            sys.stderr.write(
                f"[warn] {category}/{variable_name}: nonnull count failed "
                f"({table}.{column}): {exc}\n"
            )
            conn.rollback()

        if total_with_criteria > 0:
            completeness_pct = 100.0 * total_nonnull / total_with_criteria

        skip_top_values = (
            extraction.lower() == "unstructured" or _is_freetext_column(column)
        )

        if total_nonnull > 0:
            implemented = "Yes"

            if skip_top_values:
                distribution_cell = (
                    f"{total_nonnull:,} rows; values not aggregated (free text)"
                )
            else:
                try:
                    with conn.cursor() as cur:
                        cur.execute(
                            f'SELECT {expression}::text AS v, COUNT(*) AS n '
                            f'FROM "{schema}"."{table}" WHERE {where_nonnull} '
                            f'GROUP BY {expression} ORDER BY n DESC LIMIT 10;'
                        )
                        rows = [(str(r[0]), int(r[1])) for r in cur.fetchall()]
                    values_cell, distribution_cell = _format_top_values_from_rows(
                        rows, total_nonnull
                    )
                except Exception as exc:
                    sys.stderr.write(
                        f"[warn] {category}/{variable_name}: top-values query "
                        f"failed: {exc}\n"
                    )
                    conn.rollback()

            # Median (IQR) for numeric-typed underlying columns. Skip when
            # an expression is used, since the raw column's data type may
            # not match the expression's output type (e.g. LEFT(zip,3) is
            # text even though zip is numeric).
            raw_type = column_types.get((table, column), "")
            if (
                v.get("expression") is None
                and _classify_metric_kind(raw_type) == "continuous"
            ):
                median_iqr_cell = _compile_continuous_filtered(
                    conn, schema, table, column, where_nonnull
                )

            # % Patient — only if the table actually has person_id.
            if (
                table in tables_with_person_id
                and total_patients and total_patients > 0
            ):
                try:
                    with conn.cursor() as cur:
                        cur.execute(
                            f'SELECT COUNT(DISTINCT "person_id") '
                            f'FROM "{schema}"."{table}" WHERE {where_nonnull};'
                        )
                        n = int(cur.fetchone()[0])
                    patient_pct = 100.0 * n / total_patients
                except Exception:
                    conn.rollback()

        out.append(VariableRow(
            category=category,
            variable=variable_name,
            description=description,
            table=table,
            column=column,
            criteria=criteria,
            values=values_cell,
            distribution=distribution_cell,
            median_iqr=median_iqr_cell,
            completeness_pct=completeness_pct,
            implemented=implemented,
            patient_pct=patient_pct,
            extraction_type=extraction,
            notes=notes,
            pii=is_pii(table, column, pii_pairs, pii_patterns),
        ))
    return out


def _compile_continuous_filtered(
    conn, schema: str, table: str, column: str, where: str
) -> str:
    """`Median (IQR)` cell for a numeric column, scoped to rows matching
    the variable's `where` clause. Returns empty string on failure or
    when the subset has no rows."""
    sql = f"""
    SELECT
      PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY "{column}")::text,
      PERCENTILE_CONT(0.5)  WITHIN GROUP (ORDER BY "{column}")::text,
      PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY "{column}")::text
    FROM "{schema}"."{table}"
    WHERE {where};
    """
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            row = cur.fetchone()
    except Exception:
        conn.rollback()
        return ""
    if not row or row[1] is None:
        return ""
    q1, median, q3 = row
    return f"Median: {median} (IQR: {q1}-{q3})"


# --------------------------------------------------------------------------- #
# Model builder — orchestrates introspection + pack enrichment
# --------------------------------------------------------------------------- #


def _git_sha() -> str:
    try:
        return subprocess.check_output(
            ["git", "-C", str(REPO_ROOT), "rev-parse", "--short", "HEAD"],
            stderr=subprocess.DEVNULL,
        ).decode("utf-8").strip()
    except Exception:
        return "unknown"


def _schema_snapshot_digest(columns: list[ColumnInfo]) -> str:
    """Stable hash of (table, column, data_type) tuples so we can diff for
    drift in a later PR."""
    h = hashlib.sha256()
    for c in sorted(columns, key=lambda x: (x.table, x.column)):
        h.update(f"{c.table}.{c.column}:{c.data_type}\n".encode("utf-8"))
    return f"sha256:{h.hexdigest()}"


def build_model(
    cohort: str,
    conn,
    dry_run: bool = False,
) -> CohortModel:
    pack = load_cohort_pack(cohort)
    categories_map = load_categories_map()
    table_descriptions = load_table_descriptions()
    column_descriptions = load_column_descriptions()
    pii_pairs, pii_patterns = load_pii_pack()
    variables_pack = load_variables_pack(pack.get("variables_pack", ""))

    schema = pack["schema_name"]

    if dry_run:
        # Synthesise empty inventory so the renderer path is still exercisable
        columns_raw: list[ColumnInfo] = []
        tables_raw: list[TableInfo] = []
        patients_per_table: dict[str, int | None] = {}
        tables_with_person_id: set[str] = set()
        total_patients: int | None = None
        variables_rows: list[VariableRow] = [
            VariableRow(
                category=v.get("category", ""),
                variable=v.get("variable", v.get("column", "")),
                description=v.get("description", ""),
                table=v.get("table", ""),
                column=v.get("column", ""),
                criteria=(v.get("criteria") or "").strip(),
                values="", distribution="",
                median_iqr="",
                completeness_pct=None,
                implemented="No",
                patient_pct=None,
                extraction_type=v.get("extraction_type", "Structured"),
                notes=v.get("notes", ""),
                pii=is_pii(
                    v.get("table", ""), v.get("column", ""),
                    pii_pairs, pii_patterns,
                ),
            ) for v in variables_pack
        ]
    else:
        total_patients = fetch_person_count(conn, schema)
        columns_raw, tables_raw, patients_per_table, tables_with_person_id = \
            introspect_cohort(conn, schema)
        column_types = {(c.table, c.column): c.data_type for c in columns_raw}
        variables_rows = resolve_variables(
            conn, schema, variables_pack, total_patients,
            pii_pairs=pii_pairs, pii_patterns=pii_patterns,
            tables_with_person_id=tables_with_person_id,
            column_types=column_types,
        )

    # Page 2 — Tables
    cohort_category_overrides = (pack.get("category_rules") or {})
    # invert cohort-level override: table -> Category
    override_map: dict[str, str] = {}
    for cat, payload in cohort_category_overrides.items():
        for t in (payload or {}).get("tables", []) or []:
            override_map[t] = cat

    def _category_for(table: str) -> str:
        return override_map.get(table) or categories_map.get(table) or "Other"

    table_rows = [
        TableRow(
            table_name=t.name,
            category=_category_for(t.name),
            row_count=t.row_count,
            column_count=t.column_count,
            patient_count_in_table=patients_per_table.get(t.name),
            purpose=table_descriptions.get(t.name, ""),
        )
        for t in tables_raw
    ]

    # Page 3 — Columns
    column_rows: list[ColumnRow] = []
    for ci in columns_raw:
        kind = _classify_metric_kind(ci.data_type)
        extraction = "Unstructured" if kind == "unstructured" else "Structured"
        values_cell = ", ".join(v for v, _ in ci.top_values[:10])
        distribution_cell = ci.value_distribution or ci.numeric_summary
        pii = is_pii(ci.table, ci.column, pii_pairs, pii_patterns)
        if pii:
            extraction = "PII"
        patient_pct: float | None = None
        if not dry_run and ci.table in tables_with_person_id:
            # Denominator is the cohort's total patient count, NOT the
            # per-table distinct-patient count — otherwise sparse tables
            # always show ~100% and the column hides real cohort coverage.
            # Tables without a person_id column (e.g. location) stay None
            # — running the query there would just fail + roll back.
            patient_pct = compute_patient_completeness(
                conn, schema, ci, total_patients
            )
        column_rows.append(ColumnRow(
            category=_category_for(ci.table),
            table=ci.table,
            column=ci.column,
            description=column_descriptions.get(f"{ci.table}.{ci.column}", ""),
            data_type=ci.data_type,
            values=values_cell,
            distribution=distribution_cell,
            median_iqr=ci.median_iqr,
            completeness_pct=ci.completeness_pct,
            patient_pct=patient_pct,
            extraction_type=extraction,
            pii=pii,
            notes="",
        ))

    # Page 1 — Summary
    date_coverage = compute_date_coverage(columns_raw)
    summary = CohortSummary(
        patient_count=total_patients,
        table_count=len(tables_raw),
        column_count=len(columns_raw),
        date_coverage=date_coverage,
    )

    return CohortModel(
        cohort=pack["cohort_name"],
        provider=pack["provider"],
        disease=pack["disease"],
        schema_name=schema,
        variant=pack.get("variant", "raw"),
        display_name=pack.get("display_name") or pack["cohort_name"],
        description=(pack.get("description") or "").strip(),
        status=pack.get("status", "active"),
        generated_at=_dt.datetime.now(_dt.timezone.utc).isoformat(timespec="seconds"),
        git_sha=_git_sha(),
        introspect_version=INTROSPECT_VERSION,
        schema_snapshot_digest=_schema_snapshot_digest(columns_raw),
        summary=summary,
        tables=table_rows,
        columns=column_rows,
        variables=variables_rows,
    )


# --------------------------------------------------------------------------- #
# Audience filter (PR 5 — applied to the canonical model pre-render)
# --------------------------------------------------------------------------- #


# Audience -> {section: visible?}. Single source of truth driving both
# the model filter and the renderer omits.
AUDIENCE_VISIBILITY: dict[str, dict[str, bool]] = {
    "technical": {"summary": True, "tables": True, "columns": True, "variables": True},
    "sales":     {"summary": True, "tables": True, "columns": False, "variables": True},
    "pharma":    {"summary": True, "tables": False, "columns": False, "variables": True},
}


def section_visible(audience: str, section: str) -> bool:
    return AUDIENCE_VISIBILITY.get(audience, AUDIENCE_VISIBILITY["technical"])[section]


def filter_for_audience(model: CohortModel, audience: str) -> CohortModel:
    if audience == "technical":
        return model
    # Drop PII rows from BOTH columns and variables for sales + pharma.
    # Variables resolver tags `pii: true` whenever (table, column) hits the
    # PII pack, so the same predicate applies to both lists.
    filtered_columns = [c for c in model.columns if not c.pii]
    filtered_variables = [v for v in model.variables if not v.pii]
    visibility = AUDIENCE_VISIBILITY[audience]
    return dataclasses.replace(
        model,
        tables=model.tables if visibility["tables"] else [],
        columns=filtered_columns if visibility["columns"] else [],
        variables=filtered_variables,
    )


# --------------------------------------------------------------------------- #
# Renderers
# --------------------------------------------------------------------------- #


def _fmt_pct(value: float | None) -> str:
    if value is None:
        return "—"
    return f"{value:.1f}%"


def write_xlsx(model: CohortModel, out_path: Path,
               audience: str = "technical") -> None:
    try:
        import pandas as pd  # noqa: F401
    except ImportError as exc:
        raise SystemExit("pandas is not installed: pip install pandas openpyxl") from exc
    import pandas as pd

    summary_rows = [
        {"metric": "cohort",         "value": model.cohort},
        {"metric": "provider",       "value": model.provider},
        {"metric": "disease",        "value": model.disease},
        {"metric": "display_name",   "value": model.display_name},
        {"metric": "schema_name",    "value": model.schema_name},
        {"metric": "variant",        "value": model.variant},
        {"metric": "patient_count",  "value": model.summary.patient_count},
        {"metric": "table_count",    "value": model.summary.table_count},
        {"metric": "column_count",   "value": model.summary.column_count},
        {"metric": "min_date",       "value": model.summary.date_coverage.min_date},
        {"metric": "max_date",       "value": model.summary.date_coverage.max_date},
        {"metric": "years_of_data",  "value": model.summary.date_coverage.years_of_data},
        {"metric": "status",         "value": model.status},
        {"metric": "generated_at",   "value": model.generated_at},
        {"metric": "git_sha",        "value": model.git_sha},
        {"metric": "introspect_version", "value": model.introspect_version},
        {"metric": "schema_snapshot_digest", "value": model.schema_snapshot_digest},
    ]
    summary_df = pd.DataFrame(summary_rows, columns=["metric", "value"])

    tables_df = pd.DataFrame(
        [{
            "Table":         t.table_name,
            "Category":      t.category,
            "Rows":          t.row_count,
            "Columns":       t.column_count,
            "Patients":      t.patient_count_in_table if t.patient_count_in_table is not None else "—",
            "Purpose":       t.purpose,
        } for t in model.tables],
        columns=["Table", "Category", "Rows", "Columns", "Patients", "Purpose"],
    )

    columns_df = pd.DataFrame(
        [{
            "Category":        c.category,
            "Table":           c.table,
            "Column":          c.column,
            "Description":     c.description,
            "Data Type":       c.data_type,
            "Values":          c.values,
            "Distribution":    c.distribution,
            "Median (IQR)":    c.median_iqr,
            "Completeness":    f"{c.completeness_pct:.1f}%",
            "% Patient":       _fmt_pct(c.patient_pct),
            "Extraction Type": c.extraction_type,
            "PII":             "Yes" if c.pii else "",
            "Notes":           c.notes,
        } for c in model.columns],
        columns=[
            "Category", "Table", "Column", "Description", "Data Type",
            "Values", "Distribution", "Median (IQR)",
            "Completeness", "% Patient", "Extraction Type", "PII", "Notes",
        ],
    )

    variables_df = pd.DataFrame(
        [{
            "Category":        v.category,
            "Variable":        v.variable,
            "Description":     v.description,
            "Table":           v.table,
            "Column(s)":       v.column,
            "Criteria":        v.criteria,
            "Values":          v.values,
            "Distribution":    v.distribution,
            "Median (IQR)":    v.median_iqr,
            "Completeness":    _fmt_pct(v.completeness_pct),
            "Implemented":     v.implemented,
            "% Patient":       _fmt_pct(v.patient_pct),
            "Extraction Type": v.extraction_type,
            "Notes":           v.notes,
        } for v in model.variables],
        columns=[
            "Category", "Variable", "Description", "Table", "Column(s)", "Criteria",
            "Values", "Distribution", "Median (IQR)", "Completeness",
            "Implemented", "% Patient", "Extraction Type", "Notes",
        ],
    )

    with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
        summary_df.to_excel(writer,   sheet_name="Summary",   index=False)
        if section_visible(audience, "tables"):
            tables_df.to_excel(writer, sheet_name="Tables", index=False)
        if section_visible(audience, "columns"):
            columns_df.to_excel(writer, sheet_name="Columns", index=False)
        if section_visible(audience, "variables"):
            variables_df.to_excel(writer, sheet_name="Variables", index=False)
        _autosize_and_wrap(writer)
    print(f"Wrote {out_path}", file=sys.stderr)


def _autosize_and_wrap(writer) -> None:
    """Auto-size every column (cap width 60) and enable word-wrap."""
    from openpyxl.styles import Alignment
    for ws in writer.book.worksheets:
        for col_idx, col in enumerate(ws.columns, start=1):
            max_len = 0
            for cell in col:
                cell.alignment = Alignment(wrap_text=True, vertical="top")
                if cell.value is None:
                    continue
                v = str(cell.value)
                if len(v) > max_len:
                    max_len = len(v)
            letter = ws.cell(row=1, column=col_idx).column_letter
            ws.column_dimensions[letter].width = min(max(12, max_len + 2), 60)


def write_html(model: CohortModel, out_path: Path,
               audience: str = "technical") -> None:
    esc = lambda s: (s or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

    def _table(rows: list[list[str]], headers: list[str]) -> str:
        th = "".join(f"<th>{esc(h)}</th>" for h in headers)
        trs = "".join(
            "<tr>" + "".join(f"<td>{esc(str(c))}</td>" for c in r) + "</tr>"
            for r in rows
        )
        return f'<table class="dd"><thead><tr>{th}</tr></thead><tbody>{trs}</tbody></table>'

    summary_html = "".join(f"<dt>{esc(k)}</dt><dd>{esc(str(v))}</dd>" for k, v in [
        ("Cohort",          model.cohort),
        ("Provider",        model.provider),
        ("Disease",         model.disease),
        ("Display name",    model.display_name),
        ("Schema",          model.schema_name),
        ("Variant",         model.variant),
        ("Patient count",   model.summary.patient_count),
        ("Table count",     model.summary.table_count),
        ("Column count",    model.summary.column_count),
        ("Date coverage",
         f"{model.summary.date_coverage.min_date} → {model.summary.date_coverage.max_date}"
         f" ({model.summary.date_coverage.years_of_data} years)"
         if model.summary.date_coverage.min_date else "—"),
        ("Status",          model.status),
        ("Generated at",    model.generated_at),
        ("Git SHA",         model.git_sha),
        ("Introspect version", model.introspect_version),
        ("Schema snapshot", model.schema_snapshot_digest),
    ])

    tables_rows = [[
        t.table_name, t.category, f"{t.row_count:,}", t.column_count,
        t.patient_count_in_table if t.patient_count_in_table is not None else "—",
        t.purpose,
    ] for t in model.tables]

    columns_rows = [[
        c.category, c.table, c.column, c.description, c.data_type,
        c.values, c.distribution, c.median_iqr,
        f"{c.completeness_pct:.1f}%", _fmt_pct(c.patient_pct),
        c.extraction_type, "Yes" if c.pii else "", c.notes,
    ] for c in model.columns]

    variables_rows = [[
        v.category, v.variable, v.description, v.table, v.column, v.criteria,
        v.values, v.distribution, v.median_iqr, _fmt_pct(v.completeness_pct),
        v.implemented, _fmt_pct(v.patient_pct), v.extraction_type, v.notes,
    ] for v in model.variables]

    sections: list[str] = [
        f'<h2>Summary</h2><dl class="summary">{summary_html}</dl>',
    ]
    if section_visible(audience, "tables"):
        sections.append(
            "<h2>Tables</h2>"
            + _table(tables_rows, ["Table", "Category", "Rows", "Columns",
                                   "Patients", "Purpose"])
        )
    if section_visible(audience, "columns"):
        sections.append(
            "<h2>Columns</h2>"
            + _table(columns_rows, [
                "Category", "Table", "Column", "Description", "Data Type",
                "Values", "Distribution", "Median (IQR)", "Completeness",
                "% Patient", "Extraction Type", "PII", "Notes",
            ])
        )
    if section_visible(audience, "variables"):
        sections.append(
            "<h2>Variables</h2>"
            + _table(variables_rows, [
                "Category", "Variable", "Description", "Table", "Column(s)",
                "Criteria", "Values", "Distribution", "Median (IQR)",
                "Completeness", "Implemented", "% Patient",
                "Extraction Type", "Notes",
            ])
        )

    body = "\n".join(sections)
    page = f"""<!doctype html>
<html lang="en"><head><meta charset="utf-8">
<title>Data Dictionary — {esc(model.display_name)}</title>
<style>
 body {{ font-family: -apple-system, Segoe UI, Roboto, Arial, sans-serif;
         margin: 24px; color: #222; }}
 h1 {{ font-size: 1.5rem; margin-bottom: 2px; }}
 h2 {{ font-size: 1.15rem; margin-top: 32px; color: #444;
       border-bottom: 2px solid #e5e7eb; padding-bottom: 4px; }}
 dl.summary {{ display: grid; grid-template-columns: max-content 1fr;
               gap: 4px 16px; font-size: 0.9rem; }}
 dl.summary dt {{ font-weight: 600; color: #555; }}
 table.dd {{ border-collapse: collapse; font-size: 0.82rem;
             width: 100%; margin-top: 8px; }}
 table.dd th, table.dd td {{ border: 1px solid #d0d4d9;
              padding: 6px 9px; vertical-align: top; text-align: left; }}
 table.dd th {{ background: #f2f4f7; font-weight: 600; }}
 table.dd tr:nth-child(even) td {{ background: #fafbfc; }}
 @media print {{
   h2 {{ page-break-before: always; }}
   h1 + dl.summary {{ page-break-after: always; }}
 }}
</style></head><body>
<h1>Data Dictionary — {esc(model.display_name)}</h1>
<div style="color:#666">{esc(model.description)}</div>

{body}

</body></html>
"""
    out_path.write_text(page, encoding="utf-8")
    print(f"Wrote {out_path}", file=sys.stderr)


def write_json(model: CohortModel, out_path: Path) -> None:
    out_path.write_text(
        json.dumps(model.to_dict(), indent=2, default=str), encoding="utf-8",
    )
    print(f"Wrote {out_path}", file=sys.stderr)


# --------------------------------------------------------------------------- #
# CLI
# --------------------------------------------------------------------------- #


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Build a four-page data dictionary for one cohort."
    )
    parser.add_argument("--cohort", required=True,
                        help="cohort slug, e.g. mtc_aat / mtc_alzheimers")
    parser.add_argument("--audience",
                        choices=("technical", "sales", "pharma"),
                        default="technical")
    parser.add_argument("--formats", nargs="+",
                        choices=("xlsx", "html", "json"),
                        default=["xlsx", "html", "json"])
    parser.add_argument("--out-dir", default=str(OUTPUT_DIR),
                        help="output directory (default: Output/)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Skip DB connection; emit pack-only skeleton. "
                             "Useful for validating packs offline.")
    args = parser.parse_args(argv)

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    if args.dry_run:
        model = build_model(args.cohort, conn=None, dry_run=True)
    else:
        psycopg = _require_psycopg()
        pack = load_cohort_pack(args.cohort)

        class _NS:
            host = None; port = None; database = None
            user = None; password = None; sslmode = None
        conn_kwargs = build_conn_kwargs(_NS())
        with psycopg.connect(**conn_kwargs) as conn:
            conn.autocommit = True
            model = build_model(args.cohort, conn, dry_run=False)

    model = filter_for_audience(model, args.audience)

    stem = f"{model.schema_name}_dictionary"
    if args.audience != "technical":
        stem += f"_{args.audience}"
    if "xlsx" in args.formats:
        write_xlsx(model, out_dir / f"{stem}.xlsx", audience=args.audience)
    if "html" in args.formats:
        write_html(model, out_dir / f"{stem}.html", audience=args.audience)
    if "json" in args.formats:
        write_json(model, out_dir / f"{stem}.json")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

