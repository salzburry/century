#!/usr/bin/env python3
"""Validate the mtc_aat_cohort clinical coding dictionary.

Single-file step-by-step validator:

    1. Load the source file (xlsx workbook or csv/tsv flat file).
    2. Check required sheets exist (workbook only).
    3. Locate the Variables sheet and canonicalise its column headers.
    4. Check required columns are present and in the expected order.
    5. Walk every row and validate values.
    6. Summarise and exit.

Usage::

    python validate_dictionary.py --input dictionaries/mtc_aat_cohort.xlsx
    python validate_dictionary.py --input dd.csv --report-json report.json
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

import pandas as pd


# --------------------------------------------------------------------------- #
# mtc_aat_cohort rules (inlined — this validator is single-cohort by design)
# --------------------------------------------------------------------------- #

COHORT_NAME = "mtc_aat_cohort"

REQUIRED_SHEETS: dict[str, list[str]] = {
    "summary": ["Summary"],
    "tables": ["Tables"],
    "variables": ["Variables", "Variable", "Data Dictionary", "Dictionary"],
}

REQUIRED_COLUMNS: list[str] = [
    "category",
    "variable",
    "description",
    "schema",
    "source_columns",
    "criteria",
    "values",
    "distribution",
    "completeness",
    "extraction_type",
]

REQUIRED_NON_EMPTY_COLUMNS: list[str] = [
    "category",
    "variable",
    "description",
    "schema",
    "source_columns",
    "completeness",
    "extraction_type",
]

COLUMN_ORDER: list[str] = [
    "category",
    "variable",
    "description",
    "schema",
    "source_columns",
    "criteria",
    "values",
    "distribution",
    "completeness",
    "extraction_type",
    "notes",
]

COLUMN_ALIASES: dict[str, list[str]] = {
    "category": ["category", "cohort category", "domain"],
    "variable": ["variable", "variable name", "field", "field name"],
    "description": ["description", "variable description", "definition"],
    "schema": ["schema", "table", "source table", "table/schema", "table(s)"],
    "source_columns": ["column(s)", "columns", "column", "source columns", "source column"],
    "criteria": ["criteria", "logic", "filter criteria", "configuration criteria"],
    "values": ["values", "value examples", "valid values"],
    "distribution": ["distribution", "value distribution"],
    "completeness": ["completeness", "completion", "% completeness", "percent completeness"],
    "extraction_type": ["extraction type", "extract type", "capture type", "mapping type"],
    "notes": ["notes", "comments"],
}

ALLOWED_EXTRACTION_TYPES: set[str] = {
    "Structured",
    "Abstracted",
    "Unstructured",
    "Derived",
    "Calculated",
    "Manual",
}

RECOMMENDED_SCHEMA_VALUES: set[str] = {
    "person",
    "observation",
    "measurement",
    "condition_occurrence",
    "drug_exposure",
    "procedure_occurrence",
    "visit_occurrence",
    "location",
    "payer_plan_period",
    "death",
    "device_exposure",
    "note",
    "document",
    "specimen",
}

# Display labels ("Heart rate", "AAT level", "pTau-217") — not SQL ids.
VARIABLE_NAME_PATTERN: str = r"^[A-Za-z][A-Za-z0-9 _/().\-]*$"

SOURCE_COLUMN_PATTERN = re.compile(r"^[a-z][a-z0-9_]*(\.[a-z][a-z0-9_]*)?$")


# --------------------------------------------------------------------------- #
# Issue and result dataclasses
# --------------------------------------------------------------------------- #


@dataclass(frozen=True)
class Issue:
    severity: str  # "error" | "warning" | "info"
    code: str
    message: str
    sheet: str | None = None
    row: int | None = None
    column: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {k: v for k, v in asdict(self).items() if v is not None}


@dataclass
class ValidationResult:
    source_path: str
    source_kind: str
    status: str
    error_count: int = 0
    warning_count: int = 0
    info_count: int = 0
    issues: list[Issue] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "cohort": COHORT_NAME,
            "source_path": self.source_path,
            "source_kind": self.source_kind,
            "status": self.status,
            "error_count": self.error_count,
            "warning_count": self.warning_count,
            "info_count": self.info_count,
            "issues": [i.to_dict() for i in self.issues],
        }


# --------------------------------------------------------------------------- #
# Small helpers
# --------------------------------------------------------------------------- #


def normalize_token(value: Any) -> str:
    """Collapse ``Table(s)`` / ``% Completeness`` / etc. to a comparable key."""
    return re.sub(r"[^a-z0-9]+", "_", str(value).strip().lower()).strip("_")


def display_value(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    if pd.isna(value):
        return ""
    return str(value).strip()


def is_blank(value: Any) -> bool:
    return display_value(value) == ""


def split_tokens(value: Any) -> list[str]:
    text = display_value(value)
    if not text:
        return []
    return [t.strip() for t in re.split(r"[,;\n]+", text) if t.strip()]


def parse_percent_like(value: Any) -> float | None:
    """Parse ``98.4%`` / ``98.4`` / ``0.984`` to a number in [0, 100]."""
    text = display_value(value)
    if not text:
        return None
    cleaned = text.replace("%", "").replace(",", "").strip()
    try:
        parsed = float(cleaned)
    except ValueError:
        return None
    if "%" in text:
        return parsed
    if 0 <= parsed <= 1:
        return parsed * 100
    return parsed


def resolve_sheet_name(
    frames: dict[str, pd.DataFrame], aliases: list[str]
) -> str | None:
    lookup = {normalize_token(name): name for name in frames}
    for alias in aliases:
        hit = lookup.get(normalize_token(alias))
        if hit:
            return hit
    return None


# --------------------------------------------------------------------------- #
# Step 1 — load the source file
# --------------------------------------------------------------------------- #


def step_load_source(path: Path) -> tuple[dict[str, pd.DataFrame], str]:
    """Return ``({sheet_name: frame}, source_kind)``.

    ``source_kind`` is ``"workbook"`` for xlsx/xlsm/xls and ``"flat_file"`` for
    csv/tsv. A flat file is exposed under a single virtual sheet named
    ``"Variables"`` so the rest of the pipeline does not branch on format.
    """
    suffix = path.suffix.lower()
    if suffix in {".xlsx", ".xlsm", ".xls"}:
        workbook = pd.read_excel(path, sheet_name=None, dtype=str)
        return {name: frame.fillna("") for name, frame in workbook.items()}, "workbook"
    if suffix in {".csv", ".tsv"}:
        sep = "\t" if suffix == ".tsv" else ","
        frame = pd.read_csv(path, dtype=str, keep_default_na=False, sep=sep)
        return {"Variables": frame.fillna("")}, "flat_file"
    raise ValueError(
        f"Unsupported source type {path.suffix!r}. "
        "Use .xlsx, .xls, .xlsm, .csv, or .tsv."
    )


# --------------------------------------------------------------------------- #
# Step 2 — check required sheets
# --------------------------------------------------------------------------- #


def step_check_required_sheets(
    frames: dict[str, pd.DataFrame], source_kind: str
) -> tuple[dict[str, str], list[Issue]]:
    """Return ``({canonical_name: actual_name}, issues)``.

    Only meaningful for workbook inputs. For flat files the step just emits an
    info message so the user knows the check was skipped.
    """
    issues: list[Issue] = []
    resolved: dict[str, str] = {}

    if source_kind != "workbook":
        issues.append(
            Issue(
                severity="info",
                code="sheet_validation_skipped",
                message=(
                    "Workbook-level tab validation was skipped because the "
                    "source is a flat file."
                ),
            )
        )
        return resolved, issues

    for canonical, aliases in REQUIRED_SHEETS.items():
        actual = resolve_sheet_name(frames, aliases)
        if actual is None:
            issues.append(
                Issue(
                    severity="error",
                    code="missing_sheet",
                    message=(
                        f"Missing required sheet '{canonical}'. "
                        f"Accepted names: {', '.join(aliases)}"
                    ),
                    sheet=canonical,
                )
            )
            continue
        resolved[canonical] = actual
        if frames[actual].dropna(how="all").empty:
            issues.append(
                Issue(
                    severity="warning",
                    code="empty_sheet",
                    message=f"Sheet '{actual}' is present but has no populated rows.",
                    sheet=actual,
                )
            )
    return resolved, issues


# --------------------------------------------------------------------------- #
# Step 3 — canonicalise column headers on the Variables sheet
# --------------------------------------------------------------------------- #


def step_canonicalize_headers(
    frame: pd.DataFrame, sheet_name: str
) -> tuple[pd.DataFrame, list[Issue], set[str]]:
    """Rename headers that match an alias to their canonical form.

    Returns the renamed frame, any issues raised (e.g. duplicated aliases),
    and the set of original header names that were matched.
    """
    issues: list[Issue] = []

    normalized: dict[str, list[str]] = {}
    for column in frame.columns:
        normalized.setdefault(normalize_token(column), []).append(str(column))

    for norm, actuals in normalized.items():
        if len(actuals) > 1:
            issues.append(
                Issue(
                    severity="warning",
                    code="duplicate_header_alias",
                    message=(
                        f"Multiple headers normalise to '{norm}': "
                        + ", ".join(actuals)
                    ),
                    sheet=sheet_name,
                )
            )

    rename_map: dict[str, str] = {}
    matched: set[str] = set()
    for canonical, aliases in COLUMN_ALIASES.items():
        for alias in aliases:
            actuals = normalized.get(normalize_token(alias), [])
            if actuals:
                rename_map[actuals[0]] = canonical
                matched.add(actuals[0])
                break

    return frame.rename(columns=rename_map).copy(), issues, matched


# --------------------------------------------------------------------------- #
# Step 4 — required columns + column order
# --------------------------------------------------------------------------- #


def step_check_required_columns(
    frame: pd.DataFrame,
    sheet_name: str,
    matched_originals: set[str],
    original_columns: list[str],
) -> tuple[list[Issue], list[str]]:
    """Return ``(issues, missing_columns)``."""
    issues: list[Issue] = []
    missing = [c for c in REQUIRED_COLUMNS if c not in frame.columns]

    for col in missing:
        aliases = COLUMN_ALIASES.get(col, [col])
        issues.append(
            Issue(
                severity="error",
                code="missing_column",
                message=(
                    f"Missing required column '{col}'. "
                    f"Accepted names: {', '.join(aliases)}"
                ),
                sheet=sheet_name,
                column=col,
            )
        )

    extras = [c for c in original_columns if c not in matched_originals]
    if extras:
        issues.append(
            Issue(
                severity="info",
                code="extra_columns",
                message=(
                    "Unmapped columns were left untouched: "
                    + ", ".join(map(str, extras))
                ),
                sheet=sheet_name,
            )
        )

    if not missing:
        present_expected = [c for c in COLUMN_ORDER if c in frame.columns]
        actual_subset = [c for c in frame.columns if c in COLUMN_ORDER]
        if actual_subset != present_expected:
            issues.append(
                Issue(
                    severity="warning",
                    code="column_order_mismatch",
                    message=(
                        "Columns do not follow the expected order. "
                        "Expected order starts with: "
                        + ", ".join(COLUMN_ORDER)
                    ),
                    sheet=sheet_name,
                )
            )

    return issues, missing


# --------------------------------------------------------------------------- #
# Step 5 — per-row checks
# --------------------------------------------------------------------------- #


def step_check_rows(frame: pd.DataFrame, sheet_name: str) -> list[Issue]:
    issues: list[Issue] = []
    allowed_extraction = {normalize_token(v) for v in ALLOWED_EXTRACTION_TYPES}
    recommended_schemas = {normalize_token(v) for v in RECOMMENDED_SCHEMA_VALUES}

    seen_variables: dict[str, int] = {}
    for row_index, row in frame.iterrows():
        excel_row = int(row_index) + 2  # +1 header, +1 for spreadsheet's 1-indexing

        # Skip fully blank rows — trailing empty row in source is common.
        if all(is_blank(row.get(c, "")) for c in frame.columns):
            continue

        # Required non-empty fields
        for col in REQUIRED_NON_EMPTY_COLUMNS:
            if is_blank(row.get(col, "")):
                issues.append(
                    Issue(
                        severity="error",
                        code="blank_required_value",
                        message=f"Required field '{col}' is blank.",
                        sheet=sheet_name,
                        row=excel_row,
                        column=col,
                    )
                )

        variable = display_value(row.get("variable", ""))
        if variable:
            if not re.fullmatch(VARIABLE_NAME_PATTERN, variable):
                issues.append(
                    Issue(
                        severity="error",
                        code="invalid_variable_name",
                        message=(
                            f"Variable name '{variable}' does not match the "
                            f"expected pattern {VARIABLE_NAME_PATTERN!r}."
                        ),
                        sheet=sheet_name,
                        row=excel_row,
                        column="variable",
                    )
                )
            if variable in seen_variables:
                issues.append(
                    Issue(
                        severity="error",
                        code="duplicate_variable",
                        message=(
                            f"Variable '{variable}' is duplicated. "
                            f"First seen on row {seen_variables[variable]}."
                        ),
                        sheet=sheet_name,
                        row=excel_row,
                        column="variable",
                    )
                )
            else:
                seen_variables[variable] = excel_row

        # Completeness
        pct = parse_percent_like(row.get("completeness", ""))
        if pct is None:
            issues.append(
                Issue(
                    severity="error",
                    code="invalid_completeness",
                    message=(
                        "Completeness must be numeric — percent like '98.4%' "
                        "or decimal like '0.984'."
                    ),
                    sheet=sheet_name,
                    row=excel_row,
                    column="completeness",
                )
            )
        elif not 0 <= pct <= 100:
            issues.append(
                Issue(
                    severity="error",
                    code="completeness_out_of_range",
                    message="Completeness must fall between 0 and 100 percent.",
                    sheet=sheet_name,
                    row=excel_row,
                    column="completeness",
                )
            )

        # Extraction type
        extraction = display_value(row.get("extraction_type", ""))
        extraction_norm = normalize_token(extraction)
        if extraction_norm and extraction_norm not in allowed_extraction:
            issues.append(
                Issue(
                    severity="warning",
                    code="unknown_extraction_type",
                    message=(
                        f"Extraction type '{extraction}' is not in the "
                        f"allow list {sorted(ALLOWED_EXTRACTION_TYPES)}."
                    ),
                    sheet=sheet_name,
                    row=excel_row,
                    column="extraction_type",
                )
            )

        # Schema / table reference
        schemas = split_tokens(row.get("schema", ""))
        if not schemas:
            issues.append(
                Issue(
                    severity="error",
                    code="missing_schema",
                    message="At least one schema/table reference is required.",
                    sheet=sheet_name,
                    row=excel_row,
                    column="schema",
                )
            )
        else:
            for schema in schemas:
                if normalize_token(schema) not in recommended_schemas:
                    issues.append(
                        Issue(
                            severity="warning",
                            code="unexpected_schema_value",
                            message=(
                                f"Schema/table '{schema}' is not in the "
                                "recommended list."
                            ),
                            sheet=sheet_name,
                            row=excel_row,
                            column="schema",
                        )
                    )

        # Source columns — expect snake_case table.column or column
        for source_column in split_tokens(row.get("source_columns", "")):
            if not SOURCE_COLUMN_PATTERN.fullmatch(source_column):
                issues.append(
                    Issue(
                        severity="warning",
                        code="unexpected_source_column_format",
                        message=(
                            f"Source column '{source_column}' does not look "
                            "like a snake_case column reference."
                        ),
                        sheet=sheet_name,
                        row=excel_row,
                        column="source_columns",
                    )
                )

        # Either values or distribution should give reviewers context.
        if is_blank(row.get("values", "")) and is_blank(row.get("distribution", "")):
            issues.append(
                Issue(
                    severity="warning",
                    code="missing_value_context",
                    message=(
                        "Both 'values' and 'distribution' are blank. One of "
                        "them is usually helpful for downstream review."
                    ),
                    sheet=sheet_name,
                    row=excel_row,
                )
            )

    return issues


# --------------------------------------------------------------------------- #
# Orchestrator + reporting
# --------------------------------------------------------------------------- #


def _summarise(issues: list[Issue]) -> tuple[int, int, int]:
    e = sum(i.severity == "error" for i in issues)
    w = sum(i.severity == "warning" for i in issues)
    n = sum(i.severity == "info" for i in issues)
    return e, w, n


def _print_step(step_num: int, title: str) -> None:
    banner = f"Step {step_num}: {title}"
    print(banner)
    print("-" * len(banner))


def _print_issues(issues: list[Issue], indent: str = "  ") -> None:
    if not issues:
        print(f"{indent}ok")
        return
    for i in issues:
        loc = []
        if i.sheet:
            loc.append(f"sheet={i.sheet}")
        if i.row is not None:
            loc.append(f"row={i.row}")
        if i.column:
            loc.append(f"column={i.column}")
        loc_str = f" ({', '.join(loc)})" if loc else ""
        print(f"{indent}[{i.severity.upper()}] {i.code}{loc_str}: {i.message}")


def validate_source(source_path: str | Path, verbose: bool = True) -> ValidationResult:
    """Run every step in order and return a :class:`ValidationResult`.

    When ``verbose`` is true (the default, as called from the CLI) the step
    banners and per-step issues are printed as the run progresses.
    """
    source = Path(source_path)
    all_issues: list[Issue] = []

    # --- Step 1: load ---
    if verbose:
        _print_step(1, f"Load source ({source})")
    frames, source_kind = step_load_source(source)
    if verbose:
        print(f"  loaded {len(frames)} sheet(s), kind={source_kind}")
        for name, frame in frames.items():
            print(f"    {name!r}: {len(frame)} rows x {len(frame.columns)} cols")
        print()

    # --- Step 2: required sheets ---
    if verbose:
        _print_step(2, "Check required sheets")
    resolved, step2_issues = step_check_required_sheets(frames, source_kind)
    all_issues.extend(step2_issues)
    if verbose:
        _print_issues(step2_issues)
        print()

    variables_sheet = resolved.get("variables") or resolve_sheet_name(
        frames, REQUIRED_SHEETS["variables"]
    )

    if variables_sheet is None:
        all_issues.append(
            Issue(
                severity="error",
                code="missing_variables_sheet",
                message=(
                    "Could not locate the variables / data dictionary sheet. "
                    f"Accepted names: {', '.join(REQUIRED_SHEETS['variables'])}"
                ),
            )
        )
        return _build_result(source, source_kind, all_issues, verbose)

    raw_frame = frames[variables_sheet]
    original_columns = list(raw_frame.columns)

    # --- Step 3: canonical headers ---
    if verbose:
        _print_step(3, f"Canonicalise headers on '{variables_sheet}'")
    canonical_frame, step3_issues, matched_originals = step_canonicalize_headers(
        raw_frame, variables_sheet
    )
    all_issues.extend(step3_issues)
    if verbose:
        _print_issues(step3_issues)
        print(f"  matched {len(matched_originals)}/{len(original_columns)} headers")
        print()

    # --- Step 4: required columns + order ---
    if verbose:
        _print_step(4, "Check required columns and column order")
    step4_issues, missing = step_check_required_columns(
        canonical_frame, variables_sheet, matched_originals, original_columns
    )
    all_issues.extend(step4_issues)
    if verbose:
        _print_issues(step4_issues)
        print()

    if missing:
        if verbose:
            print("  skipping row-level checks until required columns are fixed.\n")
        return _build_result(source, source_kind, all_issues, verbose)

    # --- Step 5: per-row checks ---
    if verbose:
        _print_step(5, f"Walk rows of '{variables_sheet}'")
    step5_issues = step_check_rows(canonical_frame, variables_sheet)
    all_issues.extend(step5_issues)
    if verbose:
        _print_issues(step5_issues)
        print()

    return _build_result(source, source_kind, all_issues, verbose)


def _build_result(
    source: Path,
    source_kind: str,
    issues: list[Issue],
    verbose: bool,
) -> ValidationResult:
    e, w, n = _summarise(issues)
    status = "passed" if e == 0 else "failed"
    result = ValidationResult(
        source_path=str(source.resolve()),
        source_kind=source_kind,
        status=status,
        error_count=e,
        warning_count=w,
        info_count=n,
        issues=issues,
    )
    if verbose:
        _print_step(6, "Summary")
        print(f"  cohort  : {COHORT_NAME}")
        print(f"  source  : {result.source_path}")
        print(f"  status  : {result.status.upper()}")
        print(f"  counts  : {e} error(s), {w} warning(s), {n} info")
    return result


# --------------------------------------------------------------------------- #
# CLI
# --------------------------------------------------------------------------- #


def _parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description=f"Validate the {COHORT_NAME} clinical coding dictionary."
    )
    p.add_argument("--input", required=True, help="Path to the dictionary file.")
    p.add_argument(
        "--report-json",
        default=None,
        help="Optional path to write a JSON validation report.",
    )
    p.add_argument(
        "--fail-on-warning",
        action="store_true",
        help="Return a non-zero exit code when any warnings are present.",
    )
    p.add_argument(
        "--quiet",
        action="store_true",
        help="Suppress per-step output. Summary still prints.",
    )
    return p


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    result = validate_source(args.input, verbose=not args.quiet)

    if args.report_json:
        Path(args.report_json).write_text(
            json.dumps(result.to_dict(), indent=2), encoding="utf-8"
        )

    if result.error_count > 0:
        return 1
    if args.fail_on_warning and result.warning_count > 0:
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
