#!/usr/bin/env python3
"""Validate the mtc_aat_cohort clinical coding dictionary.

Single-file validator workflow:

    1. Load the source file (xlsx workbook or csv/tsv flat file).
    2. Check required sheets exist (workbook only).
    3. Locate the Variables sheet and canonicalize its column headers.
    4. Check required columns are present and in the expected order.
    5. Walk every row and validate values.
    6. Summarize and exit.

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
# mtc_aat_cohort rules (inlined - this validator is single-cohort by design)
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
    "source_columns": [
        "column(s)",
        "columns",
        "column",
        "source columns",
        "source column",
    ],
    "criteria": ["criteria", "logic", "filter criteria", "configuration criteria"],
    "values": ["values", "value examples", "valid values"],
    "distribution": ["distribution", "value distribution"],
    "completeness": [
        "completeness",
        "completion",
        "% completeness",
        "percent completeness",
    ],
    "extraction_type": [
        "extraction type",
        "extract type",
        "capture type",
        "mapping type",
    ],
    "notes": ["notes", "comments"],
}

ALLOWED_EXTRACTION_TYPES: set[str] = {
    "Structured",
    "Abstract",
    "Abstraction",
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

# Display labels ("Heart rate", "AAT level", "pTau-217") - not SQL ids.
VARIABLE_NAME_PATTERN: str = r"^[A-Za-z][A-Za-z0-9 _/().\-]*$"

SOURCE_COLUMN_PATTERN = re.compile(r"^[a-z][a-z0-9_]*(\.[a-z][a-z0-9_]*)?$")


PACKS_DIR = Path(__file__).resolve().parent / "packs"


def _deep_merge(base: Any, overlay: Any) -> Any:
    """Same merge semantics as the generator: lists append, scalars replace,
    dicts deep-merge. Used when a cohort pack's ``validator`` section extends
    the module-level defaults.
    """
    if isinstance(base, dict) and isinstance(overlay, dict):
        merged: dict[str, Any] = {**base}
        for key, value in overlay.items():
            merged[key] = _deep_merge(base.get(key), value) if key in base else value
        return merged
    if isinstance(base, list) and isinstance(overlay, list):
        return [*base, *overlay]
    return overlay


def apply_profile_overrides(profile: dict[str, Any]) -> None:
    """Overlay ``profile`` onto the module-level validator constants.

    ``profile`` follows the schema of the optional ``validator:`` block in a
    cohort pack. Anything missing falls back to the compiled-in default.

    Merge semantics are uniform across every field and match the generator:

      * lists  -> append (overlay extends base; stable order)
      * dicts  -> deep-merge (recurse per-key)
      * scalars/strings -> replace

    Sets are round-tripped through their list representation so they honor
    the same append rule. The merge is in-place on module globals so every
    step function picks up the overrides without threading a config
    argument through each call.
    """
    global COHORT_NAME, REQUIRED_SHEETS, REQUIRED_COLUMNS
    global REQUIRED_NON_EMPTY_COLUMNS, COLUMN_ORDER, COLUMN_ALIASES
    global ALLOWED_EXTRACTION_TYPES, RECOMMENDED_SCHEMA_VALUES
    global VARIABLE_NAME_PATTERN

    if "cohort_name" in profile:
        COHORT_NAME = str(profile["cohort_name"])

    if "required_sheets" in profile:
        REQUIRED_SHEETS = _deep_merge(REQUIRED_SHEETS, profile["required_sheets"])

    if "required_columns" in profile:
        REQUIRED_COLUMNS = _deep_merge(REQUIRED_COLUMNS, list(profile["required_columns"]))

    if "required_non_empty_columns" in profile:
        REQUIRED_NON_EMPTY_COLUMNS = _deep_merge(
            REQUIRED_NON_EMPTY_COLUMNS, list(profile["required_non_empty_columns"])
        )

    if "column_order" in profile:
        COLUMN_ORDER = _deep_merge(COLUMN_ORDER, list(profile["column_order"]))

    if "column_aliases" in profile:
        COLUMN_ALIASES = _deep_merge(COLUMN_ALIASES, profile["column_aliases"])

    if "allowed_extraction_types" in profile:
        # list-append + dedupe via set, so the rule stays consistent.
        ALLOWED_EXTRACTION_TYPES = set(
            _deep_merge(
                sorted(ALLOWED_EXTRACTION_TYPES),
                list(profile["allowed_extraction_types"]),
            )
        )

    if "recommended_schema_values" in profile:
        RECOMMENDED_SCHEMA_VALUES = set(
            _deep_merge(
                sorted(RECOMMENDED_SCHEMA_VALUES),
                list(profile["recommended_schema_values"]),
            )
        )

    if "variable_name_pattern" in profile:
        VARIABLE_NAME_PATTERN = str(profile["variable_name_pattern"])


def load_cohort_profile(cohort: str, packs_dir: Path = PACKS_DIR) -> dict[str, Any]:
    """Load ``packs/cohorts/<cohort>.yaml`` and return the flat profile dict.

    The returned dict has ``cohort_name`` populated from the pack's top-level
    ``cohort_name``, then merged with the optional ``validator:`` section.
    Raises FileNotFoundError if the pack doesn't exist.
    """
    try:
        import yaml
    except ImportError as exc:  # pragma: no cover
        raise SystemExit(
            "PyYAML is required for --cohort / --profile loading. "
            "Install with: pip install pyyaml"
        ) from exc

    path = packs_dir / "cohorts" / f"{cohort}.yaml"
    if not path.is_file():
        raise FileNotFoundError(
            f"cohort pack missing: {path}. Available: "
            + ", ".join(
                sorted(p.stem for p in (packs_dir / "cohorts").glob("*.yaml"))
            )
        )
    data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    profile = dict(data.get("validator", {}))
    profile.setdefault("cohort_name", data.get("cohort_name"))
    return profile


def load_profile_file(path: str | Path) -> dict[str, Any]:
    """Load a stand-alone validator profile YAML or JSON."""
    path = Path(path)
    if path.suffix.lower() in {".yaml", ".yml"}:
        try:
            import yaml
        except ImportError as exc:  # pragma: no cover
            raise SystemExit(
                "PyYAML is required to load YAML profiles. "
                "Install with: pip install pyyaml"
            ) from exc
        return yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if path.suffix.lower() == ".json":
        return json.loads(path.read_text(encoding="utf-8"))
    raise ValueError(f"Unsupported profile format: {path.suffix!r}. Use .yaml or .json.")


# --------------------------------------------------------------------------- #
# Issue and result dataclasses
# --------------------------------------------------------------------------- #


@dataclass(frozen=True)
class Issue:
    severity: str
    code: str
    message: str
    sheet: str | None = None
    row: int | None = None
    column: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {key: value for key, value in asdict(self).items() if value is not None}


@dataclass
class ValidationResult:
    source_path: str
    source_kind: str
    status: str
    cohort: str = ""  # snapshotted from COHORT_NAME at construction time
    error_count: int = 0
    warning_count: int = 0
    info_count: int = 0
    issues: list[Issue] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        # ``cohort`` is captured at build-time (see _build_result) so that a
        # later call to apply_profile_overrides doesn't change the JSON
        # report we hand back to the caller.
        return {
            "cohort": self.cohort or COHORT_NAME,
            "source_path": self.source_path,
            "source_kind": self.source_kind,
            "status": self.status,
            "error_count": self.error_count,
            "warning_count": self.warning_count,
            "info_count": self.info_count,
            "issues": [issue.to_dict() for issue in self.issues],
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
    return [token.strip() for token in re.split(r"[,;\n]+", text) if token.strip()]


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


def resolve_sheet_name(frames: dict[str, pd.DataFrame], aliases: list[str]) -> str | None:
    lookup = {normalize_token(name): name for name in frames}
    for alias in aliases:
        actual = lookup.get(normalize_token(alias))
        if actual:
            return actual
    return None


def frame_has_content(frame: pd.DataFrame) -> bool:
    """Return True when at least one cell contains non-whitespace content."""
    if frame.empty:
        return False

    normalized = frame.fillna("").astype(str)
    return normalized.apply(
        lambda column: column.map(lambda value: bool(value.strip()))
    ).any().any()


# --------------------------------------------------------------------------- #
# Step 1 - load the source file
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
        separator = "\t" if suffix == ".tsv" else ","
        frame = pd.read_csv(path, dtype=str, keep_default_na=False, sep=separator)
        return {"Variables": frame.fillna("")}, "flat_file"

    raise ValueError(
        f"Unsupported source type {path.suffix!r}. "
        "Use .xlsx, .xls, .xlsm, .csv, or .tsv."
    )


# --------------------------------------------------------------------------- #
# Step 2 - check required sheets
# --------------------------------------------------------------------------- #


def step_check_required_sheets(
    frames: dict[str, pd.DataFrame],
    source_kind: str,
) -> tuple[dict[str, str], list[Issue]]:
    """Return ``({canonical_name: actual_name}, issues)``."""
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

    for canonical_name, aliases in REQUIRED_SHEETS.items():
        actual_name = resolve_sheet_name(frames, aliases)
        if actual_name is None:
            issues.append(
                Issue(
                    severity="error",
                    code="missing_sheet",
                    message=(
                        f"Missing required sheet '{canonical_name}'. "
                        f"Accepted names: {', '.join(aliases)}"
                    ),
                    sheet=canonical_name,
                )
            )
            continue

        resolved[canonical_name] = actual_name

        if not frame_has_content(frames[actual_name]):
            issues.append(
                Issue(
                    severity="warning",
                    code="empty_sheet",
                    message=f"Sheet '{actual_name}' is present but has no populated rows.",
                    sheet=actual_name,
                )
            )

    return resolved, issues


# --------------------------------------------------------------------------- #
# Step 3 - canonicalize column headers on the Variables sheet
# --------------------------------------------------------------------------- #


def step_canonicalize_headers(
    frame: pd.DataFrame,
    sheet_name: str,
) -> tuple[pd.DataFrame, list[Issue], set[str]]:
    """Rename headers that match an alias to their canonical form."""
    issues: list[Issue] = []
    normalized_headers: dict[str, list[str]] = {}

    for column in frame.columns:
        normalized_headers.setdefault(normalize_token(column), []).append(str(column))

    for normalized_name, actual_columns in normalized_headers.items():
        if len(actual_columns) > 1:
            issues.append(
                Issue(
                    severity="warning",
                    code="duplicate_header_alias",
                    message=(
                        f"Multiple headers normalize to '{normalized_name}': "
                        + ", ".join(actual_columns)
                    ),
                    sheet=sheet_name,
                )
            )

    rename_map: dict[str, str] = {}
    matched_originals: set[str] = set()
    for canonical_name, aliases in COLUMN_ALIASES.items():
        for alias in aliases:
            actual_columns = normalized_headers.get(normalize_token(alias), [])
            if actual_columns:
                rename_map[actual_columns[0]] = canonical_name
                matched_originals.add(actual_columns[0])
                break

    return frame.rename(columns=rename_map).copy(), issues, matched_originals


# --------------------------------------------------------------------------- #
# Step 4 - required columns + column order
# --------------------------------------------------------------------------- #


def step_check_required_columns(
    frame: pd.DataFrame,
    sheet_name: str,
    matched_originals: set[str],
    original_columns: list[str],
) -> tuple[list[Issue], list[str]]:
    """Return ``(issues, missing_columns)``."""
    issues: list[Issue] = []
    missing_columns = [column for column in REQUIRED_COLUMNS if column not in frame.columns]

    for column in missing_columns:
        aliases = COLUMN_ALIASES.get(column, [column])
        issues.append(
            Issue(
                severity="error",
                code="missing_column",
                message=(
                    f"Missing required column '{column}'. "
                    f"Accepted names: {', '.join(aliases)}"
                ),
                sheet=sheet_name,
                column=column,
            )
        )

    extra_columns = [column for column in original_columns if column not in matched_originals]
    if extra_columns:
        issues.append(
            Issue(
                severity="info",
                code="extra_columns",
                message="Unmapped columns were left untouched: " + ", ".join(extra_columns),
                sheet=sheet_name,
            )
        )

    if not missing_columns:
        present_expected = [column for column in COLUMN_ORDER if column in frame.columns]
        actual_subset = [column for column in frame.columns if column in COLUMN_ORDER]
        if actual_subset != present_expected:
            issues.append(
                Issue(
                    severity="warning",
                    code="column_order_mismatch",
                    message=(
                        "Columns do not follow the expected order. "
                        "Expected order starts with: " + ", ".join(COLUMN_ORDER)
                    ),
                    sheet=sheet_name,
                )
            )

    return issues, missing_columns


# --------------------------------------------------------------------------- #
# Step 5 - per-row checks
# --------------------------------------------------------------------------- #


def step_check_rows(frame: pd.DataFrame, sheet_name: str) -> list[Issue]:
    issues: list[Issue] = []
    allowed_extraction = {normalize_token(value) for value in ALLOWED_EXTRACTION_TYPES}
    recommended_schemas = {normalize_token(value) for value in RECOMMENDED_SCHEMA_VALUES}

    seen_variables: dict[str, int] = {}
    for row_index, row in frame.iterrows():
        excel_row = int(row_index) + 2

        if all(is_blank(row.get(column, "")) for column in frame.columns):
            continue

        for column in REQUIRED_NON_EMPTY_COLUMNS:
            if is_blank(row.get(column, "")):
                issues.append(
                    Issue(
                        severity="error",
                        code="blank_required_value",
                        message=f"Required field '{column}' is blank.",
                        sheet=sheet_name,
                        row=excel_row,
                        column=column,
                    )
                )

        variable_name = display_value(row.get("variable", ""))
        if variable_name:
            if not re.fullmatch(VARIABLE_NAME_PATTERN, variable_name):
                issues.append(
                    Issue(
                        severity="error",
                        code="invalid_variable_name",
                        message=(
                            f"Variable name '{variable_name}' does not match the "
                            f"expected pattern {VARIABLE_NAME_PATTERN!r}."
                        ),
                        sheet=sheet_name,
                        row=excel_row,
                        column="variable",
                    )
                )

            normalized_variable = normalize_token(variable_name)
            if normalized_variable in seen_variables:
                issues.append(
                    Issue(
                        severity="error",
                        code="duplicate_variable",
                        message=(
                            f"Variable '{variable_name}' is duplicated. "
                            f"First seen on row {seen_variables[normalized_variable]}."
                        ),
                        sheet=sheet_name,
                        row=excel_row,
                        column="variable",
                    )
                )
            else:
                seen_variables[normalized_variable] = excel_row

        # Skip typed completeness checks when the cell is blank - the
        # ``blank_required_value`` error above already flags it once.
        # Reporting it twice just inflates the error count.
        completeness_raw = row.get("completeness", "")
        if not is_blank(completeness_raw):
            completeness = parse_percent_like(completeness_raw)
            if completeness is None:
                issues.append(
                    Issue(
                        severity="error",
                        code="invalid_completeness",
                        message=(
                            "Completeness must be numeric - percent like '98.4%' "
                            "or decimal like '0.984'."
                        ),
                        sheet=sheet_name,
                        row=excel_row,
                        column="completeness",
                    )
                )
            elif not 0 <= completeness <= 100:
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

        extraction_type = display_value(row.get("extraction_type", ""))
        normalized_extraction_type = normalize_token(extraction_type)
        if normalized_extraction_type and normalized_extraction_type not in allowed_extraction:
            issues.append(
                Issue(
                    severity="warning",
                    code="unknown_extraction_type",
                    message=(
                        f"Extraction type '{extraction_type}' is not in the allow list "
                        f"{sorted(ALLOWED_EXTRACTION_TYPES)}."
                    ),
                    sheet=sheet_name,
                    row=excel_row,
                    column="extraction_type",
                )
            )

        # Skip ``missing_schema`` when the cell is blank - the
        # ``blank_required_value`` error already covers it. Only emit
        # unexpected-schema warnings when there's actually content.
        schemas = split_tokens(row.get("schema", ""))
        for schema in schemas:
            if normalize_token(schema) not in recommended_schemas:
                issues.append(
                    Issue(
                        severity="warning",
                        code="unexpected_schema_value",
                        message=(
                            f"Schema/table '{schema}' is not in the recommended list."
                        ),
                        sheet=sheet_name,
                        row=excel_row,
                        column="schema",
                    )
                )

        for source_column in split_tokens(row.get("source_columns", "")):
            if not SOURCE_COLUMN_PATTERN.fullmatch(source_column):
                issues.append(
                    Issue(
                        severity="warning",
                        code="unexpected_source_column_format",
                        message=(
                            f"Source column '{source_column}' does not look like a "
                            "snake_case column reference."
                        ),
                        sheet=sheet_name,
                        row=excel_row,
                        column="source_columns",
                    )
                )

        if is_blank(row.get("values", "")) and is_blank(row.get("distribution", "")):
            issues.append(
                Issue(
                    severity="warning",
                    code="missing_value_context",
                    message=(
                        "Both 'values' and 'distribution' are blank. One of them is "
                        "usually helpful for downstream review."
                    ),
                    sheet=sheet_name,
                    row=excel_row,
                )
            )

    return issues


# --------------------------------------------------------------------------- #
# Orchestrator + reporting
# --------------------------------------------------------------------------- #


def _summarize(issues: list[Issue]) -> tuple[int, int, int]:
    error_count = sum(issue.severity == "error" for issue in issues)
    warning_count = sum(issue.severity == "warning" for issue in issues)
    info_count = sum(issue.severity == "info" for issue in issues)
    return error_count, warning_count, info_count


def _print_step(step_num: int, title: str) -> None:
    banner = f"Step {step_num}: {title}"
    print(banner)
    print("-" * len(banner))


def _print_issues(issues: list[Issue], indent: str = "  ") -> None:
    if not issues:
        print(f"{indent}ok")
        return

    for issue in issues:
        location_parts = []
        if issue.sheet:
            location_parts.append(f"sheet={issue.sheet}")
        if issue.row is not None:
            location_parts.append(f"row={issue.row}")
        if issue.column:
            location_parts.append(f"column={issue.column}")
        location = f" ({', '.join(location_parts)})" if location_parts else ""
        print(f"{indent}[{issue.severity.upper()}] {issue.code}{location}: {issue.message}")


def _print_summary(result: ValidationResult) -> None:
    _print_step(6, "Summary")
    print(f"  cohort  : {COHORT_NAME}")
    print(f"  source  : {result.source_path}")
    print(f"  status  : {result.status.upper()}")
    print(
        f"  counts  : {result.error_count} error(s), "
        f"{result.warning_count} warning(s), {result.info_count} info"
    )


def validate_source(source_path: str | Path, verbose: bool = True) -> ValidationResult:
    """Run every step in order and return a :class:`ValidationResult`."""
    source = Path(source_path)
    all_issues: list[Issue] = []

    if verbose:
        _print_step(1, f"Load source ({source})")
    frames, source_kind = step_load_source(source)
    if verbose:
        print(f"  loaded {len(frames)} sheet(s), kind={source_kind}")
        for name, frame in frames.items():
            print(f"    {name!r}: {len(frame)} rows x {len(frame.columns)} cols")
        print()

    if verbose:
        _print_step(2, "Check required sheets")
    resolved_sheets, step2_issues = step_check_required_sheets(frames, source_kind)
    all_issues.extend(step2_issues)
    if verbose:
        _print_issues(step2_issues)
        print()

    variables_sheet = resolved_sheets.get("variables") or resolve_sheet_name(
        frames,
        REQUIRED_SHEETS["variables"],
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

    if verbose:
        _print_step(3, f"Canonicalize headers on '{variables_sheet}'")
    canonical_frame, step3_issues, matched_originals = step_canonicalize_headers(
        raw_frame,
        variables_sheet,
    )
    all_issues.extend(step3_issues)
    if verbose:
        _print_issues(step3_issues)
        print(f"  matched {len(matched_originals)}/{len(original_columns)} headers")
        print()

    if verbose:
        _print_step(4, "Check required columns and column order")
    step4_issues, missing_columns = step_check_required_columns(
        canonical_frame,
        variables_sheet,
        matched_originals,
        original_columns,
    )
    all_issues.extend(step4_issues)
    if verbose:
        _print_issues(step4_issues)
        print()

    if missing_columns:
        if verbose:
            print("  skipping row-level checks until required columns are fixed.\n")
        return _build_result(source, source_kind, all_issues, verbose)

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
    error_count, warning_count, info_count = _summarize(issues)
    result = ValidationResult(
        source_path=str(source.resolve()),
        source_kind=source_kind,
        status="passed" if error_count == 0 else "failed",
        cohort=COHORT_NAME,
        error_count=error_count,
        warning_count=warning_count,
        info_count=info_count,
        issues=issues,
    )
    if verbose:
        _print_summary(result)
    return result


# --------------------------------------------------------------------------- #
# CLI
# --------------------------------------------------------------------------- #


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Validate a Century clinical coding dictionary."
    )
    parser.add_argument("--input", required=True, help="Path to the dictionary file.")
    parser.add_argument(
        "--cohort",
        default=None,
        help=(
            "Load validator rules from packs/cohorts/<cohort>.yaml. "
            "The pack's optional ``validator:`` section overlays the "
            "built-in defaults."
        ),
    )
    parser.add_argument(
        "--profile",
        default=None,
        help=(
            "Load a stand-alone validator profile file (YAML or JSON) that "
            "overlays the built-in defaults. Mutually exclusive with --cohort."
        ),
    )
    parser.add_argument(
        "--report-json",
        default=None,
        help="Optional path to write a JSON validation report.",
    )
    parser.add_argument(
        "--fail-on-warning",
        action="store_true",
        help="Return a non-zero exit code when any warnings are present.",
    )
    parser.add_argument(
        "--quiet",
        action="store_true",
        help="Suppress per-step output and only print the final summary.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)

    if args.cohort and args.profile:
        print("--cohort and --profile are mutually exclusive.", file=sys.stderr)
        return 2

    try:
        if args.cohort:
            apply_profile_overrides(load_cohort_profile(args.cohort))
        elif args.profile:
            apply_profile_overrides(load_profile_file(args.profile))
    except (FileNotFoundError, ValueError) as exc:
        print(f"Profile load failed: {exc}", file=sys.stderr)
        return 1

    try:
        result = validate_source(args.input, verbose=not args.quiet)
    except (FileNotFoundError, ValueError, OSError) as exc:
        print(f"Validation failed to start: {exc}", file=sys.stderr)
        return 1

    if args.quiet:
        _print_summary(result)

    if args.report_json:
        Path(args.report_json).write_text(
            json.dumps(result.to_dict(), indent=2),
            encoding="utf-8",
        )

    if result.error_count > 0:
        return 1
    if args.fail_on_warning and result.warning_count > 0:
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
