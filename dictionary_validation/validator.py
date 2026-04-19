from __future__ import annotations

import argparse
import json
import re
import sys
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import pandas as pd

PACKAGE_ROOT = Path(__file__).resolve().parent
PROFILE_DIR = PACKAGE_ROOT / "profiles"

SNAKE_CASE_PATTERN = re.compile(r"^[a-z][a-z0-9_]*$")
SOURCE_COLUMN_PATTERN = re.compile(r"^[a-z][a-z0-9_]*(\.[a-z][a-z0-9_]*)?$")


@dataclass(frozen=True)
class Issue:
    severity: str
    code: str
    message: str
    sheet: str | None = None
    row: int | None = None
    column: str | None = None

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        return {key: value for key, value in data.items() if value is not None}


@dataclass(frozen=True)
class ValidationResult:
    profile_name: str
    source_path: str
    source_kind: str
    status: str
    error_count: int
    warning_count: int
    info_count: int
    issues: list[Issue]

    def to_dict(self) -> dict[str, Any]:
        return {
            "profile_name": self.profile_name,
            "source_path": self.source_path,
            "source_kind": self.source_kind,
            "status": self.status,
            "error_count": self.error_count,
            "warning_count": self.warning_count,
            "info_count": self.info_count,
            "issues": [issue.to_dict() for issue in self.issues],
        }


def normalize_token(value: str) -> str:
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


def load_profile(profile_name_or_path: str) -> dict[str, Any]:
    candidate = Path(profile_name_or_path)
    if candidate.exists():
        profile_path = candidate
    else:
        profile_path = PROFILE_DIR / f"{profile_name_or_path}.json"

    if not profile_path.exists():
        raise FileNotFoundError(
            f"Could not find validation profile '{profile_name_or_path}'. "
            f"Checked {profile_path}"
        )

    return json.loads(profile_path.read_text(encoding="utf-8"))


def list_profiles() -> list[str]:
    return sorted(path.stem for path in PROFILE_DIR.glob("*.json"))


def load_source_frames(source_path: Path) -> tuple[dict[str, pd.DataFrame], str]:
    suffix = source_path.suffix.lower()

    if suffix in {".xlsx", ".xlsm", ".xls"}:
        workbook = pd.read_excel(source_path, sheet_name=None, dtype=str)
        cleaned = {name: frame.fillna("") for name, frame in workbook.items()}
        return cleaned, "workbook"

    if suffix in {".csv", ".tsv"}:
        separator = "\t" if suffix == ".tsv" else ","
        frame = pd.read_csv(source_path, dtype=str, keep_default_na=False, sep=separator)
        return {"Variables": frame.fillna("")}, "flat_file"

    raise ValueError(
        f"Unsupported source type '{source_path.suffix}'. "
        "Use .xlsx, .xls, .xlsm, .csv, or .tsv."
    )


def resolve_sheet_name(
    frames: dict[str, pd.DataFrame],
    aliases: list[str],
) -> str | None:
    normalized_to_actual = {normalize_token(name): name for name in frames}
    for alias in aliases:
        actual_name = normalized_to_actual.get(normalize_token(alias))
        if actual_name:
            return actual_name
    return None


def canonicalize_columns(
    frame: pd.DataFrame,
    column_aliases: dict[str, list[str]],
) -> tuple[pd.DataFrame, list[Issue], set[str]]:
    issues: list[Issue] = []
    normalized_columns: dict[str, list[str]] = {}
    for column in frame.columns:
        normalized_columns.setdefault(normalize_token(column), []).append(str(column))

    for normalized_name, actual_columns in normalized_columns.items():
        if len(actual_columns) > 1:
            issues.append(
                Issue(
                    severity="warning",
                    code="duplicate_header_alias",
                    message=(
                        f"Multiple headers normalize to '{normalized_name}': "
                        + ", ".join(actual_columns)
                    ),
                )
            )

    rename_map: dict[str, str] = {}
    matched_columns: set[str] = set()
    for canonical_name, aliases in column_aliases.items():
        normalized_aliases = [normalize_token(alias) for alias in aliases]
        for alias in normalized_aliases:
            actual_columns = normalized_columns.get(alias, [])
            if actual_columns:
                rename_map[actual_columns[0]] = canonical_name
                matched_columns.add(actual_columns[0])
                break

    renamed = frame.rename(columns=rename_map).copy()
    return renamed, issues, matched_columns


def validate_sheet_presence(
    frames: dict[str, pd.DataFrame],
    profile: dict[str, Any],
    source_kind: str,
) -> tuple[dict[str, str], list[Issue]]:
    issues: list[Issue] = []
    resolved: dict[str, str] = {}

    required_sheets = profile.get("required_sheets", {})
    if source_kind != "workbook":
        if required_sheets:
            issues.append(
                Issue(
                    severity="info",
                    code="sheet_validation_skipped",
                    message=(
                        "Workbook-level tab validation was skipped because the source is a flat file."
                    ),
                )
            )
        return resolved, issues

    for canonical_name, aliases in required_sheets.items():
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

        if frames[actual_name].dropna(how="all").empty:
            issues.append(
                Issue(
                    severity="warning",
                    code="empty_sheet",
                    message=f"Sheet '{actual_name}' is present but has no populated rows.",
                    sheet=actual_name,
                )
            )

    return resolved, issues


def validate_column_order(
    actual_columns: list[str],
    expected_columns: list[str],
    sheet_name: str,
) -> list[Issue]:
    present_columns = [column for column in expected_columns if column in actual_columns]
    actual_subset = [column for column in actual_columns if column in expected_columns]
    if actual_subset == present_columns:
        return []
    return [
        Issue(
            severity="warning",
            code="column_order_mismatch",
            message=(
                "Columns do not follow the expected order. "
                f"Expected order starts with: {', '.join(expected_columns)}"
            ),
            sheet=sheet_name,
        )
    ]


def validate_variables_sheet(
    frame: pd.DataFrame,
    actual_sheet_name: str,
    profile: dict[str, Any],
) -> list[Issue]:
    rules = profile["variables_sheet"]
    issues: list[Issue] = []

    canonical_frame, header_issues, matched_columns = canonicalize_columns(
        frame,
        rules["column_aliases"],
    )
    issues.extend(
        Issue(
            severity=issue.severity,
            code=issue.code,
            message=issue.message,
            sheet=actual_sheet_name,
        )
        for issue in header_issues
    )

    required_columns = rules.get("required_columns", [])
    missing_columns = [column for column in required_columns if column not in canonical_frame.columns]
    for column in missing_columns:
        aliases = rules["column_aliases"].get(column, [column])
        issues.append(
            Issue(
                severity="error",
                code="missing_column",
                message=f"Missing required column '{column}'. Accepted names: {', '.join(aliases)}",
                sheet=actual_sheet_name,
                column=column,
            )
        )

    extra_columns = [column for column in frame.columns if column not in matched_columns]
    if extra_columns:
        issues.append(
            Issue(
                severity="info",
                code="extra_columns",
                message="Unmapped columns were left untouched: " + ", ".join(map(str, extra_columns)),
                sheet=actual_sheet_name,
            )
        )

    if missing_columns:
        return issues

    issues.extend(
        validate_column_order(
            list(canonical_frame.columns),
            rules.get("column_order", required_columns),
            actual_sheet_name,
        )
    )

    required_non_empty_columns = rules.get("required_non_empty_columns", required_columns)
    recommended_schema_values = {
        normalize_token(value) for value in rules.get("recommended_schema_values", [])
    }
    allowed_extraction_types = {
        normalize_token(value) for value in rules.get("allowed_extraction_types", [])
    }

    seen_variables: dict[str, int] = {}
    for row_index, row in canonical_frame.iterrows():
        excel_row = int(row_index) + 2

        if all(is_blank(row.get(column, "")) for column in canonical_frame.columns):
            continue

        for column in required_non_empty_columns:
            if is_blank(row.get(column, "")):
                issues.append(
                    Issue(
                        severity="error",
                        code="blank_required_value",
                        message=f"Required field '{column}' is blank.",
                        sheet=actual_sheet_name,
                        row=excel_row,
                        column=column,
                    )
                )

        variable_name = display_value(row.get("variable", ""))
        if variable_name:
            if not re.fullmatch(rules.get("variable_name_pattern", SNAKE_CASE_PATTERN.pattern), variable_name):
                issues.append(
                    Issue(
                        severity="error",
                        code="invalid_variable_name",
                        message="Variable names must be lower snake_case.",
                        sheet=actual_sheet_name,
                        row=excel_row,
                        column="variable",
                    )
                )

            if variable_name in seen_variables:
                issues.append(
                    Issue(
                        severity="error",
                        code="duplicate_variable",
                        message=(
                            f"Variable '{variable_name}' is duplicated. "
                            f"First seen on row {seen_variables[variable_name]}."
                        ),
                        sheet=actual_sheet_name,
                        row=excel_row,
                        column="variable",
                    )
                )
            else:
                seen_variables[variable_name] = excel_row

        completeness_value = row.get("completeness", "")
        parsed_completeness = parse_percent_like(completeness_value)
        if parsed_completeness is None:
            issues.append(
                Issue(
                    severity="error",
                    code="invalid_completeness",
                    message=(
                        "Completeness must be numeric, either as a percent like '98.4%' "
                        "or a decimal like '0.984'."
                    ),
                    sheet=actual_sheet_name,
                    row=excel_row,
                    column="completeness",
                )
            )
        elif not 0 <= parsed_completeness <= 100:
            issues.append(
                Issue(
                    severity="error",
                    code="completeness_out_of_range",
                    message="Completeness must fall between 0 and 100 percent.",
                    sheet=actual_sheet_name,
                    row=excel_row,
                    column="completeness",
                )
            )

        extraction_type = display_value(row.get("extraction_type", ""))
        normalized_extraction_type = normalize_token(extraction_type)
        if normalized_extraction_type and allowed_extraction_types:
            if normalized_extraction_type not in allowed_extraction_types:
                issues.append(
                    Issue(
                        severity="warning",
                        code="unknown_extraction_type",
                        message=(
                            f"Extraction type '{extraction_type}' is not in the configured allow list."
                        ),
                        sheet=actual_sheet_name,
                        row=excel_row,
                        column="extraction_type",
                    )
                )

        schemas = split_tokens(row.get("schema", ""))
        if not schemas:
            issues.append(
                Issue(
                    severity="error",
                    code="missing_schema",
                    message="At least one schema/table reference is required.",
                    sheet=actual_sheet_name,
                    row=excel_row,
                    column="schema",
                )
            )
        else:
            for schema in schemas:
                normalized_schema = normalize_token(schema)
                if recommended_schema_values and normalized_schema not in recommended_schema_values:
                    issues.append(
                        Issue(
                            severity="warning",
                            code="unexpected_schema_value",
                            message=(
                                f"Schema/table '{schema}' is not in the current recommended list."
                            ),
                            sheet=actual_sheet_name,
                            row=excel_row,
                            column="schema",
                        )
                    )

        source_columns = split_tokens(row.get("source_columns", ""))
        for source_column in source_columns:
            if not SOURCE_COLUMN_PATTERN.fullmatch(source_column):
                issues.append(
                    Issue(
                        severity="warning",
                        code="unexpected_source_column_format",
                        message=(
                            f"Source column '{source_column}' does not look like a standard snake_case "
                            "column reference."
                        ),
                        sheet=actual_sheet_name,
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
                        "Both 'values' and 'distribution' are blank. "
                        "One of them is usually helpful for downstream review."
                    ),
                    sheet=actual_sheet_name,
                    row=excel_row,
                )
            )

    return issues


def summarize_issues(issues: list[Issue]) -> tuple[int, int, int]:
    error_count = sum(issue.severity == "error" for issue in issues)
    warning_count = sum(issue.severity == "warning" for issue in issues)
    info_count = sum(issue.severity == "info" for issue in issues)
    return error_count, warning_count, info_count


def validate_source(source_path: str | Path, profile: dict[str, Any]) -> ValidationResult:
    source = Path(source_path)
    frames, source_kind = load_source_frames(source)
    resolved_sheets, issues = validate_sheet_presence(frames, profile, source_kind)

    variables_aliases = profile["required_sheets"].get("variables", ["Variables"])
    variables_sheet_name = (
        resolved_sheets.get("variables") or resolve_sheet_name(frames, variables_aliases)
    )
    if variables_sheet_name is None:
        issues.append(
            Issue(
                severity="error",
                code="missing_variables_sheet",
                message=(
                    "Could not locate the variables/data dictionary sheet. "
                    f"Accepted names: {', '.join(variables_aliases)}"
                ),
            )
        )
    else:
        issues.extend(
            validate_variables_sheet(
                frames[variables_sheet_name],
                variables_sheet_name,
                profile,
            )
        )

    error_count, warning_count, info_count = summarize_issues(issues)
    status = "passed" if error_count == 0 else "failed"
    return ValidationResult(
        profile_name=profile["profile_name"],
        source_path=str(source.resolve()),
        source_kind=source_kind,
        status=status,
        error_count=error_count,
        warning_count=warning_count,
        info_count=info_count,
        issues=issues,
    )


def write_json_report(result: ValidationResult, output_path: Path) -> None:
    output_path.write_text(
        json.dumps(result.to_dict(), indent=2),
        encoding="utf-8",
    )


def print_result(result: ValidationResult) -> None:
    print(f"Profile: {result.profile_name}")
    print(f"Source:  {result.source_path}")
    print(f"Type:    {result.source_kind}")
    print(f"Status:  {result.status.upper()}")
    print(
        "Counts:  "
        f"{result.error_count} error(s), "
        f"{result.warning_count} warning(s), "
        f"{result.info_count} info message(s)"
    )

    if not result.issues:
        return

    print("\nIssues:")
    severity_order = {"error": 0, "warning": 1, "info": 2}
    for issue in sorted(
        result.issues,
        key=lambda item: (
            severity_order.get(item.severity, 99),
            item.sheet or "",
            item.row or 0,
            item.column or "",
            item.code,
        ),
    ):
        location = []
        if issue.sheet:
            location.append(f"sheet={issue.sheet}")
        if issue.row is not None:
            location.append(f"row={issue.row}")
        if issue.column:
            location.append(f"column={issue.column}")
        location_text = f" ({', '.join(location)})" if location else ""
        print(f"- [{issue.severity.upper()}] {issue.code}{location_text}: {issue.message}")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Validate a Century clinical coding dictionary workbook or flat-file export."
        )
    )
    parser.add_argument(
        "--input",
        required=False,
        help="Path to the workbook or flat file to validate.",
    )
    parser.add_argument(
        "--profile",
        default="mtc_aat_cohort",
        help="Profile name from dictionary_validation/profiles or a direct JSON file path.",
    )
    parser.add_argument(
        "--report-json",
        help="Optional path to write a JSON validation report.",
    )
    parser.add_argument(
        "--fail-on-warning",
        action="store_true",
        help="Return a non-zero exit code when warnings are present.",
    )
    parser.add_argument(
        "--list-profiles",
        action="store_true",
        help="List the available built-in validation profiles and exit.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.list_profiles:
        for profile_name in list_profiles():
            print(profile_name)
        return 0

    if not args.input:
        parser.error("--input is required unless --list-profiles is used.")

    profile = load_profile(args.profile)
    result = validate_source(args.input, profile)
    print_result(result)

    if args.report_json:
        write_json_report(result, Path(args.report_json))

    if result.error_count > 0:
        return 1
    if args.fail_on_warning and result.warning_count > 0:
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
