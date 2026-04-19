from __future__ import annotations

import contextlib
import io
import sys
import unittest
from pathlib import Path
import shutil
import uuid

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from validate_dictionary import main, validate_source


class ValidateDictionaryTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.temp_root = PROJECT_ROOT / "tests" / ".tmp"
        cls.temp_root.mkdir(parents=True, exist_ok=True)

    def test_valid_workbook_passes(self) -> None:
        temp_dir = self._make_temp_dir("valid")
        try:
            workbook_path = temp_dir / "valid_dictionary.xlsx"
            self._write_valid_workbook(workbook_path)

            result = validate_source(workbook_path, verbose=False)

            self.assertEqual("passed", result.status)
            self.assertEqual(0, result.error_count)
        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)

    def test_invalid_workbook_reports_key_errors(self) -> None:
        temp_dir = self._make_temp_dir("invalid")
        try:
            workbook_path = temp_dir / "invalid_dictionary.xlsx"
            self._write_invalid_workbook(workbook_path)

            result = validate_source(workbook_path, verbose=False)
            codes = {issue.code for issue in result.issues}

            self.assertEqual("failed", result.status)
            self.assertIn("missing_sheet", codes)
            self.assertIn("invalid_variable_name", codes)
            self.assertIn("duplicate_variable", codes)
            self.assertIn("invalid_completeness", codes)
        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)

    def test_flat_file_skips_tab_validation(self) -> None:
        temp_dir = self._make_temp_dir("flat")
        try:
            csv_path = temp_dir / "variables.csv"
            pd.DataFrame(
                [
                    {
                        "Category": "Demographics",
                        "Variable": "sex",
                        "Description": "Biological sex at birth.",
                        "Schema": "person",
                        "Column(s)": "gender_concept_name",
                        "Criteria": "",
                        "Values": "Female, Male",
                        "Distribution": "Female: 55%; Male: 45%",
                        "Completeness": "99.1%",
                        "Extraction Type": "Structured",
                    }
                ]
            ).to_csv(csv_path, index=False)

            result = validate_source(csv_path, verbose=False)
            codes = {issue.code for issue in result.issues}

            self.assertEqual("passed", result.status)
            self.assertIn("sheet_validation_skipped", codes)
        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)

    def test_blank_tables_sheet_is_flagged(self) -> None:
        temp_dir = self._make_temp_dir("blank_sheet")
        try:
            workbook_path = temp_dir / "blank_tables_dictionary.xlsx"
            summary = pd.DataFrame([{"metric": "patient_count", "value": "1000"}])
            tables = pd.DataFrame([{"table": "   ", "description": ""}])
            variables = pd.DataFrame(
                [
                    {
                        "Category": "Demographics",
                        "Variable": "Sex",
                        "Description": "Biological sex at birth.",
                        "Schema": "person",
                        "Column(s)": "gender_concept_name",
                        "Criteria": "",
                        "Values": "Female, Male",
                        "Distribution": "Female: 55%; Male: 45%",
                        "Completeness": "99.1%",
                        "Extraction Type": "Structured",
                    }
                ]
            )

            with pd.ExcelWriter(workbook_path, engine="openpyxl") as writer:
                summary.to_excel(writer, sheet_name="Summary", index=False)
                tables.to_excel(writer, sheet_name="Tables", index=False)
                variables.to_excel(writer, sheet_name="Variables", index=False)

            result = validate_source(workbook_path, verbose=False)
            codes = {issue.code for issue in result.issues}

            self.assertIn("empty_sheet", codes)
        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)

    def test_quiet_mode_still_prints_summary(self) -> None:
        temp_dir = self._make_temp_dir("quiet")
        try:
            workbook_path = temp_dir / "quiet_dictionary.xlsx"
            self._write_valid_workbook(workbook_path)

            output = io.StringIO()
            with contextlib.redirect_stdout(output):
                exit_code = main(["--input", str(workbook_path), "--quiet"])

            self.assertEqual(0, exit_code)
            rendered = output.getvalue()
            self.assertIn("Step 6: Summary", rendered)
            self.assertIn("status  : PASSED", rendered)
        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)

    def _make_temp_dir(self, prefix: str) -> Path:
        temp_dir = self.temp_root / f"{prefix}_{uuid.uuid4().hex}"
        temp_dir.mkdir(parents=True, exist_ok=False)
        return temp_dir

    def _write_valid_workbook(self, workbook_path: Path) -> None:
        summary = pd.DataFrame(
            [{"metric": "patient_count", "value": "1000"}]
        )
        tables = pd.DataFrame(
            [{"table": "person", "description": "Patient-level demographic table"}]
        )
        variables = pd.DataFrame(
            [
                    {
                        "Category": "Demographics",
                        "Variable": "Sex",
                        "Description": "Biological sex at birth.",
                        "Schema": "person",
                        "Column(s)": "gender_concept_name",
                    "Criteria": "",
                    "Values": "Female, Male",
                    "Distribution": "Female: 55%; Male: 45%",
                    "Completeness": "99.1%",
                    "Extraction Type": "Structured",
                },
                    {
                        "Category": "Vitals",
                        "Variable": "Heart rate",
                        "Description": "Heart rate captured during the office visit.",
                        "Schema": "observation",
                        "Column(s)": "value_as_number",
                    "Criteria": "observation_concept_name = 'Heart rate'",
                    "Values": "",
                    "Distribution": "Median: 72",
                    "Completeness": "0.94",
                    "Extraction Type": "Structured",
                }
            ]
        )

        with pd.ExcelWriter(workbook_path, engine="openpyxl") as writer:
            summary.to_excel(writer, sheet_name="Summary", index=False)
            tables.to_excel(writer, sheet_name="Tables", index=False)
            variables.to_excel(writer, sheet_name="Variables", index=False)

    def _write_invalid_workbook(self, workbook_path: Path) -> None:
        summary = pd.DataFrame([{"metric": "patient_count", "value": "1000"}])
        variables = pd.DataFrame(
            [
                {
                    "Category": "Demographics",
                    "Variable": "1sex!",
                    "Description": "Biological sex at birth.",
                    "Schema": "person",
                    "Column(s)": "gender_concept_name",
                    "Criteria": "",
                    "Values": "Female, Male",
                    "Distribution": "",
                    "Completeness": "not_a_percent",
                    "Extraction Type": "Structured",
                },
                {
                    "Category": "Demographics",
                    "Variable": "1sex!",
                    "Description": "Duplicate variable example.",
                    "Schema": "person",
                    "Column(s)": "gender_concept_name",
                    "Criteria": "",
                    "Values": "Female, Male",
                    "Distribution": "",
                    "Completeness": "99%",
                    "Extraction Type": "Structured",
                }
            ]
        )

        with pd.ExcelWriter(workbook_path, engine="openpyxl") as writer:
            summary.to_excel(writer, sheet_name="Summary", index=False)
            variables.to_excel(writer, sheet_name="Variables", index=False)


if __name__ == "__main__":
    unittest.main()
