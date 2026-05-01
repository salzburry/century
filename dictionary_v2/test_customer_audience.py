"""Tests for the customer audience added in PR-B.

These exercise dictionary_v2/build_dictionary.py specifically. The root
test_build_dictionary.py suite continues to cover the original
build_dictionary.py module (which doesn't know about customer).
"""
from __future__ import annotations

import importlib.util
import sys
import tempfile
import unittest
from pathlib import Path

# Force-load the v2 build_dictionary module under the canonical name.
_REPO_ROOT = Path(__file__).resolve().parent.parent
_V2_PATH = Path(__file__).resolve().parent / "build_dictionary.py"
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))
_spec = importlib.util.spec_from_file_location("build_dictionary", _V2_PATH)
bd = importlib.util.module_from_spec(_spec)
sys.modules["build_dictionary"] = bd
_spec.loader.exec_module(bd)


def _make_table(name: str) -> "bd.TableRow":
    return bd.TableRow(
        table_name=name, category="Demographics", description="", purpose="",
        inclusion_criteria="", data_source="Normalized", source_table="OMOP",
        row_count=10, column_count=2, patient_count_in_table=10,
    )


def _make_column(table: str, column: str = "year_of_birth") -> "bd.ColumnRow":
    return bd.ColumnRow(
        category="Demographics", table=table, column=column,
        description="A column.", data_type="integer",
        values="", distribution="", median_iqr="",
        completeness_pct=100.0, patient_pct=100.0,
        extraction_type="Direct", pii=False, notes="",
        nullable="YES", example="1980", coding_schema="",
        data_source="Normalized",
    )


def _make_variable(table: str, column: str = "year_of_birth") -> "bd.VariableRow":
    return bd.VariableRow(
        category="Demographics", variable="Age", description="Patient age",
        table=table, column=column, criteria="year_of_birth IS NOT NULL",
        values="", distribution="", median_iqr="",
        completeness_pct=100.0, implemented="Yes", patient_pct=100.0,
        extraction_type="Direct", pii=False, notes="",
        inclusion_criteria="Patients with a birth year on file.",
        field_type="integer", example="44", coding_schema="",
        data_source="Normalized",
    )


def _make_model(tables, columns, variables) -> "bd.CohortModel":
    return bd.CohortModel(
        cohort="c", provider="p", disease="d", display_name="dn",
        schema_name="sn", variant="raw", description="",
        generated_at="2026-01-01", status="active",
        git_sha="abc", introspect_version="0.5.0",
        schema_snapshot_digest="sha",
        summary=bd.CohortSummary(
            patient_count=10, table_count=len(tables),
            column_count=len(columns),
            date_coverage=bd.DateCoverage(
                min_date="2020-01-01", max_date="2025-01-01",
                years_of_data=5.0,
            ),
        ),
        tables=tables, columns=columns, variables=variables,
    )


class CustomerLayoutTests(unittest.TestCase):
    """The customer audience trims columns from each sheet to match
    the reviewer feedback: drop debug fields from Summary, drop
    Data Source / Source Table from Tables, reduce Columns to a
    schema-description tab, and remove Coding Schema / Implemented /
    Data Source from Variables."""

    def test_audience_registered(self):
        self.assertIn("customer", bd.AUDIENCE_VISIBILITY)
        self.assertEqual(
            bd.AUDIENCE_VISIBILITY["customer"],
            {"summary": True, "tables": True, "columns": True, "variables": True},
        )

    def test_summary_drops_debug_fields(self):
        xl_keys = [k for k, _, _ in bd.summary_layout("customer") if k is not None]
        for dropped in ("variant", "column_count", "status", "git_sha",
                        "introspect_version", "schema_snapshot_digest"):
            self.assertNotIn(dropped, xl_keys)
        # Customer-relevant fields stay.
        for kept in ("cohort", "provider", "disease", "patient_count",
                     "table_count", "generated_at"):
            self.assertIn(kept, xl_keys)

    def test_tables_drops_data_source_and_source_table(self):
        headers = [label for label, _ in bd.tables_layout("customer")]
        self.assertNotIn("Data Source", headers)
        self.assertNotIn("Source Table", headers)
        # Keep what the reviewer left in.
        for kept in ("Table", "Description", "Inclusion Criteria",
                     "Rows", "Columns", "Patients"):
            self.assertIn(kept, headers)

    def test_columns_is_exactly_four_fields(self):
        headers = [label for label, _ in bd.columns_layout("customer")]
        self.assertEqual(headers,
                         ["Table(s)", "Column", "Description", "Field Type"])

    def test_variables_keeps_both_inclusion_and_criteria(self):
        headers = [label for label, _ in bd.variables_layout("customer")]
        self.assertIn("Inclusion Criteria", headers)
        self.assertIn("Criteria", headers)
        # Reviewer said remove these three.
        for dropped in ("Coding Schema", "Implemented", "Data Source"):
            self.assertNotIn(dropped, headers)

    def test_other_audiences_unaffected(self):
        # PR-A baseline shape preserved for technical / sales / pharma.
        for aud in ("technical", "sales", "pharma"):
            tables = [l for l, _ in bd.tables_layout(aud)]
            self.assertIn("Data Source", tables, msg=f"{aud} Tables should keep Data Source")
            cols = [l for l, _ in bd.columns_layout(aud)]
            self.assertIn("Coding Schema", cols, msg=f"{aud} Columns should keep Coding Schema")


class CustomerTableFilterTests(unittest.TestCase):
    """Internal scaffolding tables (cohort_patients, standard_profile_data_model,
    dv_tokenized_profile_data) are stripped from the customer dictionary
    only — technical retains them for debugging."""

    def test_filter_drops_internal_tables_for_customer(self):
        tables = [_make_table("person"), _make_table("cohort_patients"),
                  _make_table("standard_profile_data_model"),
                  _make_table("dv_tokenized_profile_data")]
        columns = [_make_column("person"), _make_column("cohort_patients")]
        variables = [_make_variable("person"), _make_variable("cohort_patients")]
        model = _make_model(tables, columns, variables)

        filtered = bd.filter_for_audience(model, "customer")
        names = {t.table_name for t in filtered.tables}
        self.assertEqual(names, {"person"})
        # Columns and variables tied to excluded tables also drop.
        self.assertEqual({c.table for c in filtered.columns}, {"person"})
        self.assertEqual({v.table for v in filtered.variables}, {"person"})

    def test_technical_keeps_internal_tables(self):
        tables = [_make_table("person"), _make_table("cohort_patients")]
        columns = [_make_column("person"), _make_column("cohort_patients")]
        variables = [_make_variable("person"), _make_variable("cohort_patients")]
        model = _make_model(tables, columns, variables)

        filtered = bd.filter_for_audience(model, "technical")
        names = {t.table_name for t in filtered.tables}
        self.assertEqual(names, {"person", "cohort_patients"})

    def test_sales_keeps_internal_tables(self):
        # Sales is for the existing internal-sales workbook and was
        # never asked to drop these tables.
        tables = [_make_table("person"), _make_table("cohort_patients")]
        model = _make_model(tables, [_make_column("person")], [_make_variable("person")])
        filtered = bd.filter_for_audience(model, "sales")
        names = {t.table_name for t in filtered.tables}
        self.assertEqual(names, {"person", "cohort_patients"})


class CustomerXlsxSmokeTests(unittest.TestCase):
    """End-to-end XLSX checks: Summary has no metric/value header row,
    sheet column counts match the per-audience layouts, and excluded
    tables don't appear."""

    def setUp(self):
        try:
            import openpyxl  # noqa: F401
            import pandas    # noqa: F401
        except ImportError:
            self.skipTest("openpyxl / pandas not installed")

        tables = [_make_table("person"), _make_table("cohort_patients")]
        columns = [_make_column("person"), _make_column("cohort_patients")]
        variables = [_make_variable("person"), _make_variable("cohort_patients")]
        model = _make_model(tables, columns, variables)
        self.filtered = bd.filter_for_audience(model, "customer")

    def test_summary_has_no_metric_value_header(self):
        import openpyxl
        with tempfile.TemporaryDirectory() as d:
            path = Path(d) / "out.xlsx"
            bd.write_xlsx(self.filtered, path, audience="customer")
            wb = openpyxl.load_workbook(path)
            ws = wb["Summary"]
            # Row 1 must be the first data row, not the literal labels.
            row1 = [c.value for c in ws[1]]
            self.assertEqual(row1, ["cohort", "c"])
            self.assertNotEqual(row1, ["metric", "value"])

    def test_columns_sheet_has_exactly_four_headers(self):
        import openpyxl
        with tempfile.TemporaryDirectory() as d:
            path = Path(d) / "out.xlsx"
            bd.write_xlsx(self.filtered, path, audience="customer")
            wb = openpyxl.load_workbook(path)
            ws = wb["Columns"]
            headers = [c.value for c in ws[1]]
            self.assertEqual(headers,
                             ["Table(s)", "Column", "Description", "Field Type"])

    def test_excluded_tables_absent_from_xlsx(self):
        import openpyxl
        with tempfile.TemporaryDirectory() as d:
            path = Path(d) / "out.xlsx"
            bd.write_xlsx(self.filtered, path, audience="customer")
            wb = openpyxl.load_workbook(path)
            ws = wb["Tables"]
            names = [ws.cell(row=r, column=1).value
                     for r in range(2, ws.max_row + 1)]
            self.assertNotIn("cohort_patients", names)
            self.assertEqual(names, ["person"])


class CustomerHtmlSmokeTests(unittest.TestCase):

    def setUp(self):
        tables = [_make_table("person"), _make_table("cohort_patients")]
        columns = [_make_column("person"), _make_column("cohort_patients")]
        variables = [_make_variable("person"), _make_variable("cohort_patients")]
        model = _make_model(tables, columns, variables)
        self.filtered = bd.filter_for_audience(model, "customer")

    def test_html_omits_debug_summary_fields(self):
        with tempfile.TemporaryDirectory() as d:
            path = Path(d) / "out.html"
            bd.write_html(self.filtered, path, audience="customer")
            html = path.read_text()
        self.assertNotIn("<dt>Variant</dt>", html)
        self.assertNotIn("<dt>Git SHA</dt>", html)
        self.assertNotIn("<dt>Schema snapshot</dt>", html)
        # Date coverage is the merged HTML field — it must remain.
        self.assertIn("<dt>Date coverage</dt>", html)

    def test_html_variables_includes_both_criteria_columns(self):
        with tempfile.TemporaryDirectory() as d:
            path = Path(d) / "out.html"
            bd.write_html(self.filtered, path, audience="customer")
            html = path.read_text()
        self.assertIn("<th>Inclusion Criteria</th>", html)
        self.assertIn("<th>Criteria</th>", html)


if __name__ == "__main__":
    unittest.main()
