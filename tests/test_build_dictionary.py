"""Tests for build_dictionary.py.

Focused on the four regressions called out in the latest review:
  P1 - % Patient denominator must be the cohort total, not the per-table
       distinct-patient count.
  P1 - Variable rows must be PII-tagged and dropped by sales/pharma audience.
  P2 - Renderers must actually omit hidden sections (not just empty them).
  P2 - resolve_variables must skip the GROUP BY for free-text /
       Unstructured columns.

Tests stub the psycopg connection so no warehouse is required.
"""
from __future__ import annotations

import re
import unittest
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import sys
REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

import build_dictionary as bd  # noqa: E402
from build_dictionary import (  # noqa: E402
    AUDIENCE_VISIBILITY,
    ColumnInfo,
    ColumnRow,
    CohortModel,
    CohortSummary,
    DateCoverage,
    TableRow,
    VariableRow,
    compute_patient_completeness,
    filter_for_audience,
    resolve_variables,
    section_visible,
    write_html,
    write_xlsx,
    _is_freetext_column,
)


class _Cursor:
    """Tiny psycopg-cursor stub.

    Each instance is fed a queue of (predicate, return_value) pairs;
    when the test code calls cur.execute(sql), the cursor returns the
    first result whose predicate matches the SQL string.
    """

    def __init__(self, script: list[tuple[str, Any]]) -> None:
        self._script = list(script)
        self._next_result: Any = None

    def execute(self, sql: str, *params) -> None:
        for needle, result in self._script:
            if needle in sql:
                self._next_result = result
                return
        raise AssertionError(f"unexpected SQL: {sql!r}")

    def fetchone(self):
        return self._next_result

    def fetchall(self):
        return self._next_result

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


class _Conn:
    def __init__(self, script: list[tuple[str, Any]]) -> None:
        self._script = script

    def cursor(self):
        return _Cursor(self._script)

    def rollback(self) -> None:
        pass


# --------------------------------------------------------------------- #
# P1 - % Patient denominator
# --------------------------------------------------------------------- #


class PatientCompletenessTests(unittest.TestCase):

    def test_uses_cohort_total_not_per_table(self):
        # Cohort has 1000 patients; the table this column lives in only
        # has rows for 50 of them; the column is non-null for all 50.
        # Correct % Patient = 50 / 1000 = 5.0% (NOT 50/50 = 100%).
        col = ColumnInfo(
            schema="x", table="t", column="c", data_type="text",
            is_nullable=True, row_count=200, null_count=0,
            completeness_pct=100.0,
        )
        conn = _Conn([("COUNT(DISTINCT", (50,))])
        pct = compute_patient_completeness(conn, "x", col, total_patients=1000)
        self.assertEqual(pct, 5.0)

    def test_returns_none_when_total_unknown(self):
        col = ColumnInfo(
            schema="x", table="t", column="c", data_type="text",
            is_nullable=True, row_count=10, null_count=0,
            completeness_pct=100.0,
        )
        conn = _Conn([])
        self.assertIsNone(compute_patient_completeness(conn, "x", col, None))
        self.assertIsNone(compute_patient_completeness(conn, "x", col, 0))

    def test_zero_rows_returns_zero(self):
        col = ColumnInfo(
            schema="x", table="t", column="c", data_type="text",
            is_nullable=True, row_count=0, null_count=0,
            completeness_pct=0.0,
        )
        conn = _Conn([])
        self.assertEqual(compute_patient_completeness(conn, "x", col, 100), 0.0)


# --------------------------------------------------------------------- #
# P1 - Variables PII tagging + audience filter
# --------------------------------------------------------------------- #


class VariablesPIITagsAndFilterTests(unittest.TestCase):

    def _make_model(self, variables: list[VariableRow]) -> CohortModel:
        return CohortModel(
            cohort="c", provider="p", disease="d", schema_name="s",
            variant="raw", display_name="C", description="",
            status="active", generated_at="t", git_sha="abc",
            introspect_version="0.0", schema_snapshot_digest="sha256:0",
            summary=CohortSummary(
                patient_count=10, table_count=0, column_count=0,
                date_coverage=DateCoverage(),
            ),
            tables=[], columns=[], variables=variables,
        )

    def test_resolver_tags_pii_from_pack(self):
        # location.zip is in our shipped pii.yaml — confirm the resolver
        # tags the row pii=True even when the SQL succeeds.
        pack = [{
            "category": "Demographics",
            "variable": "ZIP code",
            "table": "location",
            "column": "zip",
            "extraction_type": "Structured",
        }]
        # Stub: count returns 0 so we don't need top-N / patient-pct.
        conn = _Conn([("COUNT(*)", (0,))])
        rows = resolve_variables(
            conn, "s", pack, total_patients=100,
            pii_pairs={("location", "zip")}, pii_patterns=[],
        )
        self.assertTrue(rows[0].pii, "expected pii=True for location.zip")

    def test_audience_filter_drops_pii_variables_for_sales(self):
        # Two variables: one PII, one not. Sales should keep only the
        # non-PII one in `variables`.
        clean = VariableRow(
            category="Diagnosis", variable="Dx", description="",
            table="condition_occurrence", column="condition_concept_name",
            criteria="", values="", distribution="",
            implemented="Yes", patient_pct=42.0,
            extraction_type="Structured", notes="", pii=False,
        )
        pii = VariableRow(
            category="Demographics", variable="ZIP", description="",
            table="location", column="zip", criteria="",
            values="", distribution="",
            implemented="Yes", patient_pct=10.0,
            extraction_type="Structured", notes="", pii=True,
        )
        m = self._make_model([clean, pii])
        sales = filter_for_audience(m, "sales")
        self.assertEqual([v.variable for v in sales.variables], ["Dx"])

    def test_audience_filter_drops_pii_variables_for_pharma(self):
        pii = VariableRow(
            category="Demographics", variable="ZIP", description="",
            table="location", column="zip", criteria="",
            values="", distribution="",
            implemented="Yes", patient_pct=10.0,
            extraction_type="Structured", notes="", pii=True,
        )
        m = self._make_model([pii])
        self.assertEqual(filter_for_audience(m, "pharma").variables, [])

    def test_technical_audience_keeps_pii(self):
        pii = VariableRow(
            category="Demographics", variable="ZIP", description="",
            table="location", column="zip", criteria="",
            values="", distribution="",
            implemented="Yes", patient_pct=10.0,
            extraction_type="Structured", notes="", pii=True,
        )
        m = self._make_model([pii])
        self.assertEqual(len(filter_for_audience(m, "technical").variables), 1)


# --------------------------------------------------------------------- #
# P2 - Renderers omit hidden sections
# --------------------------------------------------------------------- #


def _make_full_model() -> CohortModel:
    return CohortModel(
        cohort="c", provider="p", disease="d", schema_name="s",
        variant="raw", display_name="C", description="",
        status="active", generated_at="t", git_sha="abc",
        introspect_version="0.0", schema_snapshot_digest="sha256:0",
        summary=CohortSummary(
            patient_count=10, table_count=1, column_count=1,
            date_coverage=DateCoverage(),
        ),
        tables=[TableRow(
            table_name="person", category="Demographics",
            row_count=10, column_count=2,
            patient_count_in_table=10, purpose="OMOP person",
        )],
        columns=[ColumnRow(
            category="Demographics", table="person", column="year_of_birth",
            description="Birth year.", data_type="integer",
            values="1948", distribution="Min: 1933 Max: 1997",
            median_iqr="Median: 1948", completeness_pct=100.0,
            patient_pct=100.0, extraction_type="Structured",
            pii=False, notes="",
        )],
        variables=[VariableRow(
            category="Demographics", variable="Birth Year",
            description="Year the patient was born.",
            table="person", column="year_of_birth", criteria="",
            values="1948", distribution="...", implemented="Yes",
            patient_pct=100.0, extraction_type="Structured",
            notes="", pii=False,
        )],
    )


class RendererOmissionTests(unittest.TestCase):

    def test_visibility_table_matches_readme(self):
        self.assertEqual(AUDIENCE_VISIBILITY["technical"],
                         {"summary": True, "tables": True, "columns": True, "variables": True})
        self.assertEqual(AUDIENCE_VISIBILITY["sales"],
                         {"summary": True, "tables": True, "columns": False, "variables": True})
        self.assertEqual(AUDIENCE_VISIBILITY["pharma"],
                         {"summary": True, "tables": False, "columns": False, "variables": True})

    def test_section_visible_helper(self):
        self.assertTrue(section_visible("technical", "columns"))
        self.assertFalse(section_visible("sales", "columns"))
        self.assertFalse(section_visible("pharma", "tables"))

    def test_html_omits_columns_for_sales(self):
        out = Path("/tmp/test_dd_sales.html")
        write_html(_make_full_model(), out, audience="sales")
        page = out.read_text()
        self.assertIn("<h2>Tables</h2>", page)
        self.assertNotIn("<h2>Columns</h2>", page)
        self.assertIn("<h2>Variables</h2>", page)

    def test_html_omits_tables_and_columns_for_pharma(self):
        out = Path("/tmp/test_dd_pharma.html")
        write_html(_make_full_model(), out, audience="pharma")
        page = out.read_text()
        self.assertNotIn("<h2>Tables</h2>", page)
        self.assertNotIn("<h2>Columns</h2>", page)
        self.assertIn("<h2>Variables</h2>", page)

    def test_xlsx_skips_hidden_sheets(self):
        try:
            import openpyxl  # noqa: F401
        except ImportError:
            self.skipTest("openpyxl not installed")
        import openpyxl as opx
        out = Path("/tmp/test_dd_sales.xlsx")
        write_xlsx(_make_full_model(), out, audience="sales")
        wb = opx.load_workbook(out)
        self.assertEqual(set(wb.sheetnames),
                         {"Summary", "Tables", "Variables"})

        out2 = Path("/tmp/test_dd_pharma.xlsx")
        write_xlsx(_make_full_model(), out2, audience="pharma")
        wb2 = opx.load_workbook(out2)
        self.assertEqual(set(wb2.sheetnames), {"Summary", "Variables"})


# --------------------------------------------------------------------- #
# P2 - resolve_variables skips GROUP BY for unstructured / free-text
# --------------------------------------------------------------------- #


class UnstructuredSkipTests(unittest.TestCase):

    def test_freetext_column_detector(self):
        self.assertTrue(_is_freetext_column("note_text"))
        self.assertTrue(_is_freetext_column("discharge_note"))
        self.assertTrue(_is_freetext_column("foo_text"))
        self.assertFalse(_is_freetext_column("year_of_birth"))
        self.assertFalse(_is_freetext_column("condition_concept_name"))

    def test_unstructured_variable_skips_group_by(self):
        # If the resolver tries to GROUP BY note_text the test will fail
        # because no GROUP BY entry is in the cursor script.
        pack = [{
            "category": "Reports",
            "variable": "Clinical Note",
            "table": "note",
            "column": "note_text",
            "extraction_type": "Unstructured",
        }]
        # Only count + patient queries are allowed; no GROUP BY.
        conn = _Conn([
            ("COUNT(*)", (123,)),
            ("COUNT(DISTINCT", (40,)),
        ])
        rows = resolve_variables(
            conn, "s", pack, total_patients=100,
            pii_pairs=set(), pii_patterns=[],
        )
        v = rows[0]
        self.assertEqual(v.implemented, "Yes")
        self.assertEqual(v.values, "")
        self.assertIn("not aggregated", v.distribution)
        self.assertEqual(v.patient_pct, 40.0)

    def test_freetext_column_name_skips_group_by_even_if_marked_structured(self):
        pack = [{
            "category": "Reports",
            "variable": "Some text",
            "table": "note",
            "column": "discharge_note",
            "extraction_type": "Structured",   # mis-marked but still text
        }]
        conn = _Conn([
            ("COUNT(*)", (5,)),
            ("COUNT(DISTINCT", (3,)),
        ])
        rows = resolve_variables(
            conn, "s", pack, total_patients=10,
            pii_pairs=set(), pii_patterns=[],
        )
        self.assertEqual(rows[0].values, "")
        self.assertIn("not aggregated", rows[0].distribution)


if __name__ == "__main__":
    unittest.main()
