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
import tempfile
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


def _var(**overrides) -> VariableRow:
    """Constructor helper so tests only set the fields they care about."""
    defaults = dict(
        category="Diagnosis",
        variable="Dx",
        description="",
        table="condition_occurrence",
        column="condition_concept_name",
        criteria="",
        values="",
        distribution="",
        median_iqr="",
        completeness_pct=None,
        implemented="No",
        patient_pct=None,
        extraction_type="Structured",
        notes="",
        pii=False,
    )
    defaults.update(overrides)
    return VariableRow(**defaults)


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
        clean = _var(
            variable="Dx", table="condition_occurrence",
            column="condition_concept_name",
            implemented="Yes", patient_pct=42.0, pii=False,
        )
        pii = _var(
            variable="ZIP", table="location", column="zip",
            implemented="Yes", patient_pct=10.0, pii=True,
        )
        m = self._make_model([clean, pii])
        sales = filter_for_audience(m, "sales")
        self.assertEqual([v.variable for v in sales.variables], ["Dx"])

    def test_audience_filter_drops_pii_variables_for_pharma(self):
        pii = _var(
            variable="ZIP", table="location", column="zip",
            implemented="Yes", patient_pct=10.0, pii=True,
        )
        m = self._make_model([pii])
        self.assertEqual(filter_for_audience(m, "pharma").variables, [])

    def test_technical_audience_keeps_pii(self):
        pii = _var(
            variable="ZIP", table="location", column="zip",
            implemented="Yes", patient_pct=10.0, pii=True,
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
        variables=[_var(
            category="Demographics", variable="Birth Year",
            description="Year the patient was born.",
            table="person", column="year_of_birth",
            values="1948", distribution="...",
            median_iqr="Median: 1948 (IQR: 1944-1952)",
            completeness_pct=100.0,
            implemented="Yes", patient_pct=100.0,
        )],
    )


class RendererOmissionTests(unittest.TestCase):

    def setUp(self):
        # Portable scratch dir — cleaned up automatically on tearDown.
        # Use tempfile.TemporaryDirectory() so the suite runs on Windows
        # (where /tmp does not exist) as well as macOS / Linux.
        self._tmp = tempfile.TemporaryDirectory()
        self.tmp_dir = Path(self._tmp.name)

    def tearDown(self):
        self._tmp.cleanup()

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
        out = self.tmp_dir / "sales.html"
        write_html(_make_full_model(), out, audience="sales")
        page = out.read_text()
        self.assertIn("<h2>Tables</h2>", page)
        self.assertNotIn("<h2>Columns</h2>", page)
        self.assertIn("<h2>Variables</h2>", page)

    def test_html_omits_tables_and_columns_for_pharma(self):
        out = self.tmp_dir / "pharma.html"
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
        out = self.tmp_dir / "sales.xlsx"
        write_xlsx(_make_full_model(), out, audience="sales")
        wb = opx.load_workbook(out)
        self.assertEqual(set(wb.sheetnames),
                         {"Summary", "Tables", "Variables"})

        out2 = self.tmp_dir / "pharma.xlsx"
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
        # Criteria-count, nonnull-count, and patient-count queries —
        # no GROUP BY pattern.
        conn = _Conn([
            ("COUNT(*)", (123,)),
            ("COUNT(DISTINCT", (40,)),
        ])
        rows = resolve_variables(
            conn, "s", pack, total_patients=100,
            pii_pairs=set(), pii_patterns=[],
            tables_with_person_id={"note"},
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
            tables_with_person_id={"note"},
        )
        self.assertEqual(rows[0].values, "")
        self.assertIn("not aggregated", rows[0].distribution)


# --------------------------------------------------------------------- #
# P1 - Median (IQR) + Completeness on Page 4
# --------------------------------------------------------------------- #


class MedianIQRAndCompletenessTests(unittest.TestCase):

    def test_numeric_column_gets_median_iqr_and_completeness(self):
        pack = [{
            "category": "Vitals", "variable": "Body Weight",
            "table": "observation", "column": "value_as_number",
            "criteria": "observation_concept_name = 'Body weight'",
            "extraction_type": "Structured",
        }]
        # Resolver runs in this order:
        #   1. COUNT(*) WHERE criteria              -> 1000 (denominator)
        #   2. COUNT(*) WHERE criteria AND NOT NULL -> 800  (numerator)
        #   3. GROUP BY value_as_number (top-10)    -> two rows
        #   4. PERCENTILE_CONT(0.25/0.5/0.75)       -> (150, 170, 190)
        #   5. COUNT(DISTINCT person_id)            -> 400

        class _Conn2:
            """COUNT(*) is called twice; state lives on the conn so each
            cursor() call sees the right count in sequence."""
            def __init__(self):
                self._count_sequence = [1000, 800]
                self._count_idx = 0

            def cursor(self):
                conn = self

                class _Cur:
                    def execute(self, sql, *params):
                        if "PERCENTILE_CONT" in sql:
                            self._next = ("150", "170", "190")
                        elif "GROUP BY" in sql:
                            self._next = [("170", 5), ("180", 3)]
                        elif "COUNT(DISTINCT" in sql:
                            self._next = (400,)
                        elif "COUNT(*)" in sql:
                            i = conn._count_idx
                            self._next = (conn._count_sequence[i],)
                            conn._count_idx += 1
                        else:
                            raise AssertionError(f"unexpected SQL: {sql!r}")

                    def fetchone(self):
                        return self._next

                    def fetchall(self):
                        return self._next

                    def __enter__(self):
                        return self

                    def __exit__(self, *exc):
                        return False

                return _Cur()

            def rollback(self):
                pass

        rows = resolve_variables(
            _Conn2(), "s", pack, total_patients=1000,
            pii_pairs=set(), pii_patterns=[],
            tables_with_person_id={"observation"},
            column_types={("observation", "value_as_number"): "numeric"},
        )
        v = rows[0]
        self.assertEqual(v.completeness_pct, 80.0)   # 800 / 1000
        self.assertIn("Median: 170", v.median_iqr)
        self.assertIn("IQR: 150-190", v.median_iqr)
        self.assertEqual(v.implemented, "Yes")
        self.assertEqual(v.patient_pct, 40.0)        # 400 / 1000

    def test_date_variable_uses_min_max_not_group_by(self):
        # Date-typed columns like condition_start_date should render a
        # Min/Max range in Distribution, not the ten most common exact
        # dates. The stub has no GROUP BY entry — if the resolver runs
        # one, _Cursor raises AssertionError and the test fails.
        pack = [{
            "category": "Diagnosis",
            "variable": "Diagnosis Start Date",
            "table": "condition_occurrence",
            "column": "condition_start_date",
            "extraction_type": "Structured",
        }]
        conn = _Conn([
            ("MIN", ("2022-01-15", "2026-03-22")),
            ("COUNT(DISTINCT", (500,)),
            ("COUNT(*)", (1000,)),
        ])
        rows = resolve_variables(
            conn, "s", pack, total_patients=1000,
            pii_pairs=set(), pii_patterns=[],
            tables_with_person_id={"condition_occurrence"},
            column_types={
                ("condition_occurrence", "condition_start_date"): "date",
            },
        )
        v = rows[0]
        self.assertIn("Min: 2022-01-15", v.distribution)
        self.assertIn("Max: 2026-03-22", v.distribution)
        self.assertEqual(v.values, "")
        self.assertEqual(v.median_iqr, "")
        self.assertEqual(v.implemented, "Yes")
        self.assertEqual(v.patient_pct, 50.0)   # 500 / 1000

    def test_timestamp_variable_also_uses_min_max(self):
        pack = [{
            "category": "Visits",
            "variable": "Visit Date",
            "table": "visit_occurrence",
            "column": "visit_start_date",
        }]
        conn = _Conn([
            ("MIN", ("2021-10-01", "2026-02-27")),
            ("COUNT(DISTINCT", (800,)),
            ("COUNT(*)", (5000,)),
        ])
        rows = resolve_variables(
            conn, "s", pack, total_patients=1000,
            tables_with_person_id={"visit_occurrence"},
            column_types={
                ("visit_occurrence", "visit_start_date"):
                    "timestamp without time zone",
            },
        )
        self.assertIn("Min: 2021-10-01", rows[0].distribution)
        self.assertIn("Max: 2026-02-27", rows[0].distribution)

    def test_categorical_column_has_no_median_iqr(self):
        pack = [{
            "category": "Diagnosis", "variable": "Dx",
            "table": "condition_occurrence",
            "column": "condition_concept_name",
            "extraction_type": "Structured",
        }]
        conn = _Conn([
            ("GROUP BY", [("Alzheimer's", 100)]),
            ("COUNT(DISTINCT", (50,)),
            ("COUNT(*)", (200,)),
        ])
        rows = resolve_variables(
            conn, "s", pack, total_patients=1000,
            pii_pairs=set(), pii_patterns=[],
            tables_with_person_id={"condition_occurrence"},
            column_types={("condition_occurrence", "condition_concept_name"): "text"},
        )
        self.assertEqual(rows[0].median_iqr, "")


# --------------------------------------------------------------------- #
# P2 - tables without person_id skip the patient-pct queries
# --------------------------------------------------------------------- #


class PatientPctSkipTests(unittest.TestCase):

    def test_compute_patient_completeness_caller_skips_no_person_table(self):
        # When the caller doesn't include the table in tables_with_person_id,
        # the resolver must not run COUNT(DISTINCT person_id). The stub will
        # raise AssertionError on any unexpected SQL.
        pack = [{
            "category": "Demographics", "variable": "Country",
            "table": "location", "column": "country_concept_name",
            "extraction_type": "Structured",
        }]
        # Note: no "COUNT(DISTINCT" entry — any such query fails the test.
        conn = _Conn([
            ("GROUP BY", [("United States", 100)]),
            ("COUNT(*)", (100,)),
        ])
        rows = resolve_variables(
            conn, "s", pack, total_patients=1000,
            pii_pairs=set(), pii_patterns=[],
            tables_with_person_id=set(),   # location is NOT in this set
            column_types={},
        )
        self.assertIsNone(rows[0].patient_pct)
        self.assertEqual(rows[0].implemented, "Yes")


# --------------------------------------------------------------------- #
# P2 - `expression` field — ZIP rollup to 3 digits
# --------------------------------------------------------------------- #


class ExpressionFieldTests(unittest.TestCase):

    def test_expression_is_used_in_group_by_and_column_is_display_only(self):
        pack = [{
            "category": "Demographics",
            "variable": "ZIP (3-digit)",
            "table": "location",
            "column": "zip",
            "expression": "LEFT(\"zip\"::text, 3)",
            "extraction_type": "Structured",
        }]
        captured: list[str] = []

        class _CaptureCursor:
            def execute(self, sql, *params):
                captured.append(sql)
                if "GROUP BY" in sql:
                    self._next_result = [("902", 40), ("950", 30)]
                else:
                    self._next_result = (100,)
            def fetchone(self):
                return self._next_result
            def fetchall(self):
                return self._next_result
            def __enter__(self):
                return self
            def __exit__(self, *exc):
                return False

        class _CaptureConn:
            def cursor(self):
                return _CaptureCursor()
            def rollback(self):
                pass

        rows = resolve_variables(
            _CaptureConn(), "s", pack, total_patients=None,
            pii_pairs=set(), pii_patterns=[],
            tables_with_person_id=set(),
            column_types={("location", "zip"): "text"},
        )
        v = rows[0]
        # Display cell still names the raw column so users see what it's about.
        self.assertEqual(v.column, "zip")
        # The expression is what's GROUP BY'd and shown in top-N output.
        group_by_sqls = [s for s in captured if "GROUP BY" in s]
        self.assertEqual(len(group_by_sqls), 1)
        self.assertIn("LEFT(\"zip\"::text, 3)", group_by_sqls[0])
        # And the resulting values are 3-digit prefixes, not raw ZIPs.
        self.assertIn("902", v.values)
        self.assertIn("950", v.values)


# --------------------------------------------------------------------- #
# P2 - Infusion Dates split into Start + End
# --------------------------------------------------------------------- #


class InfusionPackShapeTests(unittest.TestCase):

    def test_aat_pack_splits_infusion_dates(self):
        import yaml as pyyaml
        path = REPO_ROOT / "packs" / "variables" / "aat.yaml"
        data = pyyaml.safe_load(path.read_text(encoding="utf-8"))
        names = [v["variable"] for v in data.get("variables", [])]
        # One combined label that overclaims is gone; two specific ones exist.
        self.assertNotIn("Infusion Dates", names)
        self.assertIn("Infusion Start Date", names)
        self.assertIn("Infusion End Date", names)


# --------------------------------------------------------------------- #
# P1 - renderers expose Median (IQR) and Completeness columns
# --------------------------------------------------------------------- #


class VariablesHeadersTests(unittest.TestCase):

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.tmp_dir = Path(self._tmp.name)

    def tearDown(self):
        self._tmp.cleanup()

    def test_html_variables_header_has_median_iqr_and_completeness(self):
        out = self.tmp_dir / "tech.html"
        write_html(_make_full_model(), out, audience="technical")
        page = out.read_text()
        # Just below the Variables <h2>, we expect these columns.
        variables_section = page.split("<h2>Variables</h2>", 1)[1]
        self.assertIn("<th>Median (IQR)</th>", variables_section)
        self.assertIn("<th>Completeness</th>", variables_section)
        self.assertIn("<th>Implemented</th>", variables_section)
        self.assertIn("<th>% Patient</th>", variables_section)
        # P3: matches Century reference label.
        self.assertIn("<th>Table(s)</th>", variables_section)

    def test_html_columns_header_uses_table_s(self):
        out = self.tmp_dir / "tech.html"
        write_html(_make_full_model(), out, audience="technical")
        page = out.read_text()
        columns_section = page.split("<h2>Columns</h2>", 1)[1]
        # Columns section ends at the next <h2>, so only look at this block.
        columns_section = columns_section.split("<h2>", 1)[0]
        self.assertIn("<th>Table(s)</th>", columns_section)
        self.assertNotIn("<th>Table</th>", columns_section)

    def test_xlsx_variables_sheet_has_median_iqr_and_completeness(self):
        try:
            import openpyxl  # noqa: F401
        except ImportError:
            self.skipTest("openpyxl not installed")
        import openpyxl as opx
        out = self.tmp_dir / "tech.xlsx"
        write_xlsx(_make_full_model(), out, audience="technical")
        wb = opx.load_workbook(out)
        ws = wb["Variables"]
        headers = [c.value for c in ws[1]]
        self.assertIn("Median (IQR)", headers)
        self.assertIn("Completeness", headers)
        self.assertIn("Implemented", headers)
        self.assertIn("% Patient", headers)
        self.assertIn("Table(s)", headers)
        self.assertNotIn("Table", headers)

    def test_xlsx_columns_sheet_uses_table_s(self):
        try:
            import openpyxl  # noqa: F401
        except ImportError:
            self.skipTest("openpyxl not installed")
        import openpyxl as opx
        out = self.tmp_dir / "tech.xlsx"
        write_xlsx(_make_full_model(), out, audience="technical")
        wb = opx.load_workbook(out)
        ws = wb["Columns"]
        headers = [c.value for c in ws[1]]
        self.assertIn("Table(s)", headers)
        self.assertNotIn("Table", headers)

    def test_xlsx_tables_sheet_still_uses_singular_table(self):
        # The Tables sheet itself describes one row per table — the
        # "Table" label is correct there; only the Columns and Variables
        # pages gained the Century-style "Table(s)" header.
        try:
            import openpyxl  # noqa: F401
        except ImportError:
            self.skipTest("openpyxl not installed")
        import openpyxl as opx
        out = self.tmp_dir / "tech.xlsx"
        write_xlsx(_make_full_model(), out, audience="technical")
        wb = opx.load_workbook(out)
        ws = wb["Tables"]
        headers = [c.value for c in ws[1]]
        self.assertIn("Table", headers)


if __name__ == "__main__":
    unittest.main()
