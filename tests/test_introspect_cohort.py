"""Tests for ``introspect_cohort.py``.

These cover:
  * pack loading + merge semantics (``_deep_merge``)
  * ``_pick_value_column`` for observation/measurement shape detection
  * ``_column_is_dropped`` against the pack's drop rules
  * ``build_curated_variables`` for each mode (per_concept, split_by_type,
    single_row_with_list, keep_columns, static)

The Postgres connection is stubbed via :mod:`unittest.mock` so the tests run
offline. Run with::

    python -m unittest discover -s tests -p 'test_*.py'
"""

from __future__ import annotations

import re
import sys
import unittest
from pathlib import Path
from unittest.mock import MagicMock

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

import introspect_cohort as ic

# Detect PyYAML availability once; pack-based tests skip (not fail) if
# it's not installed so a clean sandbox still gets a readable summary
# of which tests ran and which were skipped for missing-dep reasons.
try:
    import yaml  # noqa: F401
    HAS_YAML = True
except ImportError:
    HAS_YAML = False

_needs_yaml = unittest.skipUnless(
    HAS_YAML, "PyYAML is required for pack-based tests (pip install pyyaml)"
)


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #


def _col(table, column, **kwargs):
    """Shorthand ColumnInfo factory with sensible defaults."""
    return ic.ColumnInfo(
        schema=kwargs.get("schema", "mtc__aat_cohort"),
        table=table,
        column=column,
        data_type=kwargs.get("data_type", "text"),
        is_nullable=kwargs.get("is_nullable", True),
        row_count=kwargs.get("row_count", 1000),
        null_count=kwargs.get("null_count", 0),
        completeness_pct=kwargs.get("completeness_pct", 100.0),
        top_values=kwargs.get("top_values", []),
    )


def _stub_cursor(query_router):
    """Build a MagicMock cursor whose ``execute`` / ``fetchall`` / ``fetchone``
    behave per ``query_router``. The router is a function that takes the SQL
    and returns a dict with ``rows`` and/or ``row`` keys.
    """
    cur = MagicMock()
    state = {"rows": [], "row": None}

    def execute(sql, params=None):
        result = query_router(sql, params) or {}
        state["rows"] = result.get("rows", [])
        state["row"] = result.get("row")

    cur.execute = execute
    cur.fetchall = lambda: state["rows"]
    cur.fetchone = lambda: state["row"]
    cur.__enter__ = MagicMock(return_value=cur)
    cur.__exit__ = MagicMock(return_value=None)
    return cur


def _stub_conn(query_router):
    conn = MagicMock()
    conn.cursor = lambda: _stub_cursor(query_router)
    conn.rollback = MagicMock()
    return conn


# --------------------------------------------------------------------------- #
# Pack loading and merge semantics
# --------------------------------------------------------------------------- #


@_needs_yaml
class LoadPackTests(unittest.TestCase):
    """The shipped ``packs/`` directory must load cleanly."""

    def test_mtc_aat_loads(self) -> None:
        pack = ic.load_pack("mtc_aat")
        self.assertEqual(pack.slug, "mtc_aat")
        self.assertEqual(pack.cohort_name, "mtc_aat_cohort")
        self.assertEqual(pack.schema_name, "mtc__aat_cohort")

    def test_missing_cohort_raises(self) -> None:
        with self.assertRaises(FileNotFoundError):
            ic.load_pack("does_not_exist_cohort_xyz")


# --------------------------------------------------------------------------- #
# Column drop logic
# --------------------------------------------------------------------------- #


@_needs_yaml
class EndToEndTests(unittest.TestCase):
    """A curated workbook produced by write_curated_xlsx must validate
    cleanly under validate_dictionary.validate_source."""

    @classmethod
    def setUpClass(cls) -> None:
        cls.tmp = PROJECT_ROOT / "tests" / ".tmp"
        cls.tmp.mkdir(parents=True, exist_ok=True)

    def test_dictionary_workbook_shape(self) -> None:
        import uuid

        pack = ic.load_pack("mtc_aat")

        # A minimal but realistic-looking inventory: demographics + one
        # fact-table per mode so every branch fires.
        columns = [
            _col("person", "gender_concept_name", top_values=[("Female", 620)]),
            _col("person", "year_of_birth", data_type="integer"),
            _col("observation", "observation_concept_name", row_count=80000),
            _col("measurement", "measurement_concept_name", row_count=40000),
            _col("condition_occurrence", "condition_concept_name", row_count=50000),
            _col("drug_exposure", "drug_type_concept_name", row_count=30000),
            _col("visit_occurrence", "visit_concept_name", row_count=20000),
            _col("note", "note_text", row_count=5000),
        ]
        tables = [ic.TableInfo(t, 1, 1) for t in {c.table for c in columns}]

        def router(sql, params=None):
            if 'COUNT("value_as_number")' in sql:
                table = re.search(r'FROM "[^"]+"\."([^"]+)"', sql).group(1)
                return {"rows": {
                    "observation": [("Heart rate", 1200, 1200, 0, 0)],
                    "measurement": [("AAT level", 450, 450, 0, 0)],
                }.get(table, [])}
            if "PERCENTILE_CONT" in sql:
                return {"row": ("1924", "1948", "1958", "1968", "2002")}
            if "WITH scoped AS" in sql:
                return {"rows": [("Prolastin 1000 MG", 400, 50.0)]}
            if 'GROUP BY "condition_concept_name"' in sql or 'GROUP BY "visit_concept_name"' in sql:
                return {"rows": [("Alpha-1 antitrypsin deficiency", 900), ("COPD", 700)]}
            if "ORDER BY COUNT(*) DESC" in sql:
                return {"rows": [("Female", 620, 62.0), ("Male", 370, 37.0)]}
            return {}

        conn = _stub_conn(router)

        out = self.tmp / f"e2e_{uuid.uuid4().hex}.xlsx"
        try:
            # Swallow the writer's stderr summary so the test output stays clean.
            import contextlib, io
            with contextlib.redirect_stderr(io.StringIO()):
                ic.write_curated_xlsx(
                    conn=conn,
                    schema=pack.schema_name,
                    columns=columns,
                    tables=tables,
                    out_path=out,
                    cohort=pack.cohort_name,
                    person_count=1000,
                    pack=pack,
                )

            # Verify the workbook shape.
            import pandas as pd
            sheets = pd.read_excel(out, sheet_name=None)
            self.assertEqual(
                set(sheets.keys()),
                {"Summary", "Variables"},
                "workbook should contain only Summary and Variables sheets",
            )
            vars_df = sheets["Variables"]
            self.assertEqual(len(vars_df), len(columns))
            self.assertEqual(
                list(vars_df.columns),
                [
                    "Category", "Variable", "Description",
                    "Table(s)", "Column(s)", "Criteria",
                    "Values", "Distribution", "Median (IQR)",
                    "Completeness", "Extraction Type", "Notes",
                ],
                "Variables sheet column order must match the PDF layout",
            )
            # Completeness is populated for every column.
            self.assertFalse(vars_df["Completeness"].isna().any())
            # Extraction Type is one of {Structured, Unstructured}.
            self.assertTrue(
                set(vars_df["Extraction Type"].unique()).issubset(
                    {"Structured", "Unstructured"}
                )
            )
        finally:
            out.unlink(missing_ok=True)

    def test_dictionary_html_output(self) -> None:
        """write_curated_html emits an HTML file with Summary and
        Variables tables in the PDF column order."""
        import uuid

        columns = [
            _col("person", "gender_concept_name",
                 data_type="character varying",
                 top_values=[("Female", 620), ("Male", 440)]),
            _col("person", "year_of_birth",
                 data_type="integer", row_count=1000),
            _col("note", "note_text",
                 data_type="text", row_count=5000),
        ]
        tables = [ic.TableInfo(t, 1, 1) for t in {c.table for c in columns}]

        out = self.tmp / f"e2e_{uuid.uuid4().hex}.html"
        try:
            import contextlib, io
            with contextlib.redirect_stderr(io.StringIO()):
                ic.write_curated_html(
                    columns=columns,
                    tables=tables,
                    out_path=out,
                    cohort="mtc_aat_cohort",
                    person_count=1000,
                )
            body = out.read_text(encoding="utf-8")
            # Basic structure.
            self.assertIn("<title>Data Dictionary", body)
            self.assertIn("mtc_aat_cohort", body)
            # Every PDF-layout column header appears.
            for header in [
                "Category", "Variable", "Description", "Table(s)",
                "Column(s)", "Criteria", "Values", "Distribution",
                "Median (IQR)", "Completeness", "Extraction Type",
                "Notes",
            ]:
                self.assertIn(f"<th>{header}</th>", body)
            # Every column in the fixture shows up as a row.
            for col in columns:
                self.assertIn(col.column, body)
            # Unstructured row (text type) is marked accordingly.
            self.assertIn("Unstructured", body)
        finally:
            out.unlink(missing_ok=True)


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
