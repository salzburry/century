"""Tests for the customer audience added in PR-B.

These exercise dictionary_v2/build_dictionary.py specifically. The root
test_build_dictionary.py suite continues to cover the original
build_dictionary.py module (which doesn't know about customer).
"""
from __future__ import annotations

import importlib.util
import shutil
import sys
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


# Repo-local output dir for file-writing smoke tests. Replaces
# tempfile.TemporaryDirectory() which has been unreliable in the
# Windows reviewer sandbox (PermissionError when openpyxl tries to
# write into the OS temp tree). Lives next to the test file and is
# gitignored.
_TEST_OUTPUT_ROOT = Path(__file__).resolve().parent / ".test_outputs"


def _output_dir(test_id: str) -> Path:
    """Return a stable, repo-local directory for one test's artifacts.

    `test_id` is `self.id()` (something like
    `dictionary_v2.test_customer_audience.CustomerXlsxSmokeTests.
    test_summary_has_no_metric_value_header`). The directory is
    cleared at the start of each call so tests start from a known
    empty state.
    """
    out = _TEST_OUTPUT_ROOT / test_id
    if out.exists():
        shutil.rmtree(out, ignore_errors=True)
    out.mkdir(parents=True, exist_ok=True)
    return out


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
        for kept in ("cohort", "provider", "disease", "patient_count",
                     "table_count", "generated_at"):
            self.assertIn(kept, xl_keys)

    def test_tables_drops_data_source_and_source_table(self):
        headers = [label for label, _ in bd.tables_layout("customer")]
        self.assertNotIn("Data Source", headers)
        self.assertNotIn("Source Table", headers)
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
        for dropped in ("Coding Schema", "Implemented", "Data Source"):
            self.assertNotIn(dropped, headers)

    def test_other_audiences_unaffected(self):
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
        tables = [_make_table("person"), _make_table("cohort_patients")]
        model = _make_model(tables, [_make_column("person")], [_make_variable("person")])
        filtered = bd.filter_for_audience(model, "sales")
        names = {t.table_name for t in filtered.tables}
        self.assertEqual(names, {"person", "cohort_patients"})


class _CustomerSmokeBase(unittest.TestCase):
    """Shared setUp for the file-writing smoke tests."""

    def setUp(self):
        tables = [_make_table("person"), _make_table("cohort_patients")]
        columns = [_make_column("person"), _make_column("cohort_patients")]
        variables = [_make_variable("person"), _make_variable("cohort_patients")]
        self.model = _make_model(tables, columns, variables)
        self.filtered = bd.filter_for_audience(self.model, "customer")


class CustomerXlsxSmokeTests(_CustomerSmokeBase):

    def setUp(self):
        try:
            import openpyxl  # noqa: F401
            import pandas    # noqa: F401
        except ImportError:
            self.skipTest("openpyxl / pandas not installed")
        super().setUp()

    def test_summary_has_no_metric_value_header(self):
        import openpyxl
        path = _output_dir(self.id()) / "out.xlsx"
        bd.write_xlsx(self.filtered, path, audience="customer")
        wb = openpyxl.load_workbook(path)
        ws = wb["Summary"]
        # Row 1 must be the first data row, not the literal labels.
        row1 = [c.value for c in ws[1]]
        self.assertEqual(row1, ["cohort", "c"])
        self.assertNotEqual(row1, ["metric", "value"])

    def test_columns_sheet_has_exactly_four_headers(self):
        import openpyxl
        path = _output_dir(self.id()) / "out.xlsx"
        bd.write_xlsx(self.filtered, path, audience="customer")
        wb = openpyxl.load_workbook(path)
        ws = wb["Columns"]
        headers = [c.value for c in ws[1]]
        self.assertEqual(headers,
                         ["Table(s)", "Column", "Description", "Field Type"])

    def test_excluded_tables_absent_from_xlsx(self):
        import openpyxl
        path = _output_dir(self.id()) / "out.xlsx"
        bd.write_xlsx(self.filtered, path, audience="customer")
        wb = openpyxl.load_workbook(path)
        ws = wb["Tables"]
        names = [ws.cell(row=r, column=1).value
                 for r in range(2, ws.max_row + 1)]
        self.assertNotIn("cohort_patients", names)
        self.assertEqual(names, ["person"])


class CustomerHtmlSmokeTests(_CustomerSmokeBase):

    def test_html_omits_debug_summary_fields(self):
        path = _output_dir(self.id()) / "out.html"
        bd.write_html(self.filtered, path, audience="customer")
        html = path.read_text()
        self.assertNotIn("<dt>Variant</dt>", html)
        self.assertNotIn("<dt>Git SHA</dt>", html)
        self.assertNotIn("<dt>Schema snapshot</dt>", html)
        # Date coverage is the merged HTML field — it must remain.
        self.assertIn("<dt>Date coverage</dt>", html)

    def test_html_variables_includes_both_criteria_columns(self):
        path = _output_dir(self.id()) / "out.html"
        bd.write_html(self.filtered, path, audience="customer")
        html = path.read_text()
        self.assertIn("<th>Inclusion Criteria</th>", html)
        self.assertIn("<th>Criteria</th>", html)


class CustomerJsonSuppressionTests(_CustomerSmokeBase):
    """JSON is an internal/debug artifact; customer audience targets
    external stakeholders who read XLSX/HTML. main() suppresses JSON
    output for customer rather than maintaining a parallel projection."""

    def test_main_skips_json_for_customer(self):
        # Drive main() end-to-end for one cohort. Use --dry-run so the
        # introspection step doesn't need a database connection.
        out_dir = _output_dir(self.id())
        argv = [
            "--cohort", "balboa_ckd",
            "--audience", "customer",
            "--formats", "xlsx", "html", "json",
            "--out-dir", str(out_dir),
            "--dry-run",
        ]
        rc = bd.main(argv)
        self.assertEqual(rc, 0)
        files = sorted(p.name for p in out_dir.iterdir() if p.is_file())
        # JSON must be absent; XLSX + HTML still get written.
        self.assertFalse(any(f.endswith(".json") for f in files),
                         msg=f"customer audience must not write JSON, got {files}")
        self.assertTrue(any(f.endswith(".xlsx") for f in files), msg=files)
        self.assertTrue(any(f.endswith(".html") for f in files), msg=files)


class CustomerLayoutConfigTests(unittest.TestCase):
    """packs/dictionary_layout.yaml drives the customer table-exclude
    list. Per-cohort overrides can replace or extend the global list."""

    def test_yaml_drives_global_excludes(self):
        excludes = bd.customer_table_excludes()
        for name in ("standard_profile_data_model", "cohort_patients",
                     "dv_tokenized_profile_data"):
            self.assertIn(name, excludes,
                          msg=f"{name} should be excluded by default")

    def test_unknown_cohort_falls_back_to_global(self):
        self.assertEqual(
            bd.customer_table_excludes("nonexistent_cohort"),
            bd.customer_table_excludes(),
        )

    def _patch_layout(self, layout):
        original = bd._load_dictionary_layout
        bd._load_dictionary_layout = lambda: layout
        self.addCleanup(lambda: setattr(bd, "_load_dictionary_layout", original))

    def test_per_cohort_exclude_tables_replaces_global(self):
        self._patch_layout({
            "customer": {"exclude_tables": ["a", "b"]},
            "cohorts": {"my_cohort": {"customer": {"exclude_tables": ["c"]}}},
        })
        self.assertEqual(bd.customer_table_excludes("my_cohort"), frozenset({"c"}))
        self.assertEqual(bd.customer_table_excludes(), frozenset({"a", "b"}))

    def test_per_cohort_extra_excludes_extends_global(self):
        self._patch_layout({
            "customer": {"exclude_tables": ["a", "b"]},
            "cohorts": {"my_cohort": {"customer": {"extra_exclude_tables": ["c"]}}},
        })
        self.assertEqual(
            bd.customer_table_excludes("my_cohort"),
            frozenset({"a", "b", "c"}),
        )

    def test_filter_for_audience_uses_cohort_specific_excludes(self):
        self._patch_layout({
            "customer": {"exclude_tables": []},
            "cohorts": {"c": {"customer": {"exclude_tables": ["person"]}}},
        })
        tables = [_make_table("person"), _make_table("cohort_patients")]
        columns = [_make_column("person"), _make_column("cohort_patients")]
        variables = [_make_variable("person"), _make_variable("cohort_patients")]
        model = _make_model(tables, columns, variables)
        # _make_model sets cohort="c", which the per-cohort override targets.
        filtered = bd.filter_for_audience(model, "customer")
        self.assertEqual({t.table_name for t in filtered.tables}, {"cohort_patients"})


class _FakeCursor:
    def __init__(self, rows):
        self._rows = rows
        self._calls = []

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, sql):
        self._calls.append(sql)

    def fetchall(self):
        return [(r,) for r in self._rows]


class _FakeConn:
    def __init__(self, rows):
        self._rows = rows
        self.queries = []

    def cursor(self):
        cur = _FakeCursor(self._rows)
        self.queries.append(cur)
        return cur

    def rollback(self):
        pass


class StrictCriteriaDerivationTests(unittest.TestCase):
    """Auto-derive `column IN (...)` from the cohort's actual top-N
    values. Replaces hand-written ILIKE patterns; the dictionary then
    describes what's really in the data."""

    def test_emits_in_list_from_top_values(self):
        conn = _FakeConn(["Lecanemab", "Leqembi", "Donanemab"])
        result = bd.derive_strict_criteria(
            conn, "schema", "drug_exposure", "drug_concept_name", top_n=50,
        )
        self.assertEqual(
            result,
            "\"drug_concept_name\" IN ('Lecanemab', 'Leqembi', 'Donanemab')",
        )

    def test_escapes_single_quotes(self):
        conn = _FakeConn(["O'Brien syndrome", "Plain value"])
        result = bd.derive_strict_criteria(
            conn, "schema", "condition", "concept_name", top_n=10,
        )
        self.assertIn("'O''Brien syndrome'", result)

    def test_top_n_zero_opts_out(self):
        conn = _FakeConn(["x"])
        self.assertEqual(
            bd.derive_strict_criteria(conn, "s", "t", "c", top_n=0),
            "",
        )

    def test_skips_freetext_columns(self):
        conn = _FakeConn(["anything"])
        for col in ("note_text", "observation_source_value_text"):
            self.assertEqual(
                bd.derive_strict_criteria(conn, "s", "t", col, top_n=50),
                "",
                msg=f"freetext column {col} should be skipped",
            )

    def test_skips_surrogate_keys(self):
        conn = _FakeConn(["1", "2", "3"])
        # Surrogate-key heuristic catches *_concept_id / *_id columns —
        # top-N of identifiers is meaningless.
        result = bd.derive_strict_criteria(
            conn, "s", "drug_exposure", "drug_concept_id", top_n=50,
        )
        self.assertEqual(result, "")

    def test_empty_result_returns_blank(self):
        conn = _FakeConn([])
        self.assertEqual(
            bd.derive_strict_criteria(conn, "s", "t", "concept_name", top_n=50),
            "",
        )

    def test_query_failure_returns_blank(self):
        class BoomConn:
            def cursor(self):
                raise RuntimeError("connection lost")
            def rollback(self):
                pass
        self.assertEqual(
            bd.derive_strict_criteria(
                BoomConn(), "s", "t", "concept_name", top_n=50,
            ),
            "",
        )

    def test_top_n_default_is_configurable(self):
        # Should match the value in packs/dictionary_layout.yaml.
        n = bd.criteria_top_n_default()
        self.assertIsInstance(n, int)
        self.assertGreater(n, 0)


if __name__ == "__main__":
    unittest.main()
