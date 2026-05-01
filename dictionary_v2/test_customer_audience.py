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


class CompileMatchBlockTests(unittest.TestCase):
    """The structured `match:` block in variable YAML compiles to a
    strict `column IN (...)` SQL clause. The data dictionary uses this
    as both the displayed Criteria and the WHERE for completeness, so
    the definition stays config-owned (not data-derived)."""

    def test_inline_values_compile_to_in_list(self):
        sql = bd.compile_match_block({
            "column": "drug_concept_name",
            "values": ["Lecanemab", "Leqembi", "Donanemab"],
        })
        self.assertEqual(
            sql,
            "\"drug_concept_name\" IN ('Lecanemab', 'Leqembi', 'Donanemab')",
        )

    def test_escapes_single_quotes(self):
        sql = bd.compile_match_block({
            "column": "concept_name",
            "values": ["O'Brien syndrome", "Plain value"],
        })
        self.assertIn("'O''Brien syndrome'", sql)
        self.assertIn("'Plain value'", sql)

    def test_dedupes_inline_values_preserving_order(self):
        sql = bd.compile_match_block({
            "column": "c",
            "values": ["A", "B", "A", "C", "B"],
        })
        self.assertEqual(sql, "\"c\" IN ('A', 'B', 'C')")

    def test_missing_column_returns_blank(self):
        self.assertEqual(bd.compile_match_block({"values": ["x"]}), "")

    def test_empty_values_returns_blank(self):
        self.assertEqual(bd.compile_match_block({"column": "c"}), "")
        self.assertEqual(bd.compile_match_block({"column": "c", "values": []}), "")

    def test_none_block_returns_blank(self):
        self.assertEqual(bd.compile_match_block(None), "")

    def test_values_file_loads_and_unions_with_inline(self):
        # values_file points at packs/<rel_path>; create a temp file
        # there and clean it up.
        rel = "_test_match_values.yaml"
        path = bd.PACKS_DIR / rel
        path.write_text("- Foo\n- Bar\n", encoding="utf-8")
        try:
            sql = bd.compile_match_block({
                "column": "concept_name",
                "values": ["Bar", "Baz"],
                "values_file": rel,
            })
        finally:
            path.unlink()
        # Order: inline first, file values second; "Bar" appears once.
        self.assertEqual(
            sql,
            "\"concept_name\" IN ('Bar', 'Baz', 'Foo')",
        )


# Load the discovery script once under a stable module name so the
# dataclass it defines (VariableObservation) survives across test cases.
import importlib.util as _ilu_disc  # noqa: E402

_DISC_PATH = Path(__file__).resolve().parent / "discover_exact_matches.py"
_disc_spec = _ilu_disc.spec_from_file_location(
    "discover_exact_matches_under_test", _DISC_PATH,
)
discover_mod = _ilu_disc.module_from_spec(_disc_spec)
sys.modules["discover_exact_matches_under_test"] = discover_mod
_disc_spec.loader.exec_module(discover_mod)


class DiscoveryScriptTests(unittest.TestCase):
    """The discovery script proposes additions to a variable's `match:`
    block — it must never modify the disease pack automatically."""

    def setUp(self):
        self.mod = discover_mod

    def _obs(self, configured, observed):
        return self.mod.VariableObservation(
            category="C", variable="V", table="t", column="c",
            criteria="c ILIKE '%x%'", configured_values=configured,
            observed=observed,
        )

    def test_partitions_observed_against_config(self):
        o = self._obs(
            configured=["Aspirin 81 MG", "Aspirin 325 MG", "Old Label"],
            observed=[("Aspirin 81 MG", 100), ("Aspirin 325 MG", 50),
                      ("acetylsalicylic acid", 5)],
        )
        self.assertEqual(
            o.configured_and_observed,
            [("Aspirin 81 MG", 100), ("Aspirin 325 MG", 50)],
        )
        self.assertEqual(
            o.missing_from_config, [("acetylsalicylic acid", 5)],
        )
        self.assertEqual(o.stale_in_config, ["Old Label"])

    def test_report_flags_candidate_additions(self):
        o = self._obs(
            configured=["A"],
            observed=[("A", 10), ("B", 3)],
        )
        md = self.mod._fmt_md([o], "test_cohort")
        self.assertIn("Observed but NOT in config", md)
        self.assertIn("`B`", md)
        # Existing config entry is shown but not flagged as candidate.
        self.assertIn("Configured & observed", md)

    def test_suggestions_yaml_unions_observed_and_configured(self):
        o = self._obs(
            configured=["A", "C"],
            observed=[("A", 10), ("B", 3)],
        )
        y = self.mod._fmt_suggestions_yaml([o])
        # Observed-first ordering, configured tail, deduped.
        self.assertIn("- A", y)
        self.assertIn("- B", y)
        self.assertIn("- C", y)
        self.assertIn("column: c", y)
        # Header line warns this is for review only.
        self.assertIn("Review and copy", y)

    def test_dry_run_writes_report_without_db(self):
        # End-to-end: --dry-run must not require psycopg / a DB.
        out_dir = _output_dir(self.id())
        rc = self.mod.main([
            "--cohort", "balboa_ckd",
            "--out-dir", str(out_dir),
            "--dry-run",
        ])
        self.assertEqual(rc, 0)
        self.assertTrue((out_dir / "balboa_ckd" / "report.md").is_file())


if __name__ == "__main__":
    unittest.main()
