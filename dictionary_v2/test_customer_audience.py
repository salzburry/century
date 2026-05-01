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

    def test_dry_run_resolves_shared_variable_pack_includes(self):
        # balboa_ckd's variables pack is a placeholder that pulls
        # everything from ckd_common via `include:`. The previous
        # discovery loader missed shared variables and produced an
        # empty report.
        out_dir = _output_dir(self.id())
        rc = self.mod.main([
            "--cohort", "balboa_ckd",
            "--out-dir", str(out_dir),
            "--dry-run",
        ])
        self.assertEqual(rc, 0)
        report = (out_dir / "balboa_ckd" / "report.md").read_text()
        # Sanity-check that at least one CKD-shared variable appears
        # under a headed section. ckd_common defines dozens of rows;
        # the report should not be empty after the header.
        self.assertGreater(
            report.count("\n## "), 0,
            msg="report should include shared-pack variables, got:\n" + report,
        )

    def test_resolve_matcher_column_prefers_match_block(self):
        col, skip = self.mod._resolve_matcher_column({
            "table": "measurement", "column": "value_as_number",
            "match": {"column": "measurement_concept_name"},
        })
        self.assertEqual(col, "measurement_concept_name")
        self.assertEqual(skip, "")

    def test_resolve_matcher_column_skips_value_columns(self):
        # value_as_number / value_as_string / value_as_concept_name etc.
        # would discover the value distribution, not the clinical
        # matcher. Discovery should refuse and ask the pack to set
        # match.column when no inference is possible.
        for col in ("value_as_number", "value_as_string",
                    "value_as_concept_name", "measurement_date"):
            matcher, skip = self.mod._resolve_matcher_column({
                "table": "measurement", "column": col,
            })
            self.assertEqual(matcher, "", msg=f"{col} should be skipped")
            self.assertIn("match.column", skip)

    def test_resolve_matcher_infers_from_criteria_lhs(self):
        # Real shape from ckd_common.yaml — Language has
        # column=value_as_concept_name (a value column) but
        # criteria points the matcher at observation_concept_name.
        # Discovery must group on the inferred matcher, not on the
        # value column, otherwise it would suggest match.values like
        # ["English", "Spanish"] redefining the variable.
        matcher, skip = self.mod._resolve_matcher_column({
            "table": "observation",
            "column": "value_as_concept_name",
            "criteria": "observation_concept_name ILIKE '%language%'",
        })
        self.assertEqual(matcher, "observation_concept_name")
        self.assertEqual(skip, "")

    def test_dry_run_flags_unscoped_rows_as_skipped(self):
        # Dry-run must mark a no-criteria / no-match row with the
        # same skip reason live discovery would emit, so offline
        # review reflects what the real DB run will do.
        matcher, scope, displayed, error = self.mod._resolve_scope({
            "table": "condition_occurrence",
            "column": "condition_concept_name",
        })
        self.assertEqual(scope, "")
        self.assertIn("no `criteria:` or `match:`", error)
        # Live and dry-run share the same constructor, so the
        # observation built from this resolution carries the error.
        obs = self.mod._build_observation(
            {"table": "condition_occurrence",
             "column": "condition_concept_name"},
            matcher, displayed, error,
        )
        self.assertIn("no `criteria:` or `match:`", obs.error)

    def test_observe_skips_unscoped_rows(self):
        # A variable with no `criteria:` and no `match:` block must NOT
        # be discovered against the whole table. WHERE TRUE would
        # propose every concept_name in the table as an exact match,
        # silently redefining a generic row like "Diagnosis".
        class _ShouldNotQuery:
            def cursor(self):
                raise AssertionError(
                    "discovery must not query when row has no scope"
                )
            def rollback(self):
                pass
        obs = self.mod._observe_one(_ShouldNotQuery(), "schema", {
            "category": "Conditions",
            "variable": "Diagnosis",
            "table": "condition_occurrence",
            "column": "condition_concept_name",
        })
        self.assertIn("no `criteria:` or `match:`", obs.error)
        self.assertEqual(obs.observed, [])

    def test_scope_prefers_broad_criteria_when_match_also_present(self):
        # Drift workflow: when a variable has BOTH a broad criteria:
        # and a curated match: list, discovery's WHERE must use the
        # broad criteria so the report can flag observed values that
        # aren't yet in match.values. Using match: here would only
        # enumerate already-configured values, making
        # missing_from_config impossible.
        matcher, scope, displayed, error = self.mod._resolve_scope({
            "table": "drug_exposure",
            "column": "drug_concept_name",
            "criteria": "drug_concept_name ILIKE '%aspirin%'",
            "match": {
                "column": "drug_concept_name",
                "values": ["Aspirin 81 MG"],
            },
        })
        self.assertEqual(error, "")
        # Broad ILIKE drives the live discovery query.
        self.assertIn("ILIKE '%aspirin%'", scope)
        # Strict IN list drives the displayed Criteria cell.
        self.assertIn("IN ('Aspirin 81 MG')", displayed)

    def test_scope_uses_match_when_no_broad_criteria(self):
        # Match-only rows still scope to the curated set — falling
        # back to WHERE TRUE would re-enumerate the whole table.
        _matcher, scope, _displayed, error = self.mod._resolve_scope({
            "table": "drug_exposure",
            "column": "drug_concept_name",
            "match": {
                "column": "drug_concept_name",
                "values": ["Aspirin 81 MG", "Aspirin 325 MG"],
            },
        })
        self.assertEqual(error, "")
        self.assertIn("Aspirin 81 MG", scope)
        self.assertIn("Aspirin 325 MG", scope)

    def test_observe_uses_match_block_as_scope(self):
        # When only a `match:` block is configured (no free-form
        # criteria), discovery must scope to that strict IN list, not
        # WHERE TRUE.
        captured = {}

        class _CaptureConn:
            def cursor(self):
                outer = self
                class _Cur:
                    def __enter__(self_inner): return self_inner
                    def __exit__(self_inner, *a): return False
                    def execute(self_inner, sql):
                        captured["sql"] = sql
                    def fetchall(self_inner):
                        return []
                return _Cur()
            def rollback(self):
                pass

        self.mod._observe_one(_CaptureConn(), "schema", {
            "table": "drug_exposure",
            "column": "drug_concept_name",
            "match": {
                "column": "drug_concept_name",
                "values": ["Aspirin 81 MG", "Aspirin 325 MG"],
            },
        })
        self.assertIn("Aspirin 81 MG", captured["sql"])
        self.assertNotIn("WHERE TRUE", captured["sql"])

    def test_report_shows_source_pack(self):
        o = self.mod.VariableObservation(
            category="Conditions", variable="CKD",
            table="condition_occurrence", column="condition_concept_name",
            criteria="condition_concept_name ILIKE '%kidney%'",
            configured_values=[], observed=[("Chronic kidney disease", 100)],
            source_pack="ckd_common",
        )
        md = self.mod._fmt_md([o], "balboa_ckd")
        self.assertIn("packs/variables/ckd_common.yaml", md)

    def test_suggestions_warn_about_shared_vs_cohort_placement(self):
        o = self.mod.VariableObservation(
            category="C", variable="V", table="t", column="c",
            criteria="c ILIKE '%x%'", configured_values=[],
            observed=[("A", 10)], source_pack="ckd_common",
        )
        y = self.mod._fmt_suggestions_yaml([o], cohort="balboa_ckd")
        # Reviewer is told where this row was defined and where to
        # paste cohort-specific overrides.
        self.assertIn("packs/variables/ckd_common.yaml", y)
        self.assertIn("packs/variables/balboa_ckd.yaml", y)
        self.assertIn("cohort-specific", y)

    def test_resolve_matcher_infers_from_in_clause(self):
        # The LHS regex must also handle `IN (...)` and `=` shapes,
        # not just ILIKE.
        for crit in (
            "drug_concept_name IN ('A', 'B')",
            "drug_concept_name = 'Aspirin'",
        ):
            matcher, _ = self.mod._resolve_matcher_column({
                "table": "drug_exposure",
                "column": "value_as_string",
                "criteria": crit,
            })
            self.assertEqual(matcher, "drug_concept_name", msg=crit)

    def test_resolve_matcher_column_uses_concept_name_directly(self):
        # When `column` is already the matcher (concept_name), no
        # match block is needed.
        col, skip = self.mod._resolve_matcher_column({
            "table": "drug_exposure", "column": "drug_concept_name",
        })
        self.assertEqual(col, "drug_concept_name")
        self.assertEqual(skip, "")


class DryRunMatchBlockTests(unittest.TestCase):
    """Dry-run path must compile `match:` blocks the same way the live
    build does, so review artifacts and offline previews don't show
    stale fuzzy criteria once packs adopt strict matchers."""

    def test_dry_run_uses_match_block_in_criteria(self):
        # Patch load_variables_pack to return a single variable with
        # an explicit match block, then drive build_model in dry-run.
        original = bd.load_variables_pack
        bd.load_variables_pack = lambda slug: [{
            "category": "Labs",
            "variable": "Aspirin",
            "table": "drug_exposure",
            "column": "drug_concept_name",
            "criteria": "drug_concept_name ILIKE '%aspirin%'",
            "match": {
                "column": "drug_concept_name",
                "values": ["Aspirin 81 MG", "Aspirin 325 MG"],
            },
        }]
        try:
            model = bd.build_model("balboa_ckd", conn=None, dry_run=True)
        finally:
            bd.load_variables_pack = original

        self.assertEqual(len(model.variables), 1)
        criteria = model.variables[0].criteria
        self.assertEqual(
            criteria,
            "\"drug_concept_name\" IN ('Aspirin 81 MG', 'Aspirin 325 MG')",
        )
        self.assertNotIn("ILIKE", criteria)


class CohortKeyResolutionTests(unittest.TestCase):
    """Per-cohort layout overrides must work whether the YAML keys by
    slug, cohort_name, or schema_name. The CLI/filename slug is the
    documented preferred key."""

    def _patch_layout(self, layout):
        original = bd._load_dictionary_layout
        bd._load_dictionary_layout = lambda: layout
        self.addCleanup(lambda: setattr(bd, "_load_dictionary_layout", original))

    def test_lookup_by_slug(self):
        self._patch_layout({
            "customer": {"exclude_tables": []},
            "cohorts": {"my_slug": {"customer": {"exclude_tables": ["X"]}}},
        })
        self.assertEqual(
            bd.customer_table_excludes(["my_slug", "my_cohort_name"]),
            frozenset({"X"}),
        )

    def test_lookup_falls_back_to_cohort_name(self):
        self._patch_layout({
            "customer": {"exclude_tables": []},
            "cohorts": {"my_cohort_name": {"customer": {"exclude_tables": ["Y"]}}},
        })
        self.assertEqual(
            bd.customer_table_excludes(["my_slug", "my_cohort_name"]),
            frozenset({"Y"}),
        )

    def test_first_matching_key_wins(self):
        self._patch_layout({
            "customer": {"exclude_tables": []},
            "cohorts": {
                "my_slug":        {"customer": {"exclude_tables": ["A"]}},
                "my_cohort_name": {"customer": {"exclude_tables": ["B"]}},
            },
        })
        self.assertEqual(
            bd.customer_table_excludes(["my_slug", "my_cohort_name"]),
            frozenset({"A"}),
        )

    def test_filter_threads_slug_through(self):
        # End-to-end: filter_for_audience must pass the CLI slug as
        # the primary lookup key, not just model.cohort.
        self._patch_layout({
            "customer": {"exclude_tables": []},
            "cohorts": {"my_slug": {"customer": {"exclude_tables": ["person"]}}},
        })
        tables = [_make_table("person"), _make_table("cohort_patients")]
        columns = [_make_column("person")]
        variables = [_make_variable("person")]
        model = _make_model(tables, columns, variables)
        # _make_model sets model.cohort = "c", which has no override.
        # Without slug threading, the per-slug entry would be missed.
        filtered = bd.filter_for_audience(model, "customer", cohort_slug="my_slug")
        self.assertEqual(
            {t.table_name for t in filtered.tables}, {"cohort_patients"},
        )


class DiscoveryApplyTests(unittest.TestCase):
    """`--apply` writes proposed match: blocks into source packs.
    Must round-trip the YAML safely (preserve comments / order),
    only touch eligible variables, and refuse cleanly when ruamel
    is unavailable."""

    def setUp(self):
        try:
            import ruamel.yaml  # noqa: F401
        except ImportError:
            self.skipTest("ruamel.yaml not installed")
        self.mod = discover_mod
        # Stage a temp variables pack under packs/variables/ so the
        # apply path can find it via PACKS_DIR/variables/<slug>.yaml.
        self.pack_slug = "_apply_test_pack"
        self.pack_path = bd.PACKS_DIR / "variables" / f"{self.pack_slug}.yaml"
        self.pack_path.write_text(
            "# Test pack. Comments must survive --apply round-trip.\n"
            "variables:\n"
            "  - category: Drugs\n"
            "    variable: Aspirin\n"
            "    table: drug_exposure\n"
            "    column: drug_concept_name\n"
            "    criteria: drug_concept_name ILIKE '%aspirin%'\n"
            "  - category: Drugs\n"
            "    variable: Lisinopril\n"
            "    table: drug_exposure\n"
            "    column: drug_concept_name\n"
            "    criteria: drug_concept_name ILIKE '%lisinopril%'\n",
            encoding="utf-8",
        )
        self.addCleanup(lambda: self.pack_path.unlink(missing_ok=True))

    def _obs(self, variable, observed):
        return self.mod.VariableObservation(
            category="Drugs", variable=variable, table="drug_exposure",
            column="drug_concept_name",
            criteria=f"drug_concept_name ILIKE '%{variable.lower()}%'",
            configured_values=[], observed=observed,
            source_pack=self.pack_slug,
        )

    def test_apply_writes_match_blocks(self):
        observations = [
            self._obs("Aspirin",
                      [("Aspirin 81 MG", 100), ("Aspirin 325 MG", 50)]),
        ]
        applied, skipped = self.mod.apply_suggestions(
            observations, auto_yes=True,
        )
        self.assertEqual(applied, 1)
        self.assertEqual(skipped, 0)
        text = self.pack_path.read_text()
        # Comment at the top must survive.
        self.assertIn("Comments must survive --apply round-trip", text)
        # Match block written under the right variable.
        self.assertIn("- Aspirin 81 MG", text)
        self.assertIn("- Aspirin 325 MG", text)

    def test_apply_skips_unobserved_variables(self):
        # Observation with no rows is not eligible — must not write.
        observations = [self._obs("Aspirin", [])]
        applied, skipped = self.mod.apply_suggestions(
            observations, auto_yes=True,
        )
        self.assertEqual(applied, 0)
        self.assertNotIn("match:", self.pack_path.read_text())

    def test_apply_skips_errored_variables(self):
        bad = self.mod.VariableObservation(
            category="C", variable="Aspirin", table="t", column="c",
            criteria="", configured_values=[],
            observed=[("v", 1)],
            error="value column",
            source_pack=self.pack_slug,
        )
        applied, _ = self.mod.apply_suggestions([bad], auto_yes=True)
        self.assertEqual(applied, 0)
        self.assertNotIn("match:", self.pack_path.read_text())

    def test_apply_skips_unknown_variable_in_pack(self):
        # Variable not present in the source file (likely lives in
        # an included pack). Must skip, not crash.
        observations = [self._obs("Nonexistent", [("X", 1)])]
        applied, skipped = self.mod.apply_suggestions(
            observations, auto_yes=True,
        )
        self.assertEqual(applied, 0)
        self.assertEqual(skipped, 1)

    def test_apply_groups_by_source_pack(self):
        # Two observations for the same pack → one read+write of the file.
        observations = [
            self._obs("Aspirin", [("Aspirin 81 MG", 100)]),
            self._obs("Lisinopril", [("Lisinopril 10 MG", 80)]),
        ]
        applied, _ = self.mod.apply_suggestions(observations, auto_yes=True)
        self.assertEqual(applied, 2)
        text = self.pack_path.read_text()
        self.assertIn("Aspirin 81 MG", text)
        self.assertIn("Lisinopril 10 MG", text)


if __name__ == "__main__":
    unittest.main()
