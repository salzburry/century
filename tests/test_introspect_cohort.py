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


class DeepMergeTests(unittest.TestCase):
    def test_lists_append(self) -> None:
        self.assertEqual(ic._deep_merge([1, 2], [3, 4]), [1, 2, 3, 4])

    def test_scalars_replace(self) -> None:
        self.assertEqual(ic._deep_merge("old", "new"), "new")
        self.assertEqual(ic._deep_merge(1, 2), 2)

    def test_dicts_deep_merge(self) -> None:
        base = {"a": 1, "nested": {"x": [1], "y": 2}}
        overlay = {"b": 2, "nested": {"x": [3], "y": 9}}
        merged = ic._deep_merge(base, overlay)
        self.assertEqual(
            merged,
            {"a": 1, "b": 2, "nested": {"x": [1, 3], "y": 9}},
        )

    def test_mixed_type_replace(self) -> None:
        # list overlaying dict falls through to the replace branch.
        self.assertEqual(ic._deep_merge({"a": 1}, [1, 2]), [1, 2])


@_needs_yaml
class LoadPackTests(unittest.TestCase):
    """The shipped ``packs/`` directory must load cleanly."""

    def test_mtc_aat_loads(self) -> None:
        pack = ic.load_pack("mtc_aat")
        self.assertEqual(pack.cohort_name, "mtc_aat_cohort")
        self.assertEqual(pack.schema_name, "mtc__aat_cohort")
        self.assertIn("dv_tokenized_profile_data", pack.tables_to_skip)
        self.assertIn("observation", pack.curation_rules)
        self.assertGreater(len(pack.drop_column_patterns), 0)
        self.assertGreater(len(pack.sampleable_types), 0)

    def test_missing_cohort_raises(self) -> None:
        with self.assertRaises(FileNotFoundError):
            ic.load_pack("does_not_exist_cohort_xyz")


# --------------------------------------------------------------------------- #
# Column drop logic
# --------------------------------------------------------------------------- #


@_needs_yaml
class ColumnIsDroppedTests(unittest.TestCase):
    def setUp(self) -> None:
        self.pack = ic.load_pack("mtc_aat")

    def test_drops_table_in_skip_set(self) -> None:
        self.assertTrue(
            ic._column_is_dropped("dv_tokenized_profile_data", "token_1", self.pack)
        )

    def test_drops_sensitive_column_everywhere(self) -> None:
        self.assertTrue(ic._column_is_dropped("person", "ssn", self.pack))

    def test_drops_id_regex(self) -> None:
        self.assertTrue(ic._column_is_dropped("person", "provider_id", self.pack))
        self.assertTrue(
            ic._column_is_dropped("observation", "observation_source_value", self.pack)
        )

    def test_keeps_concept_name(self) -> None:
        self.assertFalse(
            ic._column_is_dropped(
                "observation", "observation_concept_name", self.pack
            )
        )


# --------------------------------------------------------------------------- #
# _pick_value_column
# --------------------------------------------------------------------------- #


class PickValueColumnTests(unittest.TestCase):
    def test_continuous_when_number_dominates(self) -> None:
        self.assertEqual(
            ic._pick_value_column(n_num=900, n_str=0, n_concept=0),
            ("value_as_number", "continuous"),
        )

    def test_categorical_string_when_string_dominates(self) -> None:
        self.assertEqual(
            ic._pick_value_column(n_num=10, n_str=900, n_concept=0),
            ("value_as_string", "categorical"),
        )

    def test_categorical_concept_when_concept_dominates(self) -> None:
        self.assertEqual(
            ic._pick_value_column(n_num=0, n_str=0, n_concept=500),
            ("value_as_concept_name", "categorical"),
        )

    def test_all_zero_defaults_to_number(self) -> None:
        self.assertEqual(
            ic._pick_value_column(n_num=0, n_str=0, n_concept=0),
            ("value_as_number", "continuous"),
        )


# --------------------------------------------------------------------------- #
# build_curated_variables — one test per mode
# --------------------------------------------------------------------------- #


@_needs_yaml
class BuildCuratedVariablesTests(unittest.TestCase):
    """Run build_curated_variables with a minimal inventory and stubbed conn."""

    def setUp(self) -> None:
        self.pack = ic.load_pack("mtc_aat")

    def _run(self, columns, router) -> list[dict[str, str]]:
        conn = _stub_conn(router)
        return ic.build_curated_variables(
            conn, self.pack.schema_name, columns, self.pack
        )

    def test_per_concept_picks_value_column_and_continuous_summary(self) -> None:
        """Numeric concept -> value_as_number + continuous distribution."""

        def router(sql, params):
            if 'COUNT("value_as_number")' in sql:
                return {"rows": [("Heart rate", 1200, 1200, 0, 0)]}
            if "PERCENTILE_CONT" in sql:
                return {"row": ("40", "55", "65", "75", "120")}
            return {}

        rows = self._run([_col("observation", "observation_concept_name")], router)
        obs_rows = [r for r in rows if r["Category"] == "Observation"]
        self.assertEqual(len(obs_rows), 1)
        self.assertEqual(obs_rows[0]["Variable"], "Heart rate")
        self.assertEqual(obs_rows[0]["Column(s)"], "value_as_number")
        self.assertIn("Median: 65", obs_rows[0]["Distribution"])
        self.assertEqual(
            obs_rows[0]["Criteria"], "observation_concept_name = 'Heart rate'"
        )

    def test_per_concept_stringy_picks_value_as_string(self) -> None:
        def router(sql, params):
            if 'COUNT("value_as_number")' in sql:
                return {"rows": [("Blood pressure", 1100, 0, 1100, 0)]}
            if "WITH scoped AS" in sql:
                return {"rows": [("128/84", 300, 27.3), ("120/80", 250, 22.7)]}
            return {}

        rows = self._run([_col("observation", "observation_concept_name")], router)
        bp = [r for r in rows if r["Variable"] == "Blood pressure"]
        self.assertEqual(len(bp), 1)
        self.assertEqual(bp[0]["Column(s)"], "value_as_string")
        self.assertIn("128/84: 300", bp[0]["Distribution"])
        self.assertIn("%", bp[0]["Distribution"])

    def test_single_row_with_list_populates_values_cell(self) -> None:
        """condition_occurrence -> exactly one Diagnosis row."""

        def router(sql, params):
            if 'GROUP BY "condition_concept_name"' in sql:
                return {
                    "rows": [
                        ("Alpha-1 antitrypsin deficiency", 900),
                        ("COPD", 700),
                    ]
                }
            return {}

        rows = self._run(
            [_col("condition_occurrence", "condition_concept_name")], router
        )
        diag = [r for r in rows if r["Category"] == "Diagnosis"]
        self.assertEqual(len(diag), 1)
        self.assertEqual(diag[0]["Variable"], "Diagnosis")
        self.assertIn("Alpha-1 antitrypsin deficiency", diag[0]["Values"])
        self.assertIn("COPD", diag[0]["Values"])

    def test_split_by_type_emits_one_row_per_split(self) -> None:
        """drug_exposure -> Prescriptions + Administrations."""
        rows = self._run(
            [_col("drug_exposure", "drug_type_concept_name")],
            lambda sql, params: {},
        )
        variables = {r["Variable"] for r in rows if r["Schema"] == "drug_exposure"}
        self.assertEqual(variables, {"Prescriptions", "Administrations"})

    def test_keep_columns_respects_variable_type(self) -> None:
        """year_of_birth (continuous) gets Min/Q1/Median/Q3/Max; gender (categorical)
        gets top-values-with-pct."""

        def router(sql, params):
            if "PERCENTILE_CONT" in sql:
                return {"row": ("1924", "1948", "1958", "1968", "2002")}
            if "ORDER BY COUNT(*) DESC" in sql:
                return {"rows": [("Female", 620, 62.0), ("Male", 370, 37.0)]}
            return {}

        rows = self._run(
            [
                _col("person", "year_of_birth", data_type="integer"),
                _col(
                    "person",
                    "gender_concept_name",
                    top_values=[("Female", 620)],
                ),
            ],
            router,
        )
        birth = [r for r in rows if r["Variable"] == "BirthYear"]
        sex = [r for r in rows if r["Variable"] == "Sex"]
        self.assertEqual(len(birth), 1)
        self.assertIn("Median: 1958", birth[0]["Distribution"])
        self.assertEqual(len(sex), 1)
        self.assertIn("Female: 620", sex[0]["Distribution"])
        self.assertIn("%", sex[0]["Distribution"])

    def test_static_row_skipped_when_table_absent(self) -> None:
        """Infusion rule exists in the pack but no infusion table in inventory -
        no Infusion Note row should be emitted."""
        rows = self._run(
            [_col("person", "gender_concept_name")], lambda sql, params: {}
        )
        self.assertFalse(
            any(r["Variable"] == "Infusion Note" for r in rows),
            "infusion static row should be skipped when table is absent",
        )

    def test_static_row_emitted_when_table_present(self) -> None:
        rows = self._run(
            [_col("note", "note_text")],
            lambda sql, params: {},
        )
        note = [r for r in rows if r["Variable"] == "Clinical Note"]
        self.assertEqual(len(note), 1)
        self.assertEqual(note[0]["Extraction Type"], "Unstructured")

    def test_per_concept_completeness_uses_picked_column(self) -> None:
        """Heart rate concept fires 1000 times but only value_as_number is
        populated 300 of them. Completeness must reflect the value column
        (30%), not the concept_name column (100%)."""

        def router(sql, params):
            if 'COUNT("value_as_number")' in sql:
                # (name, total, n_num, n_str, n_concept) -> 300/1000 numeric
                return {"rows": [("Heart rate", 1000, 300, 0, 0)]}
            if "PERCENTILE_CONT" in sql:
                return {"row": ("40", "55", "65", "75", "120")}
            return {}

        rows = self._run([_col("observation", "observation_concept_name")], router)
        hr = [r for r in rows if r["Variable"] == "Heart rate"][0]
        self.assertEqual(hr["Completeness"], "30.0%")

    def test_split_by_type_populates_values_and_distribution(self) -> None:
        """The split rows must fill Values + Distribution so the validator
        does not fire missing_value_context on every drug row."""

        def router(sql, params):
            if "WITH scoped AS" in sql:
                # category per-concept top-N (drug_concept_name scoped to
                # a specific drug_type_concept_name)
                return {
                    "rows": [
                        ("Prolastin 1000 MG", 400, 50.0),
                        ("Zemaira 1000 MG", 300, 37.5),
                    ]
                }
            return {}

        rows = self._run(
            [_col("drug_exposure", "drug_type_concept_name")], router
        )
        for drug_row in [r for r in rows if r["Schema"] == "drug_exposure"]:
            self.assertTrue(
                drug_row["Values"] and drug_row["Distribution"],
                f"split row {drug_row['Variable']} should have Values + "
                f"Distribution filled, got {drug_row!r}",
            )
            self.assertIn("Prolastin", drug_row["Values"])

    def test_static_row_has_values_and_distribution(self) -> None:
        """Static rows (notes/documents) must populate Values + Distribution
        so the validator does not warn on every unstructured row."""
        rows = self._run(
            [_col("note", "note_text", row_count=5000)],
            lambda sql, params: {},
        )
        note = [r for r in rows if r["Variable"] == "Clinical Note"][0]
        self.assertTrue(note["Values"])
        self.assertIn("5,000", note["Distribution"])

    def test_static_row_skipped_when_source_column_absent(self) -> None:
        """Table exists, but the recipe's source column (note_text) is not
        in the inventory. The row must NOT be emitted with a fallback
        100% completeness - that would hide schema drift. Expect a
        stderr warning and no Clinical Note row."""
        import contextlib, io

        columns = [
            _col("note", "some_other_column", row_count=5000),
        ]
        conn = _stub_conn(lambda sql, params=None: {})
        err = io.StringIO()
        with contextlib.redirect_stderr(err):
            rows = ic.build_curated_variables(
                conn, self.pack.schema_name, columns, self.pack
            )

        self.assertFalse(
            any(r["Variable"] == "Clinical Note" for r in rows),
            "static row should skip when its source column is absent",
        )
        self.assertIn("skipping static row", err.getvalue())
        self.assertIn("note_text", err.getvalue())

    def test_split_by_type_uses_slice_completeness(self) -> None:
        """Split rows must derive completeness from the displayed value
        column (drug_concept_name) scoped to each drug type, not from
        the group_by column (drug_type_concept_name)."""

        def router(sql, params=None):
            if "WITH scoped AS" in sql:
                return {"rows": [("Prolastin 1000 MG", 400, 80.0)]}
            if "COUNT(*) AS total" in sql and 'COUNT("drug_concept_name")' in sql:
                # 1000 rows matched the drug_type, but only 500 populate
                # drug_concept_name -> 50.0% completeness for this slice
                return {"row": (1000, 500)}
            return {}

        rows = self._run(
            [_col("drug_exposure", "drug_type_concept_name")], router
        )
        for drug_row in [r for r in rows if r["Schema"] == "drug_exposure"]:
            self.assertEqual(drug_row["Completeness"], "50.0%")

    def test_keep_columns_categorical_values_cell_is_full_list(self) -> None:
        """Values for a categorical keep_columns row must show the full
        top-N as a comma-separated list (e.g. "Female, Male"), not only
        the first value."""

        def router(sql, params=None):
            if "ORDER BY COUNT(*) DESC" in sql:
                return {"rows": [
                    ("Female", 620, 62.0),
                    ("Male", 370, 37.0),
                    ("Non-binary", 10, 1.0),
                ]}
            return {}

        rows = self._run(
            [
                _col("person", "gender_concept_name",
                     top_values=[("Female", 620), ("Male", 370), ("Non-binary", 10)])
            ],
            router,
        )
        sex = [r for r in rows if r["Variable"] == "Sex"][0]
        self.assertEqual(sex["Values"], "Female, Male, Non-binary")

    def test_measurement_without_value_as_string_does_not_error(self) -> None:
        """Warehouse variants where ``measurement.value_as_string`` is
        absent must not crash the per_concept path. The generator
        should detect the missing column from the inventory and build
        a COUNT SELECT that skips it, returning n_str = 0."""

        def router(sql, params=None):
            if 'COUNT("value_as_string"' in sql:
                # If the generator still emits the old hardcoded SELECT
                # this branch would fire, which is exactly the bug.
                raise AssertionError(
                    "value_as_string was referenced even though it's not in "
                    "the inventory - schema drift was not respected"
                )
            if 'COUNT("value_as_number")' in sql or 'n_num' in sql:
                # Simulate Postgres returning (name, total, n_num, 0, n_concept)
                return {"rows": [("AAT level", 100, 60, 0, 40)]}
            if "PERCENTILE_CONT" in sql:
                return {"row": ("10", "20", "30", "40", "50")}
            if "WITH scoped AS" in sql:
                return {"rows": [("positive", 30, 50.0), ("negative", 30, 50.0)]}
            return {}

        # Inventory advertises measurement with no value_as_string column.
        columns = [
            _col("measurement", "measurement_concept_name", row_count=100),
            _col("measurement", "value_as_number", row_count=100),
            _col("measurement", "value_as_concept_name", row_count=100),
        ]
        rows = self._run(columns, router)
        labs = [r for r in rows if r["Schema"] == "measurement"]
        self.assertTrue(labs, "measurement rows should still be emitted")

    def test_loinc_style_names_are_sanitized_for_validator(self) -> None:
        """Raw LOINC concept names (with '[', ']', '#', ':', '/') must
        be rewritten so the emitted Variable matches the validator's
        VARIABLE_NAME_PATTERN. The raw name survives elsewhere (it's
        still in the Criteria)."""

        loinc = "C reactive protein [Mass/volume] in Serum or Plasma by High sensitivity method"

        def router(sql, params=None):
            if 'COUNT("value_as_number")' in sql or 'n_num' in sql:
                return {"rows": [(loinc, 100, 100, 0, 0)]}
            if "PERCENTILE_CONT" in sql:
                return {"row": ("0.1", "1.0", "2.0", "5.0", "20.0")}
            return {}

        rows = self._run(
            [_col("measurement", "measurement_concept_name")], router
        )
        lab = [r for r in rows if r["Schema"] == "measurement"][0]
        variable = lab["Variable"]
        # No forbidden characters.
        for ch in "[]#:,{}":
            self.assertNotIn(ch, variable)
        # Validator pattern accepts it.
        import re as _re
        self.assertTrue(
            _re.fullmatch(r"^[A-Za-z][A-Za-z0-9 _/().\-]*$", variable),
            f"sanitized label still does not match VARIABLE_NAME_PATTERN: {variable!r}",
        )
        # Raw LOINC is preserved in Criteria.
        self.assertIn(loinc, lab["Criteria"])

    def test_standard_profile_data_model_is_skipped(self) -> None:
        """PHI-class ingest echo must not appear in the inventory."""
        # Verified indirectly: the pack has it in tables_to_skip.
        pack = ic.load_pack("mtc_aat")
        self.assertIn("standard_profile_data_model", pack.tables_to_skip)

    def test_infusion_recipe_is_absent(self) -> None:
        """The mtc_aat pack should no longer carry an 'infusion' recipe -
        the warehouse table is drug_exposure-shaped, not a notes table."""
        pack = ic.load_pack("mtc_aat")
        self.assertNotIn("infusion", pack.curation_rules)

    def test_per_concept_duplicate_names_get_disambiguated(self) -> None:
        """Two distinct concept rows that normalize to the same Variable
        key must get a ``(table)`` suffix so the validator does not fire
        duplicate_variable on the auto-generated output."""

        def router(sql, params=None):
            if 'COUNT("value_as_number")' in sql:
                # Both tables have a concept that would produce the same
                # Variable label ("Total") if emitted bare.
                table = re.search(r'FROM "[^"]+"\."([^"]+)"', sql).group(1)
                return {"rows": {
                    "observation": [("Total", 100, 100, 0, 0)],
                    "measurement": [("Total", 80, 80, 0, 0)],
                }.get(table, [])}
            if "PERCENTILE_CONT" in sql:
                return {"row": ("1", "2", "3", "4", "5")}
            return {}

        rows = self._run(
            [
                _col("observation", "observation_concept_name"),
                _col("measurement", "measurement_concept_name"),
            ],
            router,
        )
        variables = [r["Variable"] for r in rows]
        # No two variables should normalize to the same key.
        normalized = [ic._normalize_variable_key(v) for v in variables]
        self.assertEqual(
            len(normalized), len(set(normalized)),
            f"duplicate normalized variable name: {variables}",
        )
        # And at least one of the two must carry a table disambiguator.
        self.assertTrue(
            any("(measurement)" in v or "(observation)" in v for v in variables),
            f"expected a (table) suffix on the collision, got {variables}",
        )

    def test_long_concept_names_that_truncate_to_same_prefix(self) -> None:
        """Two concepts whose display names are identical for the first
        61 characters would truncate to the same ``<prefix>...`` string.
        Dedupe must keep them distinct."""

        long_a = "Alpha-1 antitrypsin deficiency with emphysematous phenotype subtype A"
        long_b = "Alpha-1 antitrypsin deficiency with emphysematous phenotype subtype B"

        def router(sql, params=None):
            if 'COUNT("value_as_number")' in sql:
                return {"rows": [(long_a, 50, 50, 0, 0), (long_b, 40, 40, 0, 0)]}
            if "PERCENTILE_CONT" in sql:
                return {"row": ("1", "2", "3", "4", "5")}
            return {}

        rows = self._run([_col("observation", "observation_concept_name")], router)
        normalized = [ic._normalize_variable_key(r["Variable"]) for r in rows]
        self.assertEqual(
            len(normalized), len(set(normalized)),
            f"truncated variable names collide: {[r['Variable'] for r in rows]}",
        )

    def test_missing_dep_error_is_runtimeerror(self) -> None:
        """Lazy imports must raise MissingDependencyError (a subclass of
        RuntimeError), not SystemExit, so unittest reports them as a
        single failed test rather than killing the whole suite."""
        self.assertTrue(issubclass(ic.MissingDependencyError, RuntimeError))

    def test_invalid_cohort_name_exits_cleanly(self) -> None:
        """A wrong --cohort should print a user-facing error and exit 1,
        not print a full traceback."""
        import contextlib, io

        err = io.StringIO()
        with contextlib.redirect_stderr(err):
            code = ic.main(["--cohort", "does_not_exist_xyz"])

        self.assertEqual(code, 1)
        stderr = err.getvalue()
        self.assertIn("cohort pack missing", stderr)
        # Should not be a Python traceback.
        self.assertNotIn("Traceback", stderr)
        # Should hint at --list-cohorts.
        self.assertIn("--list-cohorts", stderr)

    def test_split_by_type_fallback_when_no_rows(self) -> None:
        """If the categorical-for-concept query returns nothing (drug type
        declared in the pack but no rows in data), the split row must
        still carry non-empty Values + Distribution so the validator
        doesn't warn missing_value_context."""

        def router(sql, params):
            if "WITH scoped AS" in sql:
                return {"rows": []}   # simulate zero matching rows
            return {}

        rows = self._run(
            [_col("drug_exposure", "drug_type_concept_name")], router
        )
        for drug_row in [r for r in rows if r["Schema"] == "drug_exposure"]:
            self.assertTrue(
                drug_row["Values"] and drug_row["Distribution"],
                f"empty split row {drug_row['Variable']} should still have "
                f"Values + Distribution filled, got {drug_row!r}",
            )
            self.assertIn("no rows", drug_row["Distribution"])


# --------------------------------------------------------------------------- #
# End-to-end: introspect -> curated XLSX -> validator  (locks the two
# sides of the pipeline together so we notice if one drifts)
# --------------------------------------------------------------------------- #


@_needs_yaml
class EndToEndTests(unittest.TestCase):
    """A curated workbook produced by write_curated_xlsx must validate
    cleanly under validate_dictionary.validate_source."""

    @classmethod
    def setUpClass(cls) -> None:
        cls.tmp = PROJECT_ROOT / "tests" / ".tmp"
        cls.tmp.mkdir(parents=True, exist_ok=True)

    def test_curated_workbook_validates_cleanly(self) -> None:
        import uuid
        import validate_dictionary as vd

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

            # Apply the AAT cohort overlay before validating so that any
            # cohort-specific schemas (infusion, etc.) are recognised.
            vd.apply_profile_overrides(vd.load_cohort_profile("mtc_aat"))
            try:
                result = vd.validate_source(out, verbose=False)
                self.assertEqual("passed", result.status)
                self.assertEqual(0, result.error_count)
            finally:
                # Leave module globals the way the other tests expect them.
                # importlib.reload resets the defaults.
                import importlib
                importlib.reload(vd)

            # The curated workbook must also carry an "All Columns" sheet
            # with one row per introspected column - a flat inventory
            # alongside the curated dictionary. Every row must have a
            # non-empty Schema and Column so the sheet is usable on its
            # own.
            import pandas as pd
            sheets = pd.read_excel(out, sheet_name=None)
            self.assertIn("All Columns", sheets)
            all_cols = sheets["All Columns"]
            self.assertEqual(len(all_cols), len(columns))
            self.assertEqual(
                set(all_cols.columns),
                {"Schema", "Column", "Data Type", "Nullable",
                 "Row Count", "Null Count", "Completeness", "Top Values"},
            )
            self.assertFalse(all_cols["Schema"].isna().any())
            self.assertFalse(all_cols["Column"].isna().any())
        finally:
            out.unlink(missing_ok=True)


@_needs_yaml
class ValidatorProfileOverrideTests(unittest.TestCase):
    """Lock down the merge semantics of apply_profile_overrides."""

    def setUp(self) -> None:
        import importlib
        import validate_dictionary as vd
        importlib.reload(vd)   # fresh defaults for each test
        self.vd = vd

    def tearDown(self) -> None:
        import importlib
        importlib.reload(self.vd)

    def test_list_overlay_appends(self) -> None:
        baseline = list(self.vd.REQUIRED_COLUMNS)
        self.vd.apply_profile_overrides({"required_columns": ["extra_col"]})
        self.assertEqual(
            self.vd.REQUIRED_COLUMNS, baseline + ["extra_col"],
            "required_columns should append, not replace",
        )

    def test_set_overlay_unions(self) -> None:
        baseline = set(self.vd.ALLOWED_EXTRACTION_TYPES)
        self.vd.apply_profile_overrides(
            {"allowed_extraction_types": ["Curated"]}
        )
        self.assertIn("Curated", self.vd.ALLOWED_EXTRACTION_TYPES)
        self.assertTrue(baseline.issubset(self.vd.ALLOWED_EXTRACTION_TYPES))

    def test_dict_overlay_deep_merges(self) -> None:
        self.vd.apply_profile_overrides({
            "column_aliases": {"variable": ["variable_label"]}
        })
        self.assertIn("variable_label", self.vd.COLUMN_ALIASES["variable"])
        self.assertIn(
            "variable", self.vd.COLUMN_ALIASES["variable"],
            "deep-merge must keep base aliases too",
        )

    def test_scalar_overlay_replaces(self) -> None:
        self.vd.apply_profile_overrides({"cohort_name": "another_cohort"})
        self.assertEqual(self.vd.COHORT_NAME, "another_cohort")

    def test_repeated_apply_is_idempotent(self) -> None:
        """Calling apply_profile_overrides twice must not stack.
        The second call's result must equal the single-call result."""
        self.vd.apply_profile_overrides({"required_columns": ["extra_a"]})
        self.vd.apply_profile_overrides({"required_columns": ["extra_b"]})
        self.assertIn("extra_b", self.vd.REQUIRED_COLUMNS)
        self.assertNotIn(
            "extra_a", self.vd.REQUIRED_COLUMNS,
            "the first overlay leaked into the second apply - overrides are not idempotent",
        )

    def test_reset_restores_defaults(self) -> None:
        """reset_profile_overrides() must undo every prior overlay."""
        baseline_cohort = self.vd.COHORT_NAME
        baseline_cols = list(self.vd.REQUIRED_COLUMNS)
        baseline_extraction = set(self.vd.ALLOWED_EXTRACTION_TYPES)

        self.vd.apply_profile_overrides({
            "cohort_name": "x",
            "required_columns": ["extra"],
            "allowed_extraction_types": ["New"],
        })
        self.vd.reset_profile_overrides()

        self.assertEqual(self.vd.COHORT_NAME, baseline_cohort)
        self.assertEqual(self.vd.REQUIRED_COLUMNS, baseline_cols)
        self.assertEqual(self.vd.ALLOWED_EXTRACTION_TYPES, baseline_extraction)


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
