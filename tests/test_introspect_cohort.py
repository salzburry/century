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


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
