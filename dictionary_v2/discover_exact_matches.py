#!/usr/bin/env python3
"""Offline discovery for converting broad variable criteria into
config-owned exact matches.

Reads a cohort's disease pack, runs each variable's existing
`criteria:` (typically ILIKE) against the cohort DB to enumerate the
actually-observed values, and compares those against any configured
`match.values:` exact list. Emits a markdown report of:

  - configured & observed   (exact matches that show up in the data)
  - missing from config     (observed but not yet in `match.values`)
  - stale in config         (in `match.values` but never observed)

This script never modifies the disease YAML. With
`--write-suggestions`, it writes a `*.suggested.yaml` proposal next
to the report, intended for human review.

Usage:
    python dictionary_v2/discover_exact_matches.py --cohort balboa_ckd
    python dictionary_v2/discover_exact_matches.py --cohort balboa_ckd \\
        --variable "Aspirin" --write-suggestions
    python dictionary_v2/discover_exact_matches.py --cohort balboa_ckd \\
        --dry-run   # offline; reports config-only without DB observations
"""
from __future__ import annotations

import argparse
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

_REPO_ROOT_FOR_IMPORTS = str(Path(__file__).resolve().parent.parent)
if _REPO_ROOT_FOR_IMPORTS not in sys.path:
    sys.path.insert(0, _REPO_ROOT_FOR_IMPORTS)

from introspect_cohort import (  # noqa: E402
    _require_psycopg,
    build_conn_kwargs,
    load_dotenv,
)

# Re-use the v2 module's pack loaders + path constants.
import importlib.util as _ilu  # noqa: E402

_V2_PATH = Path(__file__).resolve().parent / "build_dictionary.py"
_spec = _ilu.spec_from_file_location("_bd_for_discovery", _V2_PATH)
_bd = _ilu.module_from_spec(_spec)
sys.modules.setdefault("_bd_for_discovery", _bd)
_spec.loader.exec_module(_bd)


REPO_ROOT = Path(__file__).resolve().parent.parent
PACKS_DIR = REPO_ROOT / "packs"
OUTPUT_DIR = REPO_ROOT / "Output" / "discovery"

load_dotenv(REPO_ROOT / ".env")


@dataclass
class VariableObservation:
    category: str
    variable: str
    table: str
    column: str
    criteria: str
    configured_values: list[str]
    observed: list[tuple[str, int]]   # (value, count) for criteria-matched rows
    error: str = ""

    @property
    def configured_set(self) -> set[str]:
        return set(self.configured_values)

    @property
    def observed_values(self) -> list[str]:
        return [v for v, _ in self.observed]

    @property
    def configured_and_observed(self) -> list[tuple[str, int]]:
        cs = self.configured_set
        return [(v, n) for v, n in self.observed if v in cs]

    @property
    def missing_from_config(self) -> list[tuple[str, int]]:
        cs = self.configured_set
        return [(v, n) for v, n in self.observed if v not in cs]

    @property
    def stale_in_config(self) -> list[str]:
        seen = {v for v, _ in self.observed}
        return [v for v in self.configured_values if v not in seen]


def _pack_for_cohort(cohort: str) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """Return (cohort_pack, variables_list) for a cohort slug.

    Uses the same loader as build_dictionary so `include:` references
    resolve — most cohort variable packs are placeholders that pull
    everything from a shared `<disease>_common` pack.
    """
    cohort_pack = _bd._yaml_load(PACKS_DIR / "cohorts" / f"{cohort}.yaml")
    if not cohort_pack:
        raise SystemExit(f"unknown cohort: {cohort}")
    variables_pack_slug = cohort_pack.get("variables_pack") or ""
    variables_list = _bd.load_variables_pack(variables_pack_slug)
    return cohort_pack, variables_list


def _resolve_configured_values(match_block: dict[str, Any] | None) -> list[str]:
    if not isinstance(match_block, dict):
        return []
    inline = [str(v) for v in (match_block.get("values") or [])]
    file_ref = (match_block.get("values_file") or "").strip()
    if file_ref:
        inline.extend(_bd._load_match_values_file(file_ref))
    seen: set[str] = set()
    out: list[str] = []
    for v in inline:
        if v not in seen:
            seen.add(v)
            out.append(v)
    return out


# Columns that hold values, not clinical concepts. Grouping by these
# yields the value distribution, which is meaningless as a Criteria
# matcher — e.g. Serum Creatinine with column=value_as_number would
# enumerate 1.0, 1.1, 0.9, ... instead of "Creatinine [Mass/volume]
# in Serum or Plasma". `value_as_concept_name` is in here too because
# observation rows store the answer (English / Spanish / ...) there
# while the clinical matcher lives in observation_concept_name.
# Discovery refuses to enumerate these unless `match.column` redirects
# to a real matcher column, or the existing `criteria:` makes the
# matcher column inferrable.
_VALUE_COLUMN_NAMES: frozenset[str] = frozenset({
    "value_as_number", "value_as_string", "value_as_concept_id",
    "value_as_concept_name",
    "value_as_datetime", "value_as_date",
    "range_low", "range_high", "unit_concept_id", "unit_source_value",
    "quantity", "days_supply", "refills",
})


# Match the LHS of the first comparison in a `criteria:` clause when
# it's a *_concept_name column — common shape is
# `observation_concept_name ILIKE '%language%'`. Lets discovery infer
# the right matcher for variables whose `column:` is a value column.
_CRITERIA_LHS_RE = re.compile(
    r"\b([a-z_]+_concept_name)\b\s*(?:ILIKE\b|=|IN\b)",
    re.IGNORECASE,
)


def _infer_matcher_from_criteria(criteria: str) -> str:
    """Return the first `<x>_concept_name` column referenced on the
    LHS of a comparison in `criteria`, or "" if none."""
    if not criteria:
        return ""
    m = _CRITERIA_LHS_RE.search(criteria)
    return m.group(1) if m else ""


def _resolve_matcher_column(v: dict[str, Any]) -> tuple[str, str]:
    """Pick the column to GROUP BY for discovery.

    Returns (matcher_column, reason). Empty matcher means discovery
    should be skipped with `reason` shown in the report.

    Priority:
      1. `match.column` if present — the explicit clinical matcher.
      2. Inference from `criteria:` LHS when that's a `*_concept_name`
         column. Handles e.g. column=value_as_concept_name with
         criteria=observation_concept_name ILIKE '%language%'.
      3. Variable's `column` if it's a non-value column.
      4. Skip with guidance asking for `match.column`.
    """
    match = v.get("match")
    if isinstance(match, dict) and (match.get("column") or "").strip():
        return match["column"].strip(), ""

    criteria = (v.get("criteria") or "").strip()
    inferred = _infer_matcher_from_criteria(criteria)
    column = (v.get("column") or "").strip()
    lowered = column.lower()
    is_value_col = (
        lowered in _VALUE_COLUMN_NAMES
        or lowered.endswith("_date")
        or lowered.endswith("_datetime")
    )

    # If the variable's display column is a value column, only the
    # inferred matcher is safe to group by. Otherwise we'd enumerate
    # values (English / Spanish / 1.0 / 1.1) rather than concepts.
    if is_value_col:
        if inferred:
            return inferred, ""
        return "", (
            f"column `{column}` is a value/date column; configure "
            f"`match.column` (e.g. measurement_concept_name) to enable "
            f"discovery"
        )

    if not column:
        return "", "missing column"

    # Display column is itself a concept-style column. Prefer it,
    # unless the criteria clearly points at a different concept
    # column (defensive — keeps inferred wins when both are present).
    if inferred and inferred != column:
        return inferred, ""
    return column, ""


def _observe_one(conn, schema: str, v: dict[str, Any]) -> VariableObservation:
    """Run the variable's existing criteria against the cohort and dump
    the distinct values it matches with row counts.

    Discovery groups by the *matcher* column (concept name), not the
    variable's display/value column — see _resolve_matcher_column.
    """
    table = v.get("table") or ""
    display_column = v.get("column") or ""
    matcher_column, skip_reason = _resolve_matcher_column(v)
    criteria = (v.get("criteria") or "").strip()
    obs = VariableObservation(
        category=v.get("category") or "",
        variable=v.get("variable") or display_column,
        table=table, column=matcher_column or display_column,
        criteria=criteria,
        configured_values=_resolve_configured_values(v.get("match")),
        observed=[],
    )
    if not table:
        obs.error = "missing table"
        return obs
    if skip_reason:
        obs.error = skip_reason
        return obs

    where = f"({criteria})" if criteria else "TRUE"
    sql = (
        f'SELECT "{matcher_column}"::text, COUNT(*) AS n '
        f'FROM "{schema}"."{table}" '
        f'WHERE {where} AND "{matcher_column}" IS NOT NULL '
        f'GROUP BY "{matcher_column}" '
        f'ORDER BY n DESC;'
    )
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            obs.observed = [(str(r[0]), int(r[1])) for r in cur.fetchall()]
    except Exception as exc:
        obs.error = str(exc)
        try:
            conn.rollback()
        except Exception:
            pass
    return obs


def discover(
    cohort: str, conn, only_variable: str | None = None,
) -> list[VariableObservation]:
    cohort_pack, variables_list = _pack_for_cohort(cohort)
    schema = cohort_pack.get("schema_name") or cohort_pack.get("schema") or cohort
    rows = list(variables_list)
    if only_variable:
        rows = [v for v in rows
                if (v.get("variable") or "").lower() == only_variable.lower()]
        if not rows:
            raise SystemExit(f"no variable named {only_variable!r} in {cohort}")

    observations: list[VariableObservation] = []
    for v in rows:
        observations.append(_observe_one(conn, schema, v))
    return observations


# --------------------------------------------------------------------------- #
# Reporting
# --------------------------------------------------------------------------- #


def _fmt_md(observations: list[VariableObservation], cohort: str) -> str:
    out: list[str] = [f"# Exact-match discovery — {cohort}", ""]
    for o in observations:
        out.append(f"## {o.category} / {o.variable}")
        out.append(f"- table: `{o.table}`")
        out.append(f"- column: `{o.column}`")
        if o.criteria:
            out.append(f"- broad criteria: `{o.criteria}`")
        else:
            out.append(f"- broad criteria: _(none)_")
        if o.error:
            out.append(f"- **error:** {o.error}")
            out.append("")
            continue

        out.append(f"- configured values: {len(o.configured_values)}")
        out.append(f"- observed distinct values: {len(o.observed)}")
        out.append("")

        if o.configured_and_observed:
            out.append("### Configured & observed")
            for val, n in o.configured_and_observed:
                out.append(f"- `{val}`  ({n:,})")
            out.append("")
        if o.missing_from_config:
            out.append("### Observed but NOT in config (candidate additions)")
            for val, n in o.missing_from_config:
                out.append(f"- [ ] `{val}`  ({n:,})")
            out.append("")
        if o.stale_in_config:
            out.append("### In config but NOT observed (candidate removals)")
            for val in o.stale_in_config:
                out.append(f"- [ ] `{val}`")
            out.append("")
    return "\n".join(out).rstrip() + "\n"


def _fmt_suggestions_yaml(
    observations: list[VariableObservation],
) -> str:
    """Per-variable proposed `match:` block, union of configured +
    observed, sorted by frequency desc with config-only rows last.

    This is a proposal for human review — never written into the
    disease pack automatically.
    """
    lines: list[str] = [
        "# Suggested `match:` blocks. Review and copy into",
        "# packs/variables/<disease>.yaml under each variable.",
        "# Generated by dictionary_v2/discover_exact_matches.py.",
        "",
    ]
    for o in observations:
        if not o.observed and not o.configured_values:
            continue
        if o.error:
            continue
        union: list[str] = []
        seen: set[str] = set()
        for val, _ in o.observed:
            if val not in seen:
                seen.add(val)
                union.append(val)
        for val in o.configured_values:
            if val not in seen:
                seen.add(val)
                union.append(val)

        lines.append(f"# {o.category} / {o.variable}")
        lines.append(f"# table={o.table} column={o.column}")
        lines.append(f"variable: {o.variable}")
        lines.append("match:")
        lines.append(f"  column: {o.column}")
        lines.append("  values:")
        for v in union:
            lines.append(f"    - {_yaml_str(v)}")
        lines.append("")
    return "\n".join(lines)


def _yaml_str(s: str) -> str:
    """Quote a string for safe YAML inclusion."""
    if any(c in s for c in (":", "#", "'", '"', "\n")) or s.strip() != s:
        escaped = s.replace('"', '\\"')
        return f'"{escaped}"'
    return s


# --------------------------------------------------------------------------- #
# CLI
# --------------------------------------------------------------------------- #


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--cohort", required=True)
    parser.add_argument("--variable", default=None,
                        help="restrict to one variable name")
    parser.add_argument("--out-dir", default=str(OUTPUT_DIR))
    parser.add_argument("--write-suggestions", action="store_true",
                        help="also emit a *.suggested.yaml proposal file")
    parser.add_argument("--dry-run", action="store_true",
                        help="skip DB; report config-only with no observations")
    args = parser.parse_args(argv)

    out_dir = Path(args.out_dir) / args.cohort
    out_dir.mkdir(parents=True, exist_ok=True)

    if args.dry_run:
        cohort_pack, variables_list = _pack_for_cohort(args.cohort)
        rows = list(variables_list)
        if args.variable:
            rows = [v for v in rows
                    if (v.get("variable") or "").lower() == args.variable.lower()]
        observations = []
        for v in rows:
            matcher, skip = _resolve_matcher_column(v)
            observations.append(VariableObservation(
                category=v.get("category") or "",
                variable=v.get("variable") or v.get("column") or "",
                table=v.get("table") or "",
                column=matcher or v.get("column") or "",
                criteria=(v.get("criteria") or "").strip(),
                configured_values=_resolve_configured_values(v.get("match")),
                observed=[],
                error=skip,
            ))
    else:
        psycopg = _require_psycopg()
        class _NS:
            host = None; port = None; database = None
            user = None; password = None; sslmode = None
        with psycopg.connect(**build_conn_kwargs(_NS())) as conn:
            conn.autocommit = True
            observations = discover(args.cohort, conn, args.variable)

    report_path = out_dir / "report.md"
    report_path.write_text(_fmt_md(observations, args.cohort), encoding="utf-8")
    print(f"Wrote {report_path}", file=sys.stderr)

    if args.write_suggestions:
        suggest_path = out_dir / "suggested.yaml"
        suggest_path.write_text(
            _fmt_suggestions_yaml(observations), encoding="utf-8",
        )
        print(f"Wrote {suggest_path}", file=sys.stderr)
        print(
            f"Review {suggest_path} and copy chosen `match:` blocks "
            f"into packs/variables/<disease>.yaml — nothing was applied "
            f"automatically.",
            file=sys.stderr,
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
