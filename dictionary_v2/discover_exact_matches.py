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

By default this script is read-only. With `--write-suggestions` it
writes a `*.suggested.yaml` proposal next to the report — still no
changes to disease YAML. With `--apply` (after an explicit y/N
confirmation, or `--apply-yes` for scripted runs) it can inject the
proposed `match:` blocks into pack files; `--target` is required and
controls whether writes go to the cohort's own pack or the shared
source pack the variable was inherited from.

Usage:
    # Read-only report.
    python dictionary_v2/discover_exact_matches.py --cohort balboa_ckd

    # Read-only report + suggested.yaml proposal.
    python dictionary_v2/discover_exact_matches.py --cohort balboa_ckd \\
        --variable "Aspirin" --write-suggestions

    # Offline preview, no DB.
    python dictionary_v2/discover_exact_matches.py --cohort balboa_ckd \\
        --dry-run

    # Write match: blocks into the cohort's own pack only (safer).
    python dictionary_v2/discover_exact_matches.py --cohort balboa_ckd \\
        --apply --target cohort

    # Write match: blocks into each variable's source pack (touches
    # files that other cohorts include — only use for clinically
    # universal values).
    python dictionary_v2/discover_exact_matches.py --cohort balboa_ckd \\
        --apply --target shared
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
    criteria: str                     # strict / displayed criteria (`match:` if any, else broad)
    configured_values: list[str]
    observed: list[tuple[str, int]]   # (value, count) for scope-matched rows
    error: str = ""
    source_pack: str = ""              # disease/cohort slug the variable lives in
    discovery_scope: str = ""          # the WHERE actually used for the GROUP BY query
                                       # — usually the broad criteria when both
                                       # are present, so reviewers can see how
                                       # missing_from_config was derived

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


def _load_variables_pack_tagged(slug: str) -> list[dict[str, Any]]:
    """Mirror build_dictionary.load_variables_pack but tag each
    variable with `_source_pack` (the YAML slug it was defined in).

    Reviewers need that provenance when copying suggested `match:`
    blocks back into packs/variables/, so cohort-specific values
    don't accidentally land in shared <disease>_common files.
    """
    if not slug:
        return []
    path = PACKS_DIR / "variables" / f"{slug}.yaml"
    if not path.is_file():
        sys.stderr.write(f"[warn] variables pack not found: {path}\n")
        return []
    data = _bd._yaml_load(path)
    out: list[dict[str, Any]] = []
    for inc in data.get("include") or []:
        out.extend(_load_variables_pack_tagged(inc))
    for v in (data.get("variables") or []):
        tagged = dict(v)
        tagged["_source_pack"] = slug
        out.append(tagged)
    return out


def _pack_for_cohort(cohort: str) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """Return (cohort_pack, variables_list) for a cohort slug.

    Uses a tagged copy of the build's pack loader so each variable
    carries its source pack slug — needed so the suggestions report
    can tell reviewers which file a candidate match block belongs
    in (per-cohort vs shared <disease>_common).
    """
    cohort_pack = _bd._yaml_load(PACKS_DIR / "cohorts" / f"{cohort}.yaml")
    if not cohort_pack:
        raise SystemExit(f"unknown cohort: {cohort}")
    variables_pack_slug = cohort_pack.get("variables_pack") or ""
    variables_list = _load_variables_pack_tagged(variables_pack_slug)
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


def _resolve_scope(
    v: dict[str, Any],
) -> tuple[str, str, str, str]:
    """Compute the discovery scope for a variable.

    Returns (matcher_column, scope_sql, displayed_criteria, error).
    Empty `error` means the row is ready to query; non-empty means
    discovery should skip with that reason. This is the single source
    of truth shared by both the live `_observe_one()` query path and
    the offline `--dry-run` reporter, so they can't drift on which
    rows are skipped or why.

    Skip reasons (mirrors the live build's logic):
      - `_resolve_matcher_column()` refused to pick a matcher (value
        column with no inference, missing column, etc.).
      - The variable has neither `criteria:` nor `match:`. WHERE TRUE
        would enumerate every concept in the table.
    """
    matcher_column, matcher_skip = _resolve_matcher_column(v)
    raw_criteria = (v.get("criteria") or "").strip()
    match_sql = _bd.compile_match_block(v.get("match"))
    # Drift detection: discovery's WHERE prefers the broad `criteria:`
    # so the report can flag observed values that aren't in the curated
    # `match.values` list yet. Using `match:` here would only enumerate
    # values that are *already* configured, making missing_from_config
    # impossible by construction. Match-only rows (no criteria) fall
    # back to the match SQL so they're still scoped to the curated set.
    scope_sql = raw_criteria or match_sql
    # Displayed criteria remains strict-when-available — what the
    # dictionary's Criteria cell shows. The build path uses the same
    # ordering for consistency.
    displayed_criteria = match_sql or raw_criteria

    if not (v.get("table") or "").strip():
        return matcher_column, scope_sql, displayed_criteria, "missing table"
    if matcher_skip:
        return matcher_column, scope_sql, displayed_criteria, matcher_skip
    if not scope_sql:
        return matcher_column, scope_sql, displayed_criteria, (
            "no `criteria:` or `match:` block configured; cannot "
            "scope discovery without redefining the variable"
        )
    return matcher_column, scope_sql, displayed_criteria, ""


def _build_observation(
    v: dict[str, Any],
    matcher_column: str,
    displayed_criteria: str,
    error: str,
    discovery_scope: str = "",
) -> VariableObservation:
    """Construct a VariableObservation header (no observed rows yet).

    Shared between live discovery and dry-run so both paths stamp
    the same fields and skip-reasons.
    """
    display_column = v.get("column") or ""
    return VariableObservation(
        category=v.get("category") or "",
        variable=v.get("variable") or display_column,
        table=v.get("table") or "",
        column=matcher_column or display_column,
        criteria=displayed_criteria,
        configured_values=_resolve_configured_values(v.get("match")),
        observed=[],
        error=error,
        source_pack=v.get("_source_pack") or "",
        discovery_scope=discovery_scope,
    )


def _observe_one(conn, schema: str, v: dict[str, Any]) -> VariableObservation:
    """Run the variable's existing scope against the cohort and dump
    the distinct values it matches with row counts.

    Discovery groups by the *matcher* column (concept name), not the
    variable's display/value column — see _resolve_matcher_column.
    Scope rules live in _resolve_scope() and are shared with dry-run.
    """
    matcher_column, scope_sql, displayed_criteria, error = _resolve_scope(v)
    obs = _build_observation(
        v, matcher_column, displayed_criteria, error,
        discovery_scope=scope_sql,
    )
    if error:
        return obs

    sql = (
        f'SELECT "{matcher_column}"::text, COUNT(*) AS n '
        f'FROM "{schema}"."{obs.table}" '
        f'WHERE ({scope_sql}) AND "{matcher_column}" IS NOT NULL '
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
        if o.source_pack:
            out.append(
                f"- source pack: `packs/variables/{o.source_pack}.yaml`"
            )
        out.append(f"- table: `{o.table}`")
        out.append(f"- column: `{o.column}`")
        # The "criteria" field is only strict when it actually came
        # from a `match:` block (i.e. it differs from the broader
        # discovery_scope). Otherwise it's the same fuzzy ILIKE
        # used to find candidates — labelling that as "strict" would
        # mislead reviewers into thinking the variable has already
        # been converted to exact matches.
        has_strict_criteria = (
            bool(o.criteria) and o.criteria != o.discovery_scope
        )
        if has_strict_criteria:
            out.append(f"- displayed criteria (strict): `{o.criteria}`")
            out.append(f"- discovery scope (broad):    `{o.discovery_scope}`")
        elif o.criteria:
            out.append(f"- criteria: `{o.criteria}`")
            out.append(
                f"- discovery scope: same as criteria "
                f"(no `match:` block configured yet)"
            )
        else:
            out.append(f"- criteria: _(none)_")
            if o.discovery_scope:
                out.append(f"- discovery scope: `{o.discovery_scope}`")
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
    observations: list[VariableObservation], cohort: str = "",
) -> str:
    """Per-variable proposed `match:` block, union of configured +
    observed, sorted by frequency desc with config-only rows last.

    This is a proposal for human review — never written into the
    disease pack automatically. Each block is annotated with the
    source pack so reviewers know whether to update the cohort's
    own variables file or the shared <disease>_common pack.
    """
    lines: list[str] = [
        "# Suggested `match:` blocks. Review and copy into the",
        "# source pack noted under each variable.",
        "#",
        "# IMPORTANT placement guidance:",
        "#   - If the proposed values are clinically appropriate for",
        "#     EVERY cohort that includes the source pack, paste into",
        "#     the listed shared <disease>_common.yaml file.",
        "#   - If the values are cohort-specific (e.g. one provider's",
        "#     local concept names), instead paste into the per-cohort",
        f"#     pack (packs/variables/{cohort or '<cohort>'}.yaml) so",
        "#     the shared common pack stays portable.",
        "#",
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
        if o.source_pack:
            lines.append(
                f"# source pack: packs/variables/{o.source_pack}.yaml"
            )
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
# `--apply`: round-trip the source pack file and inject/update the
# `match:` block on each variable. Uses ruamel.yaml when available so
# comments and key ordering survive; falls back to refusing the apply
# rather than silently destroying the file.
# --------------------------------------------------------------------------- #


def _eligible_for_apply(o: VariableObservation) -> bool:
    """Only variables whose live discovery produced observations are
    eligible. No observations → nothing to apply (and skipped/error
    rows must never be written into packs)."""
    return bool(o.observed) and not o.error and bool(o.source_pack)


def _suggested_values_for(o: VariableObservation) -> list[str]:
    """Union of observed (frequency-ordered) + currently-configured
    values. Mirrors the suggestions YAML's ordering."""
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
    return union


def _confirm(prompt: str) -> bool:
    """y/N prompt. Returns False on EOF / non-tty so scripted runs
    without --apply-yes default to safe."""
    try:
        return input(prompt).strip().lower() in ("y", "yes")
    except EOFError:
        return False


def _load_source_variable(
    source_pack: str, variable_name: str, yaml_rt: Any = None,
) -> Any:
    """Read a variable's full definition straight from its source pack.

    Returns the raw mapping (table/column/criteria/description/etc.)
    or None if the pack or variable can't be found. Bypasses
    load_variables_pack so we get the row exactly as authored,
    without internal tags or include flattening.

    If `yaml_rt` is a ruamel YAML instance, the returned mapping is
    a CommentedMap that can carry leading comments via
    yaml_set_comment_before_after_key — needed so --auto-stub
    annotates cohort packs with a real YAML comment instead of
    persisting an `_auto_stub_origin` field. Without yaml_rt the
    function falls back to pyyaml plain-dict loading (used by
    callers that just want the field values).
    """
    path = PACKS_DIR / "variables" / f"{source_pack}.yaml"
    if not path.is_file():
        return None

    if yaml_rt is not None:
        with path.open("r", encoding="utf-8") as f:
            data = yaml_rt.load(f)
    else:
        data = _bd._yaml_load(path)
    if not isinstance(data, dict):
        return None

    for row in data.get("variables") or []:
        if (row.get("variable") or "").strip() == variable_name:
            if yaml_rt is not None:
                # Deep-copy so the caller's mutations don't leak into
                # any later read of the source pack within the same run.
                import copy
                clone = copy.deepcopy(row)
                # Strip our in-memory provenance tag if it somehow
                # leaked in. The disk copy never has it; defensive.
                for k in [k for k in list(clone.keys()) if isinstance(k, str) and k.startswith("_")]:
                    del clone[k]
                return clone
            return {k: v for k, v in row.items() if not k.startswith("_")}
    return None


def _attach_stub_comment(row: Any, source_pack: str) -> None:
    """Add a leading comment on a newly-stubbed cohort variable so
    later readers can see this row was auto-copied from a shared
    pack and ought to be reviewed before shipping. Best-effort:
    silently no-ops if ruamel's comment API isn't reachable on
    this row type (e.g. plain dict in tests).
    """
    msg = (
        f"Auto-stubbed from packs/variables/{source_pack}.yaml via "
        f"discover_exact_matches.py --auto-stub. Verify clinical "
        f"fit before shipping."
    )
    try:
        first_key = next(iter(row.keys()))
        row.yaml_set_comment_before_after_key(
            first_key, before=msg, indent=4,
        )
    except (AttributeError, StopIteration):
        # Plain dict (in unit tests with stub data) — leave a marker
        # field instead so the test can still assert provenance.
        row.setdefault("_auto_stub_origin", source_pack)


def _ask_per_variable(
    action: str,
    obs: VariableObservation,
    dest_path: Path,
) -> str:
    """Prompt for one variable. Returns 'y', 'n', 'all', or 'q'.

    Renders a structured block showing source / target / action /
    reason so the reviewer can see at a glance whether they're
    updating an existing row or copying a shared row into the
    cohort pack as a per-cohort override.

    `action` is "update" (existing row's match block changes) or
    "stub" (a new cohort-override row is being added from source).
    """
    if action == "update":
        action_label = "UPDATE variable"
        reason = (
            "row already exists in target pack; only the match: "
            "block will change"
        )
    else:
        action_label = "ADD cohort override"
        reason = (
            "row is inherited from shared pack; discovered values "
            "came from one cohort only"
        )

    sample = ", ".join(f'"{v}"' for v, _ in obs.observed[:2])
    if len(obs.observed) > 2:
        sample += ", …"

    block = (
        f"\n  Variable: {obs.category} / {obs.variable}\n"
        f"  Source:   packs/variables/{obs.source_pack}.yaml\n"
        f"  Target:   packs/variables/{dest_path.stem}.yaml\n"
        f"  Action:   {action_label}\n"
        f"  Values:   {len(obs.observed)} ({sample})\n"
        f"  Reason:   {reason}\n"
        f"  Proceed?  [y]es / [n]o / [a]ll-remaining / [q]uit: "
    )
    try:
        raw = input(block).strip().lower()
    except EOFError:
        return "n"
    if raw in ("y", "yes"):
        return "y"
    if raw in ("a", "all"):
        return "all"
    if raw in ("q", "quit"):
        return "q"
    return "n"


def apply_suggestions(
    observations: list[VariableObservation],
    target: str,
    cohort_slug: str | None = None,
    auto_yes: bool = False,
    auto_stub: bool = False,
) -> tuple[int, int]:
    """Interactively (or with auto_yes) inject `match:` blocks back
    into pack files.

    `target` must be one of:
      - "shared":  write to the variable's source pack (e.g. the
                   shared ckd_common.yaml). Use only when the proposed
                   values are clinically appropriate for every cohort
                   that includes the source pack.
      - "cohort":  write to the cohort's own pack
                   (packs/variables/<cohort_slug>.yaml). Variables
                   that don't already exist in that file are skipped
                   with a message unless `auto_stub=True`, in which
                   case the variable's full definition is copied from
                   its source pack into the cohort pack first, then
                   the match block is attached. Shared packs are never
                   modified.

    `auto_stub` is opt-in and only valid with target='cohort'. It
    NEVER writes to shared packs.

    Interactive mode (auto_yes=False) prompts per variable with
    [update]/[stub] labels so each row is approved individually.
    `auto_yes=True` skips prompts entirely.

    Returns (applied, skipped). Refuses to write if ruamel.yaml is
    not installed, since pyyaml round-trip would destroy comments.
    """
    if target not in ("shared", "cohort"):
        raise ValueError(f"target must be 'shared' or 'cohort', got {target!r}")
    if target == "cohort" and not cohort_slug:
        raise ValueError("target='cohort' requires cohort_slug")
    if auto_stub and target != "cohort":
        raise ValueError("auto_stub=True requires target='cohort'")

    eligible = [o for o in observations if _eligible_for_apply(o)]
    if not eligible:
        print("[apply] nothing eligible to apply.", file=sys.stderr)
        return (0, 0)

    try:
        from ruamel.yaml import YAML  # type: ignore
    except ImportError:
        print(
            "[apply] ruamel.yaml is not installed — refusing to write "
            "to packs because pyyaml round-trip would destroy comments. "
            "Install with: pip install ruamel.yaml",
            file=sys.stderr,
        )
        return (0, len(eligible))

    yaml_rt = YAML(typ="rt")
    yaml_rt.preserve_quotes = True
    yaml_rt.width = 4096   # don't reflow long IN list lines

    # Lazy-load each pack file at most once and keep a single mutable
    # in-memory copy. Writes happen at the end so a `quit` mid-loop
    # leaves untouched files on disk.
    pack_cache: dict[Path, Any] = {}
    touched_paths: set[Path] = set()

    def _load_pack(path: Path) -> Any:
        if path not in pack_cache:
            with path.open("r", encoding="utf-8") as f:
                pack_cache[path] = yaml_rt.load(f)
        return pack_cache[path]

    # Source-pack lookups for auto-stub. Cached per source-slug.
    # Loaded via the same yaml_rt as the destination so the returned
    # row is a CommentedMap — required for the leading provenance
    # comment on stubbed entries.
    source_var_cache: dict[tuple[str, str], Any] = {}

    def _source_def(source_pack: str, variable: str) -> Any:
        key = (source_pack, variable)
        if key not in source_var_cache:
            source_var_cache[key] = _load_source_variable(
                source_pack, variable, yaml_rt=yaml_rt,
            )
        return source_var_cache[key]

    print(
        f"[apply] {len(eligible)} candidate(s) — target={target}"
        + (" auto-stub=on" if auto_stub else "")
        + (" (auto-yes)" if auto_yes else " (per-variable prompts)"),
        file=sys.stderr,
    )

    applied = 0
    skipped = 0
    accept_all = False   # set when user picks 'all' to skip remaining prompts

    for o in eligible:
        dest_pack = cohort_slug if target == "cohort" else o.source_pack
        dest_path = PACKS_DIR / "variables" / f"{dest_pack}.yaml"
        if not dest_path.is_file():
            print(
                f"[apply] {dest_path} not found; skipping {o.variable}",
                file=sys.stderr,
            )
            skipped += 1
            continue

        data = _load_pack(dest_path)
        if not isinstance(data, dict):
            print(
                f"[apply] {dest_path}: top-level YAML is not a mapping; "
                f"skipping {o.variable}",
                file=sys.stderr,
            )
            skipped += 1
            continue
        # Make sure variables: exists AND points at a mutable
        # container we own. `data.get('variables') or []` would
        # return a detached fresh list when the YAML had `[]`,
        # which silently swallows append()s.
        if data.get("variables") is None:
            data["variables"] = []
        rows = data["variables"]
        rows_by_name = {(r.get("variable") or "").strip(): r for r in rows}
        existing = rows_by_name.get(o.variable)

        # Decide what the action is: update (mutate existing row),
        # stub (auto-copy from source pack), or skip.
        if existing is not None:
            action = "update"
        elif target == "cohort" and auto_stub:
            src = _source_def(o.source_pack, o.variable)
            if src is None:
                print(
                    f"[apply] auto-stub: source definition for "
                    f"{o.variable!r} not found in "
                    f"packs/variables/{o.source_pack}.yaml; skipping",
                    file=sys.stderr,
                )
                skipped += 1
                continue
            action = "stub"
        else:
            if target == "cohort":
                print(
                    f"[apply] target=cohort: variable {o.variable!r} "
                    f"is not defined in {dest_path.name}. Pass "
                    f"--auto-stub to copy its base definition from "
                    f"packs/variables/{o.source_pack}.yaml; skipping.",
                    file=sys.stderr,
                )
            else:
                print(
                    f"[apply] {dest_pack}: variable {o.variable!r} not "
                    f"found in {dest_path.name} (likely lives in a "
                    f"different included pack); skipping",
                    file=sys.stderr,
                )
            skipped += 1
            continue

        # Per-variable confirmation, unless auto-yes / accept-all.
        if not auto_yes and not accept_all:
            answer = _ask_per_variable(action, o, dest_path)
            if answer == "q":
                print(
                    "[apply] quit — pending changes discarded; "
                    "no files written.",
                    file=sys.stderr,
                )
                return (0, len(eligible))
            if answer == "all":
                accept_all = True
            elif answer != "y":
                skipped += 1
                continue

        # Mutate the in-memory pack.
        if action == "update":
            existing["match"] = {
                "column": o.column,
                "values": _suggested_values_for(o),
            }
        else:   # stub: copy the source row (CommentedMap) into the cohort pack
            new_row = _source_def(o.source_pack, o.variable)
            # _source_def already returned a deep-copy, but cache it
            # away so the next iteration (if any) reads a fresh one.
            source_var_cache.pop((o.source_pack, o.variable), None)
            new_row["match"] = {
                "column": o.column,
                "values": _suggested_values_for(o),
            }
            rows.append(new_row)
            _attach_stub_comment(rows[-1], o.source_pack)

        touched_paths.add(dest_path)
        applied += 1

    # Write all touched files at the end so a `quit` or error
    # mid-loop never half-applies. Each file write is atomic at the
    # filesystem level: dump to a sibling temp file, fsync, then
    # os.replace() onto the destination — that way a crash, disk
    # error, or interruption mid-dump leaves the original pack
    # intact rather than truncated.
    import os
    import tempfile
    for path in touched_paths:
        tmp_fd, tmp_name = tempfile.mkstemp(
            prefix=f"{path.stem}.", suffix=".yaml.tmp", dir=str(path.parent),
        )
        tmp_path = Path(tmp_name)
        try:
            with os.fdopen(tmp_fd, "w", encoding="utf-8") as f:
                yaml_rt.dump(pack_cache[path], f)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp_path, path)
        except Exception:
            # Best-effort cleanup of the temp file; never leak it.
            try:
                tmp_path.unlink()
            except OSError:
                pass
            raise
        print(f"[apply] updated {path}", file=sys.stderr)

    print(
        f"[apply] applied {applied} match block(s); skipped {skipped}",
        file=sys.stderr,
    )
    return (applied, skipped)


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
    parser.add_argument("--apply", action="store_true",
                        help="after writing the report, prompt to write "
                             "match: blocks into pack files (requires "
                             "--target; ruamel.yaml required)")
    parser.add_argument("--apply-yes", action="store_true",
                        help="implies --apply with no interactive prompt; "
                             "for scripted use")
    parser.add_argument("--target", choices=("cohort", "shared"),
                        default=None,
                        help="where --apply writes match: blocks. "
                             "`cohort` writes to packs/variables/<cohort>.yaml "
                             "and skips variables that don't already live "
                             "there (safe default for per-cohort work). "
                             "`shared` writes to each variable's source "
                             "pack — only use when the values are clinically "
                             "appropriate for every cohort that includes it.")
    parser.add_argument("--auto-stub", action="store_true",
                        help="when --target cohort encounters a variable "
                             "that doesn't yet live in the cohort pack, "
                             "copy its full base definition from the "
                             "source pack into the cohort pack first, "
                             "then attach the match: block. Shared packs "
                             "are never modified. Only valid with "
                             "--target cohort.")
    parser.add_argument("--dry-run", action="store_true",
                        help="skip DB; report config-only with no observations")
    args = parser.parse_args(argv)

    # Validate --apply / --auto-stub contracts before any DB work or
    # report writing — a live run shouldn't spend time querying the
    # warehouse only to discover the CLI args were incomplete.
    if (args.apply or args.apply_yes) and not args.target:
        print(
            "[apply] --apply requires --target {cohort|shared}. "
            "Pick `cohort` to write to packs/variables/"
            f"{args.cohort}.yaml only (safer), or `shared` to "
            "write to each variable's source pack (touches files "
            "that other cohorts include).",
            file=sys.stderr,
        )
        return 2

    if args.auto_stub and args.target != "cohort":
        print(
            "[apply] --auto-stub is only valid with --target cohort. "
            "It would otherwise write inferred definitions into "
            "shared packs, which the safety contract forbids.",
            file=sys.stderr,
        )
        return 2

    out_dir = Path(args.out_dir) / args.cohort
    out_dir.mkdir(parents=True, exist_ok=True)

    if args.dry_run:
        cohort_pack, variables_list = _pack_for_cohort(args.cohort)
        rows = list(variables_list)
        if args.variable:
            rows = [v for v in rows
                    if (v.get("variable") or "").lower() == args.variable.lower()]
        # Reuse the live path's scope-resolution helper so dry-run
        # previews show the same skip reasons (no `criteria:` /
        # `match:`, value-column matcher, etc.) the live discovery
        # would emit. Keeps offline review honest.
        observations = []
        for v in rows:
            matcher, scope, displayed, error = _resolve_scope(v)
            observations.append(
                _build_observation(
                    v, matcher, displayed, error,
                    discovery_scope=scope,
                )
            )
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
            _fmt_suggestions_yaml(observations, cohort=args.cohort),
            encoding="utf-8",
        )
        print(f"Wrote {suggest_path}", file=sys.stderr)
        print(
            f"Review {suggest_path} — each block names its source "
            f"pack. Paste into that file if the values are shared, "
            f"or into packs/variables/{args.cohort}.yaml if they're "
            f"cohort-specific. Nothing was applied automatically.",
            file=sys.stderr,
        )

    if args.apply or args.apply_yes:
        # --target was already validated up front (see top of main()),
        # so by this point we know it's set and DB work / report
        # writing has succeeded.
        apply_suggestions(
            observations,
            target=args.target,
            cohort_slug=args.cohort,
            auto_yes=args.apply_yes,
            auto_stub=args.auto_stub,
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
