#!/usr/bin/env python3
"""Validate packs/*.yaml and write VALIDATION_REPORT.md.

What this catches:

  - A cohort pack pointing at a missing variables pack.
  - An `include:` chain that references a non-existent pack.
  - Duplicate (Category, Variable) pairs within one cohort — usually
    the sign that two inherited packs redundantly redeclare a row.
  - Category labels on a variable row that don't appear in
    packs/categories.yaml (keeps the Columns and Variables pages
    using the same Category vocabulary).
  - Potentially unsafe ILIKE patterns:
      * prefix-only (`ILIKE 'ARIA%'` — misses `MRI finding - ARIA-H`)
      * no wildcards at all (`ILIKE 'ARIA'` — exact match masquerading
        as a fuzzy match)
  - Variable rows missing a required field (table / column).
  - Catch-all variable rows that have a named clinical concept but
    no `criteria:` (would silently summarise every non-null row —
    the "Other Laboratory Measurements" shape the reviewer flagged).
  - Customer-visible prose (description / notes) that leaks pack
    mechanics, cohort short names, or SQL fragments. See
    packs/STYLE.md for the full denylist. Currently warnings; will
    promote to errors under --strict once the editorial pass lands.

What it does NOT catch:
  - Whether a criteria actually finds data in a live warehouse
    (that's `build_dictionary.py`'s job at runtime).
  - Column-level PII correctness against the schema.

Exit code is 0 when the report is written; the number of findings is
echoed on stderr. Use `--strict` to exit non-zero when any findings
exist.

Usage:
  python scripts/validate_packs.py
  python scripts/validate_packs.py --strict
  python scripts/validate_packs.py --out custom/path.md
"""
from __future__ import annotations

import argparse
import re
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parent.parent
PACKS_DIR = REPO_ROOT / "packs"
DEFAULT_OUT = REPO_ROOT / "VALIDATION_REPORT.md"


def _yaml_load(path: Path) -> dict[str, Any]:
    import yaml
    if not path.is_file():
        return {}
    return yaml.safe_load(path.read_text(encoding="utf-8")) or {}


@dataclass
class Finding:
    severity: str          # "error" | "warning"
    cohort: str
    message: str


@dataclass
class CohortReport:
    slug: str
    provider: str
    disease: str
    variables_pack: str
    variables: list[dict[str, Any]] = field(default_factory=list)
    findings: list[Finding] = field(default_factory=list)

    @property
    def category_counts(self) -> dict[str, int]:
        out: dict[str, int] = {}
        for v in self.variables:
            cat = v.get("category", "")
            out[cat] = out.get(cat, 0) + 1
        return out


# --------------------------------------------------------------------------- #
# Helpers — shared with build_dictionary.load_variables_pack but standalone
# so the validator has no dependency on psycopg / pandas.
# --------------------------------------------------------------------------- #


def _resolve_variables(disease_slug: str, seen: set[str] | None = None,
                       findings: list[str] | None = None
                       ) -> list[dict[str, Any]]:
    """Transitive include resolution. Logs missing packs into `findings`.

    Uses the same override semantics as
    build_dictionary.load_variables_pack(): a local row replaces an
    inherited row with the same (category, variable) key. Without
    this, a cohort pack that legitimately overrides a shared
    variable (e.g. an --auto-stub'd row carrying a strict match:
    block) would be flagged as a duplicate variable here even
    though the build correctly treats it as an override.
    """
    seen = seen or set()
    findings = findings if findings is not None else []
    if disease_slug in seen:
        findings.append(f"circular include detected: {disease_slug}")
        return []
    seen.add(disease_slug)
    path = PACKS_DIR / "variables" / f"{disease_slug}.yaml"
    if not path.is_file():
        findings.append(f"variables pack not found: {path}")
        return []
    data = _yaml_load(path)

    out: list[dict[str, Any]] = []
    for inc in data.get("include") or []:
        out.extend(_resolve_variables(inc, seen, findings))

    def _key(r: dict[str, Any]) -> tuple[str, str]:
        return (
            (r.get("category") or "").strip(),
            (r.get("variable") or "").strip(),
        )

    index_by_key: dict[tuple[str, str], int] = {
        _key(r): i for i, r in enumerate(out)
    }
    for local_row in (data.get("variables") or []):
        key = _key(local_row)
        if key in index_by_key:
            out[index_by_key[key]] = local_row
        else:
            index_by_key[key] = len(out)
            out.append(local_row)
    return out


def _load_known_categories() -> set[str]:
    """Allowed category labels = table-keyed `categories:` union with
    the Page-4-only `variable_only_categories:` list in categories.yaml."""
    data = _yaml_load(PACKS_DIR / "categories.yaml")
    table_keyed = set((data.get("categories") or {}).keys())
    var_only = set(data.get("variable_only_categories") or [])
    return table_keyed | var_only


# --------------------------------------------------------------------------- #
# Checks
# --------------------------------------------------------------------------- #


def _check_unsafe_ilike(criteria: str) -> list[str]:
    """Return human-readable warnings for each unsafe ILIKE in `criteria`."""
    warnings: list[str] = []
    # Catch: ILIKE '<word>%'  — prefix only, no leading %
    # Catch: ILIKE '<word>'   — literal match
    for m in re.finditer(r"ILIKE\s*'([^']*)'", criteria, re.IGNORECASE):
        pat = m.group(1)
        if "%" not in pat:
            warnings.append(f"exact-match pattern ILIKE '{pat}' (no wildcards)")
        elif pat.endswith("%") and not pat.startswith("%"):
            # prefix-only is often a miss; allow it but flag
            warnings.append(f"prefix-only pattern ILIKE '{pat}' (consider '%...%')")
    return warnings


# Variable names that strongly imply a specific clinical filter. If the pack
# author wrote the variable "APOE Genotype" but forgot a criteria, the row
# silently counts every measurement.value_as_concept_name — which is not
# what the variable name promises.
_CONCEPT_SPECIFIC_NAME_PATTERNS = [
    re.compile(p, re.IGNORECASE) for p in (
        r"APOE", r"amyloid", r"tau", r"GFAP", r"neurofilament",
        r"MoCA", r"MMSE", r"FAQ", r"CDR", r"ADAS", r"Dementia Severity",
        r"ARIA", r"PET", r"MRI", r"EEG", r"blood pressure", r"heart rate",
    )
]


def _has_scope(v: dict[str, Any]) -> bool:
    """A variable is scoped if it has any matcher — free-form
    `criteria:` (legacy) OR a structured `match:` block (the v2
    schema's strict-IN form). Both checks below previously keyed
    off only `criteria:`, which means a future match-only row
    would have looked unscoped and triggered false warnings.
    """
    if (v.get("criteria") or "").strip():
        return True
    match = v.get("match")
    if isinstance(match, dict):
        if (match.get("values") or []) or (match.get("values_file") or "").strip():
            return True
    return False


def _check_missing_criteria(v: dict[str, Any]) -> str | None:
    """Flag clinically-specific variable rows that lack a scope."""
    if _has_scope(v):
        return None
    name = v.get("variable") or ""
    for p in _CONCEPT_SPECIFIC_NAME_PATTERNS:
        if p.search(name):
            return f"{name!r} has no criteria/match — would summarise every {v.get('column','?')} row"
    return None


_ID_COLUMN_RE = re.compile(r"(?:^|_)(concept_id|id)$", re.IGNORECASE)
_ID_NAME_RE = re.compile(r"\b(id|concept\s*id|identifier)\b", re.IGNORECASE)


# Customer-visible prose denylist. See packs/STYLE.md.
#
# Each entry is (compiled_regex, human_readable_label). Labels appear
# in the validation report so authors know which rule fired.
#
# Word-boundary anchored where the term might also appear as a
# substring of an unrelated word ("MTC" lights up inside many strings;
# pinning it with \b avoids false positives on, e.g., "MTC Practice
# Network" if that ever appears in legitimate copy).
_PROSE_DENYLIST: list[tuple[re.Pattern[str], str]] = [
    # Pack file references — internal-only vocabulary.
    (re.compile(r"\b(adrd|aat|alzheimers|respiratory|copd|asthma|ckd|"
                r"retinal|dr|amd|mash|ibd)_common\b", re.IGNORECASE),
     "pack-file reference"),

    # Cohort short-name leakage. Standalone uppercase tokens used as
    # tags ("MTC AAT", "RMN Alzheimer's"). Customer copy should name
    # the disease, not our internal cohort slug.
    (re.compile(r"\b(MTC|RMN)\b"),
     "cohort short-name (MTC / RMN)"),

    # Pack-mechanics phrases.
    (re.compile(r"\bcohort[- ]defining\b", re.IGNORECASE),
     "pack mechanics: 'cohort-defining'"),
    (re.compile(r"\bcaptured\s+in\b", re.IGNORECASE),
     "pack mechanics: 'captured in'"),
    (re.compile(r"\binherit(s|ed|ing)?\s+(from|by)\b", re.IGNORECASE),
     "pack mechanics: 'inherits from / inherited by'"),
    (re.compile(r"\bowned\s+here\b", re.IGNORECASE),
     "pack mechanics: 'owned here'"),
    (re.compile(r"\bredacted\s+by\b", re.IGNORECASE),
     "pack mechanics: 'redacted by'"),
    (re.compile(r"\b(technical|sales|pharma)\s+audience\b", re.IGNORECASE),
     "audience tag in customer copy"),
    (re.compile(r"\bfor\s+both\b", re.IGNORECASE),
     "pack mechanics: 'for both' (cohort-cross-reference)"),
    (re.compile(r"\bsurface\s+it\b", re.IGNORECASE),
     "pack mechanics: 'surface it'"),

    # SQL fragments. Spaces around FROM/JOIN avoid false positives on
    # English "from" / "join". ILIKE/SELECT are unambiguous.
    (re.compile(r"\bILIKE\b"), "SQL fragment: ILIKE"),
    (re.compile(r"\bSELECT\b"), "SQL fragment: SELECT"),
    (re.compile(r"\sJOIN\s", re.IGNORECASE), "SQL fragment: JOIN"),
    (re.compile(r"\sFROM\s+\w+_\w+", re.IGNORECASE),
     "SQL fragment: FROM <table>"),

    # Generator vocabulary leaking into prose.
    (re.compile(r"\bextraction_type\b"), "generator key 'extraction_type'"),
    (re.compile(r"\bvalue_as_concept_name\b"),
     "generator column 'value_as_concept_name'"),

    # QA / editing rationale leaking into customer notes. These read
    # like internal review chatter rather than clinical caveats and
    # should move to YAML comments instead.
    (re.compile(r"\bbanned\s+wildcard", re.IGNORECASE),
     "QA language: 'banned wildcard' (move to YAML comment)"),
    (re.compile(r"\bcriteria\s+restricted", re.IGNORECASE),
     "QA language: 'criteria restricted' (move to YAML comment)"),
    (re.compile(r"\bearlier\s+draft", re.IGNORECASE),
     "QA language: 'earlier draft' (move to YAML comment)"),
    (re.compile(r"\bdeliberately\b", re.IGNORECASE),
     "QA language: 'deliberately' (move rationale to YAML comment)"),
    (re.compile(r"\bTODO\b"),
     "QA language: 'TODO' (move to YAML comment)"),
    (re.compile(r"\bAND-qualifier\b", re.IGNORECASE),
     "QA language: 'AND-qualifier' (move SQL rationale to YAML comment)"),
    (re.compile(r"\bdouble-?count\w*", re.IGNORECASE),
     "QA language: 'double count' (rephrase as a clinical caveat)"),

    # Schema / generator vocabulary in backticks. OMOP physical-table
    # and column names are implementation detail; customer copy should
    # name the clinical concept instead.
    (re.compile(r"`(value_as_(string|number|concept_name|datetime)|"
                r"\w*_concept_(name|id)|drug_(concept_id|exposure)|"
                r"condition_occurrence|procedure_occurrence|"
                r"observation|measurement|note_text|drug_concept_name)`"),
     "backticked schema field in customer copy "
     "(rephrase using the clinical concept)"),

    # Markdown emphasis is a generator artefact; customer prose should
    # not carry **bold** or *italic* runs.
    (re.compile(r"\*\*[^*]+\*\*"),
     "markdown emphasis '**...**' (drop or rephrase)"),

    # Colon-form Implemented label. The earlier rule caught only
    # `Implemented = No`; the colon form leaks via copy/paste from
    # generator output.
    (re.compile(r"\bImplemented\s*[:=]\s*(Yes|No)\b", re.IGNORECASE),
     "generator field 'Implemented: Yes/No' in customer copy"),

    # Prose-quality patterns. Catch the two failure modes the most
    # recent review surfaced — article + vowel mismatches and the
    # mechanical "matches the X family / matches a X drug" templates.
    # All warnings; labels make the fix obvious.
    #
    # Article-vowel rule excludes `u` and `h` deliberately: pronunciation
    # depends on the next sound, not the spelling. "a unit" / "a urea" /
    # "a useful" / "a university" are correct because the /j/-glide
    # sounds like a consonant; "an hour" / "an honor" are correct because
    # the h is silent. Catching only a/e/i/o flags the real mistakes
    # ("a Anti-amyloid", "a Erythropoiesis", "a Oxygen", "a Inhaled")
    # without false-positiving on legitimate /j/-prefix or silent-h words.
    (re.compile(r"\ba\s+[AEIOaeio]"),
     "article-vowel mismatch ('a' before a/e/i/o-initial word — "
     "usually should be 'an')"),
    (re.compile(r"\bmatches\s+the\s+.+?\s+family\b", re.IGNORECASE),
     "generic template: 'matches the X family' (rewrite as a clinical "
     "definition; see packs/STYLE.md)"),
    (re.compile(r"\bmatches\s+an?\s+\w+.*\s+(drug|entry|report)\b",
                re.IGNORECASE),
     "auto-translated SQL phrasing ('matches a X drug / entry / report'); "
     "rewrite as a clinical definition"),
]


def _check_prose_quality(text: str) -> list[str]:
    """Return human-readable warnings for each denylist hit in `text`."""
    if not text:
        return []
    warnings: list[str] = []
    for pat, label in _PROSE_DENYLIST:
        if pat.search(text):
            warnings.append(label)
    return warnings


def _check_id_column_name_mismatch(v: dict[str, Any]) -> str | None:
    """Catch the Infusion-Drug-style mistake — variable named as a
    business-facing concept (`Drug`, `Diagnosis`) but the column is
    an opaque ID (`drug_concept_id`, `condition_concept_id`).

    Allows the pairing when the variable name itself contains "ID" /
    "Concept ID" / "Identifier", signalling the author intends the
    row to render opaque identifiers."""
    column = (v.get("column") or "").strip()
    expression = (v.get("expression") or "").strip()
    if expression:
        # Expression-backed rows are opting into whatever the SQL
        # returns; we can't second-guess the type from here.
        return None
    if not column:
        return None
    if not _ID_COLUMN_RE.search(column):
        return None
    name = v.get("variable") or ""
    if _ID_NAME_RE.search(name):
        return None
    return (
        f"{name!r} points at ID column {column!r} but the variable "
        f"name doesn't signal that. Rename to '{name} (Concept ID)' "
        f"or resolve to a concept_name via `expression:`."
    )


def validate_cohort(slug: str, known_categories: set[str]) -> CohortReport:
    cohort_path = PACKS_DIR / "cohorts" / f"{slug}.yaml"
    data = _yaml_load(cohort_path)
    report = CohortReport(
        slug=slug,
        provider=str(data.get("provider") or "?"),
        disease=str(data.get("disease") or "?"),
        variables_pack=str(data.get("variables_pack") or ""),
    )

    for field_name in ("provider", "disease", "schema_name", "cohort_name"):
        if not data.get(field_name):
            report.findings.append(Finding(
                "error", slug, f"cohort pack missing required field: {field_name}"
            ))

    if not report.variables_pack:
        report.findings.append(Finding(
            "warning", slug,
            "cohort pack has no `variables_pack` — Page 4 will be empty",
        ))
        return report

    include_issues: list[str] = []
    variables = _resolve_variables(report.variables_pack, findings=include_issues)
    for msg in include_issues:
        report.findings.append(Finding("error", slug, msg))

    report.variables = variables

    # Duplicate (category, variable) detection
    seen: dict[tuple[str, str], int] = {}
    for v in variables:
        key = (v.get("category", ""), v.get("variable", ""))
        seen[key] = seen.get(key, 0) + 1
    for (cat, var), n in seen.items():
        if n > 1:
            report.findings.append(Finding(
                "error", slug,
                f"duplicate variable: {cat}/{var} appears {n} times",
            ))

    # Per-row checks
    for v in variables:
        cat = v.get("category", "")
        var = v.get("variable", v.get("column", "?"))

        # Missing required fields
        if not v.get("table"):
            report.findings.append(Finding(
                "error", slug, f"{cat}/{var}: missing `table`"
            ))
        if not v.get("column"):
            report.findings.append(Finding(
                "error", slug, f"{cat}/{var}: missing `column`"
            ))

        # Category recognised
        if cat and cat not in known_categories:
            report.findings.append(Finding(
                "warning", slug,
                f"{cat}/{var}: category {cat!r} not in packs/categories.yaml",
            ))

        # ILIKE sanity
        criteria = v.get("criteria") or ""
        for msg in _check_unsafe_ilike(criteria):
            report.findings.append(Finding(
                "warning", slug, f"{cat}/{var}: {msg}",
            ))

        # Specific-sounding name with no criteria
        catch_all = _check_missing_criteria(v)
        if catch_all:
            report.findings.append(Finding(
                "warning", slug, f"{cat}/{var}: {catch_all}",
            ))

        # ID column rendered under a non-ID-named variable
        id_mismatch = _check_id_column_name_mismatch(v)
        if id_mismatch:
            report.findings.append(Finding(
                "warning", slug, f"{cat}/{var}: {id_mismatch}",
            ))

        # Prose quality: customer-visible strings against the STYLE.md
        # denylist. inclusion_criteria is also rendered to customers
        # (and is the only prose sales / pharma see for compound-SQL
        # rows), so it gets the same treatment as description / notes.
        for prose_field in ("description", "notes", "inclusion_criteria"):
            text = v.get(prose_field) or ""
            for label in _check_prose_quality(text):
                report.findings.append(Finding(
                    "warning", slug,
                    f"{cat}/{var}: {prose_field} hits style denylist — {label}",
                ))

        # Any criteria-bearing row (compound OR single-clause) needs
        # The inclusion-prose guard fires when a row has any scope —
        # legacy `criteria:` OR a structured `match:` block. Both
        # render as a non-blank Criteria cell that needs paired
        # human-readable prose. The friendly auto-translation
        # ("Records where the observation concept matches 'language'.")
        # was removed because it produced QA-style copy rather than
        # Flatiron-style clinical prose; without explicit prose,
        # derive_inclusion_criteria returns empty and the rendered
        # workbook ships a blank Inclusion Criteria cell.
        if _has_scope(v) and not (v.get("inclusion_criteria") or "").strip():
            report.findings.append(Finding(
                "error", slug,
                f"{cat}/{var}: criteria/match has no `inclusion_criteria:` "
                f"prose — sales / pharma / customer audiences would see a "
                f"blank Inclusion Criteria cell. Add a one-sentence row-"
                f"inclusion description to the pack row.",
            ))

    return report


# --------------------------------------------------------------------------- #
# Report rendering
# --------------------------------------------------------------------------- #


def render_report(reports: list[CohortReport]) -> str:
    lines: list[str] = []
    lines.append("# Pack validation report")
    lines.append("")
    lines.append(
        "Generated by `scripts/validate_packs.py`. One section per cohort "
        "pack under `packs/cohorts/*.yaml`. Findings are split into "
        "`error` (blocks the cohort from producing a valid dictionary) "
        "and `warning` (output still renders but should be reviewed)."
    )
    lines.append("")

    # Summary table
    lines.append("## Summary")
    lines.append("")
    lines.append("| Cohort | Provider | Disease | Variables pack | Variables | Errors | Warnings |")
    lines.append("|---|---|---|---|---:|---:|---:|")
    total_err = 0
    total_warn = 0
    for r in reports:
        errs = sum(1 for f in r.findings if f.severity == "error")
        warns = sum(1 for f in r.findings if f.severity == "warning")
        total_err += errs
        total_warn += warns
        lines.append(
            f"| {r.slug} | {r.provider} | {r.disease} | "
            f"{r.variables_pack or '—'} | {len(r.variables)} | "
            f"{errs} | {warns} |"
        )
    lines.append("")
    lines.append(
        f"**Totals:** {total_err} error(s), {total_warn} warning(s) "
        f"across {len(reports)} cohort(s)."
    )
    lines.append("")

    # Per-cohort detail
    for r in reports:
        lines.append(f"## {r.slug}")
        lines.append("")
        lines.append(
            f"- Provider: **{r.provider}**  "
            f"- Disease: **{r.disease}**  "
            f"- Variables pack: `{r.variables_pack}`  "
            f"- Total variables: {len(r.variables)}"
        )
        lines.append("")

        # Category breakdown
        counts = r.category_counts
        if counts:
            lines.append("**Variables by category:**")
            lines.append("")
            lines.append("| Category | Count |")
            lines.append("|---|---:|")
            for cat, n in sorted(counts.items()):
                lines.append(f"| {cat or '(blank)'} | {n} |")
            lines.append("")

        # Findings
        errs = [f for f in r.findings if f.severity == "error"]
        warns = [f for f in r.findings if f.severity == "warning"]
        if not r.findings:
            lines.append("_No findings — pack is clean._")
        else:
            if errs:
                lines.append("**Errors:**")
                lines.append("")
                for f in errs:
                    lines.append(f"- ❌ {f.message}")
                lines.append("")
            if warns:
                lines.append("**Warnings:**")
                lines.append("")
                for f in warns:
                    lines.append(f"- ⚠ {f.message}")
                lines.append("")

        # Full variable list (table-keyed so reviewers can spot overlap)
        if r.variables:
            lines.append("<details><summary>All variables</summary>")
            lines.append("")
            lines.append("| Category | Variable | Table | Column | Criteria? |")
            lines.append("|---|---|---|---|---|")
            for v in r.variables:
                lines.append(
                    f"| {v.get('category','')} "
                    f"| {v.get('variable', v.get('column',''))} "
                    f"| {v.get('table','')} "
                    f"| {v.get('column','')} "
                    f"| {'Yes' if v.get('criteria') else 'No'} |"
                )
            lines.append("")
            lines.append("</details>")
            lines.append("")

    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", default=str(DEFAULT_OUT))
    parser.add_argument("--strict", action="store_true",
                        help="exit non-zero if any findings are present")
    args = parser.parse_args(argv)

    known_categories = _load_known_categories()
    cohort_files = sorted((PACKS_DIR / "cohorts").glob("*.yaml"))
    reports = [validate_cohort(p.stem, known_categories) for p in cohort_files]

    out_path = Path(args.out)
    out_path.write_text(render_report(reports), encoding="utf-8")

    total_err = sum(f.severity == "error" for r in reports for f in r.findings)
    total_warn = sum(f.severity == "warning" for r in reports for f in r.findings)
    print(
        f"Wrote {out_path}  ({len(reports)} cohort(s), "
        f"{total_err} error(s), {total_warn} warning(s))",
        file=sys.stderr,
    )

    if args.strict and (total_err or total_warn):
        return 1
    if total_err:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
