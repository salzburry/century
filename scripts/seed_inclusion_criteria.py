#!/usr/bin/env python3
"""One-shot editorial helper.

Walks every packs/variables/*.yaml, finds variable rows with compound
SQL criteria (containing AND / OR) that lack an explicit
`inclusion_criteria:` key, and writes a templated prose sentence based
on the row's table + variable name. Inserted as a sibling key right
after the `criteria:` block so YAML grouping comments that introduce
the next section stay attached to the next section.

Templates avoid the SQL-translation patterns reviewers flagged ("matches
the X family", "matches a Y drug") — they read as clinical statements
about what kind of record is included, not as code descriptions.

Idempotent by default: rows that already carry an inclusion_criteria
are left alone. Pass `--rewrite` to re-seed any auto-generated prose
(detected by matching the legacy template patterns); hand-written
prose is preserved either way.

Run from repo root:
  python scripts/seed_inclusion_criteria.py
  python scripts/seed_inclusion_criteria.py --rewrite
"""
from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parent.parent
PACKS_DIR = REPO_ROOT / "packs" / "variables"

OR_AND_RE = re.compile(r"\s+(AND|OR)\s+", re.IGNORECASE)


def _is_compound(criteria: str | None) -> bool:
    return bool(criteria) and bool(OR_AND_RE.search(criteria))


# Strip trailing helper words / parenthetical suffixes from a variable
# name so the rendered prose doesn't double up the source-table hint
# ("Spirometry / PFT (Procedure)" + "procedures performed for…" →
# "Spirometry / PFT procedures performed for…"). Conservative — only
# drops a single trailing parenthetical or a known suffix word, never
# a parenthetical embedded in the middle of the name.
_TRAILING_NOISE = ("Diagnosis",)
_TRAILING_PARENS_NOISE = re.compile(
    r"\s*\((?:Procedure|Document|Report|Therapy|Test)\)\s*$",
    re.IGNORECASE,
)


def _clean_variable(var: str) -> str:
    var = var.strip()
    var = _TRAILING_PARENS_NOISE.sub("", var).strip()
    for noise in _TRAILING_NOISE:
        if var.endswith(" " + noise):
            return var[: -len(noise)].rstrip()
    return var


# `Document (X)` → `X` so the document template doesn't render
# "Document (MRI / PET / EEG) documents". A trailing "report" or
# "documents" word inside the parens is dropped so the rendered
# template doesn't double up either ("PFT report" + "reports filed
# for the patient" → "PFT reports filed for the patient"). Falls back
# to the cleaned variable when no parenthetical content is present.
_PAREN_CONTENT_RE = re.compile(r"\(([^)]+)\)")
_DOC_TRAILING_RE = re.compile(
    r"\s+(?:reports?|documents?)\b\s*$", re.IGNORECASE,
)


def _document_subject(var: str) -> str:
    m = _PAREN_CONTENT_RE.search(var)
    if m:
        inner = m.group(1).strip()
        return _DOC_TRAILING_RE.sub("", inner).strip()
    return _clean_variable(var)


def _condition_template(var: str) -> str:
    return f"Records of patients with a recorded diagnosis of {_clean_variable(var)}."


def _drug_template(var: str) -> str:
    return f"Records of patients receiving {_clean_variable(var)}."


def _measurement_template(var: str) -> str:
    return f"Records of {_clean_variable(var)} measurements for the patient."


def _procedure_template(var: str) -> str:
    return f"Records of {_clean_variable(var)} procedures performed for the patient."


def _document_template(var: str) -> str:
    return f"Records of {_document_subject(var)} reports filed for the patient."


def _infusion_template(var: str) -> str:
    return f"Records of {_clean_variable(var)} infusion episodes for the patient."


def _note_template(var: str) -> str:
    return f"Records where the clinical note describes {_clean_variable(var)}."


def _abstracted_template(var: str) -> str:
    return f"Records where an abstracted {_clean_variable(var)} event is captured for the patient."


def _observation_template(column: str, var: str) -> str:
    cleaned = _clean_variable(var)
    if column == "value_as_concept_name":
        return f"Records of {cleaned} captured as a structured observation."
    if column == "value_as_number":
        return f"Records of {cleaned} measurements for the patient."
    if column == "value_as_string":
        return f"Records of {cleaned} captured as observation text."
    return f"Records of {cleaned} observations for the patient."


def _template_for(row: dict) -> str:
    table = (row.get("table") or "").strip()
    column = (row.get("column") or "").strip()
    var = (row.get("variable") or column or "this concept").strip()
    extraction = (row.get("extraction_type") or "").strip().lower()

    if extraction == "abstracted":
        return _abstracted_template(var)
    if table == "observation":
        return _observation_template(column, var)
    if table == "condition_occurrence":
        return _condition_template(var)
    if table == "drug_exposure":
        return _drug_template(var)
    if table == "measurement":
        return _measurement_template(var)
    if table == "procedure_occurrence":
        return _procedure_template(var)
    if table == "document":
        return _document_template(var)
    if table == "infusion":
        return _infusion_template(var)
    if table == "note":
        return _note_template(var)
    return f"Records of {var} captured in {table or 'the source table'} for the patient."


# Patterns that identify previously auto-seeded prose. If
# `--rewrite` is set, any inclusion_criteria matching one of these is
# treated as overwritable; everything else (hand-written prose) is
# preserved.
#
# Two generations of templates listed here:
#   1. Original "matches the X / matches a Y" SQL-translation shapes
#      (the regression that triggered the rewrite).
#   2. Current "Records of … for the patient" shapes — listed so a
#      future template tweak can re-seed cleanly without losing
#      hand-written prose. Hand-written rows are recognised by NOT
#      matching any of these.
_LEGACY_PATTERNS: list[re.Pattern[str]] = [
    # Generation 1 — SQL translation.
    re.compile(r"^Records where the diagnosis concept matches the .+ family\.$"),
    re.compile(r"^Records where the medication ingredient matches an? .+ drug\.$"),
    re.compile(r"^Records where the measurement is for .+\.$"),
    re.compile(r"^Records where the procedure concept matches an? .+ entry\.$"),
    re.compile(r"^Records where the observation concept matches .+\.$"),
    re.compile(r"^Records where the observation captures an? .+ value\.$"),
    re.compile(r"^Records where the observation captures .+\.$"),
    re.compile(r"^Records where the document type matches an? .+ report\.$"),
    re.compile(r"^Records of .+ infusions for the patient\.$"),
    re.compile(r"^Records where the note text describes .+\.$"),
    re.compile(r"^Records where the abstracted concept captures .+ for the patient\.$"),
    # Generation 2 — current "Records of …" shapes.
    re.compile(r"^Records of patients with a recorded diagnosis of .+\.$"),
    re.compile(r"^Records of patients receiving .+\.$"),
    re.compile(r"^Records of .+ measurements for the patient\.$"),
    re.compile(r"^Records of .+ procedures performed for the patient\.$"),
    re.compile(r"^Records of .+ documents attached to the patient encounter\.$"),
    re.compile(r"^Records of .+ reports filed for the patient\.$"),
    re.compile(r"^Records of .+ infusion episodes for the patient\.$"),
    re.compile(r"^Records of .+ captured as a structured observation\.$"),
    re.compile(r"^Records of .+ captured as observation text\.$"),
    re.compile(r"^Records of .+ observations for the patient\.$"),
    re.compile(r"^Records where the clinical note describes .+\.$"),
    re.compile(
        r"^Records where an abstracted .+ event is captured for the patient\.$"
    ),
]


def _is_legacy_auto_seed(text: str) -> bool:
    return any(pat.match(text) for pat in _LEGACY_PATTERNS)


# Match a `criteria:` line. The capture groups expose:
#   (indent, body)
_CRITERIA_LINE_RE = re.compile(r"^(?P<indent>\s+)criteria:\s*(?P<rest>.*)$")
_INCLUSION_LINE_RE = re.compile(r"^\s+inclusion_criteria:\s*(?P<rest>.*)$")


def _find_block_end(lines: list[str], start_idx: int, row_indent: str) -> int:
    """End index of a YAML scalar (single-line or folded) starting at start_idx."""
    rest = lines[start_idx]
    after = rest.split(":", 1)[1].lstrip()
    if not after.startswith(">") and not after.startswith("|"):
        return start_idx + 1
    i = start_idx + 1
    n = len(lines)
    while i < n:
        line = lines[i]
        if not line.strip():
            j = i + 1
            while j < n and not lines[j].strip():
                j += 1
            if j < n and (lines[j].startswith(row_indent + " ")
                          or lines[j].startswith(row_indent + "\t")):
                i = j + 1
                continue
            return i
        if line.startswith(row_indent + " ") or line.startswith(row_indent + "\t"):
            i += 1
            continue
        return i
    return i


def _process_file(path: Path, rewrite: bool) -> int:
    text = path.read_text(encoding="utf-8")
    data = yaml.safe_load(text) or {}
    rows = data.get("variables") or []

    # Build per-row plan: either INSERT a new key or REPLACE existing prose.
    # Keyed on (table, variable) so the line walk can match without
    # reparsing.
    plan: dict[tuple[str, str], tuple[str, str]] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        crit = row.get("criteria")
        existing = (row.get("inclusion_criteria") or "").strip()
        key = (
            (row.get("table") or "").strip(),
            (row.get("variable") or row.get("column") or "").strip(),
        )
        if existing:
            if rewrite and _is_legacy_auto_seed(existing):
                plan[key] = ("REPLACE", _template_for(row))
            continue
        if not _is_compound(crit):
            continue
        plan[key] = ("INSERT", _template_for(row))

    if not plan:
        return 0

    lines = text.splitlines(keepends=True)
    out: list[str] = []
    i = 0
    n = len(lines)
    written = 0

    while i < n:
        line = lines[i]
        m_crit = _CRITERIA_LINE_RE.match(line)
        m_inc = _INCLUSION_LINE_RE.match(line)

        if m_crit:
            row_indent = m_crit.group("indent")
            seen_table = ""
            seen_variable = ""
            j = i - 1
            while j >= 0:
                prev = lines[j]
                stripped = prev.lstrip()
                if not prev.strip():
                    j -= 1
                    continue
                if stripped.startswith("- "):
                    kv = stripped[2:]
                    if kv.startswith("table:"):
                        seen_table = kv.split(":", 1)[1].strip()
                    elif kv.startswith("variable:"):
                        seen_variable = kv.split(":", 1)[1].strip()
                    break
                if stripped.startswith("table:") and not seen_table:
                    seen_table = stripped.split(":", 1)[1].strip()
                elif stripped.startswith("variable:") and not seen_variable:
                    seen_variable = stripped.split(":", 1)[1].strip()
                j -= 1

            end_idx = _find_block_end(lines, i, row_indent)
            for k in range(i, end_idx):
                out.append(lines[k])

            entry = plan.get((seen_table, seen_variable))
            if entry and entry[0] == "INSERT":
                out.append(f"{row_indent}inclusion_criteria: {entry[1]}\n")
                written += 1
            i = end_idx
            continue

        if m_inc and rewrite:
            # Identify which row this line belongs to and check the plan.
            row_indent = line[: len(line) - len(line.lstrip())]
            seen_table = ""
            seen_variable = ""
            j = i - 1
            while j >= 0:
                prev = lines[j]
                stripped = prev.lstrip()
                if not prev.strip():
                    j -= 1
                    continue
                if stripped.startswith("- "):
                    kv = stripped[2:]
                    if kv.startswith("table:"):
                        seen_table = kv.split(":", 1)[1].strip()
                    elif kv.startswith("variable:"):
                        seen_variable = kv.split(":", 1)[1].strip()
                    break
                if stripped.startswith("table:") and not seen_table:
                    seen_table = stripped.split(":", 1)[1].strip()
                elif stripped.startswith("variable:") and not seen_variable:
                    seen_variable = stripped.split(":", 1)[1].strip()
                j -= 1
            entry = plan.get((seen_table, seen_variable))
            end_idx = _find_block_end(lines, i, row_indent)
            if entry and entry[0] == "REPLACE":
                out.append(f"{row_indent}inclusion_criteria: {entry[1]}\n")
                written += 1
            else:
                for k in range(i, end_idx):
                    out.append(lines[k])
            i = end_idx
            continue

        out.append(line)
        i += 1

    if written:
        path.write_text("".join(out), encoding="utf-8")
    return written


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--rewrite", action="store_true",
        help="Re-seed any inclusion_criteria that matches a known "
             "auto-generated template (preserves hand-written prose).",
    )
    args = parser.parse_args(argv)

    total = 0
    for path in sorted(PACKS_DIR.glob("*.yaml")):
        n = _process_file(path, rewrite=args.rewrite)
        if n:
            verb = "rewrote" if args.rewrite else "added"
            print(f"  {verb} {n:3d}  {path.name}", file=sys.stderr)
            total += n
    print(
        f"\nTotal {('rewritten' if args.rewrite else 'added')}: {total}.",
        file=sys.stderr,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
