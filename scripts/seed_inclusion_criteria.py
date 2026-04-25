#!/usr/bin/env python3
"""One-shot editorial helper.

Walks every packs/variables/*.yaml, finds variable rows with compound
SQL criteria (containing AND / OR) that lack an explicit
`inclusion_criteria:` key, and writes a templated prose sentence based
on the row's table + variable name. Inserted as a sibling key right
after the `criteria:` block so YAML grouping comments that introduce
the next section stay attached to the next section.

Templates are formulaic on purpose — Flatiron's own copy is highly
formulaic ("Records are included for each lab test of the types listed
in the knowledge center..."). Customer reviewers want predictable,
parallel sentences, not artisanal prose for every row. Authors can
tighten any individual sentence later by editing the YAML directly.

Idempotent: running twice is a no-op because the second pass parses
each file with PyYAML and skips rows that already have an explicit
inclusion_criteria.

Run from repo root:  python scripts/seed_inclusion_criteria.py
"""
from __future__ import annotations

import re
import sys
from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parent.parent
PACKS_DIR = REPO_ROOT / "packs" / "variables"

OR_AND_RE = re.compile(r"\s+(AND|OR)\s+", re.IGNORECASE)


def _is_compound(criteria: str | None) -> bool:
    return bool(criteria) and bool(OR_AND_RE.search(criteria))


# Table -> sentence template. {var} is the variable's display name
# inserted verbatim so domain-specific punctuation stays intact.
_TABLE_TEMPLATES: dict[str, str] = {
    "condition_occurrence":
        "Records where the diagnosis concept matches the {var} family.",
    "drug_exposure":
        "Records where the medication ingredient matches a {var} drug.",
    "measurement":
        "Records where the measurement is for {var}.",
    "procedure_occurrence":
        "Records where the procedure concept matches a {var} entry.",
    "document":
        "Records where the document type matches a {var} report.",
    "infusion":
        "Records of {var} infusions for the patient.",
    "note":
        "Records where the note text describes {var}.",
}


def _observation_template(column: str) -> str:
    if column == "value_as_concept_name":
        return "Records where the observation concept matches {var}."
    if column == "value_as_number":
        return "Records where the observation captures a {var} value."
    if column == "value_as_string":
        return "Records where the observation captures {var}."
    return "Records where the observation matches {var}."


def _template_for(row: dict) -> str:
    table = (row.get("table") or "").strip()
    column = (row.get("column") or "").strip()
    var = (row.get("variable") or column or "this concept").strip()
    extraction = (row.get("extraction_type") or "").strip().lower()

    if extraction == "abstracted":
        return f"Records where the abstracted concept captures {var} for the patient."
    if table == "observation":
        return _observation_template(column).format(var=var)
    if table in _TABLE_TEMPLATES:
        return _TABLE_TEMPLATES[table].format(var=var)
    return f"Records where {table or 'the source table'} captures {var}."


# Match a `criteria:` line. The capture groups expose:
#   (indent, body)
# where indent is the leading whitespace and body is everything after
# `criteria:` (possibly the YAML `>` folded scalar marker).
_CRITERIA_LINE_RE = re.compile(r"^(?P<indent>\s+)criteria:\s*(?P<rest>.*)$")


def _find_criteria_block_end(lines: list[str], start_idx: int,
                             row_indent: str) -> int:
    """Return the index AFTER the last line of the criteria block.

    `start_idx` is the index of the `criteria:` line itself. For a
    single-line criteria (`criteria: x ILIKE 'y'`), the block is just
    that one line. For a folded scalar (`criteria: >` followed by
    indented content), the block includes every subsequent line whose
    indentation is greater than `row_indent`.
    """
    rest = lines[start_idx]
    # Strip the `<indent>criteria:` prefix to see what's after.
    after = rest.split(":", 1)[1].lstrip()
    if not after.startswith(">") and not after.startswith("|"):
        # Single-line criteria.
        return start_idx + 1
    # Folded / literal scalar — consume continuation lines indented
    # more than the row indent.
    i = start_idx + 1
    n = len(lines)
    while i < n:
        line = lines[i]
        if not line.strip():
            # Blank line inside a folded scalar is allowed; check the
            # next non-blank for indentation.
            j = i + 1
            while j < n and not lines[j].strip():
                j += 1
            if j < n and (lines[j].startswith(row_indent + " ")
                          or lines[j].startswith(row_indent + "\t")):
                i = j + 1
                continue
            return i
        # Non-blank line: still part of the scalar if indented past the
        # row indent.
        if line.startswith(row_indent + " ") or line.startswith(row_indent + "\t"):
            i += 1
            continue
        return i
    return i


def _process_file(path: Path) -> int:
    text = path.read_text(encoding="utf-8")
    data = yaml.safe_load(text) or {}
    rows = data.get("variables") or []
    # Build set of (table, variable, criteria_first_clause) for fast
    # match against parsed rows that need an inclusion_criteria.
    needs: dict[tuple[str, str], str] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        crit = row.get("criteria")
        if not _is_compound(crit):
            continue
        if (row.get("inclusion_criteria") or "").strip():
            continue
        key = (
            (row.get("table") or "").strip(),
            (row.get("variable") or row.get("column") or "").strip(),
        )
        needs[key] = _template_for(row)

    if not needs:
        return 0

    lines = text.splitlines(keepends=True)

    # Walk the file, find each row's `criteria:` line, identify which
    # row it belongs to by looking back for `variable:` and `table:`
    # within the same row, and insert `inclusion_criteria:` right
    # after the criteria block.
    out: list[str] = []
    i = 0
    n = len(lines)
    inserted = 0
    while i < n:
        line = lines[i]
        m = _CRITERIA_LINE_RE.match(line)
        if not m:
            out.append(line)
            i += 1
            continue
        row_indent = m.group("indent")
        # Look back within the current row to find table + variable.
        j = i - 1
        seen_table = ""
        seen_variable = ""
        while j >= 0:
            prev = lines[j]
            stripped = prev.lstrip()
            if not prev.strip():
                j -= 1
                continue
            # New list item starts: `<row_indent_minus_2>- key: ...`
            # Stop scanning back when we hit the row's `- ` marker.
            if stripped.startswith("- "):
                # This is the start of the current row.
                # Check `- key: value` for table / variable.
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

        # Compute end of criteria block.
        end_idx = _find_criteria_block_end(lines, i, row_indent)

        # Append the criteria block lines as-is.
        for k in range(i, end_idx):
            out.append(lines[k])

        prose = needs.get((seen_table, seen_variable))
        if prose:
            # Insert the new key with the row's indent.
            # Use a single-line scalar (short prose, fits on one line).
            out.append(f"{row_indent}inclusion_criteria: {prose}\n")
            inserted += 1

        i = end_idx

    if inserted:
        path.write_text("".join(out), encoding="utf-8")
    return inserted


def main() -> int:
    total = 0
    for path in sorted(PACKS_DIR.glob("*.yaml")):
        n = _process_file(path)
        if n:
            print(f"  +{n:3d}  {path.name}", file=sys.stderr)
            total += n
    print(
        f"\nAdded {total} inclusion_criteria entries.",
        file=sys.stderr,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
