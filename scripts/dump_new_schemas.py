#!/usr/bin/env python3
"""Raw schema dumps for the five backlog cohorts that don't have a
raw PDF under Output/ yet.

Runs `introspect_cohort.py --schema <name>` for each cohort listed
below and writes Summary + Tables + Columns output (xlsx + html) into
  Output/raw/<schema>/<schema>.xlsx
  Output/raw/<schema>/<schema>.html

No pack is required — the existing `--schema` mode in
introspect_cohort.py synthesises a lightweight Pack on the fly and
walks the schema directly. That's exactly what we want for
discovery: look at a cohort's real table shape, concept-name
distributions, and row counts BEFORE committing to a pack design.

Once you have the dumps, mine them the way Nimbus / Balboa / DRG
were mined (grep the concept_name distributions, identify the
disease-defining diagnosis family, spot the standard-of-care
medications actually present, note the cohort-specific abstraction
tables) and then add the matching <disease>_common + per-cohort
packs to packs/variables/ + packs/cohorts/.

Usage:
    python3 scripts/dump_new_schemas.py
    # or from the repo root:
    py scripts/dump_new_schemas.py

Requirements:
    - requirements.txt installed (psycopg, pandas, openpyxl, pyyaml)
    - .env populated with PG* credentials pointing at a warehouse
      that actually has the schemas below

Behaviour:
    - Any schema missing on the warehouse logs a `[skip]` line and
      the loop continues to the next cohort, so one missing schema
      doesn't block the other four.
    - Exit code is 1 if ANY schema failed, 0 if all succeeded —
      useful for CI / wrapper scripts that want to know whether
      the full sweep landed.
"""
from __future__ import annotations

import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent

# Backlog cohorts without raw PDFs (README §2.1 rows 11, 12, 15, 18
# plus RVC AMD). Schema names match the best-guess on-warehouse
# layout; edit here if the live warehouse ships them under different
# names.
SCHEMAS: list[str] = [
    "rmn_alzheimers_cohort",
    "newtown_mash_cohort",
    "newtown_ibd_cohort",
    "rvc_dr_curated",
    "rvc_amd_curated",
]


def _dump_one(schema: str, out_base: Path) -> int:
    """Run introspect_cohort.py --schema <name> for one schema.
    Returns the subprocess exit code (0 = success)."""
    out_dir = out_base / schema
    out_dir.mkdir(parents=True, exist_ok=True)
    cmd = [
        sys.executable,
        str(REPO_ROOT / "introspect_cohort.py"),
        "--schema", schema,
        "--out-xlsx", str(out_dir / f"{schema}.xlsx"),
        "--out-html", str(out_dir / f"{schema}.html"),
    ]
    print(f"==> {schema}", flush=True)
    return subprocess.call(cmd, cwd=str(REPO_ROOT))


def main() -> int:
    out_base = REPO_ROOT / "Output" / "raw"
    out_base.mkdir(parents=True, exist_ok=True)

    failures: list[str] = []
    for schema in SCHEMAS:
        rc = _dump_one(schema, out_base)
        if rc != 0:
            failures.append(schema)
            print(
                f"    [skip] {schema}: introspect_cohort exited {rc}. "
                f"If the schema isn't provisioned yet, that's expected.",
                file=sys.stderr,
            )

    print("", file=sys.stderr)
    if failures:
        print(
            f"{len(failures)}/{len(SCHEMAS)} schema(s) failed: {failures}.\n"
            f"Most likely cause: the schema isn't on the warehouse yet.\n"
            f"Check with:\n"
            f"    python3 introspect_cohort.py --list-schemas",
            file=sys.stderr,
        )
        return 1

    print(
        f"All {len(SCHEMAS)} schemas dumped. Outputs under:\n"
        f"    {out_base}",
        file=sys.stderr,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
