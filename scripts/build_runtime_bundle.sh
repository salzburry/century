#!/usr/bin/env bash
# Package the Century data dictionary tooling into a single zip.
#
# Output: century-dictionary.zip at the repo root, containing the v2
# build script, the discovery script, the introspection backbone,
# the validator, all packs, and runtime requirements.
#
# Usage:
#     bash scripts/build_runtime_bundle.sh
#
# Workflow once unzipped:
#     pip install -r requirements.txt
#     # 1. Discover proposed match: blocks. --apply requires
#     #    --target {cohort|shared}; --auto-stub copies inherited
#     #    rows into the cohort pack as per-cohort overrides.
#     python dictionary_v2/discover_exact_matches.py --cohort <slug> \
#         --write-suggestions --apply --target cohort --auto-stub
#     # 2. Build the customer-audience dictionary.
#     python dictionary_v2/build_dictionary.py --cohort <slug> \
#         --audience customer

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

OUT_ZIP="century-dictionary.zip"
STAGE_DIR="$(mktemp -d)"
BUNDLE_DIR="$STAGE_DIR/century-dictionary"

cleanup() { rm -rf "$STAGE_DIR"; }
trap cleanup EXIT

mkdir -p "$BUNDLE_DIR"

# Files / directories to include. Keep this list tight — only what's
# needed at runtime to generate a dictionary. Tests, caches, the
# legacy root build script, the cohort-onboarding helper, and the
# bundle-build script itself are excluded; they live in the source
# repo and aren't useful inside the runtime artifact.
FILES=(
  introspect_cohort.py
  requirements.txt
)
DIRS=(
  dictionary_v2
  packs
)
# scripts/ is selectively copied — only validate_packs.py is shipped.
EXTRA_FILES=(
  scripts/validate_packs.py
)

for f in "${FILES[@]}"; do
  if [[ -f "$f" ]]; then
    cp "$f" "$BUNDLE_DIR/"
  fi
done

for d in "${DIRS[@]}"; do
  if [[ -d "$d" ]]; then
    # rsync-style copy excluding caches and editor turds.
    mkdir -p "$BUNDLE_DIR/$d"
    (
      cd "$d"
      find . \
        -type d \( -name __pycache__ -o -name .test_outputs -o -name .pytest_cache \) -prune -o \
        -type f \( -name "*.py" -o -name "*.yaml" -o -name "*.yml" -o -name "*.md" -o -name "*.sh" -o -name "*.txt" \) \
        ! -name "test_*.py" ! -name "*_test.py" -print
    ) | while IFS= read -r rel; do
      mkdir -p "$BUNDLE_DIR/$d/$(dirname "$rel")"
      cp "$d/$rel" "$BUNDLE_DIR/$d/$rel"
    done
  fi
done

for f in "${EXTRA_FILES[@]}"; do
  if [[ -f "$f" ]]; then
    mkdir -p "$BUNDLE_DIR/$(dirname "$f")"
    cp "$f" "$BUNDLE_DIR/$f"
  fi
done

# Drop the old, separate zips inside the bundle if any leaked in.
rm -f "$BUNDLE_DIR"/*.zip

# In-zip README — code-focused. No mention of the project's
# reference PDFs or backlog docs; this is the runtime artifact.
cat > "$BUNDLE_DIR/README.md" <<'README_EOF'
# Century data dictionary — runtime bundle

Single zip containing the v2 build path and the offline exact-match
discovery tooling. Generates a customer- or sales-facing data
dictionary for any registered cohort.

For a worked end-to-end example (mtc_aat → sales spec), see
[`run.md`](./run.md).

---

## 1. Folder layout

```
century-dictionary/
├── README.md                            ← you are here
├── run.md                               ← worked example: mtc_aat / sales
├── requirements.txt
├── introspect_cohort.py                 ← Postgres schema walker
├── dictionary_v2/
│   ├── build_dictionary.py              ← main build (audiences)
│   └── discover_exact_matches.py        ← discovery + --apply / --auto-stub
├── scripts/
│   └── validate_packs.py                ← pack lint (no DB needed)
└── packs/                               ← cohort + variable + descriptor packs
    ├── categories.yaml                  ← table → Category map
    ├── column_descriptions.yaml         ← OMOP column semantics
    ├── pii.yaml                         ← PII allowlist + regex
    ├── table_descriptions.yaml          ← table → purpose blurb
    ├── dictionary_layout.yaml           ← per-audience layout overrides
    ├── STYLE.md                         ← prose-quality bar for customer copy
    ├── cohorts/                         ← per-cohort descriptors (13 cohorts)
    └── variables/                       ← shared <disease>_common + per-cohort
```

---

## 2. One-time setup

```bash
unzip century-dictionary.zip
cd century-dictionary
pip install -r requirements.txt

# Warehouse credentials — fill these in:
cat > .env <<'ENV'
PGHOST=warehouse.example.com
PGPORT=5432
PGDATABASE=century
PGUSER=readonly_user
PGPASSWORD=********
PGSSLMODE=require
ENV
```

`requirements.txt` pins:
- `pandas`, `openpyxl` — XLSX writer.
- `pyyaml` — pack parsing.
- `psycopg[binary]` — Postgres driver (live runs only; `--dry-run`
  skips the DB connection).
- `ruamel.yaml` — round-trips packs without destroying comments.
  Required only when running `discover_exact_matches.py --apply`.

---

## 3. Build a dictionary

```bash
python dictionary_v2/build_dictionary.py \
    --cohort <cohort_slug> \
    --audience <audience>
```

### Cohort slugs
balboa_ckd, drg_ckd, mtc_aat, mtc_alzheimers, newtown_ibd,
newtown_mash, nimbus_asthma, nimbus_az_asthma, nimbus_az_copd,
nimbus_copd, rmn_alzheimers, rvc_amd_curated, rvc_dr_curated.

### Audiences

| `--audience` | Sheets | Notes |
|---|---|---|
| `technical` (default) | Summary + Tables + Columns + Variables | Full debug fields, raw SQL Criteria, PII visible. Writes xlsx + html + json. |
| `sales` | Summary (trimmed) + Variables (Tempus-style: Category, Variable, Description, Value Sets, Notes, Type, Proposal, Completeness) | PII dropped. JSON suppressed. Stakeholder spec. |
| `pharma` | Summary + Variables | PII dropped. JSON kept. |
| `customer` | Summary, Tables, Columns, Variables (all trimmed) | PII dropped, internal scaffolding tables filtered, JSON suppressed. |

### Other build flags
```bash
--formats xlsx html json   # pick formats (default = all three)
--out-dir /path/to/dir
--dry-run                  # skip DB; pack-only smoke test
```

---

## 4. Exact-match discovery (optional, for tightening Criteria)

The dictionary ships best when each variable's Criteria is a strict
`column IN ('val1', 'val2', ...)` list instead of a fuzzy `ILIKE`
pattern. The discovery script enumerates the actual values your
warehouse holds for each variable and proposes a `match:` block.

### Read-only report
```bash
python dictionary_v2/discover_exact_matches.py --cohort <slug>
# → Output/discovery/<slug>/report.md
```

### Read-only report + suggestions YAML
```bash
python dictionary_v2/discover_exact_matches.py --cohort <slug> \
    --write-suggestions
# → Output/discovery/<slug>/{report.md, suggested.yaml}
```

### Apply match blocks back into packs (interactive, per-variable)
```bash
# Safer: writes ONLY into packs/variables/<slug>.yaml.
# Inherited rows are skipped unless --auto-stub is passed.
python dictionary_v2/discover_exact_matches.py --cohort <slug> \
    --apply --target cohort

# --auto-stub: when --target cohort hits a variable that isn't in
# the cohort pack yet, copy its full base definition from the
# source pack into the cohort pack first, then attach the match
# block. Shared packs are NEVER modified.
python dictionary_v2/discover_exact_matches.py --cohort <slug> \
    --apply --target cohort --auto-stub

# Touches each variable's source pack — including shared
# <disease>_common.yaml files. Only use when the values are
# clinically appropriate for every cohort that includes the source.
python dictionary_v2/discover_exact_matches.py --cohort <slug> \
    --apply --target shared
```

Interactive `--apply` prompts per variable:
```
  Variable: Diagnosis / Alzheimer's
  Source:   packs/variables/adrd_common.yaml
  Target:   packs/variables/mtc_alzheimers.yaml
  Action:   ADD cohort override
  Values:   12 ("Alzheimer disease, late onset", …)
  Reason:   row is inherited from shared pack; discovered values came from one cohort only
  Proceed?  [y]es / [n]o / [a]ll-remaining / [q]uit:
```

- **UPDATE variable** — row already in target pack; only the `match:` block changes.
- **ADD cohort override** — `--auto-stub` is copying a shared row into the cohort pack.
- `all` accepts every remaining row; `quit` aborts (no files written).

Add `--apply-yes` to skip prompts entirely (scripted runs).

### Where should match values live?

| Pack | Scope | When to put values here |
|---|---|---|
| `<cohort>.yaml` (e.g. `mtc_aat.yaml`) | One cohort only | **Default** for newly-discovered values. `--target cohort --auto-stub` writes here. |
| `<disease>_common.yaml` (e.g. `aat_common.yaml`) | All cohorts of one disease | Promote here only after confirming values are valid across every cohort that includes the pack. |
| Umbrella `<x>_common.yaml` (e.g. `adrd_common.yaml`) | A whole disease family | Promote here only after confirming values are valid across the umbrella. |

The default flow keeps shared packs untouched. Promotion is a deliberate follow-up.

---

## 5. Validate packs offline (no DB)

```bash
python scripts/validate_packs.py --strict
```
Lints every cohort's pack chain (cohort → variables_pack → includes)
for missing fields, dangling refs, malformed `match:` / `value_set:`
blocks, unknown `proposal:` values, etc. Exits non-zero on errors.

---

## 6. Troubleshooting

- **`ModuleNotFoundError: psycopg`** — install deps:
  `pip install -r requirements.txt`. Or use `--dry-run` to skip DB.
- **`--apply` says "ruamel.yaml is not installed"** —
  `pip install ruamel.yaml`. The script refuses to write without
  it because pyyaml round-trip would destroy comments.
- **`--apply` exits 2 with "requires --target"** — pick
  `--target cohort` (safer) or `--target shared`.
- **`--apply --target cohort` skips most variables** — they live
  in shared packs and the cohort pack doesn't have them yet.
  Pass `--auto-stub` to copy each shared row into the cohort
  pack as a per-cohort override before applying the match block.
- **Empty Variables sheet** — your cohort's variables pack is a
  placeholder pulling from `<disease>_common.yaml`. That's normal;
  the build resolves `include:` chains automatically.
README_EOF

# Worked example: generate the sales dictionary for mtc_aat.
cat > "$BUNDLE_DIR/run.md" <<'RUN_EOF'
# Walkthrough: generate the **sales** data dictionary for `mtc_aat`

End-to-end example. Produces the Tempus-style sales spec for the
MTC AAT cohort (anti-amyloid therapy patients): a single-sheet
workbook with the columns the partner reviewer asked for.

---

## What you'll end up with

```
Output/
├── mtc__aat_cohort_dictionary_sales.xlsx      ← hand this to the reviewer
└── mtc__aat_cohort_dictionary_sales.html      ← same content, browsable
```

Sheets in the xlsx:
- **Summary** — cohort cover (provider, disease, patient count, date coverage).
- **Variables** — Tempus-style spec with these columns:
  `Category | Variable | Description | Value Sets | Notes | Type | Proposal | Completeness`

JSON is intentionally not produced for the sales audience — the partner
bundle never carries the internal `CohortModel` dump.

---

## Step 0 — prerequisites

You only need to do this once per environment.

```bash
unzip century-dictionary.zip
cd century-dictionary

pip install -r requirements.txt

# Warehouse credentials:
cat > .env <<'ENV'
PGHOST=warehouse.example.com
PGPORT=5432
PGDATABASE=century
PGUSER=readonly_user
PGPASSWORD=********
PGSSLMODE=require
ENV
```

---

## Step 1 — sanity-check offline (no DB needed)

Confirms the packs load and the layout is correct without touching
the warehouse:

```bash
python dictionary_v2/build_dictionary.py \
    --cohort mtc_aat \
    --audience sales \
    --dry-run
```

Expect to see:
```
Wrote Output/mtc__aat_cohort_dictionary_sales.xlsx
Wrote Output/mtc__aat_cohort_dictionary_sales.html
```

If you open the xlsx, the `Variables` sheet header row will read
exactly:
```
Category | Variable | Description | Value Sets | Notes | Type | Proposal | Completeness
```

`Value Sets` and `Proposal` cells will be empty — those come from
curated YAML fields (see Step 3 below). The dry-run skips
DB-derived columns, so `Completeness` is `—`.

---

## Step 2 — real build against the warehouse

```bash
python dictionary_v2/build_dictionary.py \
    --cohort mtc_aat \
    --audience sales
```

`Completeness` is now populated from live cohort counts. Variables
the cohort doesn't actually carry data for (`Implemented = No`)
are dropped automatically from the sales / customer artifacts —
they'd otherwise render as 0% rows and add noise. Internal
audiences (`technical`, `pharma`) keep them so QA can see gaps.

Hand off the xlsx + html as the sales artifact.

---

## Step 3 (optional) — populate `Value Sets` and `Proposal`

Both columns are authored in the cohort's variables YAML
(`packs/variables/mtc_aat.yaml`) under each `variable:` row.
Edit the file directly; the next `--audience sales` run picks them up.

```yaml
- category: Demographics
  variable: Education level
  description: The patient's highest level of education received.
  table: observation
  column: value_as_concept_name
  value_set:                              # ← curated clinical reference values
    - Less than high school
    - High school diploma or equivalent
    - Some college, no degree
    - Associate degree
    - Bachelor's degree
    - Master's degree
    - Doctoral or professional degree
  proposal: Custom                        # ← Standard | Custom
```

The `value_set:` cell renders newline-separated in the workbook
(matches the reference Google sheet). `proposal:` must be exactly
`Standard` or `Custom` when set; the validator rejects anything
else.

If you accidentally write a scalar instead of a list:
```yaml
value_set: "Yes, No, Unknown"             # ← scalar; auto-split by builder
```
the renderer normalizes it to a 3-item list, but the validator
will still flag it as an error so you fix the YAML.

After editing:
```bash
# Lint the packs (no DB required):
python scripts/validate_packs.py --strict

# Re-run the build:
python dictionary_v2/build_dictionary.py --cohort mtc_aat --audience sales
```

---

## Step 4 (optional) — tighten Criteria with discovery

This addresses the reviewer's "Criteria should be exact matches,
not ILIKE" feedback. Replaces fuzzy `criteria: drug_concept_name
ILIKE '%lecanemab%'` with strict `match: { column, values }` blocks
populated from the live cohort.

The sales sheet doesn't show a `Criteria` column directly, but
tightening Criteria still affects which rows feed `Completeness`
and the underlying value distributions, so it's worth doing once
per cohort.

```bash
# 4a. Read-only report — what does mtc_aat actually contain for
#     each variable's existing broad criteria?
python dictionary_v2/discover_exact_matches.py --cohort mtc_aat
# → Output/discovery/mtc_aat/report.md

# 4b. Apply observed values into packs/variables/mtc_aat.yaml.
#     --auto-stub copies each inherited row from the shared
#     aat_common pack into mtc_aat.yaml first, then attaches the
#     match block. Walks one variable at a time with a
#     [UPDATE]/[ADD cohort override] prompt.
python dictionary_v2/discover_exact_matches.py \
    --cohort mtc_aat \
    --apply --target cohort --auto-stub
```

After step 4b, `git diff packs/variables/mtc_aat.yaml` shows the
exact rows that were added/updated. Each new row carries a
provenance comment:

```yaml
  # Auto-stubbed from packs/variables/aat_common.yaml via discover_exact_matches.py --auto-stub. Verify clinical fit before shipping.
  - category: Medications
    variable: Anti-amyloid Therapy (Administration)
    table: drug_exposure
    column: drug_concept_name
    criteria: drug_concept_name ILIKE '%lecanemab%' OR ...
    match:
      column: drug_concept_name
      values:
        - Lecanemab
        - Lecanemab-irmb
        - Leqembi
        - Donanemab-azbt
        - Kisunla
```

For non-interactive (CI) use:
```bash
python dictionary_v2/discover_exact_matches.py \
    --cohort mtc_aat \
    --apply-yes --target cohort --auto-stub
```

Re-run the build to pick up the new match blocks:
```bash
python dictionary_v2/build_dictionary.py --cohort mtc_aat --audience sales
```

---

## Quick reference

| Task | Command |
|---|---|
| Sanity-check (no DB) | `python dictionary_v2/build_dictionary.py --cohort mtc_aat --audience sales --dry-run` |
| Build sales dictionary | `python dictionary_v2/build_dictionary.py --cohort mtc_aat --audience sales` |
| Read-only discovery | `python dictionary_v2/discover_exact_matches.py --cohort mtc_aat` |
| Apply match blocks | `python dictionary_v2/discover_exact_matches.py --cohort mtc_aat --apply --target cohort --auto-stub` |
| Validate packs | `python scripts/validate_packs.py --strict` |
RUN_EOF

# zip from the parent so the archive contains a single
# `century-dictionary/` top-level directory.
rm -f "$REPO_ROOT/$OUT_ZIP"
( cd "$STAGE_DIR" && zip -qr "$REPO_ROOT/$OUT_ZIP" "century-dictionary" )

# Sanity report.
COUNT=$(unzip -l "$REPO_ROOT/$OUT_ZIP" | tail -1 | awk '{print $2}')
SIZE=$(stat -c%s "$REPO_ROOT/$OUT_ZIP" 2>/dev/null || stat -f%z "$REPO_ROOT/$OUT_ZIP")
echo "wrote $OUT_ZIP — $COUNT files, $SIZE bytes"
