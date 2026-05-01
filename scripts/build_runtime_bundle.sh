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
  README.md
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

# Bundle README so a stranger unzipping it knows what to do.
cat > "$BUNDLE_DIR/BUNDLE_README.md" <<'EOF'
# Century data dictionary — runtime bundle

Single zip containing the v2 build path **and** the offline
exact-match discovery tooling. Everything you need to generate a
customer-facing data dictionary for any registered cohort.

---

## 1. One-time setup

```bash
# 1. Unzip wherever you want to run from.
unzip century-dictionary.zip
cd century-dictionary

# 2. Install Python deps.
pip install -r requirements.txt

# 3. Configure DB credentials.
#    Create a .env file with the warehouse connection (or export the
#    same vars in your shell):
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
  skips it).
- `ruamel.yaml` — round-trips packs without destroying comments.
  Only required when running `discover_exact_matches.py --apply`.

---

## 2. Build a dictionary (the main thing)

```bash
python dictionary_v2/build_dictionary.py \
    --cohort <cohort_slug> \
    --audience customer
```

Output lands in `Output/<schema>_dictionary_<audience>.{xlsx,html}`.

### Cohort slugs available
balboa_ckd, drg_ckd, mtc_aat, mtc_alzheimers, newtown_ibd,
newtown_mash, nimbus_asthma, nimbus_az_asthma, nimbus_az_copd,
nimbus_copd, rmn_alzheimers, rvc_amd_curated, rvc_dr_curated

### Audience choices
| `--audience` | What ships | When to use |
|---|---|---|
| `technical` (default) | Full Summary + Tables + Columns + Variables, all debug fields, raw SQL Criteria, PII visible. | Internal review. |
| `sales` | Tempus-style spec: Summary cover + single Variables sheet (`Category, Variable, Description, Value Sets, Notes, Type, Proposal, Completeness`). Tables + Columns sheets dropped. PII rows dropped. `Value Sets` and `Proposal` come from curated YAML fields (`value_set:`, `proposal:`). | Sales / pharma-partner spec. |
| `pharma` | Only Summary + Variables; Tables & Columns hidden; PII dropped. | Pharma partner outputs. |
| `customer` | All four sheets but trimmed (drops debug fields like git_sha / variant / column_count, drops internal scaffolding tables, drops PII rows). Keeps the configured `Criteria` column side-by-side with `Inclusion Criteria` per reviewer feedback. JSON not produced. | Customer-facing dictionary — what the reviewer signs off on. |

### Other build flags
```bash
# Pick which formats get written (default = all three).
--formats xlsx html json

# Custom output directory.
--out-dir /path/to/somewhere

# Skip DB; emit pack-only skeleton (sanity-check the packs offline).
--dry-run
```

---

## 3. Exact-match discovery (optional, for tightening Criteria)

The dictionary ships best when each variable's Criteria is a strict
`column IN ('val1', 'val2', ...)` list instead of a fuzzy `ILIKE`
pattern. The discovery script enumerates the actual values your
warehouse holds for each variable and proposes a `match:` block.

### Read-only report
```bash
python dictionary_v2/discover_exact_matches.py \
    --cohort <cohort_slug>
```
Writes `Output/discovery/<cohort>/report.md` listing per variable:
- configured & observed (exact matches that show up in the data)
- missing from config (observed but not yet curated)
- stale in config (curated but never observed)

### Read-only report + suggestions YAML
```bash
python dictionary_v2/discover_exact_matches.py \
    --cohort <cohort_slug> \
    --write-suggestions
```
Adds `Output/discovery/<cohort>/suggested.yaml` with a proposed
`match:` block per variable, annotated with the source pack.

### Write match: blocks back into packs (interactive, per-variable)
```bash
# Safer: writes ONLY into packs/variables/<cohort_slug>.yaml.
# Variables that live solely in a shared pack are skipped (use
# --auto-stub below to copy them over automatically).
python dictionary_v2/discover_exact_matches.py \
    --cohort <cohort_slug> \
    --apply --target cohort

# --auto-stub: when --target cohort hits a variable that isn't
# in the cohort pack yet, copy its full base definition (table,
# column, criteria, description, ...) from the source pack into
# the cohort pack first, then attach the match: block. Shared
# packs are NEVER modified. Each stubbed row gets a leading
# YAML comment recording the source pack.
python dictionary_v2/discover_exact_matches.py \
    --cohort <cohort_slug> \
    --apply --target cohort --auto-stub

# Touches each variable's source pack — including shared
# <disease>_common.yaml files. Use only when the values are
# clinically appropriate for every cohort that includes the source.
# (--auto-stub is rejected here; auto-stub is cohort-only.)
python dictionary_v2/discover_exact_matches.py \
    --cohort <cohort_slug> \
    --apply --target shared
```

### Per-variable prompts
Interactive `--apply` walks each candidate one at a time and shows a
structured block so you can see source / target / action / reason
before approving:

```
  Variable: Diagnosis / Alzheimer's
  Source:   packs/variables/adrd_common.yaml
  Target:   packs/variables/mtc_alzheimers.yaml
  Action:   ADD cohort override
  Values:   12 ("Alzheimer disease, late onset", "Mild cognitive impairment of uncertain etiology", …)
  Reason:   row is inherited from shared pack; discovered values came from one cohort only
  Proceed?  [y]es / [n]o / [a]ll-remaining / [q]uit:
```

- **UPDATE variable** — row already exists in the target pack; only its `match:` block changes.
- **ADD cohort override** — row is inherited from a shared pack and `--auto-stub` is copying it into the cohort pack as a per-cohort override.
- `all` accepts every remaining row without further prompts.
- `quit` aborts the whole run; no files are written (changes are kept in memory until the loop completes).

Add `--apply-yes` to skip the prompts entirely (for scripted runs).

### Where should match values live? (pack-tier guidance)
| Pack | Scope | When to put values here |
|---|---|---|
| `<cohort>.yaml` (e.g. `mtc_alzheimers.yaml`) | One cohort only | **Default** for newly-discovered values from a single cohort. `--target cohort --auto-stub` writes here. |
| `<disease>_common.yaml` (e.g. `alzheimers_common.yaml`) | All cohorts of one disease | Promote here only after confirming the values are valid across every cohort that includes this pack (e.g. MTC Alzheimer's *and* RMN Alzheimer's). |
| `<umbrella>_common.yaml` (e.g. `adrd_common.yaml`) | A whole disease family | Promote here only after confirming values are valid across the umbrella (e.g. all ADRD cohorts including MTC AAT, MTC Alzheimer's, RMN Alzheimer's). |

The default discovery flow (`--target cohort --auto-stub`) keeps
shared packs untouched. Promotion to a shared pack is a deliberate
follow-up step (currently a manual edit; `--target shared` exists
for the rare case where you've already validated that the values
are universally appropriate).

### Other discovery flags
```bash
# Restrict to a single variable (case-insensitive name match).
--variable "Aspirin"

# Offline preview (no DB) — same skip reasons live discovery would emit.
--dry-run

# Custom output directory.
--out-dir /path/to/somewhere
```

---

## 4. End-to-end workflow — worked example: `mtc_aat`

`mtc_aat` is the MTC cohort of patients on anti-amyloid therapies
(Leqembi / Kisunla / Aduhelm). Its variables pack inherits everything
from the shared `aat_common` → `adrd_common` chain, so on a fresh
clone every variable lives in a shared pack — a perfect case for
`--auto-stub`.

### A. Quickest path (no discovery, just build)

```bash
# Sanity-check: dry-run uses no DB and proves the packs load.
python dictionary_v2/build_dictionary.py --cohort mtc_aat --dry-run

# Real build against the warehouse — writes
# Output/mtc__aat_cohort_dictionary_customer.{xlsx,html}.
python dictionary_v2/build_dictionary.py \
    --cohort mtc_aat \
    --audience customer
```

That's it for the basic case. Skip to step C if the existing
`criteria: ILIKE '%...%'` matchers are good enough for this round.

### B. Tighten Criteria with discovery (one-time per cohort)

If you want the dictionary's `Criteria` cells to be exact
`column IN ('Lecanemab', 'Lecanemab-irmb', ...)` lists drawn
from the cohort's actual data instead of fuzzy ILIKE:

```bash
# B1. Read-only report — what the cohort actually contains for
#     every variable's existing broad criteria. Writes
#     Output/discovery/mtc_aat/report.md.
python dictionary_v2/discover_exact_matches.py --cohort mtc_aat

# B2. Same plus a YAML proposal file you can copy from manually.
python dictionary_v2/discover_exact_matches.py \
    --cohort mtc_aat \
    --write-suggestions

# B3. Apply observed values directly into packs/variables/mtc_aat.yaml,
#     auto-stubbing each shared row into the cohort pack first.
#     Walks one variable at a time with [UPDATE]/[ADD cohort override]
#     prompts; answer y / n / all / quit.
python dictionary_v2/discover_exact_matches.py \
    --cohort mtc_aat \
    --apply --target cohort --auto-stub

# B4. (Scripted CI runs) skip the prompt entirely.
python dictionary_v2/discover_exact_matches.py \
    --cohort mtc_aat \
    --apply-yes --target cohort --auto-stub
```

After step B3/B4, `git diff packs/variables/mtc_aat.yaml` shows
exactly what was stubbed in. Each new row carries a leading
`# Auto-stubbed from packs/variables/aat_common.yaml ...` comment.

### C. Build the customer-facing dictionary

```bash
python dictionary_v2/build_dictionary.py \
    --cohort mtc_aat \
    --audience customer
```

Output:
```
Output/mtc__aat_cohort_dictionary_customer.xlsx
Output/mtc__aat_cohort_dictionary_customer.html
```

(JSON is not produced for the `customer` audience by design.)

### D. Hand-off
The `.xlsx` is what the reviewer signs off on. The `.html` is the
same content in a single-page browsable form.

### Other audiences for the same cohort
```bash
# Internal (debug fields, raw SQL Criteria, PII visible).
python dictionary_v2/build_dictionary.py --cohort mtc_aat --audience technical

# Sales (drops Columns sheet, drops PII).
python dictionary_v2/build_dictionary.py --cohort mtc_aat --audience sales

# Pharma (Summary + Variables only, drops PII).
python dictionary_v2/build_dictionary.py --cohort mtc_aat --audience pharma
```

---

## 5. Validate packs offline (no DB)

```bash
python scripts/validate_packs.py --strict
```
Lints every cohort's pack chain (cohort → variables_pack → includes)
for missing fields, dangling table refs, etc. Exits non-zero on
errors. Safe to wire into CI.

---

## 6. What's in this bundle

```
century-dictionary/
├── BUNDLE_README.md                    ← you are here
├── README.md                           ← full project guide (architecture, audiences)
├── requirements.txt
├── introspect_cohort.py                ← Postgres schema walker
├── dictionary_v2/
│   ├── build_dictionary.py             ← main build (audiences, customer)
│   └── discover_exact_matches.py       ← discovery + --apply / --auto-stub
├── scripts/
│   └── validate_packs.py               ← pack lint (no DB needed)
└── packs/                              ← cohort + variable + descriptor packs
    ├── categories.yaml
    ├── column_descriptions.yaml
    ├── pii.yaml
    ├── table_descriptions.yaml
    ├── dictionary_layout.yaml          ← per-audience layout overrides
    ├── STYLE.md
    ├── cohorts/                        ← per-cohort descriptors (13 cohorts)
    └── variables/                      ← shared <disease>_common + per-cohort
```

Excluded from the runtime bundle (live in the source repo):
- `tests/` and `dictionary_v2/test_*.py` — run from a source checkout.
- The legacy root `build_dictionary.py` — superseded by the v2 module.
- `scripts/dump_new_schemas.py` — only used to onboard new cohorts.
- `scripts/build_runtime_bundle.sh` — only used to rebuild this zip.

---

## 7. Troubleshooting

- **`ModuleNotFoundError: psycopg`** — install deps:
  `pip install -r requirements.txt`. Or use `--dry-run` if you
  just want to validate the packs.
- **`--apply` says "ruamel.yaml is not installed"** — install it:
  `pip install ruamel.yaml`. The script refuses to write without
  it because pyyaml round-trip would destroy comments.
- **`--apply` exits 2 with "requires --target"** — pick one:
  `--target cohort` (safer, per-cohort only) or `--target shared`
  (touches shared packs).
- **`--apply --target cohort` skips most variables** — they live in
  shared packs (`*_common.yaml`) and the cohort pack doesn't have
  them yet. Pass `--auto-stub` to copy each shared row into the
  cohort pack as a per-cohort override before applying the match block.
- **Empty Variables sheet** — your cohort's variables pack is
  probably a placeholder pulling from `<disease>_common.yaml`.
  That's normal; the build resolves `include:` chains automatically.

---

## 8. Rebuild this zip from a source checkout

```bash
bash scripts/build_runtime_bundle.sh
# wrote century-dictionary.zip — N files, M bytes
```
EOF

# zip from the parent so the archive contains a single
# `century-dictionary/` top-level directory.
rm -f "$REPO_ROOT/$OUT_ZIP"
( cd "$STAGE_DIR" && zip -qr "$REPO_ROOT/$OUT_ZIP" "century-dictionary" )

# Sanity report.
COUNT=$(unzip -l "$REPO_ROOT/$OUT_ZIP" | tail -1 | awk '{print $2}')
SIZE=$(stat -c%s "$REPO_ROOT/$OUT_ZIP" 2>/dev/null || stat -f%z "$REPO_ROOT/$OUT_ZIP")
echo "wrote $OUT_ZIP — $COUNT files, $SIZE bytes"
