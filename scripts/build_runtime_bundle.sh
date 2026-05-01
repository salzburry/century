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
#     # 1. Discover proposed match: blocks (with optional --apply
#     #    interactive prompt to write straight into packs/variables/).
#     python dictionary_v2/discover_exact_matches.py --cohort <slug> \
#         --write-suggestions --apply
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

# Files / directories to include. Keep this list short and explicit
# rather than copying everything in the repo — tests, output
# directories, and the old runtime zip itself are intentionally
# excluded.
FILES=(
  introspect_cohort.py
  build_dictionary.py
  requirements.txt
  README.md
)
DIRS=(
  dictionary_v2
  packs
  scripts
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
| `sales` | Drops the Columns sheet and PII rows. | Account / sales-engineering decks. |
| `pharma` | Only Summary + Variables; Tables & Columns hidden; PII dropped. | Pharma partner outputs. |
| `customer` | All four sheets but trimmed (drops debug fields, internal tables, raw SQL). PII dropped. JSON not produced. | Customer-facing dictionary — what the reviewer signs off on. |

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

### Write match: blocks back into packs (interactive)
```bash
# Safer: writes ONLY into packs/variables/<cohort_slug>.yaml.
# Variables that live solely in a shared pack are skipped (the cohort
# pack must already define the variable to be overridable).
python dictionary_v2/discover_exact_matches.py \
    --cohort <cohort_slug> \
    --apply --target cohort

# Touches each variable's source pack — including shared
# <disease>_common.yaml files. Use only when the values are
# clinically appropriate for every cohort that includes the source.
python dictionary_v2/discover_exact_matches.py \
    --cohort <cohort_slug> \
    --apply --target shared
```
Both prompt `[apply] proceed? [y/N]:` before writing. Add
`--apply-yes` for non-interactive scripted runs.

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

## 4. End-to-end workflow

```bash
# 1. (Optional, one-time per cohort) discover exact-match candidates.
python dictionary_v2/discover_exact_matches.py \
    --cohort balboa_ckd \
    --write-suggestions

# 2. Review Output/discovery/balboa_ckd/{report.md,suggested.yaml}.

# 3. Apply approved match: blocks back into the cohort pack.
python dictionary_v2/discover_exact_matches.py \
    --cohort balboa_ckd \
    --apply --target cohort

# 4. Build the customer dictionary.
python dictionary_v2/build_dictionary.py \
    --cohort balboa_ckd \
    --audience customer

# 5. Hand off Output/<schema>_dictionary_customer.{xlsx,html}.
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
├── README.md                           ← full project guide
├── requirements.txt
├── introspect_cohort.py                ← Postgres schema walker
├── build_dictionary.py                 ← legacy entrypoint
├── dictionary_v2/
│   ├── build_dictionary.py             ← v2 build (audiences, customer)
│   └── discover_exact_matches.py       ← discovery + --apply
├── scripts/
│   ├── validate_packs.py
│   ├── dump_new_schemas.py             ← raw-dump helper
│   └── build_runtime_bundle.sh         ← rebuild this zip
└── packs/                              ← cohort + variable + descriptor packs
    ├── categories.yaml
    ├── column_descriptions.yaml
    ├── pii.yaml
    ├── table_descriptions.yaml
    ├── dictionary_layout.yaml          ← per-audience layout overrides
    ├── STYLE.md
    ├── cohorts/                        ← per-cohort descriptors
    └── variables/                      ← shared <disease>_common + per-cohort
```

Tests are intentionally NOT shipped in the runtime bundle. To run
them, work from the source repo (`python -m unittest`).

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
