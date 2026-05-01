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
exact-match discovery tooling.

## Quick start

```bash
pip install -r requirements.txt

# Optional: discover candidate exact matches against a live cohort
# DB. Writes Output/discovery/<cohort>/{report.md, suggested.yaml}.
# With --apply, prompts to inject `match:` blocks straight into
# packs/variables/<source_pack>.yaml (ruamel.yaml required).
python dictionary_v2/discover_exact_matches.py --cohort <slug> \
    --write-suggestions --apply

# Build the dictionary for a cohort + audience.
python dictionary_v2/build_dictionary.py --cohort <slug> \
    --audience customer
```

Audience choices: `technical` | `sales` | `pharma` | `customer`.
See README.md for the full guide.
EOF

# zip from the parent so the archive contains a single
# `century-dictionary/` top-level directory.
rm -f "$REPO_ROOT/$OUT_ZIP"
( cd "$STAGE_DIR" && zip -qr "$REPO_ROOT/$OUT_ZIP" "century-dictionary" )

# Sanity report.
COUNT=$(unzip -l "$REPO_ROOT/$OUT_ZIP" | tail -1 | awk '{print $2}')
SIZE=$(stat -c%s "$REPO_ROOT/$OUT_ZIP" 2>/dev/null || stat -f%z "$REPO_ROOT/$OUT_ZIP")
echo "wrote $OUT_ZIP — $COUNT files, $SIZE bytes"
