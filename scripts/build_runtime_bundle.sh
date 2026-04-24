#!/usr/bin/env bash
#
# Build a self-contained runtime zip for deploying build_dictionary.py
# to a server. Includes ONLY what is needed to run the generator against
# a live warehouse — no tests, no reference PDFs, no historical design
# docs, no generated artifacts.
#
# Why a script (not a committed zip):
#   The previous tracked `century-dictionary-runtime.zip` went stale
#   because every pack edit required a refresh commit, and nothing
#   enforced that. Regenerating from source avoids that class of bug.
#   The zip itself is gitignored (see .gitignore, `*.zip`).
#
# Output:
#   ./century-dictionary-runtime.zip  (repo root)
#
# Usage:
#   bash scripts/build_runtime_bundle.sh
#   # or, from repo root:
#   scripts/build_runtime_bundle.sh
#
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_ROOT"

OUT="century-dictionary-runtime.zip"

# Shell out to git for the authoritative "what's tracked and shipping"
# list, then filter to the runtime-relevant paths. Using `git ls-files`
# instead of a glob means the bundle can never accidentally pull in an
# untracked dry-run output or a local .env.
INCLUDE_PATHS=(
  ".env.example"
  "README.md"
  "VALIDATION_REPORT.md"
  "build_dictionary.py"
  "introspect_cohort.py"
  "requirements.txt"
  "scripts/validate_packs.py"
)

# All tracked files under packs/ — grabs every cohort and variable
# YAML without hard-coding the list (so new cohorts land automatically).
mapfile -t PACK_FILES < <(git ls-files "packs/")
INCLUDE_PATHS+=( "${PACK_FILES[@]}" )

# Sanity-check every path exists and is tracked.
for p in "${INCLUDE_PATHS[@]}"; do
  if [ ! -f "$p" ]; then
    echo "error: $p not found — aborting bundle build" >&2
    exit 1
  fi
done

rm -f "$OUT"
# -X strips extra file attributes that can cause cross-OS churn.
# -q keeps the output tight so CI logs stay readable.
zip -q -X "$OUT" "${INCLUDE_PATHS[@]}"

# Emit a short manifest so the caller can eyeball what's inside.
echo "Wrote $OUT"
SIZE_KB=$(( $(stat -c%s "$OUT" 2>/dev/null || stat -f%z "$OUT") / 1024 ))
FILE_COUNT=${#INCLUDE_PATHS[@]}
GIT_SHA=$(git rev-parse --short HEAD 2>/dev/null || echo "unknown")
echo "  size:   ${SIZE_KB} KB"
echo "  files:  ${FILE_COUNT}"
echo "  commit: ${GIT_SHA}"
