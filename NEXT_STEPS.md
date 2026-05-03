# Next steps — post-v2-feedback rollout

The engineering loop on the v2 feedback branch (Commits A–D + the
review fixes) is closed; nothing in the codebase is half-built. What
remains is operational rollout and content backfill that depend on a
live warehouse and SME input. This file captures the agreed running
order so the work isn't carried in session memory.

Order is not arbitrary — each step de-risks the next. The live
mtc_aat pilot validates table/column assumptions before the all-
cohort batch; the CI gate locks in pack quality before content
backfill starts editing every cohort YAML.

---

## 1. Live mtc_aat pilot

The acid test. Unit tests prove mechanics; only a live warehouse
proves table/column assumptions and concept-ID usefulness. Start
with mtc_aat because it's the smallest pack — fastest signal on
whether the v2 path holds together against real data.

```bash
python dictionary_v2/discover_exact_matches.py \
    --cohort mtc_aat --mode concept-ids --write-suggestions

python dictionary_v2/build_dictionary.py \
    --cohort mtc_aat --audience sales

python scripts/build_all_cohorts.py
```

Reading the output:
- `Output/discovery/mtc_aat/report.md` — does each variable's
  observed concept-ID set look clinically sensible? Anything
  unexpected is a pack-criteria problem, not a discovery bug.
- `Output/mtc_aat_dictionary_sales.xlsx` — open in Excel and walk
  the Variables sheet. Coverage column should be populated; no
  variable should be silently 0%.
- `Output/BUILD_SUMMARY.md` — `unimplemented%` column flags
  cohorts ≥25% for follow-up discovery + criteria tightening.

Success criterion: a sales workbook for mtc_aat that the sales
team would actually send to a buyer, with no manual post-edits.

---

## 2. Operationalize the quality gate

Wire `scripts/validate_packs.py --strict` into CI as a required
gate. It already exits non-zero on errors; just needs to be added
to the repo's pipeline config so a malformed `match:` block, a
broken audience contract, or a Windows/path regression can't
merge undetected.

```bash
python scripts/validate_packs.py --strict
python -m unittest dictionary_v2.test_customer_audience
```

If runtime budget allows, run the dictionary_v2 test suite in CI
too — 277 tests, ~8s wall time. It catches the cross-cutting
regressions (override semantics, audience layouts, concept-ID
parity between apply / suggestions / report / prompt) that the
validator alone can't see.

Success criterion: a PR that breaks any of the above is blocked
at merge time, not at the next live build.

---

## 3. All-cohort live batch

Once mtc_aat is clean and CI is gating, run the batch against the
warehouse for the full fleet:

```bash
python scripts/build_all_cohorts.py
```

Use `Output/BUILD_SUMMARY.md` as the work queue:
- Cohorts ≥25% `unimplemented%` get the high-callout — those go
  to the front of the discovery + criteria-tightening queue.
- Per-cohort error blocks are recorded but don't kill the batch;
  one bad pack doesn't block the rest of the fleet.

Discovery promotion safety: use `--target cohort --auto-stub`
first. Only promote match blocks into shared
`<disease>_common.yaml` packs after confirming the concept IDs
are clinically valid across every cohort that includes the
shared pack. Promotion is deliberate; the auto-stub default
keeps shared packs untouched on purpose.

Success criterion: every cohort builds without error and
`unimplemented%` is below 25% for cohorts that legitimately
carry the relevant data. Cohorts that genuinely don't carry a
variable's data should be the only ones still flagged.

---

## 4. Cohort metadata backfill

13/13 cohort YAMLs are currently missing the freshness fields
the cover renderer ships when populated:

- `data_cutoff_date` — ISO date string, latest data the cohort
  reflects.
- `last_etl_run` — ISO date / datetime string, when the cohort
  was last refreshed.
- `known_limitations` — YAML list of plain-language caveats
  (one per line). Renders as a bulleted list under a
  `Known limitations` header on the stakeholder cover.
- `sign_off` — mapping with `reviewer`, `date`, `notes` keys.
  Renders as a `Reviewed by:` line on the cover.

These cannot be inferred from code — each cohort needs a
30-second touch from whoever owns the data pipeline / scientific
sign-off. Validator already enforces shape (string dates, list
limitations, mapping sign-off); it does NOT require the fields
to be present, so backfill is non-blocking.

Per-cohort tracker (tick as populated):

- [ ] balboa_ckd
- [ ] drg_ckd
- [ ] mtc_aat
- [ ] mtc_alzheimers
- [ ] newtown_ibd
- [ ] newtown_mash
- [ ] nimbus_asthma
- [ ] nimbus_az_asthma
- [ ] nimbus_az_copd
- [ ] nimbus_copd
- [ ] rmn_alzheimers
- [ ] rvc_amd_curated
- [ ] rvc_dr_curated

Success criterion: every cohort cover ships at least the
`data_cutoff_date` line. `sign_off` and `known_limitations`
follow as SMEs review.

---

## 5. Stakeholder sign-off

For one representative cohort (suggest mtc_aat once step 1 is
clean), generate one workbook per audience and walk it with the
actual audience owner:

```bash
for audience in technical pharma sales customer; do
    python dictionary_v2/build_dictionary.py \
        --cohort mtc_aat --audience "$audience"
done
```

Confirm with each owner: "Would you send / use this?" Sign-off
is more valuable now than another round of code polishing — the
audience contract is locked in `docs/audiences.md`, and any
remaining gap is one the contract itself missed.

Success criterion: one named owner per audience has signed off
on the rendered workbook. Disagreements feed back into
`docs/audiences.md` as a contract change, not an ad-hoc patch.

---

## Future work (explicit, deferred — not on the critical path)

These are real candidate features that didn't make the v2
feedback cycle. Listed here so they're not lost; not scheduled.

- **Drift detection against prior JSON / build outputs.** Diff
  this build's `CohortModel` JSON against the previous build's
  to surface "this variable's coverage dropped 40% week over
  week" before a stakeholder spots it. Useful once the pipeline
  is on a regular cadence.
- **PDF rendering.** XLSX + HTML cover the current ask. PDF
  only if a stakeholder explicitly needs an immutable
  print-ready format.
- **Notes normalization.** Pack notes are currently free-form.
  Normalizing into a structured taxonomy (e.g. "data caveat" /
  "definition note" / "coverage caveat") would let the renderer
  group them — but only worth doing once SMEs are ready to
  curate, otherwise the structure decays.

---

## Out of scope (closed)

- Audience contract (`docs/audiences.md`) — locked.
- Concept-ID matching (`--mode concept-ids`, mutually-exclusive
  with `match.values`, validator enforces shape) — closed.
- Optional `match.name_column:` override for non-canonical
  display columns — landed with identifier-shape validation.
- Cohort freshness schema (`data_cutoff_date`, `last_etl_run`,
  `known_limitations`, `sign_off`) — schema and renderer done;
  population is step 4 above.
- Batch runner (`scripts/build_all_cohorts.py`) +
  `BUILD_SUMMARY.md` — closed.
- Runtime bundle (`century-dictionary.zip`) — closed; rebuilt
  by `scripts/build_runtime_bundle.sh` on each material change.
