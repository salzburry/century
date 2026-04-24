# Century data dictionaries — canonical plan

Single source of truth for:

- answering `century/ask.pdf` ("Clinical Registries to Schemas")
- prioritising which registries to deliver
- guiding the engineering work that turns `introspect_cohort.py` into
  a reusable cohort-dictionary pipeline

---

## 0. Working definitions

These terms need to stay precise because the ask mixes them.

- **Column**: one physical schema column, e.g.
  `condition_occurrence.condition_concept_name`.
- **Variable**: one semantic, disease-facing concept on Page 4 — config-
  driven, potentially mapped to one or more columns plus a criteria filter.
- **Cohort dictionary**: one deliverable for one (provider, disease) pair.
- **Canonical model**: the single structured object every renderer consumes.
- **Audience filter**: a render-time rule that hides or shows parts of the
  canonical model for `technical`, `sales`, or `pharma`.

Important: Page 3 is about columns. Page 4 is about variables. They are
not the same output.

## 1. The ask (`century/ask.pdf`)

Four decisions + one deliverable shape.

### 1.1 Decision log

1. **One dictionary per (provider, disease) pair.** Start there.
2. **Multi-site providers (Nimbus vs Nimbus AZ): keep separate for now.**
   Revisit consolidation later as an explicit follow-up.
3. **Sales / data science / pharma share one four-page structure** and
   differ only by audience-specific visibility rules at render time.
4. **Automate by extending `introspect_cohort.py`**, not greenfield.

### 1.2 Four-page deliverable

- Page 1 — Summary: provider, disease, cohort/schema, patient count,
  years of data, table count, column count, variant, site notes.
- Page 2 — Tables: one row per source table.
- Page 3 — Columns and descriptions: one row per physical column.
- Page 4 — Variables: one row per semantic disease-level variable,
  config-driven.

## 2. Registries to deliver

Source: `century/ask.pdf` + `century/Adobe Scan 19 Apr 2026.pdf`.

### 2.1 Registry backlog

| # | Provider | Disease | Schema | Priority | Raw dump | Status | Notes |
|---|---|---|---|---|---|---|---|
| 1 | Nira | MS (1 site) | `ms_leaf_nira_registry` | Medium | — | Missing dump | First site we started with |
| 2 | Nira | MS (all sites) | `nira_ms_cohort` | Medium | — | Missing dump | |
| 3 | Nira | MG (all sites) | `nira_ms_cohort` | Low | — | Missing dump | Draft ask maps this to the MS schema; confirm before implementation |
| 4 | Nimbus | COPD | `nimbus_copd_curated` | High | `minbuscopdcurated.pdf` | Packs committed; awaiting live build | Filename typo (`minbus`); consolidation with Nimbus AZ open |
| 5 | Nimbus | Asthma | `nimbus_asthma_curated` | High | `nimbusasthmacurated.pdf` | Packs committed; awaiting live build | |
| 6 | Nimbus AZ | COPD | `nimbus_az_copd_cohort` | High | `nimbusazcopd.pdf` | Packs committed; awaiting live build | Abstracted variables (FEV1 etc.) in progress |
| 7 | Nimbus AZ | Asthma | `nimbus_az_asthma_cohort` | High | `nimbusazasthma.pdf` | Packs committed; awaiting live build | |
| 8 | Balboa | Renal | `balboa_ckd_cohort` | High | `balboackd.pdf` | Packs committed; awaiting live build | |
| 9 | MTC | Alzheimer's | `mtc_alzheimers_cohort` | High | `mtcalzhiemer.pdf` | Dump available | Filename typo (`alzhiemer`) |
| 10 | MTC | AAT | `mtc_aat_cohort` | High | `mtcaat.pdf` | Dump available | Anti-amyloid therapies |
| 11 | Newtown | MASH | `newtown_mash_cohort` | High | — | Missing dump | |
| 12 | Newtown | IBD | `newtown_ibd_cohort` | Low | — | Missing dump | |
| 13 | DRG | Renal | `drg_ckd_cohort` | High | `drgckd.pdf` | Packs committed; awaiting live build | |
| 14 | PRINE | Renal | TBD | — | — | Blocked | Schema not yet provisioned |
| 15 | Rocky Mountain Neurology | Alzheimer's | `rmn_alzheimers_cohort` | — | — | Missing dump | |
| 16 | Southland Neurologic Institute | TBD | TBD | — | — | Blocked | Disease and schema unconfirmed |
| 17 | Eye Health America (EHA) | TBD | TBD | High | — | Blocked | Disease and schema unconfirmed |
| 18 | RVC | DR | `rvc_dr_curated` | — | — | Missing dump | |

### 2.2 Backlog summary

- 8 of 18 have a raw introspection dump in `Output/`.
- All 8 of those also have committed `packs/cohorts/*.yaml` + variable
  packs (MTC AAT, MTC Alzheimer's, Nimbus COPD, Nimbus Asthma,
  Nimbus AZ COPD, Nimbus AZ Asthma, Balboa CKD, DRG CKD). The six
  non-MTC cohorts are awaiting their first live build + clinical
  Variables-sheet review.
- 7 more are runnable once the cohort dump is generated.
- 3 are blocked on upstream schema provisioning or unresolved metadata.

## 3. Verified baseline from `Output/*.pdf`

Counts verified against the Summary pages of the current raw dumps.

| File | Cohort | Patients | Tables | Columns |
|---|---|---:|---:|---:|
| `mtcaat.pdf` | `mtc_aat_cohort` | 1,067 | 15 | 307 |
| `mtcalzhiemer.pdf` | `mtc_alzheimers_cohort` | 3,755 | 14 | 284 |
| `balboackd.pdf` | `balboa_ckd_cohort` | 81,183 | 11 | 256 |
| `drgckd.pdf` | `drg_ckd_cohort` | 53,213 | 9 | 200 |
| `nimbusazcopd.pdf` | `nimbus_az_copd_cohort` | 6,710 | 14 | 300 |
| `nimbusazasthma.pdf` | `nimbus_az_asthma_cohort` | 4,334 | 14 | 300 |
| `minbuscopdcurated.pdf` | `nimbus_copd_curated` | 7,233 | 12 | 253 |
| `nimbusasthmacurated.pdf` | `nimbus_asthma_curated` | 3,654 | 11 | 242 |

## 4. What the existing outputs prove

Every file in `Output/` is an Adobe Scan / photo-style PDF of the HTML
emitted by the current generator. Shared shape: Summary + Variables.
Enough to show the technical inventory works; the gaps are:

### 4.1 Export-path problems
- PDFs carry browser chrome, URL bar text, tab text, OCR-like corruption.
- Root cause: iPhone scan-to-PDF loop, not clean render.
- Direct HTML → PDF render replaces the scan loop entirely.

### 4.2 Content-model problems
- `Category`, `Description`, `Criteria`, `Notes` blank everywhere
  (hardcoded `""` in `introspect_cohort.py` around lines 736 and 819).
- `% Patient` doesn't exist — only row-level `Completeness` today.
- Summary lacks provider, disease, years of data, variant, site/schema notes.

### 4.3 Representation problems
- The current `Variables` sheet is really Page 3 (columns), not Page 4.
- Page 4 semantic variables don't exist yet.
- `TableInfo` is collected (`introspect_cohort.py:266`) but never rendered.
- `_compile_date_range` (`introspect_cohort.py:505`) runs per column but
  isn't rolled up into `years_of_data`.

### 4.4 Data-quality problems on the page
- Surrogate keys (`person_id`, `*_occurrence_id`, `*_concept_id`) are
  summarised like measurements → useless scientific-notation medians
  (`9.22e+18`, `IQR: 2.31e+18–6.9e+18`).
- `Values` truncates at 60 chars mid-string; fine for HTML, lossy in XLSX.
- Top-N depth (5) is too shallow for `*_concept_name` columns.
- Empty tables still emit a row per column.
- `dv_tokenized_profile_data` contributes many low-signal `token_*`
  fields (111 in `mtc_aat`).
- `standard_profile_data_model` exposes PII (`first_name`, `last_name`,
  `email`, `cellphone`, `date_of_birth`, `address1`) without flags —
  blocks clean sales/pharma outputs.

### 4.5 Presentation problems
- File naming inconsistent (`minbuscopdcurated`, `mtcalzhiemer`).
- Long-cell layout not tuned for print.
- `Extraction Type` is binary today; reference uses three-way
  (Structured / Abstracted / Unstructured).

## 5. Current repo reality

Explicit so the plan matches the codebase we have.

### 5.1 What already exists
- `introspect_cohort.py` does schema introspection. Per-table row /
  column counts, row-level completeness, top values, numeric
  summaries, per-column date ranges — all already computed.
- `build_dictionary.py` — the four-page generator. Wraps
  `introspect_cohort.py` with cohort packs, audience filters, and
  HTML / XLSX / JSON renderers.
- `packs/` directory is committed. Today it carries:
  - `packs/cohorts/mtc_aat.yaml`, `packs/cohorts/mtc_alzheimers.yaml`
  - `packs/cohorts/nimbus_copd.yaml`,
    `packs/cohorts/nimbus_asthma.yaml`,
    `packs/cohorts/nimbus_az_copd.yaml`,
    `packs/cohorts/nimbus_az_asthma.yaml`
  - `packs/cohorts/balboa_ckd.yaml`,
    `packs/cohorts/drg_ckd.yaml`
  - Variable packs are layered. Every cohort has its own ETL, so the
    final source of truth is always a per-cohort pack — disease-common
    bases exist for reuse, never as the cohort's variables_pack target:
    - shared bases that multiple cohorts include:
      `packs/variables/adrd_common.yaml`,
      `packs/variables/aat_common.yaml` (includes `adrd_common`),
      `packs/variables/alzheimers_common.yaml` (includes `adrd_common`),
      `packs/variables/respiratory_common.yaml`,
      `packs/variables/copd_common.yaml` (includes `respiratory_common`),
      `packs/variables/asthma_common.yaml` (includes `respiratory_common`),
      `packs/variables/ckd_common.yaml` (top of the renal chain — no
      renal_common parent yet because CKD is the only renal disease
      in scope; refactor to introduce one when AKI lands)
    - per-cohort final packs — `cohort.variables_pack` always points
      at one of these, never at a `*_common` base directly:
      `packs/variables/mtc_aat.yaml` (includes `aat_common`,
      placeholder, no cohort-specific overrides yet),
      `packs/variables/mtc_alzheimers.yaml` (includes
      `alzheimers_common`, placeholder, no overrides yet),
      `packs/variables/nimbus_copd.yaml` (includes `copd_common`, plus
      the Nimbus-curated `eosinophil_standardized` row),
      `packs/variables/nimbus_az_copd.yaml` (includes `copd_common`,
      placeholder, no overrides yet),
      `packs/variables/nimbus_asthma.yaml` (includes `asthma_common`,
      placeholder, no overrides yet),
      `packs/variables/nimbus_az_asthma.yaml` (includes `asthma_common`,
      placeholder, no overrides yet),
      `packs/variables/balboa_ckd.yaml` (includes `ckd_common`,
      placeholder, no overrides yet),
      `packs/variables/drg_ckd.yaml` (includes `ckd_common`,
      placeholder, no overrides yet)
  - `packs/categories.yaml`, `packs/pii.yaml`,
    `packs/table_descriptions.yaml`, `packs/column_descriptions.yaml`
- `scripts/validate_packs.py` + `VALIDATION_REPORT.md` — static
  pack-lint covering duplicate variables, unknown categories, unsafe
  ILIKE, missing criteria on clinically-specific rows, and column
  vs variable-name mismatches (e.g. `_concept_id` column under a row
  whose name doesn't mention "ID").
- `century-dictionary-runtime.zip` — self-contained runtime bundle
  at the repo root. ~90 KB, contains exactly what a server needs to
  run the generator: the two Python entrypoints, the validator,
  `packs/`, `requirements.txt`, `.env.example`, and a snapshot of
  `VALIDATION_REPORT.md`. No tests, no reference PDFs, no historical
  design docs. Extract, `pip install -r requirements.txt`, fill in
  `.env` from `.env.example`, and `python3 build_dictionary.py
  --cohort <slug>` runs.
- `scripts/dump_new_schemas.py` — raw-dump helper for the backlog
  cohorts that don't have a reference PDF under `Output/` yet
  (Rocky Mountain Neurology Alzheimer's, Newtown MASH, Newtown IBD,
  RVC DR, RVC AMD). Hands each schema name to
  `introspect_cohort.py --schema`, which walks the warehouse without
  needing a cohort pack. Output lands under `Output/raw/<schema>/`
  (gitignored — dumps carry real warehouse distributions). Run once
  the schemas are provisioned; mine the resulting files the same
  way the existing Nimbus / Balboa / DRG cohorts were mined, then
  commit proper `<disease>_common` + per-cohort packs.

### 5.2 What is NOT a clinical validation
`VALIDATION_REPORT.md` is a structural lint, not a clinical
validation. It proves the packs are internally consistent. It does
**not** prove each variable's criteria actually matches the intended
concepts in a live warehouse, nor that the returned distributions
are clinically sensible. Clinical validation still requires
reviewing the generated `Output/<schema>_dictionary.xlsx` per cohort
— look at the Variables sheet and confirm that:

- Rows marked `Implemented: Yes` actually contain the concepts
  the variable name promises (e.g. `Amyloid-beta 42` → the
  Distribution column shows A-beta 42 concept names, not unrelated
  measurement labels).
- `Implemented: No` rows reflect a real cohort gap, not an ILIKE
  that's too narrow.
- ID-backed rows (anything with `Concept ID` in the name) are
  clearly labeled as opaque identifiers, not a reviewer-friendly name.

### 5.3 Still to build
- First live Variables-sheet review for the four Nimbus cohorts —
  confirm exacerbation attribution, FEV1 / FVC / FEV1-FVC ratio
  abstraction state, Smoking Status / BMI source tables, and the
  ACT / CAT / mMRC / FeNO / Total IgE rows.
- First live Variables-sheet review for Balboa CKD and DRG CKD —
  confirm eGFR / UACR availability (DRG dump shows urea-creatinine
  ratio in urine, not the canonical albumin-side UACR), SGLT2 / MRA /
  ESA utilisation rates, and Dialysis Initiation / Kidney Transplant
  abstraction state.
- Per-disease variable packs beyond Alzheimer's / AAT / COPD /
  Asthma / CKD (MASH, IBD, DR) — clinical curation work per the
  registry backlog in §2.1.
- WeasyPrint PDF renderer (PR 7 in the shipping plan).
- Schema-drift detection (PR 8).
- Batch `--all` runner + combined Nimbus / Nimbus AZ views (PR 9).

## 6. Guiding principles

1. One dictionary per (provider, disease) pair.
2. One canonical model for all outputs.
3. Config first, heuristics second, empty only if both fail.
4. Audience filtering at render time, not extraction time.
5. Deterministic outputs so reruns are stable.
6. No manual editing of generated dictionary files.

## 7. Target architecture

```
pack YAML + schema introspection
        ↓
   CohortModel
        ↓
 audience filter
        ↓
   HTML render
     │  │  │
     │  │  └─► XLSX export
     │  └────► JSON export
     └───────► PDF render (WeasyPrint → fallback Chromium)
```

### 7.1 Core design calls
- Extend the existing `Pack` and `load_pack()` in `introspect_cohort.py`.
- One canonical `CohortModel`; every renderer reads from it.
- Audience filters at render time, not by branching extraction.
- Config-driven enrichment first; heuristics only as logged last-resort.
- Outputs deterministic; reruns on unchanged inputs produce identical files.

### 7.2 Renderer choice
- Preferred default: **WeasyPrint** — pure Python, deterministic, no
  Chromium/sandbox overhead. Use if it installs cleanly in the target
  environment.
- Fallback: headless Chromium for pages that need JS (none today).
- Do not lock in a renderer before environment validation.

## 8. Canonical model

Build one canonical object before any renderer runs.

```jsonc
{
  "cohort": "mtc_aat_cohort",
  "provider": "MTC",
  "disease": "AAT",
  "schema_name": "mtc__aat_cohort",
  "variant": "raw",
  "display_name": "MTC AAT",
  "description": "...",
  "generated_at": "2026-04-21T09:34:00Z",
  "git_sha": "f7118b2",
  "introspect_version": "0.4.0",
  "schema_snapshot_digest": "sha256:...",
  "summary": {
    "patient_count": 1067,
    "table_count": 15,
    "column_count": 307,
    "date_coverage": {
      "min_date": "2022-01-28",
      "max_date": "2026-02-27",
      "years_of_data": 4.1,
      "contributing_columns": [
        "visit_occurrence.visit_start_date",
        "condition_occurrence.condition_start_date"
      ]
    }
  },
  "tables":    [...],
  "columns":   [...],
  "variables": [...]
}
```

HTML, PDF, XLSX, and JSON all render from this one object.
Reproducibility stamp (`generated_at`, `git_sha`, `introspect_version`,
`schema_snapshot_digest`) is mandatory for auditability.

## 9. Config strategy

### 9.1 Cohort packs
`packs/cohorts/<cohort>.yaml` extends the existing `Pack` shape:

```yaml
provider: MTC
disease: AAT
display_name: MTC AAT
description: >
  Patients in the MTC AAT cohort with alpha-1 antitrypsin deficiency-
  related clinical activity.
variant: raw
site_group: MTC
status: active        # or: wip (skip strict validator gates)
notes:
  - Technical dictionary generated from schema introspection.
category_rules:
  demographics: { tables: [person, cohort_patients] }
  diagnosis:    { tables: [condition_occurrence] }
  medications:  { tables: [drug_exposure, infusion] }
```

### 9.2 Shared config files
- `packs/categories.yaml` — table → Category map.
- `packs/column_descriptions.yaml` — OMOP column semantics.
- `packs/table_descriptions.yaml` — table → purpose.
- `packs/abstracted/<cohort>.yaml` — columns marked `Abstracted`.
- `packs/variables/<disease>.yaml` — Page 4 clinical concepts.

### 9.3 Fill order
1. Explicit cohort or disease config.
2. Shared table/column config.
3. Last-resort heuristic.
4. Empty field only if everything above fails.

Every heuristic fill is logged so the config can absorb it later.

## 10. Audience model

### 10.1 Presets
- **technical** — Summary + Tables + Columns + Variables. No redaction.
- **sales** — Summary + Tables + Variables. PII dropped.
- **pharma** — Summary + Variables. PII dropped; no raw column inventory.

### 10.2 PII requirement
PII redaction is **not optional** for sales / pharma outputs. Any column
with `pii: true` in the canonical model is suppressed before render.
The audience presets and the PII tagging ship in the same PR.

## 11. Output contract

Every run can produce all four formats from the same model:

- `Output/<schema>_dictionary.html`
- `Output/<schema>_dictionary.pdf`
- `Output/<schema>_dictionary.xlsx`
- `Output/<schema>_dictionary.json`

Minimum sections by audience:

| Audience | Summary | Tables | Columns | Variables |
|---|---|---|---|---|
| technical | ✓ | ✓ | ✓ | ✓ |
| sales | ✓ | ✓ | — | ✓ |
| pharma | ✓ | — | — | ✓ |

## 12. Shipping order

Small, reviewable PRs that build on each other.

### PR 1 — tighten the current output
No new config. Changes:
- Exclude surrogate-key columns (`*_id`, `*_concept_id`) from
  `_compile_continuous`. Still listed in inventory; Distribution and
  Median left blank.
- Collapse empty tables (`row_count == 0`) to one summary row.
- Integer-cast numeric summaries; avoid scientific notation for
  magnitudes < 1e9.
- Raise `--sample-values` default to 20 for columns whose name matches
  `*_concept_name`; keep 5 elsewhere.
- Rename the existing `Variables` sheet to `Columns`.
- Add `Extraction Type = Abstracted` support (read from optional
  `packs/abstracted/<cohort>.yaml`).
- Auto-size XLSX columns and enable word wrap.
- Standardise output filenames: `Output/<schema>_dictionary.{xlsx,html}`.

### PR 2 — populate the blank fields
- Ship `packs/categories.yaml` (table → Category).
- Ship `packs/column_descriptions.yaml` (OMOP column semantics).
- Tag PII columns via `packs/pii.yaml` (or inline on `categories.yaml`).
- Populate `Category`, `Description`, `Criteria`, `Notes` where config exists.

### PR 3 — patient-level `% Patient`
- Add `_compile_patient_completeness` — for tables with `person_id`,
  `COUNT(DISTINCT person_id WHERE col IS NOT NULL) / total_patients`.
- Keep row-level `Completeness` alongside.
- Mark `% Patient` as `—` for tables without `person_id`.

### PR 4 — Summary + Tables sheet
- Summary adds `provider`, `disease`, `variant`, `display_name`,
  `years_of_data`, contributing-columns list.
- New `Tables` sheet: one row per table with `table_name, row_count,
  column_count, patient_count, purpose`.
- `purpose` from `packs/table_descriptions.yaml`; heuristic fallback logged.

### PR 5 — audience presets + PII redaction
- `--audience {technical,sales,pharma}` flag.
- PII redaction enforced for sales and pharma; never ship audiences
  without redaction in the same PR.

### PR 6 — canonical `CohortModel` refactor
- Consolidate dataclasses evolved through PR 1–5 into one shape.
- Emit `generated_at`, `git_sha`, `introspect_version`,
  `schema_snapshot_digest`.
- Add JSON export path.

### PR 7 — direct HTML → PDF
- `pip install weasyprint` optional dep.
- `--out-pdf` flag. Print CSS: page breaks between sheets, repeated
  table headers, page numbers, title block with provider/disease/years.
- Retire the Adobe Scan loop.

### PR 8 — validation + drift detection
- Per-cohort validation report (JSON).
- Hard-fail rules: `patient_count > 0`, tables / columns non-empty,
  `provider` and `disease` populated unless pack declares `status: wip`.
- Schema drift diff vs prior XLSX; non-zero exit on `--strict-drift`.

### PR 9 — batch runner + combined views
- `introspect_cohort.py --all` iterates `packs/cohorts/*.yaml`; per-
  cohort errors land in the validation report without killing the batch.
- Opt-in `--combine nimbus+nimbus_az` unions two schemas with source
  flag per row.

## 13. Not forced into the script yet

Real work items that shouldn't block the core pipeline from being correct:

- Clinical curation of `packs/variables/<disease>.yaml` per disease
  (alzheimers, aat, copd, asthma, ckd first).
- Resolving blocked schemas (PRINE, Southland, EHA).
- Replacing historical Adobe Scan PDFs in `Output/` until direct-render
  PDF export exists (PR 7).

## 14. Open questions for the next Sanjay sync

1. Are Page-4 variable lists disease-driven and reusable across providers
   (one `alzheimers.yaml` for MTC + RMN), or provider-specific?
2. For multi-disease providers (MTC, Nimbus, Nimbus AZ), one workbook
   per disease or any combined provider workbook?
3. Does `years_of_data` use all relevant clinical dates, or only an
   approved subset (e.g. `visit_start_date` only)?
4. For abstracted variables with no data yet, show placeholder rows with
   `Implemented: No`, or hide until populated?
5. For WIP/blocked cohorts, ship partial dictionaries with
   `status: wip` or wait for the schema?
6. Will Nimbus and Nimbus AZ eventually merge, or stay permanently separate?
7. Does Nira MG truly reuse `nira_ms_cohort`, or is that a draft
   placeholder mapping that needs correcting?

## 15. Acceptance criteria

The program is good enough when:

- PDFs are rendered directly from HTML — no browser chrome, no OCR
  corruption.
- Every cohort output has Summary, Tables, Columns, Variables.
- `provider`, `disease`, `years_of_data`, `% Patient` populate automatically.
- `Category` and `Description` come from config, not manual fill-in.
- Filenames follow `Output/<schema>_dictionary.{xlsx,html,pdf,json}`.
- One CLI path runs any cohort; per-cohort differences live in
  `packs/cohorts/<name>.yaml` + `packs/variables/<disease>.yaml`.
- HTML, PDF, XLSX, JSON all render from one `CohortModel`.
- PII is redacted in sales and pharma outputs.
- Reruns on unchanged inputs are byte-identical (deterministic).
- Drift detection flags schema changes since the last run.
- Tests cover canonical model, pack loading, audience filters, PII redaction.

## 16. Source files referenced

- `century/Data dictionary.pdf` — reference layout (MTC Alzheimers).
- `century/ask.pdf` — primary ask.
- `century/Adobe Scan 19 Apr 2026.pdf` — `clinical` schema list.
- `Output/*.pdf` — current raw cohort dumps (OCR'd scans).
- `introspect_cohort.py` — generator being extended.
- `tests/test_introspect_cohort.py` — test suite to extend.

## 17. Final planning note

The main technical gap is no longer schema introspection — that already
works. The main gap is the transition from raw inventory to:

- a canonical model
- config-driven enrichment
- audience-aware rendering
- direct, validated document export

That is the work this plan sequences.
