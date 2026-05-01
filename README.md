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
| 11 | Newtown | MASH | `newtown_mash_cohort` | High | `newtown mash.pdf` | Packs committed; awaiting live build | |
| 12 | Newtown | IBD | `newtown_ibd_cohort` | Low | `newton ibd.pdf` | Packs committed; awaiting live build | Filename has "newton" typo; schema uses correct "newtown" |
| 13 | DRG | Renal | `drg_ckd_cohort` | High | `drgckd.pdf` | Packs committed; awaiting live build | |
| 14 | PRINE | Renal | TBD | — | — | Blocked | Schema not yet provisioned |
| 15 | Rocky Mountain Neurology | Alzheimer's | `rmn_alzheimers_cohort` | — | `rmn alzheimers.pdf` | Packs committed; awaiting live build | |
| 16 | Southland Neurologic Institute | TBD | TBD | — | — | Blocked | Disease and schema unconfirmed |
| 17 | Eye Health America (EHA) | TBD | TBD | High | — | Blocked | Disease and schema unconfirmed |
| 18 | RVC | DR | `rvc_dr_curated` | — | `rvc dr.pdf` | Packs committed; awaiting live build | |
| 19 | RVC | AMD | `rvc_amd_curated` | — | `rvc amd.pdf` | Packs committed; awaiting live build | Added during the retinal pack buildout alongside RVC DR |

### 2.2 Backlog summary

- 13 of 19 have a raw introspection dump in `Output/`.
- All 13 of those also have committed `packs/cohorts/*.yaml` + variable
  packs (MTC AAT, MTC Alzheimer's, Nimbus COPD / Asthma, Nimbus AZ
  COPD / Asthma, Balboa CKD, DRG CKD, RMN Alzheimer's, Newtown MASH,
  Newtown IBD, RVC DR, RVC AMD). The 11 non-MTC cohorts are awaiting
  their first live build + clinical Variables-sheet review.
- 3 more are runnable once the cohort dump is generated
  (Nira MS / MS-all / MG — missing dump).
- 3 are blocked on upstream schema provisioning or unresolved metadata
  (PRINE, Southland, EHA).

## 3. Verified baseline from `Output/*.pdf`

Raw introspection dumps under `Output/` — each is a scan / HTML-render
archive of `introspect_cohort.py --schema <name>` output against the
live warehouse. Used as evidence for pack curation. Counts verified
against the Summary pages.

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
| `rmn alzheimers.pdf` | `rmn_alzheimers_cohort` | (per dump) | 11 | (per dump) |
| `newtown mash.pdf` | `newtown_mash_cohort` | (per dump) | 12 | (per dump) |
| `newton ibd.pdf` | `newtown_ibd_cohort` | (per dump) | 11 | (per dump) |
| `rvc dr.pdf` | `rvc_dr_curated` | (per dump) | 12 | (per dump) |
| `rvc amd.pdf` | `rvc_amd_curated` | (per dump) | 12 | (per dump) |

The last five rows were added after the `scripts/dump_new_schemas.py`
sweep and drove the MASH / IBD / DR / AMD / RMN Alzheimer's pack
curation. Patient / column counts for those five are in the PDFs
themselves; they're not repeated here because the numbers are
site-specific and shift between pulls.

## 4. Current generator output

`build_dictionary.py --cohort <slug>` writes up to three deliverables
per cohort. `--formats` accepts any combination of `xlsx`, `html`,
`json` (default = all three):

- `Output/<schema>_dictionary.xlsx` — four-sheet workbook
- `Output/<schema>_dictionary.html` — single-page HTML, same shape
- `Output/<schema>_dictionary.json` — canonical model dump
  (suppressed for `--audience customer` — JSON is an internal
  debug artifact, customers consume xlsx/html)

PDF output is not currently produced by the generator; the WeasyPrint
renderer is tracked in §5.3.

### 4.1 Workbook shape

Every workbook ships the same four sheets. Layout follows the
Flatiron-style data-dictionary convention — clinical Description and
Inclusion Criteria lead each table; observed-data signals follow.

| Sheet | Rows | Columns |
|---|---|---|
| Summary | ~17 key/value | provider, disease, patient_count, table_count, column_count, date coverage, years_of_data, generated_at, git_sha, schema_snapshot_digest |
| Tables | one per warehouse table | Table, Category, Description, Inclusion Criteria, Data Source, Source Table, Rows, Columns, Patients |
| Columns | one per physical column | Category, Table, Column, Description, Field Type, Nullable, Example, Coding Schema, Values, Distribution, Median (IQR), Completeness, % Patient, Data Source, PII, Notes |
| Variables | one per clinical concept in the cohort's variable pack | Category, Variable, Description, Inclusion Criteria, Table(s), Column(s), [Criteria — technical + customer], Field Type, Example, Coding Schema, Values, Distribution, Median (IQR), Completeness, Implemented, % Patient, Data Source, Notes |

`Data Source` uses the Flatiron typology — Normalized / Derived /
Abstracted / NLP / Enhanced — derived from each row's
`extraction_type` plus an allowlist of curated tables in
`build_dictionary.derive_data_source`. Pack rows can override per-row
with an explicit `data_source:` key.

`Inclusion Criteria` (prose) renders for every audience. The
`Criteria` column (the SQL or `match: { values: [...] }` matcher)
shows for `technical` and `customer` audiences — config-owned exact
matches were the headline reviewer ask — and is hidden for `sales`
and `pharma`. See `packs/STYLE.md` for the prose-quality bar each
customer-visible string must meet.

All fields populate from the canonical `CohortModel` — nothing is
hardcoded-empty any more.

### 4.2 Styling

- XLSX: styled navy/white header row, frozen top row + auto-filter
  on the three data sheets (Summary stays plain as key/value), tuned
  column widths per header name.
- HTML: sticky `<thead>`, modern system-ui font stack, zebra
  striping, hover highlights, summary card, print-friendly CSS.
- PII flagged on the Columns sheet (and dropped from sales / pharma
  audience outputs, per `packs/pii.yaml`).

### 4.3 Audience filtering

Four audiences, each with a different section-visibility rule
(`AUDIENCE_VISIBILITY` in `build_dictionary.py`):

| Audience | Summary | Tables | Columns | Variables |
|---|---|---|---|---|
| technical | ✓ | ✓ | ✓ | ✓ |
| sales | ✓ (trimmed) | — | — | ✓ (Tempus-style) |
| pharma | ✓ | — | — | ✓ |
| customer | ✓ (trimmed) | ✓ (trimmed) | ✓ (trimmed) | ✓ (trimmed) |

Run with `--audience sales` or `--audience pharma` to switch; PII-
flagged variables are dropped from the Variables sheet in both
non-technical audiences.

### 4.4 Data-quality filters baked into the generator

- Surrogate keys (`person_id`, `*_occurrence_id`, `*_concept_id`)
  are excluded from the continuous summary — their Median (IQR) /
  Distribution cells stay empty instead of reporting
  `9.22e+18 (IQR: 2.3e+18–6.9e+18)`.
- Unstructured / free-text columns (`note_text`, `*_text`, `*_note`)
  are aggregated as row counts only — no `GROUP BY` over free text.
- `*_concept_name` columns sample 20 distinct values (vs. 5 for
  ordinary categoricals) so reviewers see the long tail.
- Empty tables collapse: one row per column with zero counts
  instead of a scientific-notation per-column summary.

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
  - `packs/cohorts/rmn_alzheimers.yaml`,
    `packs/cohorts/newtown_mash.yaml`,
    `packs/cohorts/newtown_ibd.yaml`,
    `packs/cohorts/rvc_dr_curated.yaml`,
    `packs/cohorts/rvc_amd_curated.yaml`
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
      in scope; refactor to introduce one when AKI lands),
      `packs/variables/mash_common.yaml` (standalone hepatology base),
      `packs/variables/ibd_common.yaml` (standalone gastroenterology base),
      `packs/variables/retinal_common.yaml` (shared ophthalmology base),
      `packs/variables/dr_common.yaml` (includes `retinal_common`),
      `packs/variables/amd_common.yaml` (includes `retinal_common`)
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
      `packs/variables/drg_ckd.yaml` (includes `ckd_common`, plus a
      cohort-specific procedure-coded Smoking Status row),
      `packs/variables/rmn_alzheimers.yaml` (includes
      `alzheimers_common`, placeholder),
      `packs/variables/newtown_mash.yaml` (includes `mash_common`,
      placeholder),
      `packs/variables/newtown_ibd.yaml` (includes `ibd_common`,
      placeholder),
      `packs/variables/rvc_dr_curated.yaml` (includes `dr_common`,
      placeholder),
      `packs/variables/rvc_amd_curated.yaml` (includes `amd_common`,
      placeholder)
  - `packs/categories.yaml`, `packs/pii.yaml`,
    `packs/table_descriptions.yaml`, `packs/column_descriptions.yaml`
- `scripts/validate_packs.py` + `VALIDATION_REPORT.md` — static
  pack-lint covering duplicate variables, unknown categories, unsafe
  ILIKE, missing criteria on clinically-specific rows, and column
  vs variable-name mismatches (e.g. `_concept_id` column under a row
  whose name doesn't mention "ID").
- `century-dictionary.zip` — self-contained runtime bundle at the
  repo root. ~165 KB, contains everything a server needs to run both
  the generator **and** the offline exact-match discovery tool:
  the legacy + v2 build entrypoints (`build_dictionary.py`,
  `dictionary_v2/build_dictionary.py`), the discovery script
  (`dictionary_v2/discover_exact_matches.py`), the introspection
  backbone, the validator, `packs/` (incl. `dictionary_layout.yaml`),
  `requirements.txt`, and `BUNDLE_README.md` describing the
  discover → apply → build workflow. No tests, no reference PDFs,
  no historical design docs. Rebuild with
  `bash scripts/build_runtime_bundle.sh`. Extract, `pip install -r
  requirements.txt`, fill in `.env` from `.env.example`, and
  `python dictionary_v2/build_dictionary.py --cohort <slug>
  --audience customer` runs.
- `scripts/dump_new_schemas.py` — raw-dump helper for new schemas
  that haven't been mined into a cohort pack yet. Hands each schema
  name to `introspect_cohort.py --schema`, which walks the warehouse
  without needing a cohort pack. Output lands under
  `Output/raw/<schema>/` (gitignored — dumps carry real warehouse
  distributions).

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

HTML, XLSX, and JSON all render from this one object today; the
PDF renderer (PR 7, WeasyPrint) plugs into the same shape.
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
- **technical** — Summary + Tables + Columns + Variables. No redaction. Raw SQL `Criteria` shown.
- **sales** — Tempus-style single-sheet spec. Summary cover plus
  a Variables sheet with exactly:
  `Category | Variable | Description | Value Sets | Notes | Type |
  Proposal | Completeness`. Tables and Columns sheets are not
  produced. PII rows dropped. `Value Sets` (newline-separated
  clinical reference values) and `Proposal` (Standard / Custom)
  are authored per row in the variables YAML
  (`value_set:`, `proposal:`); rows that aren't curated yet
  render those cells empty.
- **pharma** — Summary + Variables. PII dropped; no raw column inventory. SQL `Criteria` hidden.
- **customer** — All four sheets but trimmed (drops debug summary
  fields, internal scaffolding tables, PII; trims Columns to
  Table / Column / Description / Field Type). Keeps the configured
  `Criteria` column side-by-side with prose `Inclusion Criteria`
  per reviewer feedback. JSON output is suppressed (customer
  consumes xlsx/html). The main stakeholder-facing run mode.

### 10.2 PII requirement
PII redaction is **not optional** for sales / pharma outputs. Any column
with `pii: true` in the canonical model is suppressed before render.
The audience presets and the PII tagging ship in the same PR.

## 11. Output contract

Every run can produce three formats from the same model. PDF
rendering is future work (see §5.3 / PR 7):

- `Output/<schema>_dictionary.html`
- `Output/<schema>_dictionary.xlsx`
- `Output/<schema>_dictionary.json` (suppressed for `--audience customer`)

Minimum sections by audience:

| Audience | Summary | Tables | Columns | Variables |
|---|---|---|---|---|
| technical | ✓ | ✓ | ✓ | ✓ |
| sales | ✓ (trimmed) | — | — | ✓ (Tempus-style) |
| pharma | ✓ | — | — | ✓ |
| customer | ✓ (trimmed) | ✓ (trimmed) | ✓ (trimmed) | ✓ (trimmed) |

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

- Every cohort output has Summary, Tables, Columns, Variables.
- `provider`, `disease`, `years_of_data`, `% Patient` populate automatically.
- `Category` and `Description` come from config, not manual fill-in.
- Filenames follow `Output/<schema>_dictionary.{xlsx,html,json}` —
  PDF rendering is tracked in §5.3 (PR 7, WeasyPrint, future work).
- One CLI path runs any cohort; per-cohort differences live in
  `packs/cohorts/<name>.yaml` + `packs/variables/<disease>.yaml`.
- HTML, XLSX, JSON all render from one `CohortModel`.
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
