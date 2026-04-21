# Century data dictionaries — plan

Single source of truth for answering `century/ask.pdf` ("Clinical
Registries to Schemas") and for the engineering work that turns
`introspect_cohort.py` into a reusable cohort-dictionary pipeline.

---

## 1. The ask (from `century/ask.pdf`)

Four open questions and one concrete deliverable list:

1. **Do we build a data dictionary per provider and per disease?**
   Direction: **one dictionary per (provider, disease) pair**. Start there.
2. **How do we handle multi-site providers with different schemas
   (Nimbus vs Nimbus AZ)?**
   Direction: separately for now; revisit consolidation later.
3. **How do we build DD's for sales, data scientists, pharma customers
   separately?**
   Direction: one four-page format shared across audiences, with
   audience-specific visibility rules at render time:
   - Page 1 → Summary (patients, years of data, tables, columns)
   - Page 2 → Tables
   - Page 3 → Columns and descriptions
   - Page 4 → Variables (config-driven, clinical concepts)
4. **How can we automate the whole process?**
   Direction: Onkar to develop. Backbone exists in
   `introspect_cohort.py`; this plan tracks the gaps.

## 2. Registries to deliver

Source: `century/ask.pdf` + schema list from
`century/Adobe Scan 19 Apr 2026.pdf`.

| # | Provider | Disease | Schema | Priority | Raw dump in `Output/` | Notes |
|---|---|---|---|---|---|---|
| 1 | Nira | MS (1 site) | `ms_leaf_nira_registry` | Medium | — | First site we started with |
| 2 | Nira | MS (all sites) | `nira_ms_cohort` | Medium | — | |
| 3 | Nira | MG (all sites) | `nira_ms_cohort` | Low | — | Same schema as MS |
| 4 | Nimbus | COPD | `nimbus_copd_curated` | High | `minbuscopdcurated.pdf` | Consolidate with Nimbus AZ? |
| 5 | Nimbus | Asthma | `nimbus_asthma_curated` | High | `nimbusasthmacurated.pdf` | |
| 6 | Nimbus AZ | COPD | `nimbus_az_copd_cohort` | High | `nimbusazcopd.pdf` | Abstracted vars (FEV1 etc.) in progress |
| 7 | Nimbus AZ | Asthma | `nimbus_az_asthma_cohort` | High | `nimbusazasthma.pdf` | |
| 8 | Balboa | Renal | `balboa_ckd_cohort` | High | `balboackd.pdf` | |
| 9 | MTC | Alzheimers | `mtc_alzheimers_cohort` | High | `mtcalzhiemer.pdf` | |
| 10 | MTC | AAT | `mtc_aat_cohort` | High | `mtcaat.pdf` | Anti-amyloid therapies |
| 11 | Newtown | MASH | `newtown_mash_cohort` | High | — | |
| 12 | Newtown | IBD | `newtown_ibd_cohort` | Low | — | |
| 13 | DRG | Renal | `drg_ckd_cohort` | High | `drgckd.pdf` | |
| 14 | PRINE | Renal | TBD | — | — | Schema not yet provisioned |
| 15 | Rocky Mountain Neurology | Alzheimers | `rmn_alzheimers_cohort` | — | — | |
| 16 | Southland Neurologic Institute | TBD | TBD | — | — | |
| 17 | Eye Health America (EHA) | TBD | TBD | High | — | |
| 18 | RVC | DR | `rvc_dr_curated` | — | — | |

8 of 18 have a raw introspect dump. 10 still to run (3 blocked on
schema provisioning).

## 3. What the existing `Output/*.pdf` tell us

Every file is an iPhone Adobe Scan of the HTML that
`introspect_cohort.py` already emits. The dumps share the same two-sheet
shape (Summary + Variables).

| File | Cohort | Patients | Tables | Columns |
|---|---|---:|---:|---:|
| `mtcaat.pdf` | `mtc_aat_cohort` | 1,067 | 15 | 307 |
| `mtcalzhiemer.pdf` | `mtc_alzheimers_cohort` | 3,753 | 14 | 256 |
| `balboackd.pdf` | `balboa_ckd_cohort` | 81,183 | 11 | 255 |
| `drgckd.pdf` | `drg_ckd_cohort` | 53,213 | 9 | 226 |
| `nimbusazcopd.pdf` | `nimbus_az_copd_cohort` | 6,710 | 14 | 300 |
| `nimbusazasthma.pdf` | `nimbus_az_asthma_cohort` | ~6,700 | ~14 | ~300 |
| `minbuscopdcurated.pdf` | `nimbus_copd_curated` | 7,233 | 12 | 253 |
| `nimbusasthmacurated.pdf` | `nimbus_asthma_curated` | ~260 | ~10 | ~243 |

### Gaps catalogued from the PDFs

**Export path**
- The PDFs contain browser chrome, URL-bar text, and OCR-like
  corruption. Root cause: iPhone scan-to-PDF loop, not a clean
  render. This is the biggest quality problem. One-step HTML →
  direct PDF render fixes it.

**Content**
- `Category`, `Description`, `Criteria`, `Notes` are blank everywhere.
  `introspect_cohort.py` hardcodes `""` for these at
  `introspect_cohort.py:736` (XLSX) and `:819` (HTML).
- `Implemented` and `% Patient` columns don't exist. Script emits
  row-level `Completeness` instead. Reference reports
  patient-level. Real semantic gap for drug_exposure / measurement
  where one patient has many rows.
- Summary lacks provider, disease, years of data, site notes, and
  curated-vs-raw flag.

**Representation**
- Every physical column is its own row (255–307 rows per cohort).
  That's Page 3 of the ask. Page 4 (clinical-concept rows) is
  missing entirely.
- No Tables sheet. `TableInfo` is collected at
  `introspect_cohort.py:266` but never written.
- No years-of-data rollup. `_compile_date_range` at
  `introspect_cohort.py:505` runs per-column but isn't aggregated.

**Data quality on the page**
- Surrogate-key columns (`person_id`, `*_occurrence_id`,
  `*_concept_id`) get Min/Max/Mean/Median in scientific notation
  (`9.22e+18`, `IQR: 2.31e+18–6.9e+18`). They're IDs, not measurements.
- `Values` truncates at 60 chars mid-sentence — fine for HTML,
  needlessly lossy for XLSX.
- Top-5 categorical depth is too shallow for
  `*_concept_name` columns; the long tail of clinical concepts matters.
- Empty tables still emit 20+ rows with blank summaries.
- `dv_tokenized_profile_data` contributes 111 `token_*` columns
  to `mtc_aat` that pollute the inventory.
- `standard_profile_data_model` exposes `first_name`, `last_name`,
  `email`, `cellphone`, `date_of_birth`, `address1` without a PII
  flag. Blocking for sales / pharma outputs.

**Presentation**
- File naming is inconsistent (`minbuscopdcurated.pdf` for Nimbus,
  `mtcalzhiemer.pdf` typo). Automation can't rely on it.
- XLSX column widths aren't auto-sized; distribution cells wrap
  awkwardly.
- `Extraction Type` is two-way (Structured / Unstructured). Reference
  uses three (Structured / Abstracted / Unstructured).

## 4. Target architecture

Turn the generator into a **single-source-of-truth pipeline**:

```
pack YAML + schema introspection
        ↓
CohortModel (canonical dataclass / JSON)
        ↓
       ┌──────── audience filter ─────────┐
       ↓                                  ↓
   HTML render                     XLSX / JSON export
       ↓
  WeasyPrint → PDF
```

Key design calls:

- **Extend the existing `Pack` dataclass in `introspect_cohort.py`**
  (`load_pack`, `packs/cohorts/*.yaml`). Don't greenfield a new config
  layer — the loader exists; add fields (`provider`, `disease`,
  `display_name`, `description`, `variant`, `site_group`,
  `notes`, `category_rules`).
- **One canonical `CohortModel`**, reused by every renderer. HTML,
  PDF, XLSX, JSON all emit from the same object. No parallel
  formatting codepaths.
- **Audience filters apply at render time**, not by branching the
  pipeline. `--audience {technical|sales|pharma}` hides rows /
  sheets from the canonical model.
- **WeasyPrint, not Chromium.** The dictionary is static HTML + CSS;
  WeasyPrint is pure Python, deterministic, no sandbox config, no
  300 MB Chromium download. Keep `--renderer=chromium` as a fallback
  for pages with JS charts (none today).
- **Config-driven first; heuristics last.** Column-name heuristics
  will misclassify (`observation_type_concept_name` matches both
  "observation" and "type"). Ship categories/descriptions as static
  YAML first; heuristics only as last-resort fallback, and log every
  heuristic hit so the pack can absorb them.

## 5. Shipping order

Nine PRs, each standalone and reviewable in isolation:

### PR 1 — tighten current output (no new deps, no new config)
- Exclude surrogate-key columns (`*_id`, `*_concept_id`) from
  `_compile_continuous`. Still listed in inventory; Distribution /
  Median left blank.
- Collapse empty tables (`row_count == 0`) to one summary row each.
- Integer-cast numeric summaries; avoid scientific notation for
  magnitudes < 1e9.
- Raise `--sample-values` default to 20 for columns whose name
  matches `*_concept_name`; keep 5 elsewhere.
- Add `Extraction Type = Abstracted` support (reads from optional
  `packs/abstracted/<cohort>.yaml`).
- Rename the existing `Variables` sheet to `Columns` to match the
  ask wording.
- Auto-size XLSX columns and enable word-wrap on long cells.
- Standardise output filenames: `Output/<schema>_dictionary.xlsx`.

### PR 2 — populate the currently-blank columns
- Ship `packs/categories.yaml` mapping table → Category:
  ```
  person, location, payer_plan_period:  Demographics
  condition_occurrence:                 Diagnosis
  drug_exposure, infusion:              Medications
  measurement:                          Labs / Biomarkers
  observation:                          Observations
  procedure_occurrence:                 Procedures
  visit_occurrence:                     Visits
  note, document:                       Reports
  standard_profile_data_model:          Profile (PII)
  dv_tokenized_profile_data:            Tokenized (derived)
  cohort_patients:                      Cohort
  ```
- Ship `packs/column_descriptions.yaml` with OMOP-standard
  descriptions for common columns. Fall back to empty for unknowns.
- Flag PII rows. Tag any column in `standard_profile_data_model`
  (or a PII allowlist) with `pii: true` in the canonical model so
  the audience filter can drop them cleanly.

### PR 3 — patient-level completeness
- Add `_compile_patient_completeness`: for each table with
  `person_id`, compute
  `COUNT(DISTINCT person_id WHERE col IS NOT NULL) / total_patients`.
- Emit new `% Patient` column alongside `Completeness`.
- Tables without `person_id` fall back to row-level `Completeness`
  and mark `% Patient` as `—`.

### PR 4 — four-page Summary + Tables
- Summary adds `years_of_data` (max-min across
  `visit_start_date`, `condition_start_date`,
  `drug_exposure_start_date`, `measurement_date` — whichever exist),
  `provider`, `disease`, `variant`, `display_name` from the pack.
- New `Tables` sheet: one row per table with
  `table_name, row_count, column_count, patient_count, purpose,
  extraction_hint`. Purpose from
  `packs/table_descriptions.yaml`; fallback to table-name heuristic
  with a stderr warning.

### PR 5 — PII redaction + audience presets (together)
- `--audience technical` (default): no redaction, all sheets.
- `--audience sales`: drop `pii: true` rows; strip `Columns` sheet;
  keep Summary + Tables + Variables.
- `--audience pharma`: drop `pii: true` rows; keep Summary + Variables
  only. Never ship audiences without PII redaction in the same PR.

### PR 6 — canonical `CohortModel` refactor
Consolidate the dataclasses that evolved through PR 1–5 into one
shape. Adds reproducibility stamp: `generated_at`, `git_sha`,
`introspect_version`, `schema_snapshot_digest`.

Shape:
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
  "tables": [...],
  "columns": [...],
  "variables": [...]
}
```

### PR 7 — WeasyPrint PDF renderer + print CSS
- `pip install weasyprint` as an optional dep.
- New `--out-pdf` CLI flag. Uses the existing HTML output as input.
- Print CSS: section breaks between sheets, repeated table headers,
  page numbers, title block with provider / disease / years of data.
- Removes the iPhone-scan step from the loop entirely.

### PR 8 — validation + schema-drift + WIP waivers
- Validator runs before export. Hard-fail rules:
  `patient_count > 0`, `tables` non-empty, `columns` non-empty,
  `provider` and `disease` populated **unless** pack declares
  `status: wip`.
- Schema-drift check: if a prior XLSX exists for the same cohort,
  diff the `Columns` sheet and warn on added / removed /
  type-changed columns. Non-zero exit on drift if
  `--strict-drift` is set.
- Validator writes a machine-readable JSON report alongside the
  XLSX so the batch runner can aggregate failures.

### PR 9 — batch runner + combined views
- `introspect_cohort.py --all` iterates over `packs/cohorts/*.yaml`,
  writes one workbook per cohort. Per-cohort errors don't kill the
  batch; they land in the validation report.
- Opt-in `--combine nimbus+nimbus_az` unions the two schemas into
  one dictionary, flagging each row with source schema.

## 6. What won't fit in the script

- Writing `packs/variables/<disease>.yaml` for each disease is
  clinical-curation work (alzheimers, aat, copd, asthma, ckd first).
  Variable packs declare the Page-4 clinical concepts and their
  `{table, column, criteria}` filters. Reused across providers with
  the same disease.
- Populating the TBD schemas (PRINE, Southland, EHA). Blocked on
  upstream provisioning.
- Replacing the Adobe Scan PDFs in `Output/` with the direct-render
  PDFs once PR 7 ships.

## 7. Open questions for the next Sanjay sync

1. Are Page-4 variable lists disease-driven (same `alzheimers.yaml`
   shared by MTC Alzheimers and RMN Alzheimers) or provider-specific?
2. For multi-disease providers (MTC Alzheimers + AAT, Nimbus COPD +
   Asthma), one workbook each or a combined one?
3. Does "years of data" take the min/max across all clinical dates,
   or only from a designated table (e.g. `visit_occurrence`)?
4. Abstracted variables with 0% completeness today — show as
   placeholder rows with `Implemented: No`, or hide until populated?
5. For WIP cohorts (PRINE, Southland, EHA) — do we ship partial
   dictionaries with `status: wip`, or wait for the schema?
6. Will Nimbus + Nimbus AZ eventually be merged into one dictionary?

## 8. Acceptance criteria

The improved program is good enough when:

- PDFs are rendered directly from HTML with no browser chrome or OCR
  corruption.
- Every cohort output has Summary + Tables + Columns + Variables
  sections.
- `years_of_data`, `provider`, `disease`, `% Patient` are populated
  automatically.
- `Category` and `Description` come from config, not manual fill-in.
- Filenames follow `Output/<schema>_dictionary.{xlsx,html,pdf,json}`.
- The same CLI invocation runs on any cohort; differences live in
  `packs/cohorts/<name>.yaml` + `packs/variables/<disease>.yaml`.
- HTML, PDF, XLSX, and JSON render from one `CohortModel` — no
  parallel format codepaths.
- PII columns are redacted in sales / pharma audience outputs.
- Running the generator twice on unchanged data produces identical
  outputs (deterministic), and drift detection flags any schema
  changes since the last run.
- Test suite covers the canonical model, the pack loader,
  PII redaction, and audience filtering.

## 9. Source files referenced

- `century/Data dictionary.pdf` — reference layout (MTC Alzheimers).
  Target look for Page 3 + Page 4.
- `century/ask.pdf` — the ask. (Duplicate: `century/century health
  ask.pdf`.)
- `century/Adobe Scan 19 Apr 2026.pdf` — `clinical` database schema list.
- `Output/*.pdf` — OCR'd scans of the HTML outputs
  `introspect_cohort.py` has already produced (8 of 18 cohorts).
- `introspect_cohort.py` — the generator being extended.
- `packs/cohorts/*.yaml` — existing cohort configs; extend in PR 1.
- `tests/test_introspect_cohort.py` — existing test suite; extend per
  PR.
