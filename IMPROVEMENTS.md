# Improvement Plan for `Output/*.pdf`

Review of the eight introspect dumps in `Output/` against the reference
layout in `century/Data dictionary.pdf` and the ask in `century/ask.pdf`.

## 1. What's in `Output/` today

Every file is a scan-to-PDF of the HTML that `introspect_cohort.py`
already emits. Same two-sheet layout: **Summary** (4 metrics) and
**Variables** (one row per physical column of every accessible table).

| File | Cohort | Patients | Tables | Columns |
|---|---|---|---:|---:|
| `mtcaat.pdf` | `mtc_aat_cohort` | 1,067 | 15 | 307 |
| `mtcalzhiemer.pdf` | `mtc_alzheimers_cohort` | 3,753 | 14 | 256 |
| `balboackd.pdf` | `balboa_ckd_cohort` | 81,183 | 11 | 255 |
| `drgckd.pdf` | `drg_ckd_cohort` | 53,213 | 9 | 226 |
| `nimbusazcopd.pdf` | `nimbus_az_copd_cohort` | 6,710 | 14 | 300 |
| `nimbusazasthma.pdf` | `nimbus_az_asthma_cohort` | ~6,700 | ~14 | ~300 |
| `minbuscopdcurated.pdf` | `nimbus_copd_curated` | 7,233 | 12 | 253 |
| `nimbusasthmacurated.pdf` | `nimbus_asthma_curated` | ~260 | ~10 | ~243 |

## 2. Gaps the PDFs reveal (common across all 8)

### Content gaps
1. **`Category` is blank everywhere.** The introspect script doesn't
   classify a row. Reviewer has to hand-fill. ⚠️ script-fixable.
2. **`Description` is blank everywhere.** Same — no built-in source.
   OMOP table+column semantics are well-known and stable; we can ship a
   static map.
3. **`Criteria` is blank everywhere.** Only meaningful once we emit
   concept-keyed variable rows (see §3), because a physical column
   has no `WHERE` clause by definition.
4. **`Implemented` and `% Patient` columns don't exist.** Reference
   PDF has them; our output has `Completeness` instead. `Completeness`
   is row-level (non-null rows / total rows). `% Patient` is
   patient-level (distinct `person_id` with ≥1 non-null value / total
   patients). Reference reports patient-level — we report row-level.

### Representation gaps
5. **Every physical column is its own row.** 307 rows for `mtc_aat`,
   255–307 across the other cohorts. Reviewers asked for a
   clinically-organised Page 4 (e.g. one row for *Blood Pressure*
   that points into `observation` with a `concept_name` criterion).
   What we emit is Page 3 from the ask (Columns sheet), not Page 4
   (Variables sheet).
6. **No Tables sheet.** `introspect_cohort.py` has a `TableInfo` list
   internally but doesn't write it. Page 2 of the four-page spec is
   missing.
7. **No `# of years of data` in Summary.** Ask specifies it. We emit
   cohort / patient_count / table_count / column_count only.

### Data-quality gaps visible on the page
8. **ID columns get Min/Max/Mean/Median.** `person_id`, `condition_occurrence_id`,
   `visit_occurrence_id`, `drug_exposure_id` show up as continuous with
   scientific notation like `9.22e+18` and an IQR like
   `(IQR: 2.31e+18–6.9e+18)`. These are surrogate keys, not measurements.
   The row should exist (inventory) but the Distribution / Median cells
   should be blank.
9. **`Values` column is truncated at 60 chars.** Introspect does
   `display = value if len(value) <= 60 else value[:57] + "..."`.
   That's OK for HTML, bad for XLSX where there's no column constraint.
10. **Top-5 only for categorical columns.** Configurable at run time
    (`--sample-values`) but defaults to 5. For clinical concept columns
    (`condition_concept_name`, `drug_concept_name`, `measurement_concept_name`,
    `observation_concept_name`, `procedure_concept_name`) the long tail matters;
    5 is too shallow.
11. **Empty tables still emit 20+ rows.** Tables with `row_count = 0`
    (e.g. several `_source_value` / `_status` variants) produce a row per
    column with blank summaries. Should fold them into a one-line
    "empty table" note.
12. **Free-text / tokenized columns pollute the inventory.**
    `dv_tokenized_profile_data` alone contributes 111 `token_*` columns
    in `mtc_aat`. Either group them as one row ("111 token columns,
    tokenized DV profile") or hide under a "derived" category.
13. **PII columns are just listed in-line.** `first_name`, `last_name`,
    `cellphone`, `date_of_birth`, `address1`, `email` in
    `standard_profile_data_model` appear alongside clinical columns with
    no flag. Sales / pharma copies must not carry these.
14. **`Extraction Type` vocabulary is two-way (Structured / Unstructured).**
    Reference PDF uses a three-way vocabulary (Structured / Abstracted /
    Unstructured). Abstracted = NLP-extracted with manual validation
    (FEV1, MoCA, cognitive scales). Current code picks Unstructured for
    `text` types and Structured otherwise.

### Presentation gaps
15. **Output is scan-to-PDF via iPhone Adobe Scan.** Not reproducible, not
    searchable, text is lossy under OCR. The script already emits XLSX +
    HTML. Adding a direct-to-PDF render (e.g. WeasyPrint / Playwright)
    would make the loop one-step.
16. **Column IDs lack readable formatting.** `9.22e+18` for a bigint
    concept id, `4.26e+05` for a row count. Render integers as integers,
    and treat concept_ids as categorical / opaque.
17. **Column widths in XLSX aren't auto-sized.** Distribution cells wrap
    awkwardly because the column is narrow.

## 3. Program-actionable improvement steps

Each is a discrete change to `introspect_cohort.py` (or a small sibling
script). Ordered by impact, grouped so that Phase 1–3 can each ship as
a standalone PR.

### Phase 1 — tighten the current output (pure script changes)

1. **Exclude surrogate-key columns from numeric summarization.**
   Skip `_id` / `_concept_id` columns from `_compile_continuous`; still
   list the row in the inventory, but leave Distribution / Median blank.
   *(files: `introspect_cohort.py` — `_classify_metric_kind`, guard list)*
2. **Collapse empty tables.** If `row_count == 0`, emit one "table is
   empty" row instead of one per column. *(files: `introspect` loop in
   `introspect_cohort.py`)*
3. **Integer-cast numeric summaries.** When all observed values are
   integers (no decimals), print Min/Max/Mean/Median as int; never use
   scientific notation unless magnitude > 1e9 and the column is a count.
   *(files: `_fmt_num`)*
4. **Raise `--sample-values` default to 20 for concept_name columns.**
   Keep 5 for everything else. *(files: `introspect`, `_compile_top_values`)*
5. **Add `Extraction Type = Abstracted` path.** Read a per-cohort list of
   abstracted column names from an optional pack
   (`packs/abstracted/<cohort>.yaml`) and emit `Abstracted` for those.
   *(files: `write_curated_xlsx`, `write_curated_html`)*
6. **Rename the `Variables` sheet to `Columns`.** Matches the ask's
   Page-3 wording. *(files: `write_curated_xlsx`, `write_curated_html`)*
7. **Auto-size XLSX columns and enable word wrap on long cells.**
   *(files: `write_curated_xlsx`, after `to_excel` apply `ws.column_dimensions`)*

### Phase 2 — populate the currently-blank columns

8. **Auto-fill `Category` from table name.**
   Ship a `packs/categories.yaml` mapping:
   ```
   person:                     Demographics
   location, payer_plan_period: Demographics
   condition_occurrence:        Diagnosis
   drug_exposure, infusion:     Medications
   measurement:                 Labs / Biomarkers
   observation:                 Observations
   procedure_occurrence:        Procedures
   visit_occurrence:            Visits
   note, document:              Reports
   standard_profile_data_model: Profile (PII)
   dv_tokenized_profile_data:   Tokenized (derived)
   cohort_patients:             Cohort
   ```
9. **Auto-fill `Description` from an OMOP column-description map.**
   Ship `packs/column_descriptions.yaml` with the OMOP-standard
   description of every common column. Fall back to empty for unknown
   columns. *(files: new `packs/column_descriptions.yaml`, load in
   `introspect_cohort.py`)*
10. **Flag PII rows.** If the table is `standard_profile_data_model`
    (or listed in a PII table pack) and the column name matches a PII
    regex, set `Extraction Type = PII` and mark `Implemented = No`
    for the sales / pharma audience view.
11. **Add `% Patient` column (patient-level completeness).**
    For each table that has `person_id`, compute
    `COUNT(DISTINCT person_id) WHERE col IS NOT NULL / total_patients`.
    For tables without `person_id`, fall back to row-level `Completeness`.
    *(files: new `_compile_patient_completeness`, call sites in
    `introspect()`)*

### Phase 3 — deliver the four-page format from the ask

12. **New Summary row: `years_of_data`.** Compute as the difference in
    years between the minimum and maximum of the following dates,
    whichever are present: `visit_occurrence.visit_start_date`,
    `condition_occurrence.condition_start_date`,
    `drug_exposure.drug_exposure_start_date`,
    `measurement.measurement_date`. Report as e.g. "2021-10-01 → 2026-02-27
    (4.4 years)".
13. **New sheet: `Tables`.** One row per table with
    `table_name, row_count, column_count, patient_count, description`.
    Description comes from `packs/table_descriptions.yaml`.
14. **New sheet: `Variables` (config-driven Page 4).**
    Reads `packs/variables/<disease>.yaml`. Each entry declares a
    clinical variable with a `{table, column, criteria, category,
    description, notes, extraction_type}` block. The script joins that
    against the introspected data to populate `Values`, `Distribution`,
    `% Patient`. Example:
    ```yaml
    - category: Biomarkers
      variable: APOE Genotype
      description: APOE allele status from genotyping.
      table: measurement
      column: value_as_concept_name
      criteria: "measurement_concept_name LIKE '%APOE%'"
      extraction_type: Structured
      notes: Key inclusion/exclusion variable for anti-amyloid therapy.
    ```
15. **Direct PDF render.** After the HTML is written, optionally call
    WeasyPrint (or Playwright) to rasterise to PDF. Gated behind
    `--out-pdf` so it's opt-in. Removes the iPhone-scan step from the
    loop.

### Phase 4 — multi-cohort batch runner

16. **New CLI: `introspect_cohort.py --all`.** Reads
    `packs/cohorts/*.yaml`, iterates, writes one workbook per cohort
    into `Output/<cohort>_dictionary.xlsx`. Stops on DB errors per-cohort
    without killing the batch.
17. **Schema-drift check.** If a prior XLSX exists, diff the
    `Columns` sheet and warn in stderr on any added / removed /
    type-changed columns so the variable pack can be updated before
    the new dictionary is signed off.
18. **Combined views.** Opt-in `--combine nimbus+nimbus_az` that unions
    the two schemas into one dictionary, flagging each row with its
    source schema.

### Phase 5 — audience-specific packaging

19. **Three sheet-visibility presets driven by `--audience`:**
    - `sales` — Summary + Tables + curated Page-4 Variables. Strip
      Columns sheet. Strip PII rows. Rename file `<cohort>_sales.xlsx`.
    - `data_science` — all sheets, no redactions.
    - `pharma` — Summary + Page-4 Variables only, with a hidden "see
      data team for column-level detail" footer.

## 4. What won't fit in the script (workflow / ops)

- Source-of-truth for the **provider → schema mapping** lives in
  `century/ask.pdf` today. Committing it as `packs/cohorts/*.yaml`
  moves it into the repo so the batch runner can read it.
- Some cohorts (PRINE Renal, Southland, EHA) don't have a schema yet.
  Block those in the runner and report "awaiting schema provisioning".
- The OCR scans in `Output/` are lossy — worth replacing them with the
  direct-from-script PDFs produced by step 15 once it ships.

## 5. Suggested ship order

PR 1: Phase 1 items (1–7). Zero new deps, safer numerics, cleaner XLSX.
PR 2: Phase 2 items (8–11). Ship the `packs/*` static maps.
PR 3: Phase 3 items (12–14). The four-page deliverable.
PR 4: Phase 3 item 15 (direct PDF). Adds WeasyPrint / Playwright dep.
PR 5: Phase 4 (batch runner + drift check).
PR 6: Phase 5 (audience packaging).
