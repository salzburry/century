# Clinical Registries → Data Dictionaries

Plan for answering `century/ask.pdf` ("Clinical Registries to Schemas").

## The ask (from `century/ask.pdf`)

Four open questions for Sanjay, and one concrete deliverable list:

1. **Do we build a data dictionary per provider and per disease?**
   Direction: 1 per provider *and* 1 per disease — start with **one dictionary per (provider, disease) pair**.
2. **How do we handle multi-site providers with different schemas (Nimbus vs Nimbus AZ)?**
   Direction: do them separately for now. Revisit consolidation once both are stable.
3. **How do we build DD's for sales, data scientists, pharma customers separately?**
   Direction: the four-page format already agreed:
   - Page 1 → Summary: # of patients, # of years of data
   - Page 2 → Tables
   - Page 3 → Columns and descriptions
   - Page 4 → Variables (config driven)
4. **How can we automate the whole process (script points at schema, etc.)?**
   Direction: Onkar to develop. Backbone already exists in `introspect_cohort.py`; this plan tracks the gaps to close so the script produces the four-page format end-to-end.

## Registries list

Source: `century/ask.pdf` + schema list from `century/Adobe Scan 19 Apr 2026.pdf`.

| # | Provider | Disease | Schema | Priority | Raw dump in `Output/` | Notes |
|---|---|---|---|---|---|---|
| 1 | Nira | MS (1 site) | `ms_leaf_nira_registry` | Medium | — | First site we started with |
| 2 | Nira | MS (all sites) | `nira_ms_cohort` | Medium | — | |
| 3 | Nira | MG (all sites) | `nira_ms_cohort` | Low | — | Same schema as MS |
| 4 | Nimbus | COPD | `nimbus_copd_curated` | High | `minbuscopdcurated.pdf` | Consolidate with Nimbus AZ? |
| 5 | Nimbus | Asthma | `nimbus_asthma_curated` | High | `nimbusasthmacurated.pdf` | |
| 6 | Nimbus AZ | COPD | `nimbus_az_copd_cohort` | High | `nimbusazcopd.pdf` | Abstracted variables (FEV1, etc.) in progress |
| 7 | Nimbus AZ | Asthma | `nimbus_az_asthma_cohort` | High | `nimbusazasthma.pdf` | |
| 8 | Balboa | Renal | `balboa_ckd_cohort` | High | `balboackd.pdf` | |
| 9 | MTC | Alzheimers | `mtc_alzheimers_cohort` | High | `mtcalzhiemer.pdf` | |
| 10 | MTC | AAT | `mtc_aat_cohort` | High | `mtcaat.pdf` | Patients on anti-amyloid therapies |
| 11 | Newtown | MASH | `newtown_mash_cohort` | High | — | |
| 12 | Newtown | IBD | `newtown_ibd_cohort` | Low | — | |
| 13 | DRG | Renal | `drg_ckd_cohort` | High | `drgckd.pdf` | |
| 14 | PRINE | Renal | TBD | — | — | Schema not yet provisioned |
| 15 | Rocky Mountain Neurology | Alzheimers | `rmn_alzheimers_cohort` | — | — | |
| 16 | Southland Neurologic Institute | TBD | TBD | — | — | |
| 17 | Eye Health America (EHA) | TBD | TBD | High | — | |
| 18 | RVC | DR | `rvc_dr_curated` | — | — | |

**8 of 18** have a raw introspect dump in `Output/`. The other 10 still need to be run through `introspect_cohort.py` (or are blocked on schema provisioning).

## Target format (per dictionary)

Four pages/sheets, one file per (provider, disease):

- **Page 1 — Summary**: cohort name, patient count, # of years of data (min/max encounter date), # of tables, # of columns.
- **Page 2 — Tables**: one row per warehouse table with row count, column count, and a one-line description.
- **Page 3 — Columns and descriptions**: one row per physical column — what `introspect_cohort.py` already produces. Matches the layout the reviewer sees in the reference PDF (`century/Data dictionary.pdf`).
- **Page 4 — Variables**: the curated, clinically-organised business-variable view. Column order:

  `Category | Variable | Description | Table | Column(s) | Criteria | Values | Distribution | Implemented | % Patient | Extraction Type | Notes`

  This is the **config-driven** page. Each disease has its own list of clinical variables (Demographics, Vitals, Diagnosis, Medications, Biomarkers, Outcomes, etc.) keyed by `{table, concept_name}`.

## Plan — steps to execute

### Phase 0 — Decisions to confirm with Sanjay (blocker)
- [ ] Confirm "1 per (provider, disease)" as the unit of delivery.
- [ ] Confirm the four-page format above is what sales / DS / pharma all get (same file, same pages).
- [ ] Confirm that Page 4 variable list is disease-driven (one template per disease, shared across providers) or provider-driven.
- [ ] Decide whether Nimbus + Nimbus AZ get a consolidated COPD dictionary later.

### Phase 1 — Catch the generator up to the four-page spec (Onkar)
`introspect_cohort.py` today emits two sheets (Summary, Variables-as-columns). It needs three changes to produce the four-page layout:

- [ ] **Summary sheet**: add `# of years of data` (max - min across encounter / visit / measurement dates).
- [ ] **Tables sheet (new)**: one row per table with `table_name, row_count, column_count, description`. Description can come from a per-schema YAML map (`packs/tables/<schema>.yaml`) — empty string if not configured.
- [ ] **Columns sheet**: rename current "Variables" sheet to `Columns` to match the ask wording (content is unchanged).
- [ ] **Variables sheet (new, config-driven)**: reads `packs/variables/<disease>.yaml` (e.g., `packs/variables/alzheimers.yaml`) that declares the clinical variables and their `{table, column, criteria}` filters. The script joins that against the introspected data to populate `Values`, `Distribution`, `Implemented`, `% Patient`, `Extraction Type`. `Category / Variable / Description / Criteria / Notes` come from the YAML; `Values / Distribution / % Patient` come from the DB.

Each variable pack ships once per disease and is reused across all providers with that disease (e.g., `alzheimers.yaml` used by both `mtc_alzheimers_cohort` and `rmn_alzheimers_cohort`).

### Phase 2 — Author the variable packs (one per disease)
One YAML file per disease. Start with the five high-priority areas:

- [ ] `packs/variables/alzheimers.yaml` — from `century/Data dictionary.pdf` (MTC Alzheimers reference). Covers Demographics, Vitals, Diagnosis, Medications, Biomarkers (APOE, A-beta, p-Tau, GFAP, NfL), Outcomes (MoCA, MMSE, FAQ, CDR, ADAS-Cog, DSRS, ARIA), Reports.
- [ ] `packs/variables/aat.yaml` — MTC AAT: anti-amyloid therapy tracking (Leqembi/Kisunla/Aduhelm infusions, ARIA-H / ARIA-E, APOE).
- [ ] `packs/variables/copd.yaml` — Nimbus / Nimbus AZ COPD. FEV1, FVC, FEV1/FVC ratio, SpO2, exacerbations, inhaler therapy.
- [ ] `packs/variables/asthma.yaml` — Nimbus / Nimbus AZ Asthma.
- [ ] `packs/variables/ckd.yaml` — Balboa / DRG / PRINE Renal. eGFR, creatinine, albuminuria, dialysis events, RAAS therapy.

Medium-priority (MS, MG, MASH, IBD, DR) author when their raw dumps land.

### Phase 3 — Generate the eight dictionaries we have raw dumps for
In priority order:

- [ ] MTC Alzheimers (`mtc_alzheimers_cohort`) → `Output/mtc_alzheimers_cohort_dictionary.xlsx`
- [ ] MTC AAT (`mtc_aat_cohort`) → `Output/mtc_aat_cohort_dictionary.xlsx`
- [ ] Balboa Renal (`balboa_ckd_cohort`) → `Output/balboa_ckd_cohort_dictionary.xlsx`
- [ ] DRG Renal (`drg_ckd_cohort`) → `Output/drg_ckd_cohort_dictionary.xlsx`
- [ ] Nimbus COPD (`nimbus_copd_curated`) → `Output/nimbus_copd_curated_dictionary.xlsx`
- [ ] Nimbus Asthma (`nimbus_asthma_curated`) → `Output/nimbus_asthma_curated_dictionary.xlsx`
- [ ] Nimbus AZ COPD (`nimbus_az_copd_cohort`) → `Output/nimbus_az_copd_cohort_dictionary.xlsx`
- [ ] Nimbus AZ Asthma (`nimbus_az_asthma_cohort`) → `Output/nimbus_az_asthma_cohort_dictionary.xlsx`

Each gets an accompanying `.html` for browser preview and PDF export.

### Phase 4 — Run introspect on the cohorts we're still missing
- [ ] `nira_ms_cohort` (covers Nira MS all sites + MG all sites)
- [ ] `ms_leaf_nira_registry` (Nira MS single site)
- [ ] `newtown_mash_cohort`
- [ ] `newtown_ibd_cohort`
- [ ] `rmn_alzheimers_cohort`
- [ ] `rvc_dr_curated`
- [ ] PRINE Renal — blocked on schema provisioning.
- [ ] Southland Neurologic Institute — blocked on schema + disease selection.
- [ ] Eye Health America — blocked on schema + disease selection.

### Phase 5 — Sales / data-science / pharma packaging
Same four-page file, filtered views:

- [ ] **Sales**: Page 1 (Summary) + Page 2 (Tables) + Page 4 rolled-up categories only. Strip Page 3.
- [ ] **Data scientists**: all four pages as-is.
- [ ] **Pharma customer**: Page 1 + Page 4 (variables) + table-of-contents highlighting disease-specific endpoints; hide raw column inventory unless asked.

## Source files referenced

- `century/Data dictionary.pdf` — reference layout (MTC Alzheimers). This is the target Page 3 + Page 4 look.
- `century/ask.pdf` — the ask itself (same content as `century/century health ask.pdf`).
- `century/Adobe Scan 19 Apr 2026.pdf` — screenshot of the `clinical` database schemas (source of truth for schema names).
- `Output/*.pdf` — scanned PDFs of the HTML outputs `introspect_cohort.py` has already produced for eight of the cohorts.
- `introspect_cohort.py` — the generator we're extending.

## Open questions for the next Sanjay sync

1. Is Page 4's variable list truly disease-driven (same `alzheimers.yaml` for MTC Alzheimers and RMN Alzheimers), or does each provider override?
2. For multi-disease providers (MTC Alzheimers + AAT, Nimbus COPD + Asthma), one file each or a combined workbook?
3. Does "# of years of data" take the min/max of encounter dates, or a per-table per-column view?
4. Are "abstracted" variables (FEV1 for Nimbus AZ, cognitive scores for MTC) expected to show as placeholder rows in Page 4 with `Implemented: No`, or hidden until populated?
