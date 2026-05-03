# Audience contract

Authoritative spec for what each `--audience` of `build_dictionary.py` ships, plus the canonical column lists per sheet. Tests in `dictionary_v2/test_customer_audience.py` enforce this contract; if you change a column name or set, update both the test and this document.

---

## Term definitions

These terms appear in multiple sheets. Defined once here so the audiences agree on what they mean.

| Term | Definition |
|---|---|
| **Observed Values** | The most-frequent non-null values seen in the variable's column for the live cohort, top-10 by row count, newline-separated within the cell. **Not a curated enum** — if a value isn't in the cohort, it doesn't appear. Reads from the structured `top_value_labels` field on `VariableRow` so OMOP labels with internal commas (e.g. `"Cancer, malignant"`) render verbatim. |
| **% Patients With Value** | Distinct cohort patients with at least one non-null row for this variable, as a percentage of the cohort's total patient count. Patient-level coverage signal. Sourced from the `patient_pct` field. Used by all stakeholder audiences (sales, customer, pharma) as the single coverage metric. |
| **Completeness** | Row-level non-null rate: rows with non-null column value among rows matching the variable's criteria/match scope, as a percentage. Methodology / QA signal — useful for auditing whether the criteria filter is sized correctly relative to the column's nullability. **Technical audience only.** Sourced from the `completeness_pct` field. |
| **Implemented** | `"Yes"` if the variable has at least one non-null row matching its scope in the live cohort; `"No"` otherwise. Stakeholder audiences (sales, customer) drop `Implemented = No` rows from the rendered output to avoid surfacing 0% rows; technical and pharma keep them so the audit can see gaps. |
| **Criteria** | The scoping clause for the variable. Three shapes, in order of preference: (1) `match.concept_ids` — canonical OMOP IDs compiled to a bare-integer `column IN (40221901, 793143, ...)` clause, no DB vocabulary lookup at build time; (2) `match.values` / `match.values_file` — string labels compiled to a quoted `column IN ('value1', 'value2', ...)` clause; (3) the broad `criteria:` field from YAML (typically `column ILIKE '%pattern%'`) when no `match:` block is configured. `match.concept_ids` and `match.values`/`values_file` are mutually exclusive — concept_ids wins if both are set, and the validator flags the misconfiguration. Sales hides this column entirely; technical / customer / pharma show it. |
| **Inclusion Criteria** | Plain-language prose ("Records are included for each X recorded for the patient") describing what each row in the variable represents. Authored in the variable YAML's `inclusion_criteria:` field. Renders for every audience that ships a Variables sheet. |

---

## Sheet visibility per audience

| Audience    | Summary | Tables | Columns | Variables | JSON |
|-------------|:-------:|:------:|:-------:|:---------:|:----:|
| `technical` | ✓       | ✓      | ✓       | ✓         | ✓    |
| `pharma`    | ✓       | —      | —       | ✓         | ✓    |
| `sales`     | ✓ cover | ✓      | —       | ✓         | —    |
| `customer`  | ✓ cover | ✓      | ✓       | ✓         | —    |

`✓ cover` = styled cover sheet (title block, hero stats, coverage rollup) instead of the bare key/value list. JSON is suppressed for stakeholder audiences (sales, customer) so partner bundles never carry the internal `CohortModel` dump.

---

## Variables sheet — column contract

The Variables sheet is the heart of every dictionary; the exact column list per audience is contractual.

### Common head (all audiences except sales)
```
Category | Variable | Description | Inclusion Criteria | Table(s) | Column(s)
```
Sales uses its own Tempus-style head — see below.

### Criteria column (technical + customer + pharma)
```
Criteria
```
Inserted between `Column(s)` and the audience-specific tail. Pharma scientists evaluate variable definitions, so the strict match Criteria IS shown. Sales has no Criteria column at all (its standalone Tempus-style layout doesn't use the shared head).

### Tail per audience

#### `technical` — full audit view
```
Field Type | Example | Coding Schema | Values | Distribution |
Median (IQR) | Completeness | Implemented | % Patients With Value |
Data Source | Notes
```
Carries **both** metrics: row-level `Completeness` (rows with non-null col / rows matching criteria) AND `% Patients With Value` (distinct patients with non-null col / cohort total).

#### `pharma` — methodology-rich evidence view
```
Field Type | Example | Coding Schema | Observed Values |
Distribution | Median (IQR) | Implemented | % Patients With Value |
Data Source | Notes
```
Designed for scientific / evidence reviewers (HEOR, RWE, protocol feasibility, market access). Carries the full methodology stack — Coding Schema, Distribution, Median (IQR), Implemented, Data Source — alongside the strict match Criteria (added by the shared head). Drops only the row-level `Completeness` column; uses `% Patients With Value` as the single coverage metric, consistent with all stakeholder audiences. Renames `Values` → `Observed Values` for label consistency.

The methodology fields are what separate pharma from customer: customer keeps the variable's *what*, pharma adds the *how*.

#### `customer` — plain-language buyer view
```
Field Type | Example | Observed Values | % Patients With Value | Notes
```
Designed for a buyer evaluating the data asset. Definitions + observed values + coverage. **Methodology fields are intentionally dropped** — `Coding Schema`, `Distribution`, `Median (IQR)`, `Implemented`, `Data Source` all live on the pharma sheet, not here. Keeps `Criteria` (added by the shared head) for transparency about how each variable is matched. Reads from the structured `top_value_labels` list so labels with internal commas (e.g. OMOP names like "Cancer, malignant") render verbatim.

This is the shortest external-facing tail — five columns. The contrast with pharma is deliberate: customer answers "what's in this cohort and how is it defined"; pharma answers "what's the methodology behind each variable."

#### `sales` — Tempus-style spec
```
Category | Variable | Description | Observed Values | Notes |
Type | Proposal | % Patients With Value
```
Stand-alone layout — does NOT use the common head. Matches the reviewer's CH-Tempus reference workbook with two label corrections: `Value Sets` → `Observed Values` (cell is observed top-N, not curated) and `Completeness` → `% Patients With Value` (sources from patient_pct).

`Type` maps to `extraction_type` (Structured / Abstracted / Unstructured / Derived / etc.). `Proposal` is the only YAML-curated field on this sheet — must be exactly `Standard` or `Custom` when set; the validator rejects anything else.

---

## Tables sheet — column contract

Both `technical` and `pharma` use the full layout; `sales` and `customer` use the trimmed customer layout.

### Technical / pharma
```
Table | Category | Description | Inclusion Criteria | Data Source |
Source Table | Rows | Columns | Patients
```

### Sales / customer (trimmed)
```
Table | Category | Description | Inclusion Criteria | Rows | Columns | Patients
```
Drops `Data Source` and `Source Table` — internal-only fields a stakeholder reviewer doesn't need.

`cohort_patients`, `standard_profile_data_model`, and `dv_tokenized_profile_data` are filtered out for sales and customer via `packs/dictionary_layout.yaml`.

---

## Columns sheet — column contract

Visibility:
- `technical`, `customer` ship Columns
- `sales`, `pharma` do NOT

### Technical (full)
```
Category | Table(s) | Column | Description | Field Type | Nullable |
Example | Coding Schema | Values | Distribution | Median (IQR) |
Completeness | % Patient | Data Source | PII | Notes
```

### Customer (trimmed)
```
Table(s) | Column | Description | Field Type
```
Just the schema map. Per the original reviewer feedback ("we only need to specify the column names, description, and field type — remove every column from Nullable to the right").

---

## Summary sheet — content contract

### Internal audiences (`technical`, `pharma`)
Bare key/value list — `metric` / `value` rows that downstream tools/tests parse:
```
cohort | provider | disease | display_name | schema_name | variant |
patient_count | table_count | column_count | min_date | max_date |
years_of_data | status | generated_at | git_sha |
introspect_version | schema_snapshot_digest
```

### Stakeholder audiences (`sales`, `customer`)
Styled cover sheet (top to bottom):
- Title block: `<display_name> — <Disease> cohort`
- Subtitle: `Provider · Schema · Generated`
- Description paragraph (from cohort YAML's `description:`)
- Hero stats block: Patients · Years of follow-up · Variables · With data (count + %)
- Date coverage line
- **Freshness facts** (optional, Commit B) — single line containing the segments that are populated:
  - `Data current to: <data_cutoff_date>`
  - `Last ETL run: <last_etl_run>`
  - `Reviewed by: <sign_off.reviewer>  ·  <sign_off.date>  ·  <sign_off.notes>`
  - Renders only when at least one segment is populated; an empty cohort YAML produces no line at all.
- **Known limitations** (optional, Commit B) — bulleted list under a `Known limitations` header, sourced from the cohort YAML's `known_limitations:` list. The header itself is suppressed when the list is empty (no dangling section headers for un-curated cohorts).
- Coverage-by-category rollup table

Internal fields (`variant`, `column_count`, `status`, `git_sha`, `introspect_version`, `schema_snapshot_digest`) are dropped from the cover.

---

## Output filename contract

```
Output/<schema>_dictionary.xlsx                      # technical (default)
Output/<schema>_dictionary_<audience>.xlsx           # pharma, sales, customer
```
`.html` mirrors `.xlsx`. `.json` is produced for `technical` and `pharma` only.

---

## Filtering rules per audience

| Filter                                    | technical | pharma | sales | customer |
|-------------------------------------------|:---------:|:------:|:-----:|:--------:|
| Drop PII rows                             | —         | ✓      | ✓     | ✓        |
| Filter internal scaffolding tables        | —         | —      | ✓     | ✓        |
| Drop rows where `implemented != "Yes"`    | —         | —      | ✓     | ✓        |
| Sort tables/columns/variables alphabetically | ✓      | ✓      | ✓     | ✓        |

The "implemented" filter has a carve-out for dry-run models (where `patient_count is None` and every row is `implemented="No"` because there's no DB). Dry-run sales and customer outputs render every row regardless.

---

## Adding a new audience

1. Add a row to `AUDIENCE_VISIBILITY` in `dictionary_v2/build_dictionary.py`.
2. Either reuse an existing tail (`_TECHNICAL_VARIABLES_TAIL`, `_PHARMA_VARIABLES_TAIL`, `_CUSTOMER_VARIABLES_TAIL`) or define a new one.
3. Map the audience in `_TABLES_LAYOUT_BY_AUDIENCE`, `_COLUMNS_LAYOUT_BY_AUDIENCE`, `_SUMMARY_LAYOUT_BY_AUDIENCE`.
4. Update `variables_layout()` if the head/criteria/tail composition is unique.
5. Update the audience matrix above and add a test asserting the exact column list.
