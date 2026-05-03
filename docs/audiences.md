# Audience contract

Authoritative spec for what each `--audience` of `build_dictionary.py` ships, plus the canonical column lists per sheet. Tests in `dictionary_v2/test_customer_audience.py` enforce this contract; if you change a column name or set, update both the test and this document.

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

### Criteria column (technical + customer only)
```
Criteria
```
Inserted between `Column(s)` and the audience-specific tail. Pharma hides it (no raw SQL). Sales has no Criteria column at all.

### Tail per audience

#### `technical` — full audit view
```
Field Type | Example | Coding Schema | Values | Distribution |
Median (IQR) | Completeness | Implemented | % Patients With Value |
Data Source | Notes
```
Carries **both** metrics: row-level `Completeness` (rows with non-null col / rows matching criteria) AND `% Patients With Value` (distinct patients with non-null col / cohort total).

#### `pharma` — technical fields, single coverage metric
```
Field Type | Example | Coding Schema | Values | Distribution |
Median (IQR) | Implemented | % Patients With Value | Data Source | Notes
```
Same as technical minus the row-level `Completeness` column. Stakeholder-facing audiences converge on `% Patients With Value` as the single coverage signal.

#### `customer` — trimmed stakeholder view
```
Field Type | Example | Observed Values | Distribution |
Median (IQR) | % Patients With Value | Notes
```
Drops `Coding Schema`, `Implemented`, `Data Source`. Renames `Values` → `Observed Values` (honest label: cell is the observed top-N from the cohort, not a curated enum). Reads from the structured `top_value_labels` list so labels with internal commas (e.g. OMOP names like "Cancer, malignant") render verbatim.

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
Styled cover sheet:
- Title block: `<display_name> — <Disease> cohort`
- Subtitle: `Provider · Schema · Generated`
- Description paragraph (from cohort YAML's `description:`)
- Hero stats block: Patients · Years of follow-up · Variables · With data (count + %)
- Date coverage line
- Coverage-by-category rollup table

Internal fields (`variant`, `column_count`, `status`, `git_sha`, `introspect_version`, `schema_snapshot_digest`) are dropped.

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
