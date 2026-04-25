# Pack prose style guide

This guide governs the strings that appear in the generated workbook —
the `description`, `notes`, table descriptions, and column descriptions
that customers read. It does not govern YAML keys, comments, or
internal documentation; those can be as detailed as needed.

## Why we have a guide

Reviewers have likened the workbook to a "generated cohort QA report"
rather than a "productized data dictionary." The Flatiron data
dictionary sets the bar: every Description is a clean, present-tense
clinical definition. Our content coverage is broader, but the prose
needs the same editorial polish.

## The rule of three

Each customer-visible string should pass three filters:

1. **Clinical, not mechanical.** Describe the data, not the pack
   that produced it. ("Diagnoses recorded for the patient" — not
   "Cohort-defining for both MTC / RMN; captured in adrd_common
   so both inherit it.")
2. **Present tense, single sentence.** A well-written Description is
   one sentence. Two is a ceiling, not a target.
3. **No internal vocabulary.** No pack file names
   (`adrd_common`, `respiratory_common`), no cohort short names
   (`MTC`, `RMN`, `Nimbus`), no audience tags
   (`technical / sales / pharma`), no SQL keywords
   (`ILIKE`, `SELECT`, `JOIN`).

## Field-by-field

### `description:` (variable rows)

The customer-visible definition of the variable.

- ✅ *Alzheimer's disease, mild cognitive impairment, amnesia, memory
  impairment, dementia, and related cognitive-function diagnoses
  recorded for the patient.*
- ❌ *Cohort-defining for both MTC / RMN Alzheimer's and MTC AAT.
  Captured in retinal_common so both cohorts surface it.*

### `notes:` (variable rows)

A clinical or data-quality caveat that helps a reader interpret the
column. Keep these short and audience-neutral.

- ✅ *Race is not reliably recorded at many sites.*
- ✅ *Estimates based on NLP of clinical text with manual validation.*
- ❌ *Emitted only in the technical audience; redacted by the PII
  filter for sales and pharma.*
- ❌ *Broad on purpose — the ADRD-defining concept family uses many
  related SNOMED strings. If a reviewer needs to split…*

If the note is pack rationale, move it to a YAML comment above the
row instead.

### `inclusion_criteria:` (added in Phase 2)

One sentence describing which rows are included, in business prose.

- ✅ *Records are included for each diagnosis recorded for each
  patient in the cohort.*
- ❌ `condition_concept_name ILIKE '%alzheimer%'`

The raw SQL `criteria:` key stays — it just isn't what we render
for non-technical audiences.

### Table descriptions (`packs/table_descriptions.yaml`)

What the table contains, in present tense. Lineage to the OMOP
table goes in a separate `source_table:` key (Phase 2), not in the
description sentence.

- ✅ *Medical diagnoses recorded for the patient.*
- ❌ *Medical diagnoses recorded for the patient. OMOP
  CONDITION_OCCURRENCE table.*

## Banned phrases

The validator flags rendered strings containing any of these,
case-insensitive. They are signals of pack-mechanics leakage.

- Pack file references: `adrd_common`, `aat_common`,
  `alzheimers_common`, `respiratory_common`, `copd_common`,
  `asthma_common`, `ckd_common`, `retinal_common`, `dr_common`,
  `amd_common`, `mash_common`, `ibd_common`
- Cohort short-name leakage: standalone `MTC`, `RMN`, `Nimbus`,
  `Balboa`, `DRG`, `Newtown`, `RVC` (when used as a tag rather
  than part of an organization's name)
- Pack mechanics: `cohort-defining`, `Captured in`, `inherits`,
  `inherited from`, `pack`, `Owned here`, `redacted by`,
  `audience`, `for both`, `surface it`
- SQL fragments: `ILIKE`, `SELECT`, ` JOIN `, ` FROM ` (with
  spaces — avoids false positives on plain English "from")
- Generator vocabulary: `extraction_type`, `value_as_concept_name`
  appearing in prose Descriptions

## How the validator uses this

Phase 0 ships these as **warnings**. The intent is to surface the
true scope of the cleanup before turning the rule into an error.
After the editorial pass lands and the warning count is zero, the
banned-phrase rule moves to **error** under `--strict`.
