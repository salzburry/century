"""Build a curated data dictionary for the mtc_aat cohort.

Rolls the raw 307-column introspect_cohort.py dump (Output/mtcaat.pdf,
an OCR of the script's HTML output on mtc__aat_cohort) into the
clinically-organised, business-variable layout used by the Century
reference dictionary (century/Data dictionary.pdf).

Column layout matches the reference PDF:

    Category | Variable | Description | Table | Column(s) | Criteria |
    Values | Distribution | Implemented | % Patient |
    Extraction Type | Notes

Distribution / % Patient values are transcribed from the raw OCR where
legible; left blank where the underlying run didn't populate them
(cohort-specific gaps, not template holes).
"""
from __future__ import annotations

from pathlib import Path

import pandas as pd


OUT_DIR = Path(__file__).resolve().parent / "Output"
COHORT = "mtc_aat_cohort"
PATIENT_COUNT = 1067

COLUMNS = [
    "Category",
    "Variable",
    "Description",
    "Table",
    "Column(s)",
    "Criteria",
    "Values",
    "Distribution",
    "Implemented",
    "% Patient",
    "Extraction Type",
    "Notes",
]


def row(
    category, variable, description, table, columns, criteria,
    values, distribution, implemented, patient_pct,
    extraction, notes,
):
    return {
        "Category": category,
        "Variable": variable,
        "Description": description,
        "Table": table,
        "Column(s)": columns,
        "Criteria": criteria,
        "Values": values,
        "Distribution": distribution,
        "Implemented": implemented,
        "% Patient": patient_pct,
        "Extraction Type": extraction,
        "Notes": notes,
    }


VARIABLES = [
    # ---- Demographics -------------------------------------------------
    row(
        "Demographics", "Ethnicity",
        "The patient's self-identified ethnicity, distinguishing Hispanic/Latino origin.",
        "person", "ethnicity_concept_name", "",
        "Hispanic or Latino, Not Hispanic or Latino",
        "Not Hispanic or Latino: 14 (1.3%); Hispanic or Latino: 2 (0.2%)",
        "Yes", "1.5%", "Structured",
        "Ethnicity is not reliably recorded; assessing potential ways to infer this with the clinic.",
    ),
    row(
        "Demographics", "Race",
        "The patient's self-identified racial category.",
        "person", "race_concept_name", "",
        "White, Asian, Black or African American, European",
        "White: 8 (0.7%); Asian: 2 (0.2%); Black or African American: 2 (0.2%); European: 1 (0.1%)",
        "Yes", "1.2%", "Structured",
        "Race is not reliably recorded; assessing potential ways to infer this with the clinic.",
    ),
    row(
        "Demographics", "Sex",
        "The patient's biological sex at birth.",
        "person", "gender_concept_name", "",
        "FEMALE, MALE",
        "FEMALE: 578 (54.2%); MALE: 489 (45.8%)",
        "Yes", "100.0%", "Structured", "",
    ),
    row(
        "Demographics", "Birth Year",
        "The year the patient was born, used for age calculation.",
        "person", "year_of_birth", "",
        "",
        "Min: 1933, Max: 1997, Mean: 1948 (std: 7.09); Median: 1948 (IQR: 1944-1952)",
        "Yes", "100.0%", "Structured", "",
    ),
    row(
        "Demographics", "ZIP code",
        "First 3 digits of the patient ZIP code (3-digit prefix).",
        "location", "zip", "",
        "327, 497, 065, 383, 537",
        "327: 1 (0.5%); 497: 1 (0.5%); 065: 1 (0.5%); 383: 1 (0.5%); 537: 1 (0.5%)",
        "Yes", "100.0%", "Structured", "",
    ),
    row(
        "Demographics", "Payer Type",
        "The insurance/payer responsible for covering the patient's care.",
        "payer_plan_period", "payer_concept_name", "",
        "FLORIDA MEDICARE, AARP SUPPLEMENTAL, FLORIDA BLUE SHIELD, FLORIDA BLUE MEDICARE SUPPLEMENT, AARP MEDICARE COMPLETE",
        "FLORIDA MEDICARE: 729 (19.0%); AARP SUPPLEMENTAL: 243 (13.2%); FLORIDA BLUE SHIELD: 102 (6.5%); FLORIDA BLUE MEDICARE SUPPLEMENT: 100 (3.4%)",
        "Yes", "100.0%", "Structured", "",
    ),

    # ---- Vitals -------------------------------------------------------
    row(
        "Vitals", "Body Weight",
        "The patient's weight taken at the office visit (lbs).",
        "observation", "value_as_number",
        'observation_concept_name = "Body weight"',
        "",
        "3663 records (8.6% of observations)",
        "Yes", "8.6%", "Structured",
        "Captured as part of the ARIA / weight bundle.",
    ),
    row(
        "Vitals", "Patient Height",
        "The patient's height recorded at the office visit (feet, inches).",
        "observation", "value_as_string",
        'observation_concept_name = "Patient height"',
        "",
        "3428 records (8.0% of observations)",
        "Yes", "8.0%", "Structured", "",
    ),
    row(
        "Vitals", "Blood Pressure",
        "Sitting blood pressure values taken at the office visit.",
        "observation", "value_as_string",
        'observation_concept_name = "Blood pressure"',
        "",
        "", "No", "0.0%", "Structured",
        "Not currently populated in mtc_aat observation records; abstraction in progress.",
    ),
    row(
        "Vitals", "Temperature",
        "The patient's temperature taken at the office visit.",
        "observation", "value_as_number",
        'observation_concept_name = "Temperature"',
        "",
        "", "No", "0.0%", "Structured",
        "Not currently populated in mtc_aat observation records.",
    ),
    row(
        "Vitals", "Heart Rate",
        "The patient's heart rate taken at the office visit.",
        "observation", "value_as_number",
        'observation_concept_name = "Heart rate"',
        "",
        "", "No", "0.0%", "Structured",
        "Not currently populated in mtc_aat observation records.",
    ),
    row(
        "Vitals", "Oxygen Saturation",
        "The patient's peripheral oxygen saturation percentage.",
        "observation", "value_as_number",
        'observation_concept_name = "Peripheral O2 saturation"',
        "",
        "", "No", "0.0%", "Structured",
        "Not currently populated in mtc_aat observation records.",
    ),
    row(
        "Vitals", "Respiratory Rate",
        "The patient's respiratory rate taken at the office visit.",
        "observation", "value_as_number",
        'observation_concept_name = "Respiratory rate"',
        "",
        "", "No", "0.0%", "Structured",
        "Not currently populated in mtc_aat observation records.",
    ),

    # ---- Observations -------------------------------------------------
    row(
        "Observations", "ARIA-H (microhemorrhage)",
        "Amyloid-related imaging abnormality - microhemorrhage or hemosiderosis findings from imaging.",
        "observation", "value_as_concept_name",
        'observation_concept_name = "ARIA-H - amyloid-related image abnormality of microhemorrhage or hemosiderosis"',
        "False, True, Positive, Negative",
        "False: 9760 (22.8%); Positive: 635 (1.5%); True: 200 (0.5%); Negative: 122 (0.3%)",
        "Yes", "11.6%", "Structured",
        "ARIA findings track amyloid therapy safety; 4980 ARIA-H observations across cohort.",
    ),
    row(
        "Observations", "ARIA-E (edema)",
        "Amyloid-related imaging abnormality - edema or effusion findings from imaging.",
        "observation", "value_as_concept_name",
        'observation_concept_name = "ARIA-E - amyloid-related image abnormality of edema or effusion"',
        "False, True, Positive, Negative",
        "False: 9760 (22.8%); Positive: 635 (1.5%); True: 200 (0.5%); Negative: 122 (0.3%)",
        "Yes", "11.6%", "Structured",
        "4980 ARIA-E observations across cohort; paired with ARIA-H for safety surveillance.",
    ),
    row(
        "Observations", "Activities of Daily Living (ADL)",
        "Whether the patient can independently perform basic self-care tasks (informant observation).",
        "observation", "value_as_concept_name",
        'observation_concept_name = "ADL"',
        "Independent, Dependent",
        "Independent: 3541 (8.3%)",
        "Yes", "8.5%", "Structured",
        "3659 ADL observations.",
    ),
    row(
        "Observations", "Allergies",
        "Observation of patient allergies.",
        "observation", "value_as_concept_name",
        'observation_concept_name = "Allergies"',
        "",
        "", "No", "0.0%", "Structured",
        "Not currently populated for mtc_aat; abstraction in progress.",
    ),

    # ---- Diagnosis ----------------------------------------------------
    row(
        "Diagnosis", "Diagnosis",
        "Medical diagnoses and conditions recorded for the patient.",
        "condition_occurrence", "condition_concept_name", "",
        "Alzheimer's disease with late onset, Alzheimer's disease unspecified, Other symptoms and signs involving cognitive functions, Mild cognitive impairment of uncertain or unknown etiology, Encounter for examination for normal comparison and control in clinical research program",
        "Alzheimer's disease with late onset: 1861 (15.2%); Alzheimer's disease, unspecified: 1109 (9.1%); Other symptoms and signs involving cognitive functions and awareness: 1011 (8.3%); Mild cognitive impairment of uncertain or unknown etiology: 922 (7.5%); Encounter for examination for normal comparison and control: 826 (6.8%)",
        "Yes", "100.0%", "Structured",
        "12,231 condition_occurrence rows. Top 5 concepts shown; long tail of co-morbidities.",
    ),
    row(
        "Diagnosis", "Diagnosis Type",
        "Source from which the diagnosis was captured (claim vs EHR problem list).",
        "condition_occurrence", "condition_type_concept_name", "",
        "Claim, EHR problem list",
        "Claim: 10326 (84.4%); EHR problem list: 1904 (15.6%)",
        "Yes", "100.0%", "Structured", "",
    ),
    row(
        "Diagnosis", "Diagnosis Start Date",
        "Date the condition was first recorded.",
        "condition_occurrence", "condition_start_date", "",
        "",
        "Min: 2022-01-28, Max: 2026-02-27",
        "Yes", "100.0%", "Structured", "",
    ),

    # ---- Medications --------------------------------------------------
    row(
        "Medications", "Prescription",
        "Medications prescribed to the patient (anti-amyloid therapies, cognitive enhancers, co-medications).",
        "drug_exposure", "drug_concept_name",
        'drug_type_concept_name = "EHR prescription"',
        "Leqembi (lecanemab-irmb), Kisunla (donanemab-azbt), Aduhelm (aducanumab-avwa), donepezil hydrochloride 10 MG Oral Tablet, donepezil hydrochloride 5 MG Oral Tablet, memantine hydrochloride 10 MG Oral Tablet",
        "Leqembi 10 MG/ML: ~26.1%; Kisunla 20 MG/ML: ~26.1%; donepezil 10 MG Oral Tablet: ~17.2%; memantine 10 MG Oral Tablet: ~7.0%; donepezil 5 MG Oral Tablet: ~6.4%",
        "Yes", "57.1%", "Structured",
        "15,693 EHR prescription records (57.1% of drug_exposure).",
    ),
    row(
        "Medications", "Administration",
        "Medications administered to the patient (EHR administration record).",
        "drug_exposure", "drug_concept_name",
        'drug_type_concept_name = "EHR administration record"',
        "Leqembi infusion, Kisunla infusion, Aduhelm infusion",
        "11,775 administration records (42.9% of drug_exposure)",
        "Yes", "42.9%", "Structured", "",
    ),
    row(
        "Medications", "Route",
        "Route of administration.",
        "drug_exposure", "route_concept_name", "",
        "oral, transdermal, intravenous, subcutaneous, inhalation",
        "oral: 13704 (49.9%); intravenous: 420 (1.5%); subcutaneous: 347 (1.3%); transdermal: 125 (0.5%)",
        "Yes", "17.1%", "Structured",
        "Most drug_exposure rows have no recorded route (Not Specified).",
    ),
    row(
        "Medications", "Drug Form",
        "Pharmaceutical dose form (tablet, capsule, injectable, etc.).",
        "drug_exposure", "dose_unit_source_value", "",
        "Tablet, Capsule, Each, Milliliter, Not Specified",
        "Tablet: 2125 (7.7%); Each: 1596 (5.8%); Milliliter: 927 (3.4%); Not Specified: 849 (3.1%)",
        "Yes", "56.0%", "Structured", "",
    ),
    row(
        "Medications", "Drug Dates",
        "Start and end dates of each drug exposure.",
        "drug_exposure",
        "drug_exposure_start_date, drug_exposure_end_date", "",
        "",
        "Start - Min: 2021-10-07, Max: 2026-02-27; End - 57.1% populated",
        "Yes", "100.0%", "Structured", "",
    ),

    # ---- Infusions ----------------------------------------------------
    row(
        "Infusions", "Infusion Drug",
        "Drug administered by infusion (anti-amyloid monoclonal antibody therapies).",
        "infusion", "drug_concept_name", "",
        "Leqembi, Kisunla, Aduhelm, donepezil 10 MG Oral Tablet, memantine 10 mg tablet",
        "Leqembi: 8936 (32.5%); Kisunla: 2691 (9.8%); donepezil 10 mg: 325 (1.2%); memantine 10 mg tablet: 192 (0.7%); donepezil 10 mg (alt): 166 (0.6%)",
        "Yes", "100.0%", "Structured",
        "Dedicated infusion table parallel to drug_exposure for anti-amyloid therapy tracking.",
    ),
    row(
        "Infusions", "Infusion Dates",
        "Start and end dates of each infusion.",
        "infusion",
        "drug_exposure_start_date, drug_exposure_end_date", "",
        "",
        "Min: 2026-03-11 (start datetime, earliest recorded); Max: 2026-03-11",
        "Yes", "100.0%", "Structured", "",
    ),

    # ---- Biomarkers / Labs -------------------------------------------
    row(
        "Biomarkers / Labs", "Laboratory Measurement",
        "Laboratory measurement concepts captured in the measurement table.",
        "measurement", "measurement_concept_name", "",
        "C reactive protein [Mass/volume] in Serum or Plasma by High sensitivity method, Laboratory comment [Text] in Report Narrative, Homocysteine [Moles/volume] in Serum or Plasma, Lipoprotein.beta.subparticle [Entitic length] in Serum or Plasma, Cholesterol in LDL [Mass/volume] in Serum or Plasma by calculation",
        "C reactive protein: 37078 (10.1%); Laboratory comment [Text] in Report Narrative: 25772 (7.0%); Homocysteine: 19247 (5.2%); Lipoprotein.beta.subparticle: 19149 (5.2%); Cholesterol in LDL: 17072 (4.6%)",
        "Yes", "99.3%", "Structured",
        "Abstraction in progress - available in March 2026.",
    ),
    row(
        "Biomarkers / Labs", "APOE Genotype",
        "APOE allele status (e2/e3/e4) from genotyping.",
        "measurement", "value_as_concept_name",
        'measurement_concept_name LIKE "%APOE%"',
        "APOE e3/e3, APOE e3/e4 (wild type), APOE e4/e4, APOE e2/e3, APOE e2/e4 (wild type)",
        "APOE e3/e3: 381 (0.1%); APOE e3/e4 (wild type): 278 (0.1%); APOE e4/e4: 63; APOE e2/e3: 23; APOE e2/e4 (wild type): 23",
        "Yes", "18.2%", "Structured",
        "Key inclusion/exclusion variable for anti-amyloid therapy eligibility.",
    ),
    row(
        "Biomarkers / Labs", "Plasma Biomarker Numeric Value",
        "Numeric result for plasma/CSF biomarker assays (A-beta 42, A-beta 40, p-Tau-181, p-Tau-217, NfL, GFAP).",
        "measurement", "value_as_number", "",
        "",
        "Min: 0, Max: 11672, Mean: 76.8 (std: 106); Median: 78 (IQR: 22-98.4)",
        "Yes", "42.4%", "Structured",
        "Aggregate across all biomarkers - see Lab Test Criteria row for individual concept filters.",
    ),
    row(
        "Biomarkers / Labs", "Lab Measurement Date",
        "Date the measurement was taken.",
        "measurement", "measurement_date", "",
        "",
        "Min: 2022-02-07, Max: 2026-01-27",
        "Yes", "99.7%", "Structured", "",
    ),

    # ---- Procedures ---------------------------------------------------
    row(
        "Procedures", "Procedure",
        "Clinical procedures performed - includes anti-amyloid infusion procedures and E&M visit complexity.",
        "procedure_occurrence", "procedure_concept_name", "",
        "Injection, lecanemab-irmb, 1 mg; Chemotherapy administration, intravenous infusion technique, up to 1 hour; Visit complexity inherent to evaluation and management; Office or other outpatient visit for evaluation and management; Assessment of and care planning for a patient with cognitive impairment",
        "Injection, lecanemab-irmb, 1 mg: 1217 (15.6%); Chemotherapy administration, IV infusion up to 1 hour: 1014 (13.0%); Visit complexity inherent to E&M: 1014 (13.0%); Office/outpatient E&M visit: 928 (11.9%); Assessment and care planning for cognitive impairment: 769 (9.9%)",
        "Yes", "95.2%", "Structured", "",
    ),
    row(
        "Procedures", "Procedure Date",
        "Date the procedure was performed.",
        "procedure_occurrence", "procedure_date", "",
        "",
        "Min: 2022-02-10, Max: 2026-03-04",
        "Yes", "100.0%", "Structured", "",
    ),

    # ---- Visits -------------------------------------------------------
    row(
        "Visits", "Visit Type",
        "The type of healthcare visit or encounter.",
        "visit_occurrence", "visit_concept_name", "",
        "Office Visit, Telehealth, Ambulatory Infusion Therapy Clinic / Center, Diagnostic, Other",
        "Office Visit: 8957 (71.6%); Telehealth: 1716 (13.7%); Ambulatory Infusion Therapy Clinic / Center: 1020 (8.2%); Diagnostic: 682 (5.5%); Other: 136 (1.1%)",
        "Yes", "100.0%", "Structured", "",
    ),
    row(
        "Visits", "Visit Date",
        "Start and end date of each visit.",
        "visit_occurrence",
        "visit_start_date, visit_end_date", "",
        "",
        "Start - Min: 2021-10-01, Max: 2026-02-27",
        "Yes", "100.0%", "Structured", "",
    ),

    # ---- Outcomes (cognitive assessments) ----------------------------
    row(
        "Outcomes", "MoCA",
        "Montreal Cognitive Assessment - brief screening test for cognitive dysfunction.",
        "observation", "value_as_number",
        'observation_concept_name = "MoCA"',
        "",
        "",
        "No", "0.0%", "Abstracted",
        "Completeness measures are estimates based on NLP of clinical text with manual validation. Abstraction in progress - available in March 2026.",
    ),
    row(
        "Outcomes", "MMSE",
        "Mini-Mental State Examination - widely used short cognitive test assessing orientation, recall, attention.",
        "observation", "value_as_number",
        'observation_concept_name = "MMSE"',
        "",
        "",
        "No", "0.0%", "Abstracted",
        "Abstraction in progress - available in March 2026.",
    ),
    row(
        "Outcomes", "FAQ",
        "Functional Activities Questionnaire - informant-based 10-item questionnaire evaluating functional ability.",
        "observation", "value_as_number",
        'observation_concept_name = "FAQ"',
        "",
        "",
        "No", "0.0%", "Abstracted",
        "Abstraction in progress - available in March 2026.",
    ),
    row(
        "Outcomes", "CDR",
        "Clinician-rated Dementia Staging Scale that summarises dementia severity across 6 domains.",
        "observation", "value_as_number",
        'observation_concept_name = "CDR"',
        "",
        "",
        "No", "0.0%", "Abstracted",
        "Abstraction in progress - available in March 2026.",
    ),
    row(
        "Outcomes", "ADAS-Cog",
        "Alzheimer's Disease Assessment Scale - Cognitive subscale.",
        "observation", "value_as_number",
        'observation_concept_name = "ADAS-Cog"',
        "",
        "",
        "No", "0.0%", "Abstracted",
        "Abstraction in progress - available in March 2026.",
    ),
    row(
        "Outcomes", "Dementia Severity Rating Scale",
        "A 12-item informant-based questionnaire to assess the severity of dementia.",
        "observation", "value_as_number",
        'observation_concept_name = "Dementia Severity Rating Scale"',
        "",
        "",
        "No", "0.0%", "Abstracted",
        "Abstraction in progress - available in March 2026.",
    ),
    row(
        "Outcomes", "ARIA",
        "Indication of increasing or stable ARIA-E and/or ARIA-H from the MRI (see Observations/ARIA rows for value distributions).",
        "observation", "value_as_concept_name",
        'observation_concept_name LIKE "ARIA%"',
        "",
        "9960 ARIA observations; see Observations rows for full breakdown",
        "Yes", "11.6%", "Structured", "",
    ),

    # ---- Reports / Notes ---------------------------------------------
    row(
        "Reports", "Clinical Note",
        "Free text of the office visit clinical note.",
        "note", "note_text",
        'note_class_concept_name = "Office Visit" OR similar',
        "",
        "6595 notes (100% Office Visit note_class); CARE PLAN: 1716 (31.2%); NEW PATIENT MEMORY: 944 (14.3%); EEG: 496 (7.5%); TELEMEDICINE: 394 (6.0%)",
        "Yes", "99.7%", "Unstructured",
        "Completeness measures are estimates based on the number of reports available.",
    ),
    row(
        "Reports", "Document (MRI / PET / EEG)",
        "Scanned/attached report documents - MRI, PET, EEG and other imaging / diagnostic reports.",
        "document", "document_type_concept_name", "",
        "MRI, Dementia Follow-up, FAQ, MMSE, CMS Registry, Rooming Chart",
        "MRI: 4914 (73.2%); Dementia Follow-up: 182 (2.5%); FAQ: 159 (1.9%); MMSE: 155 (1.9%); CMS Registry: 98 (1.5%); Rooming Chart: 88 (1.5%)",
        "Yes", "76.4%", "Unstructured",
        "Completeness measures are estimates based on the number of reports available.",
    ),
    row(
        "Reports", "PET Scan Result",
        "Indication of positive / negative PET scan result.",
        "observation", "value_as_concept_name",
        'observation_concept_name = "PET scan result"',
        "",
        "", "No", "0.0%", "Abstracted",
        "Abstraction in progress - available in March 2026.",
    ),
]


def write_outputs():
    summary_rows = [
        {"metric": "cohort", "value": COHORT},
        {"metric": "patient_count", "value": PATIENT_COUNT},
        {"metric": "variable_count", "value": len(VARIABLES)},
        {"metric": "category_count", "value": len({r["Category"] for r in VARIABLES})},
    ]
    summary_df = pd.DataFrame(summary_rows, columns=["metric", "value"])
    variables_df = pd.DataFrame(VARIABLES, columns=COLUMNS)

    xlsx_path = OUT_DIR / "mtc_aat_cohort_curated.xlsx"
    with pd.ExcelWriter(xlsx_path, engine="openpyxl") as writer:
        summary_df.to_excel(writer, sheet_name="Summary", index=False)
        variables_df.to_excel(writer, sheet_name="Variables", index=False)
    print(f"Wrote {xlsx_path}")

    html_path = OUT_DIR / "mtc_aat_cohort_curated.html"
    summary_html = summary_df.to_html(index=False, escape=True, border=0)
    variables_html = variables_df.to_html(index=False, escape=True, border=0)
    page = f"""<!doctype html>
<html lang=\"en\">
<head>
<meta charset=\"utf-8\">
<title>Data Dictionary - {COHORT}</title>
<style>
  body {{ font-family: -apple-system, Segoe UI, Roboto, Arial, sans-serif;
          margin: 24px; color: #222; }}
  h1 {{ font-size: 1.4rem; margin-bottom: 4px; }}
  h2 {{ font-size: 1.1rem; margin-top: 28px; color: #444; }}
  table {{ border-collapse: collapse; font-size: 0.82rem;
           margin-top: 8px; width: 100%; }}
  th, td {{ border: 1px solid #ccc; padding: 6px 10px;
            vertical-align: top; text-align: left; }}
  th {{ background: #f2f4f7; font-weight: 600; }}
  tr:nth-child(even) td {{ background: #fafbfc; }}
</style>
</head>
<body>
<h1>Data Dictionary</h1>
<div>Cohort: <code>{COHORT}</code></div>

<h2>Summary</h2>
{summary_html}

<h2>Variables</h2>
{variables_html}
</body>
</html>
"""
    html_path.write_text(page, encoding="utf-8")
    print(f"Wrote {html_path}")


if __name__ == "__main__":
    write_outputs()
