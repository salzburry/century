#!/usr/bin/env python3
"""Apply curated Flatiron-style inclusion_criteria mappings.

One-shot helper that takes a `MAPPINGS` dict keyed on
`(table, variable)` and replaces the matching row's inclusion_criteria
in any packs/variables/*.yaml file. Hand-curated prose in MAPPINGS
beats the auto-generated templates from `seed_inclusion_criteria.py`.

Idempotent — running twice writes the same content.

Usage:  python scripts/rewrite_inclusion_criteria.py
"""
from __future__ import annotations

import re
import sys
from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parent.parent
PACKS_DIR = REPO_ROOT / "packs" / "variables"


# Curated Flatiron-style inclusion_criteria. Each entry mines the
# row's `description` and `criteria` fields for the actual clinical
# entities and composes a single Flatiron-style sentence ("Records
# are included for each X..."). Naming the ingredient/condition list
# inline is what lifts these from "auto-generated" to "curated".
#
# Keyed on (table, variable) so the line walk can match without a
# YAML reparse per row. Rows not in this map keep whatever
# inclusion_criteria they currently have.
MAPPINGS: dict[tuple[str, str], str] = {
    # --- adrd_common.yaml -------------------------------------------------
    ("condition_occurrence", "Alzheimer's / Cognitive Impairment Diagnosis"):
        "Records are included for each diagnosis of Alzheimer's disease, "
        "mild cognitive impairment (MCI), amnesia, memory impairment, or "
        "dementia recorded for the patient, including all onset variants "
        "and related cognitive-function diagnoses.",
    ("measurement", "APOE Genotype"):
        "Records are included for each APOE genotyping result "
        "(apolipoprotein E allele status) recorded for the patient.",
    ("measurement", "Amyloid-beta 42 (A-beta 42)"):
        "Records are included for each amyloid-beta 42 (Aβ42) measurement "
        "in plasma or cerebrospinal fluid recorded for the patient.",
    ("measurement", "Amyloid-beta 40 (A-beta 40)"):
        "Records are included for each amyloid-beta 40 (Aβ40) measurement "
        "recorded for the patient — used alongside Aβ42 to compute the "
        "Aβ42/Aβ40 ratio.",
    ("measurement", "GFAP"):
        "Records are included for each glial fibrillary acidic protein "
        "(GFAP) measurement recorded for the patient — an astrocyte-"
        "activation and neuroinflammation marker.",
    ("measurement", "p-Tau-181"):
        "Records are included for each phosphorylated-tau-181 (p-Tau-181) "
        "measurement recorded for the patient — an Alzheimer's-disease "
        "biomarker.",
    ("measurement", "p-Tau-217"):
        "Records are included for each phosphorylated-tau-217 (p-Tau-217) "
        "measurement recorded for the patient — an Alzheimer's-disease "
        "biomarker.",
    ("measurement", "Neurofilament Light Chain (NfL)"):
        "Records are included for each neurofilament light chain (NfL) "
        "measurement recorded for the patient — an axonal-injury and "
        "neurodegeneration marker.",
    ("observation", "MoCA"):
        "Records are included for each Montreal Cognitive Assessment "
        "(MoCA) score abstracted from the patient's clinical notes.",
    ("observation", "MMSE"):
        "Records are included for each Mini-Mental State Examination "
        "(MMSE) score abstracted from the patient's clinical notes.",
    ("observation", "FAQ"):
        "Records are included for each Functional Activities "
        "Questionnaire (FAQ) score abstracted from the patient's "
        "clinical notes.",
    ("observation", "CDR"):
        "Records are included for each Clinical Dementia Rating (CDR) "
        "score abstracted from the patient's clinical notes.",
    ("observation", "ADL"):
        "Records are included for each activities-of-daily-living "
        "assessment recorded for the patient.",
    ("observation", "Dementia Severity Rating Scale"):
        "Records are included for each Dementia Severity Rating Scale "
        "(DSRS) score abstracted from the patient's clinical notes.",
    ("observation", "ADAS-Cog"):
        "Records are included for each ADAS-Cog (Alzheimer's Disease "
        "Assessment Scale, Cognitive subscale) score abstracted from the "
        "patient's clinical notes.",
    ("document", "Document (MRI / PET / EEG)"):
        "Records are included for each MRI, PET, EEG, CT, or other "
        "neuroimaging document attached to the patient encounter.",
    ("observation", "PET Scan Result"):
        "Records are included for each amyloid-PET or other PET-scan "
        "result abstracted from the patient's clinical notes.",
    ("observation", "Oxygen Saturation"):
        "Records are included for each peripheral oxygen saturation "
        "(O2 saturation, SpO2) measurement recorded for the patient.",
    ("observation", "Body Weight"):
        "Records are included for each body-weight measurement recorded "
        "for the patient at an office visit.",

    # --- alzheimers_common.yaml ------------------------------------------
    ("drug_exposure", "Symptomatic ADRD Therapy (Prescription)"):
        "Records are included for each prescription of a cholinesterase "
        "inhibitor (donepezil, rivastigmine, galantamine), the NMDA "
        "antagonist memantine, or a ChEI–memantine combination "
        "(e.g. Namzaric) recorded for the patient.",
    ("drug_exposure", "Symptomatic ADRD Therapy (Administration)"):
        "Records are included for each administration of a cholinesterase "
        "inhibitor (donepezil, rivastigmine, galantamine), the NMDA "
        "antagonist memantine, or a ChEI–memantine combination "
        "(e.g. Namzaric) recorded for the patient.",
    # The Route row already carries hand-written prose — left untouched.

    # --- aat_common.yaml -------------------------------------------------
    ("drug_exposure", "Anti-amyloid Therapy (Prescription)"):
        "Records are included for each prescription of an anti-amyloid "
        "monoclonal antibody — lecanemab (Leqembi), donanemab (Kisunla), "
        "or aducanumab (Aduhelm) — recorded for the patient.",
    ("drug_exposure", "Anti-amyloid Therapy (Administration)"):
        "Records are included for each administration of an anti-amyloid "
        "monoclonal antibody — lecanemab (Leqembi), donanemab (Kisunla), "
        "or aducanumab (Aduhelm) — recorded for the patient.",
    ("procedure_occurrence", "Infusion Procedure Codes (candidate AAT attribution)"):
        "Records are included for each infusion procedure that may "
        "represent anti-amyloid therapy — explicit AAT drug-name matches "
        "(lecanemab, donanemab, aducanumab) plus generic HCPCS infusion-"
        "administration codes. Confirm anti-amyloid attribution by "
        "cross-referencing the Anti-amyloid Therapy (Administration) row "
        "on patient and date.",

    # --- respiratory_common.yaml -----------------------------------------
    ("observation", "Oxygen Saturation (SpO2)"):
        "Records are included for each peripheral oxygen saturation "
        "(SpO2, O2 saturation) measurement recorded for the patient.",
    ("observation", "BMI (Body Mass Index)"):
        "Records are included for each body-mass-index (BMI) measurement "
        "recorded for the patient.",
    ("observation", "Smoking Status"):
        "Records are included for each smoking-status observation "
        "(current, former, or never smoker; tobacco use; cigarette use) "
        "recorded for the patient.",
    ("measurement", "Blood Eosinophils"):
        "Records are included for each blood eosinophil count "
        "(absolute or percentage, from CBC or differential panels) "
        "recorded for the patient. Sputum and tissue-biopsy eosinophil "
        "measurements are excluded.",
    ("procedure_occurrence", "Spirometry / Pulmonary Function Test (Procedure)"):
        "Records are included for each spirometry, diffusing-capacity "
        "(DLCO), lung-volume, or bronchodilator-responsiveness procedure "
        "performed for the patient.",
    ("procedure_occurrence", "Pulse Oximetry (Procedure)"):
        "Records are included for each pulse-oximetry or peripheral "
        "blood-oxygen procedure billed for the patient.",
    ("document", "Document (Chest imaging / PFT report)"):
        "Records are included for each chest-imaging or pulmonary-"
        "function-test document attached to the patient encounter — "
        "chest X-ray, chest CT, spirometry, or PFT reports.",

    # --- copd_common.yaml ------------------------------------------------
    ("condition_occurrence", "COPD Diagnosis"):
        "Records are included for each diagnosis of chronic obstructive "
        "pulmonary disease, emphysema, chronic bronchitis, or chronic "
        "obstructive airway disease recorded for the patient.",
    ("drug_exposure", "Short-acting Bronchodilator (SABA / SAMA)"):
        "Records are included for each prescription or administration of "
        "a short-acting beta-agonist (albuterol, levalbuterol) or short-"
        "acting muscarinic antagonist (ipratropium) recorded for the "
        "patient — including branded inhalers (ProAir, Ventolin, "
        "Proventil, Atrovent, Combivent).",
    ("drug_exposure", "Long-acting Beta-agonist (LABA)"):
        "Records are included for each long-acting beta-agonist exposure "
        "(salmeterol, formoterol, olodaterol, indacaterol, vilanterol, "
        "arformoterol) recorded for the patient.",
    ("drug_exposure", "Long-acting Muscarinic Antagonist (LAMA)"):
        "Records are included for each long-acting muscarinic-antagonist "
        "exposure (tiotropium, umeclidinium, aclidinium, revefenacin, or "
        "inhaled glycopyrrolate) recorded for the patient. Oral and "
        "injectable glycopyrrolate (used for secretion management, not "
        "COPD) are excluded.",
    ("drug_exposure", "Inhaled Corticosteroid (ICS)"):
        "Records are included for each inhaled-corticosteroid exposure "
        "(fluticasone, budesonide, beclomethasone, mometasone, "
        "ciclesonide) recorded for the patient — covers mono-ICS and "
        "ICS appearing inside combination products.",
    ("drug_exposure", "COPD Triple Therapy (ICS + LABA + LAMA)"):
        "Records are included for each single-inhaler triple-therapy "
        "exposure — fluticasone/umeclidinium/vilanterol (Trelegy) or "
        "budesonide/glycopyrrolate/formoterol (Breztri) — recorded for "
        "the patient.",
    ("drug_exposure", "Systemic Corticosteroid (Oral / IV)"):
        "Records are included for each systemic corticosteroid exposure "
        "(prednisone, prednisolone, methylprednisolone, dexamethasone) "
        "recorded for the patient. Attributing a course to a COPD "
        "exacerbation requires a temporal cross-reference with the "
        "COPD Exacerbation diagnosis row.",
    ("drug_exposure", "Phosphodiesterase-4 Inhibitor (Roflumilast)"):
        "Records are included for each roflumilast (Daliresp) exposure "
        "recorded for the patient — an oral PDE4 inhibitor used as add-on "
        "therapy for severe chronic-bronchitis-predominant COPD.",
    ("procedure_occurrence", "Supplemental Oxygen Therapy"):
        "Records are included for each supplemental-oxygen procedure "
        "billed for the patient — long-term oxygen therapy, home oxygen, "
        "or in-clinic oxygen administration.",
    ("measurement", "FEV1 (Forced Expiratory Volume in 1 second)"):
        "Records are included for each FEV1 (forced expiratory volume in "
        "one second) measurement abstracted from a pulmonary-function "
        "report for the patient.",
    ("measurement", "FVC (Forced Vital Capacity)"):
        "Records are included for each FVC (forced vital capacity) "
        "measurement abstracted from a pulmonary-function report for the "
        "patient.",
    ("measurement", "FEV1 / FVC Ratio"):
        "Records are included for each post-bronchodilator FEV1/FVC ratio "
        "abstracted from a pulmonary-function report for the patient — "
        "ratio < 0.70 confirms COPD per GOLD criteria.",
    ("condition_occurrence", "COPD Exacerbation (Abstracted)"):
        "Records are included for each acute COPD-exacerbation event "
        "captured by a condition-side acute-exacerbation diagnosis on "
        "an obstructive, bronchitis, COPD, or emphysema concept for the "
        "patient.",
    ("observation", "CAT Score (COPD Assessment Test)"):
        "Records are included for each COPD Assessment Test (CAT) score "
        "abstracted from the patient's clinical notes — CAT ≥ 10 "
        "typically triggers therapy escalation.",
    ("observation", "mMRC Dyspnea Scale"):
        "Records are included for each modified Medical Research Council "
        "(mMRC) dyspnea-scale score abstracted from the patient's "
        "clinical notes — paired with CAT for GOLD A/B/E grouping.",

    # --- asthma_common.yaml ----------------------------------------------
    ("condition_occurrence", "Asthma Diagnosis"):
        "Records are included for each diagnosis of asthma recorded for "
        "the patient — including severity / persistence variants (mild "
        "intermittent, mild/moderate/severe persistent), exercise-induced "
        "asthma, cough-variant asthma, status asthmaticus, and reactive "
        "airway disease.",
    ("drug_exposure", "Short-acting Beta-agonist (SABA — Reliever)"):
        "Records are included for each short-acting beta-agonist reliever "
        "exposure (albuterol, levalbuterol — branded as ProAir, Ventolin, "
        "Proventil, Xopenex) recorded for the patient.",
    ("drug_exposure", "Inhaled Corticosteroid (ICS — Controller)"):
        "Records are included for each inhaled-corticosteroid controller "
        "exposure (fluticasone, budesonide, beclomethasone, mometasone, "
        "ciclesonide) recorded for the patient. ICS-LABA combinations are "
        "captured separately.",
    ("drug_exposure", "ICS + LABA Combination"):
        "Records are included for each single-inhaler ICS-LABA "
        "combination exposure recorded for the patient — budesonide/"
        "formoterol (Symbicort), fluticasone/salmeterol (Advair), "
        "fluticasone/vilanterol (Breo), or mometasone/formoterol "
        "(Dulera).",
    ("drug_exposure", "Leukotriene Modifier (LTRA / 5-Lipoxygenase Inhibitor)"):
        "Records are included for each leukotriene-modifier exposure "
        "recorded for the patient — leukotriene-receptor antagonists "
        "montelukast (Singulair) and zafirlukast (Accolate), and the "
        "5-lipoxygenase inhibitor zileuton (Zyflo).",
    ("drug_exposure", "Long-acting Muscarinic Antagonist (LAMA — Add-on)"):
        "Records are included for each tiotropium (Spiriva Respimat) "
        "exposure recorded for the patient — add-on controller therapy "
        "for severe asthma uncontrolled on ICS-LABA.",
    ("drug_exposure", "Asthma Biologic (Anti-IgE / Anti-IL5 / Anti-IL4R / Anti-TSLP)"):
        "Records are included for each asthma-biologic exposure recorded "
        "for the patient — omalizumab (Xolair, anti-IgE), mepolizumab "
        "(Nucala) and reslizumab (Cinqair, anti-IL5), benralizumab "
        "(Fasenra, anti-IL5R), dupilumab (Dupixent, anti-IL4R), or "
        "tezepelumab (Tezspire, anti-TSLP).",
    ("drug_exposure", "Systemic Corticosteroid (Oral / IV)"):
        "Records are included for each systemic-corticosteroid exposure "
        "(prednisone, prednisolone, methylprednisolone, dexamethasone) "
        "recorded for the patient. Attributing a course to an asthma "
        "exacerbation requires a temporal cross-reference with the "
        "Asthma Exacerbation diagnosis row.",
    ("measurement", "Total IgE (Immunoglobulin E)"):
        "Records are included for each total serum IgE measurement "
        "recorded for the patient. Allergen-specific IgE panels are "
        "excluded.",
    ("measurement", "FeNO (Fractional Exhaled Nitric Oxide)"):
        "Records are included for each fractional-exhaled-nitric-oxide "
        "(FeNO) measurement recorded for the patient — an airway-"
        "eosinophilic-inflammation marker used to phenotype type-2-high "
        "asthma.",
    ("condition_occurrence", "Asthma Exacerbation (Abstracted)"):
        "Records are included for each acute-asthma-exacerbation event "
        "abstracted for the patient — status asthmaticus, or asthma "
        "with acute exacerbation, with-exacerbation, or attack "
        "qualifiers.",
    ("observation", "ACT Score (Asthma Control Test)"):
        "Records are included for each Asthma Control Test (ACT) score "
        "abstracted from the patient's clinical notes — ACT < 20 "
        "indicates poorly controlled asthma.",

    # --- ckd_common.yaml -------------------------------------------------
    ("observation", "Blood Pressure (Systolic / Diastolic, numeric)"):
        "Records are included for each systolic or diastolic blood-"
        "pressure measurement stored as a numeric observation for the "
        "patient. Combined-format readings (e.g. \"120/80\") appear "
        "under the Blood Pressure (Combined) row instead.",
    ("observation", "Heart Rate"):
        "Records are included for each heart-rate or pulse measurement "
        "recorded for the patient at an office visit.",
    ("condition_occurrence", "CKD Diagnosis"):
        "Records are included for each diagnosis of chronic kidney "
        "disease, end-stage renal disease (ESRD), dependence on renal "
        "dialysis, hypertensive renal disease, or diabetic nephropathy "
        "recorded for the patient.",
    ("condition_occurrence", "Acute Kidney Injury"):
        "Records are included for each diagnosis of acute kidney injury "
        "or acute renal failure (AKI) recorded for the patient.",
    ("condition_occurrence", "Proteinuria / Albuminuria"):
        "Records are included for each diagnosis of proteinuria, "
        "albuminuria, or microalbuminuria recorded for the patient — "
        "the A-stage driver of CKD G/A staging.",
    ("condition_occurrence", "Hypertension"):
        "Records are included for each diagnosis of essential or primary "
        "hypertension, hypertensive disorder, or benign hypertension "
        "recorded for the patient. Hypertensive renal disease is "
        "captured under the CKD Diagnosis row to avoid double counting.",
    ("condition_occurrence", "Vitamin D Deficiency"):
        "Records are included for each diagnosis of vitamin D deficiency "
        "(including 25-hydroxyvitamin D deficiency) recorded for the "
        "patient.",
    ("measurement", "Serum Creatinine"):
        "Records are included for each serum, plasma, or blood "
        "creatinine measurement recorded for the patient. Urine-"
        "creatinine measurements are excluded.",
    ("measurement", "Estimated GFR (eGFR)"):
        "Records are included for each estimated glomerular filtration "
        "rate (eGFR) value abstracted for the patient — derived from "
        "serum creatinine, age, and sex; drives CKD G1–G5 staging.",
    ("measurement", "BUN (Urea Nitrogen)"):
        "Records are included for each blood-urea-nitrogen (BUN) "
        "measurement recorded for the patient.",
    ("measurement", "Urine Albumin / Creatinine Ratio (UACR)"):
        "Records are included for each urine albumin/creatinine ratio "
        "(UACR) measurement recorded for the patient — gold-standard "
        "albuminuria marker driving CKD A1–A3 staging.",
    ("measurement", "Hemoglobin"):
        "Records are included for each blood hemoglobin measurement "
        "recorded for the patient. Glycated hemoglobin (HbA1c) is "
        "captured separately.",
    ("measurement", "Serum Potassium"):
        "Records are included for each serum, plasma, or blood "
        "potassium measurement recorded for the patient.",
    ("measurement", "Serum Calcium"):
        "Records are included for each serum, plasma, or blood calcium "
        "measurement recorded for the patient. Urine calcium and "
        "calcium-channel-blocker drug mentions are excluded.",
    ("measurement", "Serum Sodium"):
        "Records are included for each serum, plasma, or blood sodium "
        "measurement recorded for the patient. Urine sodium and sodium-"
        "bicarbonate drug mentions are excluded.",
    ("measurement", "HbA1c (Hemoglobin A1c)"):
        "Records are included for each glycated-hemoglobin (HbA1c, "
        "A1c) measurement recorded for the patient.",
    ("procedure_occurrence", "Dialysis (Hemodialysis or Peritoneal)"):
        "Records are included for each dialysis session — hemodialysis "
        "or peritoneal — recorded for the patient.",
    ("procedure_occurrence", "Dialysis Vascular Access"):
        "Records are included for each dialysis vascular-access "
        "procedure recorded for the patient — AV fistula creation, AV "
        "graft placement, or dialysis catheter insertion.",
    ("procedure_occurrence", "Kidney Biopsy"):
        "Records are included for each renal or kidney biopsy procedure "
        "recorded for the patient.",
    ("procedure_occurrence", "End-Stage Renal Disease (ESRD) Monthly Services"):
        "Records are included for each CPT 9095X-9096X monthly ESRD-"
        "related E&M service billed for the patient. These are billing "
        "records rather than dialysis-session procedures.",
    ("drug_exposure", "RAAS Blockade (ACE Inhibitor / ARB)"):
        "Records are included for each prescription or administration of "
        "an ACE inhibitor (lisinopril, enalapril, ramipril, benazepril, "
        "captopril) or angiotensin-receptor blocker (losartan, valsartan, "
        "olmesartan, irbesartan, candesartan, telmisartan) recorded for "
        "the patient.",
    ("drug_exposure", "SGLT2 Inhibitor"):
        "Records are included for each SGLT2-inhibitor exposure "
        "(empagliflozin/Jardiance, dapagliflozin/Farxiga, "
        "canagliflozin/Invokana, ertugliflozin) recorded for the "
        "patient.",
    ("drug_exposure", "Mineralocorticoid Receptor Antagonist (MRA)"):
        "Records are included for each mineralocorticoid-receptor-"
        "antagonist exposure recorded for the patient — steroidal MRAs "
        "(spironolactone/Aldactone, eplerenone) and the non-steroidal "
        "MRA finerenone (Kerendia).",
    ("drug_exposure", "Loop Diuretic"):
        "Records are included for each loop-diuretic exposure "
        "(furosemide/Lasix, bumetanide, torsemide, ethacrynic acid) "
        "recorded for the patient.",
    ("drug_exposure", "Calcium Channel Blocker"):
        "Records are included for each calcium-channel-blocker exposure "
        "recorded for the patient — dihydropyridines (amlodipine/Norvasc, "
        "nifedipine/Procardia, felodipine, nicardipine, isradipine) and "
        "non-dihydropyridines (diltiazem, verapamil).",
    ("drug_exposure", "Statin (HMG-CoA Reductase Inhibitor)"):
        "Records are included for each statin exposure (atorvastatin, "
        "rosuvastatin, simvastatin, pravastatin, pitavastatin, "
        "lovastatin, fluvastatin) recorded for the patient.",
    ("drug_exposure", "Sodium Bicarbonate (Acidosis Correction)"):
        "Records are included for each oral-sodium-bicarbonate exposure "
        "recorded for the patient — used to correct metabolic acidosis "
        "in advanced CKD.",
    ("drug_exposure", "Phosphate Binder"):
        "Records are included for each phosphate-binder exposure "
        "(sevelamer/Renvela/Renagel, calcium acetate/PhosLo, lanthanum, "
        "sucroferric oxyhydroxide, ferric citrate) recorded for the "
        "patient.",
    ("drug_exposure", "Potassium Binder"):
        "Records are included for each potassium-binder exposure "
        "(patiromer/Veltassa, sodium zirconium cyclosilicate/Lokelma, "
        "sodium polystyrene sulfonate/Kayexalate) recorded for the "
        "patient.",
    ("drug_exposure", "Erythropoiesis-Stimulating Agent (ESA)"):
        "Records are included for each erythropoiesis-stimulating-agent "
        "exposure recorded for the patient — epoetin alfa (Epogen, "
        "Procrit), darbepoetin alfa (Aranesp), or methoxy polyethylene "
        "glycol-epoetin beta (Mircera).",
    ("condition_occurrence", "Dialysis Initiation (Abstracted)"):
        "Records are included for each first-dialysis transition event "
        "abstracted for the patient — captured via a dependence-on-"
        "dialysis or end-stage-renal-disease condition row. Full "
        "algorithmic definition requires joining the first dialysis "
        "procedure with the first ESRD condition per patient.",
    ("condition_occurrence", "Kidney Transplant (Abstracted)"):
        "Records are included for each kidney-transplant event "
        "abstracted for the patient — kidney transplant, renal "
        "transplant, post-transplant kidney, or transplanted-kidney "
        "concepts.",

    # --- mash_common.yaml ------------------------------------------------
    ("condition_occurrence", "MASH / NAFLD / Fatty Liver"):
        "Records are included for each diagnosis of metabolic-"
        "dysfunction-associated steatohepatitis (MASH/NASH), non-"
        "alcoholic fatty liver disease (NAFLD/MASLD), or fatty change "
        "of the liver recorded for the patient.",
    ("condition_occurrence", "Cirrhosis / Advanced Liver Fibrosis"):
        "Records are included for each diagnosis of cirrhosis, hepatic "
        "fibrosis, or liver fibrosis recorded for the patient — the "
        "MASH-progression endpoint.",
    ("condition_occurrence", "Upper GI Comorbidities (Gastritis / Epigastric pain / H. pylori)"):
        "Records are included for each diagnosis of epigastric pain, "
        "gastritis, Helicobacter pylori infection, or gastroesophageal "
        "reflux (GERD) recorded for the patient — the comorbidity "
        "burden that drives PPI utilisation in MASH cohorts.",
    ("drug_exposure", "Proton Pump Inhibitor (PPI)"):
        "Records are included for each proton-pump-inhibitor exposure "
        "(pantoprazole, omeprazole, esomeprazole, lansoprazole, "
        "rabeprazole, dexlansoprazole) recorded for the patient.",
    ("drug_exposure", "Mucosal Protectant (Sucralfate)"):
        "Records are included for each sucralfate (Carafate) exposure "
        "recorded for the patient — a mucosal protectant for peptic-"
        "ulcer and reflux-related mucosal injury.",
    ("drug_exposure", "Bowel Preparation (Polyethylene Glycol)"):
        "Records are included for each polyethylene-glycol bowel-prep "
        "exposure (GoLytely, Miralax) recorded for the patient.",

    # --- ibd_common.yaml -------------------------------------------------
    ("condition_occurrence", "IBD Diagnosis"):
        "Records are included for each diagnosis of ulcerative colitis, "
        "Crohn's disease, inflammatory bowel disease (IBD), or regional "
        "enteritis recorded for the patient. Microscopic, collagenous, "
        "and lymphocytic colitis are excluded.",
    ("condition_occurrence", "GI Symptoms (Diarrhea / Rectal Bleeding / Epigastric Pain)"):
        "Records are included for each diagnosis of diarrhea, rectal "
        "bleeding or hemorrhage, epigastric pain, or abdominal pain "
        "recorded for the patient — IBD flare-activity signals.",
    ("drug_exposure", "5-ASA (Aminosalicylate)"):
        "Records are included for each 5-aminosalicylate exposure "
        "recorded for the patient — mesalamine (Apriso, Asacol, Pentasa, "
        "Lialda, Delzicol), sulfasalazine, balsalazide, or olsalazine.",
    ("drug_exposure", "Corticosteroid (Topical / Systemic / Budesonide)"):
        "Records are included for each corticosteroid exposure for IBD "
        "flares — topical hydrocortisone (Anusol), oral prednisone or "
        "methylprednisolone, or budesonide (Entocort, Uceris) — recorded "
        "for the patient.",
    ("drug_exposure", "Pancreatic Enzyme Replacement (Pancrelipase)"):
        "Records are included for each pancrelipase exposure (Creon, "
        "Zenpep, Pancreaze) recorded for the patient — indicated for "
        "pancreatic insufficiency.",
    ("drug_exposure", "IBD Biologic / Immunomodulator (Abstracted)"):
        "Records are included for each IBD-targeted biologic or "
        "immunomodulator exposure abstracted for the patient — anti-TNF "
        "(infliximab/Remicade, adalimumab/Humira, golimumab, "
        "certolizumab), anti-integrin (vedolizumab/Entyvio, natalizumab), "
        "anti-IL-12/23 (ustekinumab/Stelara, risankizumab, mirikizumab), "
        "JAK inhibitors (tofacitinib, upadacitinib), S1P modulators "
        "(ozanimod, etrasimod), or traditional immunomodulators "
        "(azathioprine, 6-mercaptopurine, methotrexate).",

    # --- drg_ckd.yaml ----------------------------------------------------
    ("procedure_occurrence", "Smoking Status (Procedure-coded)"):
        "Records are included for each smoking-status SNOMED procedure "
        "recorded for the patient — current/former/never tobacco user, "
        "current/former/never smoker, or explicit smoking-status "
        "concepts. Cessation and counselling procedures (CPT 99406-"
        "99407) are excluded.",

    # --- retinal_common.yaml ---------------------------------------------
    ("condition_occurrence", "Intraocular Lens / Post-Cataract State"):
        "Records are included for each diagnosis of intraocular-lens "
        "presence, pseudophakia, or aphakia recorded for the patient — "
        "post-cataract-surgery status that affects OCT interpretation "
        "and intraocular-injection planning.",
    ("condition_occurrence", "Type 2 Diabetes Mellitus"):
        "Records are included for each diagnosis of type 2 diabetes "
        "mellitus recorded for the patient. Type 1 diabetes and "
        "gestational diabetes are excluded.",
    ("observation", "Macular Grid Total Volume (OCT)"):
        "Records are included for each total-macular-volume OCT "
        "measurement (macular grid total volume) recorded for the "
        "patient.",
    ("observation", "Average Retinal Thickness (OCT)"):
        "Records are included for each average-retinal-thickness OCT "
        "measurement recorded for the patient.",
    ("procedure_occurrence", "Intravitreal Injection"):
        "Records are included for each intravitreal injection of a "
        "pharmacologic agent recorded for the patient — the primary "
        "therapy for diabetic macular edema and exudative AMD. Agent "
        "identity (aflibercept, ranibizumab, bevacizumab) is captured "
        "separately under Medications when available.",
    ("procedure_occurrence", "Retinal Photocoagulation / Laser"):
        "Records are included for each retinal-laser-photocoagulation "
        "procedure (panretinal or focal) recorded for the patient.",

    # --- dr_common.yaml --------------------------------------------------
    ("condition_occurrence", "Diabetic Retinopathy"):
        "Records are included for each diagnosis of diabetic retinopathy "
        "recorded for the patient — proliferative (PDR) and "
        "nonproliferative (NPDR) variants across mild/moderate/severe "
        "severities, with or without diabetic macular edema (DME). "
        "Non-diabetic retinopathy variants are excluded.",
    ("condition_occurrence", "DR Staging (PDR vs NPDR)"):
        "Records are included for each proliferative- or "
        "nonproliferative-diabetic-retinopathy diagnosis recorded for "
        "the patient — PDR or NPDR severity-stratification labels.",
    ("condition_occurrence", "Cataract"):
        "Records are included for each cataract diagnosis recorded for "
        "the patient. Concepts explicitly negating cataract (\"no "
        "cataract\") are excluded.",
    ("drug_exposure", "Insulin"):
        "Records are included for each insulin exposure recorded for "
        "the patient — basal insulins (glargine/Lantus, detemir, "
        "degludec/Tresiba) and prandial insulins (aspart/Novolog, "
        "lispro/Humalog, regular, NPH).",

    # --- amd_common.yaml -------------------------------------------------
    ("condition_occurrence", "Age-Related Macular Degeneration"):
        "Records are included for each diagnosis of age-related macular "
        "degeneration (AMD/ARMD) recorded for the patient — covers all "
        "severities and both exudative (wet) and non-exudative (dry/"
        "atrophic) variants.",
    ("condition_occurrence", "Exudative (Wet) AMD with Choroidal Neovascularization"):
        "Records are included for each diagnosis of exudative (wet) AMD "
        "with active choroidal neovascularization recorded for the "
        "patient — the subset that drives intravitreal anti-VEGF "
        "utilisation.",
    ("condition_occurrence", "Non-exudative (Dry) AMD"):
        "Records are included for each diagnosis of non-exudative (dry) "
        "AMD recorded for the patient — including early-stage and "
        "geographic-atrophy variants.",
    ("condition_occurrence", "Vitreous Degeneration"):
        "Records are included for each diagnosis of vitreous "
        "degeneration or posterior vitreous detachment recorded for the "
        "patient.",
    ("drug_exposure", "OTC Supplements (Vitamin D / Calcium / Omega-3 / AREDS2 Antioxidants)"):
        "Records are included for each over-the-counter supplement "
        "exposure recorded for the patient — vitamin D, calcium, fish "
        "oil, and omega-3 fatty acids (general supplementation), "
        "together with the AREDS2 antioxidant ingredients lutein, "
        "zeaxanthin, and zinc–copper. Vitamin C, vitamin E, vitamin D, "
        "calcium, and omega-3 are not part of NEI's AREDS2 formula; "
        "the row reports the broader OTC footprint, not AREDS2 "
        "adherence specifically.",
    ("drug_exposure", "GLP-1 Receptor Agonist"):
        "Records are included for each GLP-1 receptor agonist exposure "
        "recorded for the patient — semaglutide (Ozempic, Wegovy), "
        "liraglutide (Victoza), dulaglutide (Trulicity), exenatide, or "
        "tirzepatide (Mounjaro).",
    ("condition_occurrence", "Diagnosis"):
        "Records are included for each diagnosis recorded for the "
        "patient. Subset rows below split out the cohort-defining and "
        "comorbidity diagnoses.",
}


# Match a `criteria:` line so the walker can locate the end of each
# row's criteria block (where the inclusion_criteria sits).
_CRITERIA_LINE_RE = re.compile(r"^(?P<indent>\s+)criteria:\s*(?P<rest>.*)$")
_INCLUSION_LINE_RE = re.compile(r"^\s+inclusion_criteria:\s*(?P<rest>.*)$")


def _find_block_end(lines: list[str], start_idx: int, row_indent: str) -> int:
    rest = lines[start_idx]
    after = rest.split(":", 1)[1].lstrip()
    if not after.startswith(">") and not after.startswith("|"):
        return start_idx + 1
    i = start_idx + 1
    n = len(lines)
    while i < n:
        line = lines[i]
        if not line.strip():
            j = i + 1
            while j < n and not lines[j].strip():
                j += 1
            if j < n and (lines[j].startswith(row_indent + " ")
                          or lines[j].startswith(row_indent + "\t")):
                i = j + 1
                continue
            return i
        if line.startswith(row_indent + " ") or line.startswith(row_indent + "\t"):
            i += 1
            continue
        return i
    return i


# Per-file overrides for keys that mean different things in different
# packs. The same `(table, variable)` tuple can appear in two different
# `_common` packs with clinically distinct interpretations — for
# example, FEV1/FVC ratio is a COPD diagnostic threshold (< 0.70 per
# GOLD) but in asthma it's the bronchodilator-reversibility marker.
# Lookup order: per-file override → global MAPPINGS.
OVERRIDES: dict[str, dict[tuple[str, str], str]] = {
    "asthma_common.yaml": {
        ("measurement", "FEV1 / FVC Ratio"):
            "Records are included for each FEV1/FVC ratio measurement "
            "abstracted from a pulmonary-function report for the "
            "patient — pre- and post-bronchodilator values used to "
            "confirm airflow obstruction and assess bronchodilator "
            "reversibility, the diagnostic criterion for asthma.",
    },
}


def _process_file(path: Path) -> int:
    text = path.read_text(encoding="utf-8")
    data = yaml.safe_load(text) or {}
    rows = data.get("variables") or []
    file_overrides = OVERRIDES.get(path.name, {})

    plan: dict[tuple[str, str], str] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        key = (
            (row.get("table") or "").strip(),
            (row.get("variable") or row.get("column") or "").strip(),
        )
        if key in file_overrides:
            plan[key] = file_overrides[key]
        elif key in MAPPINGS:
            plan[key] = MAPPINGS[key]

    if not plan:
        return 0

    lines = text.splitlines(keepends=True)
    out: list[str] = []
    i = 0
    n = len(lines)
    written = 0

    while i < n:
        line = lines[i]
        m_crit = _CRITERIA_LINE_RE.match(line)
        m_inc = _INCLUSION_LINE_RE.match(line)

        if m_crit:
            row_indent = m_crit.group("indent")
            seen_table = ""
            seen_variable = ""
            j = i - 1
            while j >= 0:
                prev = lines[j]
                stripped = prev.lstrip()
                if not prev.strip():
                    j -= 1
                    continue
                if stripped.startswith("- "):
                    kv = stripped[2:]
                    if kv.startswith("table:"):
                        seen_table = kv.split(":", 1)[1].strip()
                    elif kv.startswith("variable:"):
                        seen_variable = kv.split(":", 1)[1].strip()
                    break
                if stripped.startswith("table:") and not seen_table:
                    seen_table = stripped.split(":", 1)[1].strip()
                elif stripped.startswith("variable:") and not seen_variable:
                    seen_variable = stripped.split(":", 1)[1].strip()
                j -= 1

            end_idx = _find_block_end(lines, i, row_indent)
            for k in range(i, end_idx):
                out.append(lines[k])

            key = (seen_table, seen_variable)
            if key in plan:
                # Skip the existing inclusion_criteria line if present
                # (we'll write a fresh one).
                if end_idx < n and _INCLUSION_LINE_RE.match(lines[end_idx]):
                    end_idx_ic = _find_block_end(lines, end_idx, row_indent)
                    end_idx = end_idx_ic
                out.append(f"{row_indent}inclusion_criteria: {plan[key]}\n")
                written += 1
            i = end_idx
            continue

        out.append(line)
        i += 1

    if written:
        path.write_text("".join(out), encoding="utf-8")
    return written


def main() -> int:
    total = 0
    for path in sorted(PACKS_DIR.glob("*.yaml")):
        n = _process_file(path)
        if n:
            print(f"  rewrote {n:3d}  {path.name}", file=sys.stderr)
            total += n
    print(f"\nTotal rewritten: {total}.", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
