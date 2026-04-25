#!/usr/bin/env python3
"""Rewrite customer-visible `notes:` entries that leak QA language.

Internal rationale ("TODO (clinical review): ...", "Implemented=No",
"AND-qualifier ...", "earlier draft", "deliberately") is fine in YAML
comments above the row but should not appear in the customer-facing
workbook. This helper takes a curated NOTES_MAPPINGS dict keyed on
`(table, variable)` and replaces the offending row's `notes:` value
with a clean clinical caveat — or removes the notes entirely when the
original was pure rationale that has no customer-facing equivalent.

Idempotent. Hand-written notes that aren't in the mapping are
preserved.

Run from repo root:  python scripts/rewrite_notes.py
"""
from __future__ import annotations

import re
import sys
from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parent.parent
PACKS_DIR = REPO_ROOT / "packs" / "variables"


# Curated `notes:` rewrites. Empty string deletes the notes line
# entirely (use when the original was pure rationale with no clinical
# caveat to preserve). Non-empty values become the new notes content.
NOTES_MAPPINGS: dict[tuple[str, str], str] = {
    # --- ckd_common.yaml -------------------------------------------------
    ("measurement", "Serum Creatinine"):
        "Scoped to serum, plasma, or blood specimens; urine creatinine "
        "is captured separately under the UACR row.",
    ("measurement", "Estimated GFR (eGFR)"):
        "Some sites store eGFR only as a derived flag in the EHR rather "
        "than as a discrete measurement; downstream consumers can "
        "compute eGFR from the Serum Creatinine row using CKD-EPI 2021 "
        "when this row is sparse.",
    ("measurement", "Hemoglobin"):
        "Scoped to whole-blood and mass-per-volume hemoglobin; glycated "
        "hemoglobin (HbA1c) and other hemoglobin variants are tracked "
        "separately.",
    ("measurement", "Serum Calcium"):
        "Scoped to serum, plasma, and blood specimens; urine calcium "
        "and calcium-channel-blocker drug mentions are excluded. "
        "Ionised vs total calcium can be split in a per-cohort row.",
    ("measurement", "Serum Sodium"):
        "Scoped to serum, plasma, and blood specimens; urine sodium and "
        "sodium-bicarbonate drug mentions are excluded — the drug is "
        "captured under the Sodium Bicarbonate medication row.",
    ("measurement", "Urine Albumin / Creatinine Ratio (UACR)"):
        "Some cohorts report a urea/creatinine ratio in urine instead "
        "of the canonical albumin/creatinine ratio; the two are "
        "different labs and the urea form does not match this row.",
    ("measurement", "HbA1c (Hemoglobin A1c)"):
        "Standard-of-care for diabetic CKD but not always stored under a "
        "measurement-side concept; sites that record HbA1c in "
        "`observation` will not surface here.",
    ("condition_occurrence", "Hypertension"):
        "Narrowed to essential, primary, and hypertensive-disorder "
        "variants so hypertensive renal disease — already captured by "
        "the CKD Diagnosis row — is not counted twice.",
    ("drug_exposure", "Calcium Channel Blocker"):
        "CCBs are foundational for blood-pressure management in CKD "
        "(where ACE-i / ARB alone rarely reaches target) but are not "
        "CKD-specific. Class-level breakdown into dihydropyridine and "
        "non-dihydropyridine subtypes is available in per-cohort packs.",
    ("drug_exposure", "Statin (HMG-CoA Reductase Inhibitor)"):
        "Standard-of-care in CKD with cardiovascular risk; not all "
        "sites surface every branded statin under the same concept.",
    ("drug_exposure", "Phosphate Binder"):
        "Utilisation typically appears in dialysis-eligible patients only.",
    ("drug_exposure", "Potassium Binder"):
        "Newer agents (patiromer, sodium zirconium cyclosilicate) may "
        "not appear in older cohorts.",
    ("drug_exposure", "Erythropoiesis-Stimulating Agent (ESA)"):
        "Utilisation typically appears in dialysis or pre-dialysis CKD "
        "patients.",
    ("drug_exposure", "SGLT2 Inhibitor"):
        "Modern standard-of-care add-on for CKD; lower utilisation in "
        "cohorts whose data predates the mid-2021 DAPA-CKD / "
        "EMPA-KIDNEY adoption window.",
    ("drug_exposure", "Mineralocorticoid Receptor Antagonist (MRA)"):
        "Finerenone (Kerendia) is the newest agent in this class; "
        "lower utilisation in older cohorts.",
    ("condition_occurrence", "Dialysis Initiation (Abstracted)"):
        "Condition-side proxy only. A full algorithmic dialysis-"
        "initiation definition requires joining the first dialysis "
        "procedure with the first ESRD or dependence-on-dialysis "
        "condition per patient.",
    ("condition_occurrence", "Kidney Transplant (Abstracted)"):
        "Some sites record transplant only as a procedure; if this row "
        "is sparse, mirror the row onto procedure_occurrence with "
        "appropriate CPT criteria.",

    # --- mash_common.yaml ------------------------------------------------
    ("condition_occurrence", "Cirrhosis / Advanced Liver Fibrosis"):
        "Confirm at the per-cohort level whether fibrosis is "
        "stratified by stage (F0–F4).",

    # --- ibd_common.yaml -------------------------------------------------
    ("drug_exposure", "IBD Biologic / Immunomodulator (Abstracted)"):
        "Standard-of-care for moderate-to-severe IBD. Lower utilisation "
        "is expected in cohorts dominated by mild disease; useful as a "
        "severity proxy regardless.",

    # --- respiratory_common.yaml -----------------------------------------
    ("observation", "BMI (Body Mass Index)"):
        "Some sites record BMI in the `measurement` table rather than "
        "`observation`; an alternate row on measurement_concept_name "
        "may be needed to surface those sites.",
    ("observation", "Smoking Status"):
        "Some warehouses map smoking status onto condition_occurrence "
        "(e.g. \"Tobacco use disorder\") instead of observation; the "
        "DRG cohort also surfaces it as a procedure-coded SNOMED "
        "concept. Pack-year burden is a separate abstracted variable.",
    ("measurement", "Blood Eosinophils"):
        "Scoped to blood / CBC / differential / absolute / percentage "
        "qualifiers so sputum and tissue-biopsy eosinophil concepts "
        "are not counted here.",

    # --- copd_common.yaml ------------------------------------------------
    ("drug_exposure", "Long-acting Muscarinic Antagonist (LAMA)"):
        "Glycopyrrolate is scoped to inhaled formulations; oral and "
        "injectable formulations are used for secretion management and "
        "are not COPD maintenance therapy.",
    ("drug_exposure", "COPD Triple Therapy (ICS + LABA + LAMA)"):
        "Cross-cuts the ICS, LABA, and LAMA rows above; reviewers "
        "should not aggregate patient counts across rows.",
    ("drug_exposure", "Systemic Corticosteroid (Oral / IV)"):
        "Captures all systemic steroid exposures. Attributing a course "
        "to a COPD exacerbation requires a temporal cross-reference "
        "with the COPD Exacerbation diagnosis row.",
    ("procedure_occurrence", "Supplemental Oxygen Therapy"):
        "DME claims often carry oxygen via HCPCS E-codes (E0424, E1390) "
        "that map to different concept names; broadening criteria to "
        "include those codes may be needed at some sites.",
    ("measurement", "FEV1 (Forced Expiratory Volume in 1 second)"):
        "Abstracted from PFT reports rather than a structured lab feed "
        "at most sites; per-cohort abstraction pipelines may be needed.",
    ("condition_occurrence", "COPD Exacerbation (Abstracted)"):
        "Confirm the exacerbation definition per cohort (condition-only "
        "vs visit-type + diagnosis + steroid course). Current criteria "
        "is narrow to avoid matching non-COPD exacerbations.",
    ("observation", "CAT Score (COPD Assessment Test)"):
        "Often captured as a scanned questionnaire rather than a "
        "structured observation; a dedicated CAT abstraction may be "
        "needed.",
    ("observation", "mMRC Dyspnea Scale"):
        "Typically lives in clinical notes rather than structured "
        "observations.",

    # --- asthma_common.yaml ----------------------------------------------
    ("drug_exposure", "Asthma Biologic (Anti-IgE / Anti-IL5 / Anti-IL4R / Anti-TSLP)"):
        "Each biologic has its own eligibility criteria (total IgE, "
        "eosinophil count, age); this row reports any-biologic "
        "exposure. Per-ingredient filtering is needed to attribute a "
        "specific agent.",
    ("drug_exposure", "Systemic Corticosteroid (Oral / IV)"):
        "Captures all systemic steroid exposures. Attributing a course "
        "to an asthma exacerbation requires a temporal cross-reference "
        "with the Asthma Exacerbation diagnosis row.",
    ("measurement", "Total IgE (Immunoglobulin E)"):
        "Scoped to total / serum / immunoglobulin-E qualifiers so "
        "allergen-specific IgE panels (which can run to hundreds of "
        "concepts) do not dominate this row.",
    ("measurement", "FeNO (Fractional Exhaled Nitric Oxide)"):
        "Not universally billed as a lab; some sites capture FeNO as "
        "an observation, in which case this row will be sparse.",
    ("condition_occurrence", "Asthma Exacerbation (Abstracted)"):
        "Confirm the exacerbation definition per cohort (condition-only "
        "vs visit-type + diagnosis + steroid course). Current criteria "
        "is narrow to avoid matching chronic or well-controlled asthma.",
    ("observation", "ACT Score (Asthma Control Test)"):
        "Often captured on paper or within the clinical note; a "
        "dedicated ACT abstraction may be needed at some sites.",

    # --- aat_common.yaml -------------------------------------------------
    ("procedure_occurrence", "Infusion Procedure Codes (candidate AAT attribution)"):
        "Row is broad — HCPCS administration lines will match non-AAT "
        "infusion activity too. Confirm anti-amyloid attribution by "
        "cross-referencing the Anti-amyloid Therapy (Administration) "
        "row on patient and date.",

    # --- drg_ckd.yaml ----------------------------------------------------
    ("procedure_occurrence", "Smoking Status (Procedure-coded)"):
        "Parallel to the observation-side Smoking Status row. "
        "Combined cohort-level counts require de-duplicating by "
        "person across the two rows. Cessation and counselling "
        "procedures (CPT 99406-99407) record an intervention, not a "
        "patient status, and are excluded.",

    # --- amd_common.yaml -------------------------------------------------
    # AREDS2 row was rewritten in-line during the rename; it already
    # has clean notes.

    # --- dr_common.yaml --------------------------------------------------
    ("drug_exposure", "GLP-1 Receptor Agonist"):
        "Worth tracking because GLP-1 receptor agonists have been "
        "associated with transient worsening of diabetic retinopathy "
        "in clinical trials — a signal reviewers frequently ask about. "
        "Lower utilisation is expected in older cohorts.",
    ("drug_exposure", "SGLT2 Inhibitor"):
        "Lower utilisation is expected at sites whose data predates "
        "broad SGLT2 adoption.",

    # --- respiratory_common.yaml -----------------------------------------
    ("document", "Document (Chest imaging / PFT report)"):
        "Sites that file PFT reports under a generic document type "
        "(e.g. \"Summary\" or \"Continuity of Care Document\") may be "
        "sparse on this row until a more specific document-type label "
        "is available.",

    # --- ckd_common.yaml additional --------------------------------------
    ("observation", "Blood Pressure (Systolic / Diastolic, numeric)"):
        "Scoped to systolic and diastolic concepts so the generic "
        "\"Blood pressure\" concept (typically stored as a string) "
        "does not leak into this numeric row. Combined-format readings "
        "appear under the Blood Pressure (Combined) row instead.",
}


_NOTES_LINE_RE = re.compile(r"^(?P<indent>\s+)notes:\s*(?P<rest>.*)$")


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


def _process_file(path: Path) -> int:
    text = path.read_text(encoding="utf-8")
    data = yaml.safe_load(text) or {}
    rows = data.get("variables") or []

    plan: dict[tuple[str, str], str] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        key = (
            (row.get("table") or "").strip(),
            (row.get("variable") or row.get("column") or "").strip(),
        )
        if key in NOTES_MAPPINGS:
            plan[key] = NOTES_MAPPINGS[key]

    if not plan:
        return 0

    lines = text.splitlines(keepends=True)
    out: list[str] = []
    i = 0
    n = len(lines)
    written = 0

    while i < n:
        line = lines[i]
        m_notes = _NOTES_LINE_RE.match(line)
        if not m_notes:
            out.append(line)
            i += 1
            continue

        row_indent = m_notes.group("indent")
        # Find which row this notes belongs to.
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
        new_notes = plan.get((seen_table, seen_variable))
        if new_notes is None:
            for k in range(i, end_idx):
                out.append(lines[k])
            i = end_idx
            continue

        if new_notes:
            out.append(f"{row_indent}notes: {new_notes}\n")
        # else: drop the notes block entirely.
        written += 1
        i = end_idx

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
    print(f"\nTotal notes rewritten: {total}.", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
