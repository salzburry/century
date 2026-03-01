# ============================================================================
# helpers.R — Reusable helper functions for the Century Health Lupus analysis
#
# Required packages (must be attached before sourcing):
#   tidyverse (dplyr, tidyr, stringr, readr, purrr, ggplot2, forcats, tibble)
#   readxl, lubridate, testthat
# ============================================================================

# Guard: check that required packages are available
.required_pkgs <- c("dplyr", "stringr", "readr", "readxl", "lubridate", "testthat")
.missing_pkgs <- .required_pkgs[!vapply(.required_pkgs, requireNamespace,
                                        logical(1), quietly = TRUE)]
if (length(.missing_pkgs) > 0) {
  stop("helpers.R requires the following packages: ",
       paste(.missing_pkgs, collapse = ", "),
       "\nInstall with: install.packages(c(",
       paste(sprintf('"%s"', .missing_pkgs), collapse = ", "), "))")
}

# ── Data Loading ─────────────────────────────────────────────────────────────

#' Load all datasets from the data/ directory
#'
#' Reads patients (CSV), encounters (CSV — pre-converted from Parquet),
#' symptoms (CSV), medications (CSV), and conditions (Excel).
#'
#' encounters.csv must be present in data_dir. It is a pre-converted copy of
#' encounters.parquet and is included in the submission because the R `arrow`
#' package is not always available. To regenerate it:
#'
#'   python3 -c "import pandas; pandas.read_parquet('data/encounters.parquet') \
#'     .to_csv('data/encounters.csv', index=False)"
#'
#' @param data_dir Path to the data directory (default "data")
#' @return Named list of raw data frames
load_all_datasets <- function(data_dir = "data") {
  enc_csv <- file.path(data_dir, "encounters.csv")
  if (!file.exists(enc_csv)) {
    stop("encounters.csv not found in ", data_dir,
         ". See helpers.R documentation for how to generate it from the Parquet source.")
  }

  list(
    patients    = readr::read_csv(file.path(data_dir, "patients.csv"),   show_col_types = FALSE),
    encounters  = readr::read_csv(enc_csv,                                show_col_types = FALSE),
    symptoms    = readr::read_csv(file.path(data_dir, "symptoms.csv"),   show_col_types = FALSE),
    medications = readr::read_csv(file.path(data_dir, "medications.csv"), show_col_types = FALSE),
    conditions  = readxl::read_excel(file.path(data_dir, "conditions.xlsx"))
  )
}

# ── UUID Normalisation ───────────────────────────────────────────────────────

#' Normalise all UUID columns to lowercase for consistent joins
#'
#' @param datasets Named list of data frames from load_all_datasets()
#' @return The same list with UUID columns lowercased
normalise_ids <- function(datasets) {
  datasets$patients    <- dplyr::mutate(datasets$patients,    PATIENT_ID = tolower(PATIENT_ID))
  datasets$encounters  <- dplyr::mutate(datasets$encounters,  Id = tolower(Id), PATIENT = tolower(PATIENT))
  datasets$symptoms    <- dplyr::mutate(datasets$symptoms,    PATIENT = tolower(PATIENT))
  datasets$medications <- dplyr::mutate(datasets$medications, PATIENT = tolower(PATIENT), ENCOUNTER = tolower(ENCOUNTER))
  datasets$conditions  <- dplyr::mutate(datasets$conditions,  PATIENT = tolower(PATIENT), ENCOUNTER = tolower(ENCOUNTER))
  datasets
}

# ── Medication Description Cleaning ──────────────────────────────────────────

#' Standardise medication descriptions to uppercase
#'
#' The raw data contains the same drug in mixed case
#' (e.g. "predniSONE 20 MG Oral Tablet" vs "PREDNISONE 20 MG ORAL TABLET").
#'
#' @param medications_df Medications data frame
#' @return Data frame with DESCRIPTION cleaned
standardise_medication_names <- function(medications_df) {
  dplyr::mutate(medications_df,
                DESCRIPTION = stringr::str_to_upper(stringr::str_trim(DESCRIPTION)))
}

# ── Date Parsing ─────────────────────────────────────────────────────────────

#' Parse date/datetime columns across all datasets
#'
#' @param datasets Named list of data frames
#' @return Same list with date columns converted
parse_dates <- function(datasets) {
  datasets$patients <- dplyr::mutate(datasets$patients,
    BIRTHDATE = as.Date(BIRTHDATE),
    DEATHDATE = as.Date(DEATHDATE)
  )

  datasets$encounters <- dplyr::mutate(datasets$encounters,
    START = lubridate::ymd_hms(START),
    STOP  = lubridate::ymd_hms(STOP)
  )

  datasets$medications <- dplyr::mutate(datasets$medications,
    START = lubridate::ymd_hms(START),
    STOP  = lubridate::ymd_hms(STOP)
  )

  datasets$conditions <- dplyr::mutate(datasets$conditions,
    START = as.Date(START),
    STOP  = as.Date(STOP)
  )

  datasets
}

# ── Symptom Parsing ──────────────────────────────────────────────────────────

#' Parse the packed SYMPTOMS string into separate numeric columns
#'
#' Converts "Rash:34;Joint Pain:39;Fatigue:9;Fever:12" into
#' individual Rash, Joint_Pain, Fatigue, Fever columns.
#'
#' @param symptoms_df Symptoms data frame
#' @return Data frame with parsed symptom columns added
parse_symptom_scores <- function(symptoms_df) {
  dplyr::mutate(symptoms_df,
    Rash       = as.integer(stringr::str_extract(SYMPTOMS, "(?<=Rash:)\\d+")),
    Joint_Pain = as.integer(stringr::str_extract(SYMPTOMS, "(?<=Joint Pain:)\\d+")),
    Fatigue    = as.integer(stringr::str_extract(SYMPTOMS, "(?<=Fatigue:)\\d+")),
    Fever      = as.integer(stringr::str_extract(SYMPTOMS, "(?<=Fever:)\\d+"))
  )
}

# ── Lupus-Only Symptom Filtering ─────────────────────────────────────────────

#' Filter symptoms to one Lupus row per patient
#'
#' The raw symptoms table contains rows for both "Lupus erythematosus" and
#' "Anemia (disorder)", often with identical symptom strings. This function
#' keeps only the Lupus pathology and deduplicates to one row per patient.
#'
#' @param symptoms_df Symptoms data frame (with PATHOLOGY column)
#' @return Deduplicated data frame: one Lupus row per patient
filter_lupus_symptoms <- function(symptoms_df) {
  symptoms_df %>%
    dplyr::filter(stringr::str_to_upper(PATHOLOGY) == "LUPUS ERYTHEMATOSUS") %>%
    dplyr::distinct(PATIENT, .keep_all = TRUE)
}

# ── Gender Backfill ──────────────────────────────────────────────────────────

#' Fill missing GENDER in symptoms from the patients table
#'
#' @param symptoms_df Symptoms data frame
#' @param patients_df Patients data frame
#' @return Symptoms data frame with GENDER filled
fill_symptom_gender <- function(symptoms_df, patients_df) {
  gender_lookup <- dplyr::select(patients_df, PATIENT_ID, PAT_GENDER = GENDER)

  symptoms_df %>%
    dplyr::mutate(GENDER = as.character(GENDER)) %>%
    dplyr::left_join(gender_lookup, by = c("PATIENT" = "PATIENT_ID")) %>%
    dplyr::mutate(GENDER = dplyr::coalesce(GENDER, PAT_GENDER)) %>%
    dplyr::select(-PAT_GENDER)
}

# ── Medication Therapy Classification ────────────────────────────────────────

#' Classify medications into therapeutic groups
#'
#' Uses exact drug-name tokens to avoid false matches (e.g. "VITAMIN" alone
#' would be too broad, so we match the full "VITAMIN B12" substring).
#'
#' @param description Character vector of (uppercase) medication descriptions
#' @return Character vector of therapy labels
classify_therapy <- function(description) {
  dplyr::case_when(
    stringr::str_detect(description, "NAPROXEN")          ~ "Naproxen (NSAID)",
    stringr::str_detect(description, "PREDNISONE")        ~ "Prednisone (Corticosteroid)",
    stringr::str_detect(description, "CYCLOSPORINE")      ~ "Cyclosporine (Immunosuppressant)",
    stringr::str_detect(description, "HYDROXYCHLOROQUINE") ~ "Hydroxychloroquine",
    stringr::str_detect(description, "VITAMIN B12")       ~ "Vitamin B12",
    TRUE                                                  ~ "Other"
  )
}

# ── Age Group Assignment ─────────────────────────────────────────────────────

#' Assign age groups based on age value
#'
#' @param age Numeric vector of ages
#' @return Factor with age group labels
assign_age_group <- function(age) {
  cut(age,
      breaks = c(-Inf, 29, 54, Inf),
      labels = c("Young (<30)", "Middle (30-54)", "Older (55+)"))
}

# ── Composite Score ──────────────────────────────────────────────────────────

#' Compute the composite symptom score (mean of 4 symptom categories)
#'
#' Uses rowMeans with na.rm = TRUE so that a missing individual score does
#' not invalidate the entire composite.
#'
#' @param df Data frame with Rash, Joint_Pain, Fatigue, Fever columns
#' @return Data frame with COMPOSITE_SCORE column added
add_composite_score <- function(df) {
  dplyr::mutate(df,
    COMPOSITE_SCORE = rowMeans(
      dplyr::across(c(Rash, Joint_Pain, Fatigue, Fever)),
      na.rm = TRUE
    )
  )
}

# ── Data Integrity Tests ────────────────────────────────────────────────────

#' Run unit tests to verify data integrity after cleaning
#'
#' @param datasets Named list of cleaned data frames
run_integrity_tests <- function(datasets) {
  patient_ids   <- unique(datasets$patients$PATIENT_ID)
  encounter_ids <- unique(datasets$encounters$Id)

  # ── Referential integrity: patient FKs ──
  testthat::test_that("All medication patients exist in patients table", {
    testthat::expect_true(all(datasets$medications$PATIENT %in% patient_ids))
  })

  testthat::test_that("All encounter patients exist in patients table", {
    testthat::expect_true(all(datasets$encounters$PATIENT %in% patient_ids))
  })

  testthat::test_that("All condition patients exist in patients table", {
    testthat::expect_true(all(datasets$conditions$PATIENT %in% patient_ids))
  })

  testthat::test_that("All symptom patients exist in patients table", {
    testthat::expect_true(all(datasets$symptoms$PATIENT %in% patient_ids))
  })

  # ── Referential integrity: encounter FKs ──
  testthat::test_that("All medication encounters exist in encounters table", {
    testthat::expect_true(all(datasets$medications$ENCOUNTER %in% encounter_ids))
  })

  testthat::test_that("All condition encounters exist in encounters table", {
    testthat::expect_true(all(datasets$conditions$ENCOUNTER %in% encounter_ids))
  })

  # ── Uniqueness ──
  testthat::test_that("Patient IDs are unique", {
    testthat::expect_equal(nrow(datasets$patients),
                           dplyr::n_distinct(datasets$patients$PATIENT_ID))
  })

  testthat::test_that("Encounter IDs are unique", {
    testthat::expect_equal(nrow(datasets$encounters),
                           dplyr::n_distinct(datasets$encounters$Id))
  })

  # ── Medication descriptions are fully uppercase ──
  testthat::test_that("All medication descriptions are uppercase", {
    descs <- datasets$medications$DESCRIPTION
    testthat::expect_true(all(descs == toupper(descs)))
  })

  # ── Symptom columns ──
  testthat::test_that("Symptom numeric columns are non-negative", {
    for (col in c("Rash", "Joint_Pain", "Fatigue", "Fever")) {
      testthat::expect_true(all(datasets$symptoms[[col]] >= 0, na.rm = TRUE))
    }
  })

  # ── Lupus deduplication ──
  testthat::test_that("Symptoms table has one row per patient", {
    testthat::expect_equal(nrow(datasets$symptoms),
                           dplyr::n_distinct(datasets$symptoms$PATIENT))
  })

  # ── Cohort containment: symptom patients ⊆ diagnosed Lupus patients ──
  testthat::test_that("All symptom-observed patients are in the diagnosed Lupus cohort", {
    lupus_ids <- unique(datasets$conditions$PATIENT[
      stringr::str_to_upper(datasets$conditions$DESCRIPTION) == "LUPUS ERYTHEMATOSUS"
    ])
    testthat::expect_true(all(datasets$symptoms$PATIENT %in% lupus_ids))
  })
}
