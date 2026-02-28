# ============================================================================
# helpers.R — Reusable helper functions for the Century Health Lupus analysis
# ============================================================================

# ── Data Loading ─────────────────────────────────────────────────────────────

#' Load all datasets from the data/ directory
#'
#' Reads patients (CSV), encounters (CSV, converted from Parquet),
#' symptoms (CSV), medications (CSV), and conditions (Excel).
#'
#' @param data_dir Path to the data directory (default "data")
#' @return Named list of raw data frames
load_all_datasets <- function(data_dir = "data") {
  list(
    patients    = read_csv(file.path(data_dir, "patients.csv"),    show_col_types = FALSE),
    encounters  = read_csv(file.path(data_dir, "encounters.csv"),  show_col_types = FALSE),
    symptoms    = read_csv(file.path(data_dir, "symptoms.csv"),    show_col_types = FALSE),
    medications = read_csv(file.path(data_dir, "medications.csv"), show_col_types = FALSE),
    conditions  = read_excel(file.path(data_dir, "conditions.xlsx"))
  )
}

# ── UUID Normalisation ───────────────────────────────────────────────────────

#' Normalise all UUID columns to lowercase for consistent joins
#'
#' @param datasets Named list of data frames from load_all_datasets()
#' @return The same list with UUID columns lowercased
normalise_ids <- function(datasets) {
  datasets$patients    <- datasets$patients    %>% mutate(PATIENT_ID = tolower(PATIENT_ID))
  datasets$encounters  <- datasets$encounters  %>% mutate(Id = tolower(Id), PATIENT = tolower(PATIENT))
  datasets$symptoms    <- datasets$symptoms    %>% mutate(PATIENT = tolower(PATIENT))
  datasets$medications <- datasets$medications %>% mutate(PATIENT = tolower(PATIENT), ENCOUNTER = tolower(ENCOUNTER))
  datasets$conditions  <- datasets$conditions  %>% mutate(PATIENT = tolower(PATIENT), ENCOUNTER = tolower(ENCOUNTER))
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
  medications_df %>%
    mutate(DESCRIPTION = str_to_upper(str_trim(DESCRIPTION)))
}

# ── Date Parsing ─────────────────────────────────────────────────────────────

#' Parse date/datetime columns across all datasets
#'
#' @param datasets Named list of data frames
#' @return Same list with date columns converted
parse_dates <- function(datasets) {
  datasets$patients <- datasets$patients %>%
    mutate(BIRTHDATE = as.Date(BIRTHDATE))

  datasets$encounters <- datasets$encounters %>%
    mutate(START = ymd_hms(START), STOP = ymd_hms(STOP))

  datasets$medications <- datasets$medications %>%
    mutate(START = ymd_hms(START), STOP = ymd_hms(STOP))

  datasets$conditions <- datasets$conditions %>%
    mutate(START = as.Date(START))

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
  symptoms_df %>%
    mutate(
      Rash       = as.integer(str_extract(SYMPTOMS, "(?<=Rash:)\\d+")),
      Joint_Pain = as.integer(str_extract(SYMPTOMS, "(?<=Joint Pain:)\\d+")),
      Fatigue    = as.integer(str_extract(SYMPTOMS, "(?<=Fatigue:)\\d+")),
      Fever      = as.integer(str_extract(SYMPTOMS, "(?<=Fever:)\\d+"))
    )
}

# ── Gender Backfill ──────────────────────────────────────────────────────────

#' Fill missing GENDER in symptoms from the patients table
#'
#' @param symptoms_df Symptoms data frame
#' @param patients_df Patients data frame
#' @return Symptoms data frame with GENDER filled
fill_symptom_gender <- function(symptoms_df, patients_df) {
  gender_lookup <- patients_df %>% select(PATIENT_ID, PAT_GENDER = GENDER)

  symptoms_df %>%
    mutate(GENDER = as.character(GENDER)) %>%
    left_join(gender_lookup, by = c("PATIENT" = "PATIENT_ID")) %>%
    mutate(GENDER = coalesce(GENDER, PAT_GENDER)) %>%
    select(-PAT_GENDER)
}

# ── Medication Therapy Classification ────────────────────────────────────────

#' Classify medications into therapeutic groups
#'
#' @param description Character vector of (uppercase) medication descriptions
#' @return Character vector of therapy labels
classify_therapy <- function(description) {
  case_when(
    str_detect(description, "NAPROXEN")          ~ "Naproxen (NSAID)",
    str_detect(description, "PREDNISONE")        ~ "Prednisone (Corticosteroid)",
    str_detect(description, "CYCLOSPORINE")      ~ "Cyclosporine (Immunosuppressant)",
    str_detect(description, "HYDROXYCHLOROQUINE") ~ "Hydroxychloroquine",
    str_detect(description, "VITAMIN")           ~ "Vitamin B12",
    TRUE                                         ~ "Other"
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
#' @param df Data frame with Rash, Joint_Pain, Fatigue, Fever columns
#' @return Data frame with COMPOSITE_SCORE column added
add_composite_score <- function(df) {
  df %>%
    mutate(COMPOSITE_SCORE = (Rash + Joint_Pain + Fatigue + Fever) / 4)
}

# ── Data Integrity Tests ────────────────────────────────────────────────────

#' Run unit tests to verify data integrity after cleaning
#'
#' @param datasets Named list of cleaned data frames
run_integrity_tests <- function(datasets) {
  patient_ids <- unique(datasets$patients$PATIENT_ID)

  # Referential integrity
  test_that("All medication patients exist in patients table", {
    expect_true(all(datasets$medications$PATIENT %in% patient_ids))
  })

  test_that("All encounter patients exist in patients table", {
    expect_true(all(datasets$encounters$PATIENT %in% patient_ids))
  })

  test_that("All condition patients exist in patients table", {
    expect_true(all(datasets$conditions$PATIENT %in% patient_ids))
  })

  test_that("All symptom patients exist in patients table", {
    expect_true(all(datasets$symptoms$PATIENT %in% patient_ids))
  })

  # Uniqueness
  test_that("Patient IDs are unique", {
    expect_equal(nrow(datasets$patients), n_distinct(datasets$patients$PATIENT_ID))
  })

  test_that("Encounter IDs are unique", {
    expect_equal(nrow(datasets$encounters), n_distinct(datasets$encounters$Id))
  })

  # Medication normalisation
  test_that("Medication descriptions have no mixed-case duplicates", {
    n_raw   <- n_distinct(datasets$medications$DESCRIPTION)
    n_upper <- n_distinct(toupper(datasets$medications$DESCRIPTION))
    expect_equal(n_raw, n_upper)
  })

  # Symptom columns
  test_that("Symptom numeric columns are non-negative", {
    for (col in c("Rash", "Joint_Pain", "Fatigue", "Fever")) {
      expect_true(all(datasets$symptoms[[col]] >= 0, na.rm = TRUE))
    }
  })
}
