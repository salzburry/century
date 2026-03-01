# helpers.R - utility functions for the lupus analysis
# These assume tidyverse, lubridate, readxl, and testthat are already loaded.

load_all_datasets <- function(data_dir = "data") {
  enc_csv <- file.path(data_dir, "encounters.csv")
  if (!file.exists(enc_csv)) {
    stop("encounters.csv not found in ", data_dir,
         ". Convert from parquet with: ",
         "python3 -c \"import pandas; pandas.read_parquet('data/encounters.parquet').to_csv('data/encounters.csv', index=False)\"")
  }

  list(
    patients    = read_csv(file.path(data_dir, "patients.csv"),    show_col_types = FALSE),
    encounters  = read_csv(enc_csv,                                show_col_types = FALSE),
    symptoms    = read_csv(file.path(data_dir, "symptoms.csv"),    show_col_types = FALSE),
    medications = read_csv(file.path(data_dir, "medications.csv"), show_col_types = FALSE),
    conditions  = read_excel(file.path(data_dir, "conditions.xlsx"))
  )
}

# lowercase all UUID columns so joins don't silently fail
normalise_ids <- function(datasets) {
  datasets$patients    <- datasets$patients    %>% mutate(PATIENT_ID = tolower(PATIENT_ID))
  datasets$encounters  <- datasets$encounters  %>% mutate(Id = tolower(Id), PATIENT = tolower(PATIENT))
  datasets$symptoms    <- datasets$symptoms    %>% mutate(PATIENT = tolower(PATIENT))
  datasets$medications <- datasets$medications %>% mutate(PATIENT = tolower(PATIENT), ENCOUNTER = tolower(ENCOUNTER))
  datasets$conditions  <- datasets$conditions  %>% mutate(PATIENT = tolower(PATIENT), ENCOUNTER = tolower(ENCOUNTER))
  datasets
}

# the raw data has the same drug in mixed case (e.g. "predniSONE" vs "PREDNISONE")
standardise_medication_names <- function(medications_df) {
  medications_df %>% mutate(DESCRIPTION = str_to_upper(str_trim(DESCRIPTION)))
}

parse_dates <- function(datasets) {
  datasets$patients <- datasets$patients %>%
    mutate(BIRTHDATE = as.Date(BIRTHDATE),
           DEATHDATE = as.Date(DEATHDATE))

  datasets$encounters <- datasets$encounters %>%
    mutate(START = ymd_hms(START), STOP = ymd_hms(STOP))

  datasets$medications <- datasets$medications %>%
    mutate(START = ymd_hms(START), STOP = ymd_hms(STOP))

  datasets$conditions <- datasets$conditions %>%
    mutate(START = as.Date(START), STOP = as.Date(STOP))

  datasets
}

# "Rash:34;Joint Pain:39;Fatigue:9;Fever:12" -> separate numeric columns
parse_symptom_scores <- function(symptoms_df) {
  symptoms_df %>%
    mutate(
      Rash       = as.integer(str_extract(SYMPTOMS, "(?<=Rash:)\\d+")),
      Joint_Pain = as.integer(str_extract(SYMPTOMS, "(?<=Joint Pain:)\\d+")),
      Fatigue    = as.integer(str_extract(SYMPTOMS, "(?<=Fatigue:)\\d+")),
      Fever      = as.integer(str_extract(SYMPTOMS, "(?<=Fever:)\\d+"))
    )
}

# symptoms table has rows for both Lupus and Anemia — keep only Lupus,
# and deduplicate so we have one row per patient
filter_lupus_symptoms <- function(symptoms_df) {
  symptoms_df %>%
    filter(str_to_upper(PATHOLOGY) == "LUPUS ERYTHEMATOSUS") %>%
    distinct(PATIENT, .keep_all = TRUE)
}

# GENDER is all NA in symptoms.csv, so fill it in from the patients table
fill_symptom_gender <- function(symptoms_df, patients_df) {
  gender_lookup <- patients_df %>% select(PATIENT_ID, PAT_GENDER = GENDER)

  symptoms_df %>%
    mutate(GENDER = as.character(GENDER)) %>%
    left_join(gender_lookup, by = c("PATIENT" = "PATIENT_ID")) %>%
    mutate(GENDER = coalesce(GENDER, PAT_GENDER)) %>%
    select(-PAT_GENDER)
}

# map drug descriptions to therapy groups
classify_therapy <- function(description) {
  case_when(
    str_detect(description, "NAPROXEN")          ~ "Naproxen (NSAID)",
    str_detect(description, "PREDNISONE")        ~ "Prednisone (Corticosteroid)",
    str_detect(description, "CYCLOSPORINE")      ~ "Cyclosporine (Immunosuppressant)",
    str_detect(description, "HYDROXYCHLOROQUINE") ~ "Hydroxychloroquine",
    str_detect(description, "VITAMIN B12")       ~ "Vitamin B12",
    TRUE                                         ~ "Other"
  )
}

assign_age_group <- function(age) {
  cut(age, breaks = c(-Inf, 29, 54, Inf),
      labels = c("Young (<30)", "Middle (30-54)", "Older (55+)"))
}

# composite = mean of the 4 symptom categories
add_composite_score <- function(df) {
  df %>%
    mutate(COMPOSITE_SCORE = rowMeans(across(c(Rash, Joint_Pain, Fatigue, Fever)),
                                     na.rm = TRUE))
}

# sanity checks on the cleaned data
run_integrity_tests <- function(datasets) {
  patient_ids   <- unique(datasets$patients$PATIENT_ID)
  encounter_ids <- unique(datasets$encounters$Id)

  # referential integrity - patients
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

  # referential integrity - encounters
  test_that("All medication encounters exist in encounters table", {
    expect_true(all(datasets$medications$ENCOUNTER %in% encounter_ids))
  })
  test_that("All condition encounters exist in encounters table", {
    expect_true(all(datasets$conditions$ENCOUNTER %in% encounter_ids))
  })

  # uniqueness
  test_that("Patient IDs are unique", {
    expect_equal(nrow(datasets$patients), n_distinct(datasets$patients$PATIENT_ID))
  })
  test_that("Encounter IDs are unique", {
    expect_equal(nrow(datasets$encounters), n_distinct(datasets$encounters$Id))
  })

  # cleaning checks
  test_that("All medication descriptions are uppercase", {
    descs <- datasets$medications$DESCRIPTION
    expect_true(all(descs == toupper(descs)))
  })
  test_that("Symptom numeric columns are non-negative", {
    for (col in c("Rash", "Joint_Pain", "Fatigue", "Fever")) {
      expect_true(all(datasets$symptoms[[col]] >= 0, na.rm = TRUE))
    }
  })
  test_that("Symptoms table has one row per patient", {
    expect_equal(nrow(datasets$symptoms), n_distinct(datasets$symptoms$PATIENT))
  })

  # all symptom patients should actually have a lupus diagnosis
  test_that("All symptom patients are in the diagnosed Lupus cohort", {
    lupus_ids <- unique(datasets$conditions$PATIENT[
      str_to_upper(datasets$conditions$DESCRIPTION) == "LUPUS ERYTHEMATOSUS"
    ])
    expect_true(all(datasets$symptoms$PATIENT %in% lupus_ids))
  })
}
