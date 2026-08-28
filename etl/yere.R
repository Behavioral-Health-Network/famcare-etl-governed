# ===
# YERE ETL PIPELINE (Refactored, Metadata-Driven) ----
#
# This script implements a metadata-driven column typing ETL for YERE,
# returning a nested list of raw and transformed objects instead of writing to
# the global environment as has been done in the past. It replaces the legacy
# monolithic ETL child doc with a clean, testable, and maintainable workflow.
#
# ===
#
#  Core design principles:
#   - PathClient is the authoritative event timeline (one row per enrollment)
#   - tiedenrollment is the canonical episode join key
#   - Form tables (ref, ia, hosp_visit, thirtyd, threem, sixm) are joined to 
#       PathClient by tiedenrollment
#   - SCD summation tables (payor, housing) are joined by
#       PARENT_DOCSERNO to all possible parent forms (ref/ia/follow-ups)
#   - Active SCD summation tables are joined into yere_full_data (one row per
#       enrollment); “all” SCD summation tables remain long form for reporting
#   - All form columns are prefixed with form name (ref_, hosp_visit, ia_,
#       thirtyd_, threem_, sixm_, payor_, housing_, client_needs_, 
#       caregiver_needs_)
#
#
# ===
#
# HOW THIS SCRIPT IS ORGANIZED
#
# 1. File paths
#      - All YERE extract paths are defined in `yere_paths`, using make_path()
#
# 2. Ingestion functions (load_yere_*)
#      - Each function loads one YERE extract using metadata from
#        analytic_fields (loaded via load_analytic_fields() in helpers.R)
#      - No column types are hard-coded; all typing is metadata-driven
#      - No renaming or cleanup should occur here
#
# 3. Transformation functions (transform_yere_*)
#      - Each function performs one major transformation step:
#          * transform_yere_pathclient()
#          * transform_yere_referral_flow()
#      - These functions join related extracts
#      - They return lists so Data Team staff can inspect intermediate objects
#      - yere-pathclient is pivoted to ensure one row per enrollment
#      - Event forms are cleaned to drop notes fields and *_code fields, prefix
#          all columns with form name, and preserve only client_number and
#          tiedenrollment as join keys
#      - SCD summation tables are cleaned to rename parent_docserno, prefix all
#          columns, and drop client_number from each. parent_docsernos are
#          joined to the first non-NA docserno from among the parent forms
#
# 4. Semantic wrapper function extract_yere_full_data() returns the final,
#      analysis-ready, wide YERE dataset (one row per enrollment). Subsetting
#      may (optionally) be performed in the parent report projects using the 
#      build_subsets() function in helpers.R.
#
# 5. Entry point
#      - run_yere_etl(...)
#      - Orchestrates ingestion → transformation → assembly
#      - Designed to be used as a {targets} target (e.g., yere_etl)
#      - Returns a nested list of all YERE objects; writing .rds files
#          is handled elsewhere in the ETL repo
#
# ===
#
# INSPECTING INTERMEDIATE OBJECTS
#
# The ETL returns a nested list so Data Team staff can inspect intermediate
#   objects without relying on global environment side effects.
#
# Example:
#   * yere <- run_yere_etl(yere_paths)
#
# Inspect raw ingestion tibbles:
#   * View(yere$raw$yere_client) # raw yere_client
#   * View(yere$raw$yere_pathclient) # raw yere_pathclient
#
# Inspect intermediate transformations:
#   * View(yere$transform$pathclient$joined_pathclient) # pivoted
#     yere_pathclient
#   * View(yere$transform$referral_flow$joined_referral_flow) # full joined
#     pathclient with pathway event tibbles and scd tables
#
# Inspect final full dataset:
#   * View(yere$yere_full_data) # wide, one row per enrollment
#
# This structure is intended to make debugging, onboarding, and unit testing
#   straightforward and to avoid reliance on global environment.
#
# However, if needed, one may assign objects to the global environment:
#   * yere_pathclient_raw <- yere$raw$yere_pathclient
#
# ===
#
# ABOUT YERE DATA STRUCTURE
#
# YERE enrollments are composed of multiple data sources:
#
#   * ProviderPlacement (program enrollment and dismissal - not joined)
#   * Client (demographics)
#   * PathClient (Pathway metadata bridge)
#   * Referral
#   * Hospital Visit Note
#   * IA (Initial Assessment)
#   * Follow-ups (thirty-day, three-month, six-month)
#   * Housing and Payor Source (active/all)
#
# The transformation layer reconstructs this program life cycle for each
#   enrollment.
#
# ===
#
# REPORTING SUBSETS
#
# YERE reporting uses two primary fiscal-period subsets:
#
#   1. dismissed_within_period
#        - Used for outcomes
#        - Includes all enrollments dismissed in the fiscal period
#
#   2. initiated_within_period
#        - Used for referral flow and program management
#        - Includes all enrollments initiated in the fiscal period,
#          regardless of whether they remain active or were dismissed
#
# Additional subsets are also possible but have not been added as of this
#   writing.
#
# ===

# ===
# 1. List file paths for all data source files. ----
#   - Uses function make_path() from helpers.R.
# ===
yere_paths <- list(
 yere_provider_placement = make_path(
  "FAMCare Q_ProviderPlacement_BHN/",
  "Q_ProviderPlacement_BHN.csv"
 ),
 yere_pathclient = make_path(
  "FAMCare YERE Extract/",
  "Q_YERE_PATHCLIENT_ENROLLMENTS.csv"
 ),
 yere_pathway_docsernos = make_path(
  "FAMCare YERE Extract/",
  "Q_YERE_PATHWAY_FORM_DOCSERNOS.csv"
 ),
 yere_client = make_path(
  "FAMCare YERE Extract/",
  "Q_YERE_CLIENT.csv"
 ),
 yere_ref = make_path(
  "FAMCare YERE Extract/",
  "Q_YERE_REFERRAL.csv"
 ),
 yere_ia = make_path(
  "FAMCare YERE Extract/",
  "Q_YERE_IA.csv"
 ),
 yere_hosp_visit = make_path(
  "FAMCare YERE Extract/",
  "Q_YERE_HOSPITAL_VISIT_NOTE.csv"
 ),
 yere_thirty_day = make_path(
  "FAMCare YERE Extract/",
  "Q_YERE_THIRTY_DAY.csv"
 ),
 yere_three_month = make_path(
  "FAMCare YERE Extract/",
  "Q_YERE_THREE_MONTH.csv"
 ),
 yere_six_month = make_path(
  "FAMCare YERE Extract/",
  "Q_YERE_SIX_MONTH.csv"
 ),
 yere_bhs = make_path(
  "FAMCare YERE Extract/",
  "Q_YERE_BHS.csv"
 ),
 yere_active_housing = make_path(
  "FAMCare YERE Extract/",
  "Q_YERE_ACTIVE_HOUSING_STATUS.csv"
 ),
 yere_all_housing = make_path(
  "FAMCare YERE Extract/",
  "Q_YERE_ALL_HOUSING_STATUS.csv"
 ),
 yere_active_payor_source = make_path(
  "FAMCare YERE Extract/",
  "Q_YERE_ACTIVE_PAYOR_SOURCE.csv"
 ),
 yere_all_payor_source = make_path(
  "FAMCare YERE Extract/",
  "Q_YERE_ALL_PAYOR_SOURCE.csv"
 ),
 yere_client_needs = make_path(
  "FAMCare YERE Extract/",
  "Q_YERE_CLIENT_NEEDS.csv"
 ),
 yere_caregiver_needs = make_path(
  "FAMCare YERE Extract/",
  "Q_YERE_CAREGIVER_NEEDS.csv"
 ),
 yere_client_family_needs = make_path(
   "FAMCare YERE Extract/",
   "Q_YERE_CLIENT_FAMILY_NEEDS.csv"
 )
)

# ===
# 2. Ingestion/Loading Functions ----
# ===

# ===
# Ingest yere_client ----
#   - one row per client
# ===
load_yere_client <- function(
    yere_paths,
    analytic_fields
) {
  load_famcare_extract(
    path = yere_paths$yere_client,
    analytic_fields = analytic_fields
  )
}

# ===
# Ingest yere_provider_placement ----
#   - one row per enrollment - available to supplement yere_pathclient but not
#       joined
#   - Renames key fields
# ===
load_yere_provider_placement <- function(
    yere_paths,
    analytic_fields
) {
  load_famcare_extract(
    path = yere_paths$yere_provider_placement,
    analytic_fields = analytic_fields
  )
}

# ===
# Ingest yere_pathclient ----
#   - Renames key fields
#   - Filters out rows with missing tiedenrollment
#   - Pivoting handled separately, so this is not one row per enrollment yet
# ===
load_yere_pathclient <- function(
    yere_paths,
    analytic_fields
) {
  load_famcare_extract(
    path = yere_paths$yere_pathclient,
    analytic_fields = analytic_fields
  )
}

# ===
# Ingest yere_pathway_docsernos ----
#   - one row per Pathway Event form
# ===
load_yere_pathway_docsernos <- function(
    yere_paths,
    analytic_fields
) {
  load_famcare_extract(
    path = yere_paths$yere_pathway_docsernos,
    analytic_fields = analytic_fields
  )
}

# ===
# Ingest yere_referral ----
#   - one row per referral for each enrollment
# ===
load_yere_ref <- function(
    yere_paths,
    analytic_fields
) {
  load_famcare_extract(
    path = yere_paths$yere_ref,
    analytic_fields = analytic_fields
  )
}

# ===
# Ingest yere_initial_assessment ----
#   - one row per initial contact for each enrollment
# ===
load_yere_ia <- function(
    yere_paths,
    analytic_fields
) {
  load_famcare_extract(
    path = yere_paths$yere_ia,
    analytic_fields = analytic_fields
  )
}

# ===
# Ingest yere_hosp_visit ----
#   - one row per hospital visit note for each enrollment
# ===
load_yere_hosp_visit <- function(
    yere_paths,
    analytic_fields
) {
  load_famcare_extract(
    path = yere_paths$yere_hosp_visit,
    analytic_fields = analytic_fields
  )
}

# ===
# Ingest yere_thirty_day ----
#   - one row per thirty-day follow-up for each enrollment
# ===
load_yere_thirty_day <- function(
    yere_paths,
    analytic_fields
) {
  load_famcare_extract(
    path = yere_paths$yere_thirty_day,
    analytic_fields = analytic_fields
  )
}

# ===
# Ingest yere_three_month ----
#   - one row per three-month follow-up for each enrollment
# ===
load_yere_three_month <- function(
    yere_paths,
    analytic_fields
) {
  load_famcare_extract(
    path = yere_paths$yere_three_month,
    analytic_fields = analytic_fields
  )
}

# ===
# Ingest yere_six_month ----
#   - one row per six-month follow-up for each enrollment
# ===
load_yere_six_month <- function(
    yere_paths,
    analytic_fields
) {
  load_famcare_extract(
    path = yere_paths$yere_six_month,
    analytic_fields = analytic_fields
  )
}

# ===
# Ingest yere_bhs ----
#   - one row per Pathway Event record for each enrollment
# ===
load_yere_bhs <- function(
    yere_paths,
    analytic_fields
) {
  load_famcare_extract(
    path = yere_paths$yere_bhs,
    analytic_fields = analytic_fields
  )
}

# ===
# Ingest yere_active_payor_source ----
#   - one row per active payor source per enrollment
#   - Renames key fields
# ===
load_yere_active_payor_source <- function(
    yere_paths,
    analytic_fields
) {
  load_famcare_extract(
    path = yere_paths$yere_active_payor_source,
    analytic_fields = analytic_fields
  )
}

# ===
# Ingest yere_all_payor_source ----
#   - long form with one row per payor source record per enrollment, which
#       means that this duplicates on enrollments
# ===
load_yere_all_payor_source <- function(
    yere_paths,
    analytic_fields
) {
  load_famcare_extract(
    path = yere_paths$yere_all_payor_source,
    analytic_fields = analytic_fields
  )
}

# ===
# Ingest yere_active_housing_status ----
#   - one row per active housing status per enrollment
# ===
load_yere_active_housing <- function(
    yere_paths,
    analytic_fields
) {
  load_famcare_extract(
    path = yere_paths$yere_active_housing,
    analytic_fields = analytic_fields
  )
}

# ===
# Ingest yere_all_housing_status ----
#   - long form with one row per housing status record per enrollment, which
#       means that this duplicates on enrollments
# ===
load_yere_all_housing <- function(
    yere_paths,
    analytic_fields
) {
  load_famcare_extract(
    path = yere_paths$yere_all_housing,
    analytic_fields = analytic_fields
  )
}

# ===
# Ingest yere_client_needs ----
#   - more than one row per client
#   - one row per enrollment
# ===
load_yere_client_needs <- function(
    yere_paths,
    analytic_fields
) {
  load_famcare_extract(
    path = yere_paths$yere_client_needs,
    analytic_fields = analytic_fields
  )
}

# ===
# Ingest yere_caregiver_needs ----
#   - more than one row per client
#   - one row per enrollment
# ===
load_yere_caregiver_needs <- function(
    yere_paths,
    analytic_fields
) {
  load_famcare_extract(
    path = yere_paths$yere_caregiver_needs,
    analytic_fields = analytic_fields
  )
}

# ===
# Ingest yere_client_family_needs ----
#   - more than one row per client
#   - one row per enrollment
# ===
load_yere_client_family_needs <- function(
    yere_paths,
    analytic_fields
) {
  load_famcare_extract(
    path = yere_paths$yere_client_family_needs,
    analytic_fields = analytic_fields
  )
}

# ===
# 3. Transformation Layer Overview ----
#
# The transformation layer converts raw YERE extracts (loaded via
# metadata-driven ingestion) into analysis-ready datasets.
#
# This layer is intentionally modular. Each function performs one major
# transformation step so that:
#   - Data Team staff can debug intermediate objects
#   - each step can be tested independently
#   - the ETL pipeline is readable and maintainable
#
# The major transformation steps are:
#   1. transform_yere_pathclient()
#        - joins client demographics and relocates to left side of tibble
#        - pivots Pathway Event form docsernos to columns
#        - retains only client demographics + enrollment metadata + docsernos
#
#   2. transform_yere_referral_flow()
#        - joins Pathway Event forms (ref, ia, hosp_visit, etc.)
#        - joins SCD tables (active payor, active housing, client needs,
#            caregiver needs)
#
#   5. build_yere_full_data()
#        - returns joined_referral_flow as a unified dataset
#
#   6. build_yere_subsets() (optional)
#        - constructs reporting subsets, including:
#            * dismissed-within-fiscal-year (for outcomes)
#            * initiated-within-fiscal-year (for referral flow)
#
# All intermediate objects are returned as list elements so Data Team staff can
# inspect them interactively during development.
# ===

# ===
# Transform yere_pathclient ----
#   - PathClient is the authoritative event timeline (enrollment, dismissal,
#       pathway events)
#   - Pivot to one row per enrollment
#   - Drop Pathway metadata columns
#   - Keep analytic fields (enrollment dates, dismissal, agency, etc.)
# ===
transform_yere_pathclient <- function(
    yere
) {
  # Load raw pathclient extract, which is duplicated by Pathway Event form rows
  df <- yere$yere_pathclient |>
    filter(
      !is.na(
        tiedenrollment
      )
    )
  
  # Normalize event names from human readable event labels into canonical
  # column names that will become the *_docserno key. If new event types are
  # ever added for the program, they must be added here.
  df <- df |>
    dplyr::mutate(
      event_key = dplyr::recode(
        pwy_event,
        "YERE Referral" = "ref_docserno",
        "YERE Initial Assessment" = "ia_docserno",
        "YERE Hospital Visit Note" = "hosp_visit_docserno",
        "YERE 30 Day" = "thirtyd_docserno",
        "YERE 3 Month" = "threem_docserno",
        "YERE 6 Month" = "sixm_docserno",
        "YERE Behavioral Health Services" = "bhs_docserno"
      )
    )
  
  # Extract enrollment-level columns, which describe the enrollment itself.
  # These columns do not vary by event type, so one distinct row per
  # (client_number, tiedenrollment) is retained.
  enrollment_cols <- c(
    "client_number",
    "tiedenrollment",
    "client_last",
    "client_first",
    "enrollment_starting_date",
    "enrollment_ending_date",
    "dismissal_reason_description",
    "age_at_enrollment",
    "agency_code",
    "agency_description",
    "enroll_path_join_source",
    "pwy_start_date",
    "pwy_end_date",
    "program_worker_employee_number",
    "program_worker_last",
    "program_worker_first"
  )
  
  enrollment <- df |>
    dplyr::select(
      tidyselect::all_of(
        enrollment_cols
      )
    ) |>
    dplyr::distinct(
      client_number,
      tiedenrollment,
      .keep_all = TRUE
    )
  
  # Merge client demographics
  enrollment <- enrollment |>
    dplyr::left_join(
      dplyr::select(
        yere$yere_client,
        client_number,
        birth_date,
        gender_description,
        race_description,
        ethnicity_description,
        mrn_mercy,
        mrn_bjc,
        mrn_ssm,
        ssn,
        ssn_last_four,
        eto_case_num,
        street,
        street2,
        city,
        state,
        zip_code,
        county_description
      ),
      by = "client_number"
    ) |>
    dplyr::rename(
      dob = birth_date
    )
  
  # Pivot only the pwy_forms_docserno column to produce one column per
  # event_docserno (ref_docserno, ia_docserno, etc.). There should only be one
  # docserno per event per enrollment. If duplicates exist, values_fn =
  # first(na.omit(.x)) selects the first non-NA value. Exception reports should
  # detect duplicates, but this ensures that duplicates do not stop the
  # pipeline.
  events <- df |>
    dplyr::select(
      client_number,
      tiedenrollment,
      event_key,
      pwy_forms_docserno
    ) |>
    dplyr::distinct() |>
    tidyr::pivot_wider(
      names_from = event_key,
      values_from = pwy_forms_docserno,
      values_fn = ~ first(
        na.omit(
          .x
        )
      ),
      values_fill = NA
    )
  
  # Merge enrollment-level data with event_level docserno columns. The join
  # be one-to-one on (client_number, tiedenrollment).
  wide <- enrollment |>
    dplyr::left_join(
      events,
      by = c(
        "client_number",
        "tiedenrollment"
      )
    )
  
  # Relocate client demographics columns to the left side of the tibble
  wide <- wide |>
    dplyr::relocate(
      client_number,
      client_last,
      client_first,
      dob,
      gender_description,
      race_description,
      ethnicity_description,
      mrn_mercy,
      mrn_bjc,
      mrn_ssm,
      ssn,
      ssn_last_four,
      eto_case_num,
      street,
      street2,
      city,
      state,
      zip_code,
      county_description,
      .before = everything()
    )
  
  # Return the final wide pathclient tibble. Output structure:
  # $joined_pathclient
  list(
    joined_pathclient = wide
  )
}

# ===
# Transform referral flow ----
#   - Joins ref, ia, hosp_visit, thirtyd, threem, sixm, bhs
#   - Prefixes all columns except tiedenrollment and client_number
#   - Joins SCD summation tables (payor, housing, client needs, caregiver 
#       needs) to ALL parent forms
#   - Collapses SCD summation tables to one active row per enrollment
# ===
transform_yere_referral_flow <- function(
    yere
) {
  
  # Helper to prefix all columns except tiedenrollment and client_number and
  # also to drop metadata
  clean_form <- function(
    df,
    prefix
  ) {
    
    # Identify *_code columns
    code_cols <- names(df)[endsWith(names(df), "_code")]
    
    # Identify *_description columns
    desc_cols <- names(df)[endsWith(names(df), "_description")]
    
    # Determine which *_code columns have matching *_description columns
    code_with_desc <- code_cols[
      sub(
        "_code$",
        "_description",
        code_cols
      ) %in% desc_cols
    ]
    
    df |>
      # Drop narrative fields
      dplyr::select(
        -tidyselect::contains(
          "notes"
        ),
      ) |>
      # Drop only *_code columns that have matching *_description columns
      dplyr::select(
        -tidyselect::all_of(
          code_with_desc
        )
      ) |>
      # Prefix all remaining columns except join keys
      dplyr::rename_with(
        ~ paste0(
          prefix,
          .x
        ),
        -tidyselect::any_of(
          c(
            "tiedenrollment",
            "client_number"
          )
        )
      )
  }
  
  # Clean each active SCD summation tables
    payor <- clean_form(
      yere$yere_active_payor_source,
      "payor_"
    ) |>
    dplyr::rename(
      parent_docserno = payor_parent_docserno
    ) |>
    dplyr::select(
      -client_number
    )
    housing <- clean_form(
      yere$yere_active_housing,
      "housing_"
    ) |>
    dplyr::rename(
      parent_docserno = housing_parent_docserno
    ) |>
    dplyr::select(
      -client_number
    )
    client_needs <- clean_form(
      yere$yere_client_needs,
      "client_needs_"
    ) |>
    dplyr::rename(
      parent_docserno = client_needs_parent_docserno
    ) |>
    dplyr::select(
      -client_number,
      -tiedenrollment,
      -client_needs_enrollment_starting_date,
      -client_needs_enrollment_ending_date
    )
    caregiver_needs <- clean_form(
      yere$yere_caregiver_needs,
      "caregiver_needs_"
    ) |>
    dplyr::rename(
      parent_docserno = caregiver_needs_parent_docserno
    ) |>
    dplyr::select(
      -client_number,
      -caregiver_needs_client_last,
      -caregiver_needs_client_first,
      -tiedenrollment,
      -caregiver_needs_enrollment_starting_date,
      -caregiver_needs_enrollment_ending_date
    )
    client_family_needs <- clean_form(
      yere$yere_client_family_needs,
      "client_family_needs_"
    ) |>
    dplyr::rename(
      parent_docserno = client_family_needs_parent_docserno
    ) |>
    dplyr::select(
      -client_number,
      -tiedenrollment,
      -client_family_needs_enrollment_starting_date,
      -client_family_needs_enrollment_ending_date
    )
  
  # Drop docserno from parent event forms to avoid suffix collisions (.x/.y) due
  # to duplication when joining with pathclient. Pathclient is the authoritative
  # source of docserno values.
  ref <- clean_form(
    yere$yere_ref,
    "ref_"
  ) |>
    dplyr::select(
      -tidyselect::ends_with(
        "docserno"
      )
    )
  
  ia <- clean_form(
    yere$yere_ia,
    "ia_"
  ) |>
    dplyr::select(
      -tidyselect::ends_with(
        "docserno"
      )
    )
  
  hosp_visit <- clean_form(
    yere$yere_hosp_visit,
    "hosp_visit_"
  ) |>
    dplyr::select(
      -tidyselect::ends_with(
        "docserno"
      )
    )
  
  thirtyd <- clean_form(
    yere$yere_thirty_day,
    "thirtyd_"
  ) |>
    dplyr::select(
      -tidyselect::ends_with(
        "docserno"
      )
    )
  
  threem <- clean_form(
    yere$yere_three_month,
    "threem_"
  ) |>
    dplyr::select(
      -tidyselect::ends_with(
        "docserno"
      )
    )
  
  sixm <- clean_form(
    yere$yere_six_month,
    "sixm_"
  ) |>
    dplyr::select(
      -tidyselect::ends_with(
        "docserno"
      )
    )
  
  bhs <- clean_form(
    yere$yere_bhs,
    "bhs_"
  ) |>
    dplyr::select(
      -tidyselect::ends_with(
        "docserno"
      )
    )
  
  # Start with pivoted pathclient
  pc <- yere$pathclient
  
  # Invariant: parent_map must contain exactly one row per (client_number,
  # tiedenrollment, parent_docserno). If this is violated, SCD collapse will
  # fail. The result is a long form tibble
  parent_map <- pc |>
    dplyr::select(
      client_number,
      tiedenrollment,
      ref_docserno,
      ia_docserno,
      hosp_visit_docserno,
      thirtyd_docserno,
      threem_docserno,
      sixm_docserno,
      bhs_docserno
    ) |>
    tidyr::pivot_longer(
      cols = tidyselect::ends_with(
        "docserno"
      ),
      names_to = "event",
      values_to = "parent_docserno"
    ) |>
    dplyr::filter(
      !is.na(
        parent_docserno
      )
    )
  
  # Helper: collapse SCD table to one row per enrollment
  collapse_scd <- function(
    scd_tbl
  ) {
    parent_map |>
      dplyr::left_join(
        scd_tbl,
        by = "parent_docserno"
      ) |>
      dplyr::arrange(
        client_number,
        tiedenrollment
      ) |>
      dplyr::group_by(
        client_number,
        tiedenrollment
      ) |>
      dplyr::summarise(
        dplyr::across(
          .cols = -c(
            parent_docserno,
            event
          ),
          .fns  = ~ first(
            na.omit(
              .x
            )
          )
        ),
        .groups = "drop"
      )
  }
  
  # Diagnostic: payor_one shows which active payor source SCD row was selected
  # for each enrollment. Useful for debugging missing or stale SCD values.
  payor_one   <- collapse_scd(
    payor
  )
  
  # Diagnostic: housing_one shows which active housing status SCD row was
  # selected for each enrollment. Useful for debugging missing or stale SCD
  # values.
  housing_one <- collapse_scd(
    housing
  )
  
  # Diagnostic: client_needs_one shows which client needs SCD row was selected for
  # each enrollment. Useful for debugging missing or stale SCD values.
  client_needs_one  <- collapse_scd(
    client_needs
  )
  
  # Diagnostic: caregiver_needs_one shows which caregiver needs SCD row was selected for
  # each enrollment. Useful for debugging missing or stale SCD values.
  caregiver_needs_one  <- collapse_scd(
    caregiver_needs
  )
  
  # Diagnostic: caregiver_needs_one shows which caregiver needs SCD row was selected for
  # each enrollment. Useful for debugging missing or stale SCD values.
  client_family_needs_one  <- collapse_scd(
    client_family_needs
  )
  
  # Start join sequence with joined "authoritative" pathclient
  joined <- pc |>
    # Join SCD "active" summaries once per enrollment
    dplyr::left_join(
      payor_one,
      by = c(
        "client_number",
        "tiedenrollment"
      )
    ) |>
    dplyr::left_join(
      housing_one,
      by = c(
        "client_number",
        "tiedenrollment"
      )
    ) |>
    dplyr::left_join(
      client_needs_one,
      by = c(
        "client_number",
        "tiedenrollment"
      )
    ) |>
    dplyr::left_join(
      caregiver_needs_one,
      by = c(
        "client_number",
        "tiedenrollment"
      )
    ) |>
    dplyr::left_join(
      client_family_needs_one,
      by = c(
        "client_number",
        "tiedenrollment"
      )
    ) |> 
    
    # Join event forms by enrollment
    dplyr::left_join(
      ref,
      by = join_by(
        "client_number",
        "tiedenrollment"
      )
    ) |>
    dplyr::left_join(
      ia,
      by = join_by(
        "client_number",
        "tiedenrollment"
      )
    ) |>
    dplyr::left_join(
      hosp_visit,
      by = join_by(
        "client_number",
        "tiedenrollment"
      )
    ) |>
    dplyr::left_join(
      thirtyd,
      by = join_by(
        "client_number",
        "tiedenrollment"
      )
    ) |>
    dplyr::left_join(
      threem,
      by = join_by(
        "client_number",
        "tiedenrollment"
      )
    ) |>
    dplyr::left_join(
      sixm,
      by = join_by(
        "client_number",
        "tiedenrollment"
      )
    ) |>
    dplyr::left_join(
      bhs,
      by = join_by(
        "client_number",
        "tiedenrollment"
      )
    )
  
  # Store the final joined referral flow table
  output <- list(
    scd = list(
      payor_one   = payor_one,
      housing_one = housing_one,
      client_needs_one = client_needs_one,
      caregiver_needs_one = caregiver_needs_one,
      client_family_needs_one = client_family_needs_one
    ),
    parent_map = parent_map,
    transformed = list(
      ref = ref,
      ia = ia,
      hosp_visit = hosp_visit,
      thirtyd = thirtyd,
      threem = threem,
      sixm = sixm,
      bhs = bhs
    ),
    joined_referral_flow = joined
  )
  
  # Return the joined_referral_flow and scd
  return(
    output
  )
  
}

# ===
# 4. Extract final yere_full_data ----
#   - Returns the final wide referral_flow table
#   - Maintained as a semantic wrapper to expose the final wide table as
#       yere_full_data
# ===

extract_yere_full_data <- function(
    referral_flow
) {
  referral_flow$joined_referral_flow
}

# ===
# 5. YERE ETL entry point ----
#   - Loads analytic_fields metadata
#   - Ingests all YERE extracts using metadata-driven loaders
#   - Applies YERE-specific transformations (e.g., pivoting, referral_flow)
#   - Returns a named list of all YERE data objects
#   - Does not write to disk or modify global env objects as the old ETL code
#       did.
# ===
run_yere_etl <- function(
    analytic_fields,
    yere_provider_placement,
    yere_pathclient,
    yere_pathway_docsernos,
    yere_client,
    yere_ref,
    yere_hosp_visit,
    yere_ia,
    yere_thirty_day,
    yere_three_month,
    yere_six_month,
    yere_bhs,
    yere_active_payor_source,
    yere_all_payor_source,
    yere_active_housing,
    yere_all_housing,
    yere_client_needs,
    yere_caregiver_needs,
    yere_client_family_needs,
    start_date = NULL,
    end_date = NULL,
    fiscal_system = c(
      "federal",
      "state"
      )
) {
  
  fiscal_system <- match.arg(
    fiscal_system
    )
  
  # ===
  # 1. Raw ingestion
  # ===
  yere_raw <- list(
    yere_client = load_yere_client(
      yere_paths,
      analytic_fields
    ),
    yere_provider_placement = load_yere_provider_placement(
      yere_paths,
      analytic_fields
    ),
    yere_pathclient = load_yere_pathclient(
      yere_paths,
      analytic_fields
    ),
    yere_pathway_docsernos = load_yere_pathway_docsernos(
      yere_paths,
      analytic_fields
    ),
    yere_ref = load_yere_ref(
      yere_paths,
      analytic_fields
    ),
    yere_ia = load_yere_ia(
      yere_paths,
      analytic_fields
    ),
    yere_hosp_visit = load_yere_hosp_visit(
      yere_paths,
      analytic_fields
    ),
    yere_thirty_day = load_yere_thirty_day(
      yere_paths,
      analytic_fields
    ),
    yere_three_month = load_yere_three_month(
      yere_paths,
      analytic_fields
    ),
    yere_six_month = load_yere_six_month(
      yere_paths,
      analytic_fields
    ),
    yere_bhs = load_yere_bhs(
      yere_paths,
      analytic_fields
    ),
    yere_active_payor_source = load_yere_active_payor_source(
      yere_paths,
      analytic_fields
    ),
    yere_all_payor_source = load_yere_all_payor_source(
      yere_paths,
      analytic_fields
    ),
    yere_active_housing = load_yere_active_housing(
      yere_paths,
      analytic_fields
    ),
    yere_all_housing = load_yere_all_housing(
      yere_paths,
      analytic_fields
    ),
    yere_client_needs = load_yere_client_needs(
      yere_paths,
      analytic_fields
    ),
    yere_caregiver_needs = load_yere_caregiver_needs(
      yere_paths,
      analytic_fields
    ),
    yere_client_family_needs = load_yere_client_family_needs(
      yere_paths,
      analytic_fields
    )
  )
  
  # ===
  # 2. Transformations
  # ===
  pathclient <- transform_yere_pathclient(
    yere_raw
  )
  # Promote pivoted pathclient to authoritative
  yere_raw$pathclient <- pathclient$joined_pathclient
  
  referral_flow <- transform_yere_referral_flow(
    yere_raw
  )
  
  full <- extract_yere_full_data(
    referral_flow
  )
  
  # Subsets are optional; only build if dates are supplied
  subsets <- NULL
  if (
    !is.null(
      start_date
    ) && !is.null(
      end_date
    )
  ) {
    subsets <- build_subsets(
      full_data = full,
      start_date = start_date,
      end_date = end_date,
      fiscal_system = fiscal_system
    )
  }
  
  # ===
  # 3. Return structured object
  # ===
  list(
    raw = yere_raw,
    transform  = list(
      pathclient = pathclient,
      referral_flow = referral_flow
    ),
    yere_full_data = full,
    subsets = subsets
  )
}
