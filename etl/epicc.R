# ===
# EPICC ETL PIPELINE (Refactored, Metadata-Driven) ----
#
# This script implements a metadata-driven column typing ETL for EPICC,
# returning a nested list of raw and transformed objects instead of writing to
# the global environment as has been done in the past. It replaces the legacy
# monolithic ETL child doc with a clean, testable, and maintainable workflow.
#
# ===
#
#  Core design principles:
#   - PathClient is the authoritative event timeline (one row per enrollment)
#   - TIEDENROLLMENT is the canonical episode join key
#   - Form tables (REF, IC, TWOW, THIRTYD, THREEM, SIXM, REENGAGE) are
#       joined to PathClient by TIEDENROLLMENT
#   - SCD summation tables (intake, payor, housing) are joined by
#       PARENT_DOCSERNO to all possible parent forms (REF/IC/follow-ups)
#   - Active SCD summation tables are joined into epicc_full_data (one row per
#       enrollment); “all” SCD summation tables remain long form for reporting
#   - All form columns are prefixed with form name (ref_, ic_, twow_,
#       thirtyd_, threem_, sixm_, reengage_, intake_, payor_, housing_)
#
#       * intake = EPICC SU Tx Referred Agency summation from PWSUBROADTXAGENCY
#
# ===
#
# HOW THIS SCRIPT IS ORGANIZED
#
# 1. File paths
#      - All EPICC extract paths are defined in `epicc_paths`, using make_path()
#
# 2. Ingestion functions (load_epicc_*)
#      - Each function loads one EPICC extract using metadata from
#        analytic_fields (loaded via load_analytic_fields() in helpers.R)
#      - No column types are hard-coded; all typing is metadata-driven
#      - No renaming or cleanup should occur here
#
# 3. Transformation functions (transform_epicc_*)
#      - Each function performs one major transformation step:
#          * transform_epicc_pathclient()
#          * transform_epicc_referral_flow()
#      - These functions join related extracts
#      - They return lists so Data Team staff can inspect intermediate objects
#      - epicc-pathclient is pivoted to ensure one row per enrollment
#      - Event forms are cleaned to drop notes fields and *_code fields, prefix
#          all columns with form name, and preserve only client_number and
#          tiedenrollment as join keys
#      - SCD summation tables are cleaned to rename parent_docserno, prefix all
#          columns, and drop client_number from each. parent_docsernos are
#          joined to the first non-NA docserno from among the parent forms
#
# 4. Semantic wrapper function extract_epicc_full_data() returns the final,
#      analysis-ready, wide EPICC dataset (one row per enrollment). Subsetting
#      may be performed in the parent report projects using the build_subsets()
#      function in helpers.R.
#
# 5. Entry point
#      - run_epicc_etl(...)
#      - Orchestrates ingestion → transformation → assembly
#      - Designed to be used as a {targets} target (e.g., epicc_etl)
#      - Returns a nested list of all EPICC objects; writing .rds files is
#          handled elsewhere in the ETL repo
#
# ===
#
# INSPECTING INTERMEDIATE OBJECTS
#
# The ETL returns a nested list so Data Team staff can inspect intermediate
#   objects without relying on global environment side effects.
#
# Example:
#   * epicc <- run_epicc_etl(epicc_paths)
#
# Inspect raw ingestion tibbles:
#   * View(epicc$raw$epicc_client) # raw epicc_client
#   * View(epicc$raw$epicc_pathclient) # raw epicc_pathclient
#
# Inspect intermediate transformations:
#   * View(epicc$transform$pathclient$joined_pathclient) # pivoted
#     epicc_pathclient
#   * View(epicc$transform$referral_flow$joined_referral_flow) # full joined
#     pathclient with pathway event tibbles and scd tables
#
# Inspect final full dataset:
#   * View(epicc$epicc_full_data) # wide, one row per enrollment
#
# This structure is intended to make debugging, onboarding, and unit testing
#   straightforward and to avoid reliance on global environment.
#
# However, if needed, one may assign objects to the global environment:
#   * epicc_pathclient_raw <- epicc$raw$epicc_pathclient
#
# ===
#
# ABOUT EPICC DATA STRUCTURE
#
# EPICC enrollments are composed of multiple data sources:
#
#   * ProviderPlacement (program enrollment and dismissal - not joined)
#   * Client (demographics)
#   * PathClient (Pathway metadata bridge)
#   * Referral
#   * IC (Initial Contact)
#   * Intake (active/all; referral to su tx agency and intake attempts)
#   * Follow-ups (two-week, thirty-day, three-month, six-month)
#   * Re-engagement (30-day re-engagement attempts)
#   * Housing and Payor Source (active/all)
#
# The transformation layer reconstructs this program life cycle for each
#   enrollment.
#
# ===
#
# REPORTING SUBSETS
#
# EPICC reporting uses two primary fiscal-period subsets:
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
epicc_paths <- list(
  epicc_provider_placement = make_path(
    "FAMCare Q_ProviderPlacement_BHN/",
    "Q_ProviderPlacement_BHN.csv"
  ),
  epicc_pathclient = make_path(
    "FAMCare EPICC Extract/",
    "Q_EPICC_PATHCLIENT_ENROLLMENTS_2023_CURRENT.csv"
  ),
  epicc_pathclient_legacy = make_path(
    "FAMCare EPICC Extract/",
    "Q_EPICC_PATHCLIENT_ENROLLMENTS_2016_2022.csv"
  ),
  epicc_pathway_docsernos = make_path(
    "FAMCare EPICC Extract/",
    "Q_EPICC_PATHWAY_FORM_DOCSERNOS.csv"
  ),
  epicc_client = make_path(
    "FAMCare EPICC Extract/",
    "Q_EPICC_CLIENT.csv"
  ),
  epicc_ref = make_path(
    "FAMCare EPICC Extract/",
    "Q_EPICC_REFERRAL.csv"
  ),
  epicc_ic = make_path(
    "FAMCare EPICC Extract/",
    "Q_EPICC_IC.csv"
  ),
  epicc_two_week = make_path(
    "FAMCare EPICC Extract/",
    "Q_EPICC_TWO_WEEK.csv"
  ),
  epicc_thirty_day = make_path(
    "FAMCare EPICC Extract/",
    "Q_EPICC_THIRTY_DAY.csv"
  ),
  epicc_three_month = make_path(
    "FAMCare EPICC Extract/",
    "Q_EPICC_THREE_MONTH.csv"
  ),
  epicc_six_month = make_path(
    "FAMCare EPICC Extract/",
    "Q_EPICC_SIX_MONTH.csv"
  ),
  epicc_reengagement = make_path(
    "FAMCare EPICC Extract/",
    "Q_EPICC_REENGAGEMENT.csv"
  ),
  epicc_active_intake = make_path(
    "FAMCare EPICC Extract/",
    "Q_EPICC_LATEST_SU_TX_AGENCY.csv"
  ),
  epicc_all_intake = make_path(
    "FAMCare EPICC Extract/",
    "Q_EPICC_ALL_SU_TX_AGENCY.csv"
  ),
  epicc_active_housing = make_path(
    "FAMCare EPICC Extract/",
    "Q_EPICC_ACTIVE_HOUSING_STATUS.csv"
  ),
  epicc_all_housing = make_path(
    "FAMCare EPICC Extract/",
    "Q_EPICC_ALL_HOUSING_STATUS.csv"
  ),
  epicc_active_payor_source = make_path(
    "FAMCare EPICC Extract/",
    "Q_EPICC_ACTIVE_PAYOR_SOURCE.csv"
  ),
  epicc_all_payor_source = make_path(
    "FAMCare EPICC Extract/",
    "Q_EPICC_ALL_PAYOR_SOURCE.csv"
  ),
  epicc_case_notes = make_path(
    "FAMCare EPICC Extract/",
    "Q_EPICC_CASE_NOTES.csv"
  ),
  epicc_support_services_tracker = make_path(
    "FAMCare EPICC Extract/",
    "Q_EPICC_SUPPORT_SERVICES_TRACKER.csv"
  )
)

# ===
# 2. Ingestion/Loading Functions ----
# ===

# ===
# Ingest epicc_client ----
#   - one row per client
# ===
load_epicc_client <- function(
  epicc_paths,
  analytic_fields
) {
  load_famcare_extract(
    path = epicc_paths$epicc_client,
    analytic_fields = analytic_fields
  )
}

# ===
# Ingest epicc_provider_placement ----
#   - one row per enrollment - available to supplement epicc_pathclient but not
#       joined
#   - Renames key fields
# ===
load_epicc_provider_placement <- function(
  epicc_paths,
  analytic_fields
) {
  load_famcare_extract(
    path = epicc_paths$epicc_provider_placement,
    analytic_fields = analytic_fields
  )
}

# ===
# Ingest epicc_pathclient ----
#   - Pivoting handled separately, so this is not one row per enrollment yet
# ===
load_epicc_pathclient <- function(
  epicc_paths,
  analytic_fields
) {
  load_famcare_extract(
    path = epicc_paths$epicc_pathclient,
    analytic_fields = analytic_fields
  )
}

# ===
# Ingest epicc_pathway_docsernos ----
#   - one row per Pathway Event form
# ===
load_epicc_pathway_docsernos <- function(
  epicc_paths,
  analytic_fields
) {
  load_famcare_extract(
    path = epicc_paths$epicc_pathway_docsernos,
    analytic_fields = analytic_fields
  )
}

# ===
# Ingest epicc_referral ----
#   - one row per referral for each enrollment
# ===
load_epicc_ref <- function(
  epicc_paths,
  analytic_fields
) {
  load_famcare_extract(
    path = epicc_paths$epicc_ref,
    analytic_fields = analytic_fields
  )
}

# ===
# Ingest epicc_initial_contact ----
#   - one row per initial contact for each enrollment
# ===
load_epicc_ic <- function(
  epicc_paths,
  analytic_fields
) {
  load_famcare_extract(
    path = epicc_paths$epicc_ic,
    analytic_fields = analytic_fields
  )
}

# ===
# Ingest epicc_two_week ----
#   - one row per two-week follow-up for each enrollment
# ===
load_epicc_two_week <- function(
  epicc_paths,
  analytic_fields
) {
  load_famcare_extract(
    path = epicc_paths$epicc_two_week,
    analytic_fields = analytic_fields
  )
}

# ===
# Ingest epicc_thirty_day ----
#   - one row per thirty-day follow-up for each enrollment
# ===
load_epicc_thirty_day <- function(
  epicc_paths,
  analytic_fields
) {
  load_famcare_extract(
    path = epicc_paths$epicc_thirty_day,
    analytic_fields = analytic_fields
  )
}

# ===
# Ingest epicc_three_month ----
#   - one row per three-month follow-up for each enrollment
# ===
load_epicc_three_month <- function(
  epicc_paths,
  analytic_fields
) {
  load_famcare_extract(
    path = epicc_paths$epicc_three_month,
    analytic_fields = analytic_fields
  )
}

# ===
# Ingest epicc_six_month ----
#   - one row per six-month follow-up for each enrollment
# ===
load_epicc_six_month <- function(
  epicc_paths,
  analytic_fields
) {
  load_famcare_extract(
    path = epicc_paths$epicc_six_month,
    analytic_fields = analytic_fields
  )
}

# ===
# Ingest epicc_reengagement ----
#   - one row per Pathway Event record for each enrollment
# ===
load_epicc_reengagement <- function(
  epicc_paths,
  analytic_fields
) {
  load_famcare_extract(
    path = epicc_paths$epicc_reengagement,
    analytic_fields = analytic_fields
  )
}

# ===
# Ingest epicc_active_intake ----
#   - one row per latest SU Tx agency referral
#   - episode of care-aware: joins to the relevant enrollment
# ===
load_epicc_active_intake <- function(
  epicc_paths,
  analytic_fields
) {
  load_famcare_extract(
    path = epicc_paths$epicc_active_intake,
    analytic_fields = analytic_fields
  )
}

# ===
# Ingest epicc_all_intake ----
#   - long form with one row per SU Tx agency referral per enrollment, which
#       means that this duplicates on enrollments
# ===
load_epicc_all_intake <- function(
  epicc_paths,
  analytic_fields
) {
  load_famcare_extract(
    path = epicc_paths$epicc_all_intake,
    analytic_fields = analytic_fields
  )
}

# ===
# Ingest epicc_active_payor_source ----
#   - one row per active payor source per enrollment
#   - Renames key fields
# ===
load_epicc_active_payor_source <- function(
  epicc_paths,
  analytic_fields
) {
  load_famcare_extract(
    path = epicc_paths$epicc_active_payor_source,
    analytic_fields = analytic_fields
  )
}

# ===
# Ingest epicc_all_payor_source ----
#   - long form with one row per payor source record per enrollment, which
#       means that this duplicates on enrollments
# ===
load_epicc_all_payor_source <- function(
  epicc_paths,
  analytic_fields
) {
  load_famcare_extract(
    path = epicc_paths$epicc_all_payor_source,
    analytic_fields = analytic_fields
  )
}

# ===
# Ingest epicc_active_housing_status ----
#   - one row per active housing status per enrollment
# ===
load_epicc_active_housing <- function(
  epicc_paths,
  analytic_fields
) {
  load_famcare_extract(
    path = epicc_paths$epicc_active_housing,
    analytic_fields = analytic_fields
  )
}

# ===
# Ingest epicc_all_housing_status ----
#   - long form with one row per housing status record per enrollment, which
#       means that this duplicates on enrollments
# ===
load_epicc_all_housing <- function(
  epicc_paths,
  analytic_fields
) {
  load_famcare_extract(
    path = epicc_paths$epicc_all_housing,
    analytic_fields = analytic_fields
  )
}

# ===
# Ingest epicc_case_notes ----
#   - more than one row per client
# ===
load_epicc_case_notes <- function(
    epicc_paths,
    analytic_fields
) {
  load_famcare_extract(
    path = epicc_paths$epicc_case_notes,
    analytic_fields = analytic_fields
  )
}

# ===
# Ingest epicc_support_services_tracker ----
#   - more than one row per client
# ===
load_epicc_support_services_tracker <- function(
    epicc_paths,
    analytic_fields
) {
  load_famcare_extract(
    path = epicc_paths$epicc_support_services_tracker,
    analytic_fields = analytic_fields
  )
}

# ===
# 3. Transformation Layer Overview ----
#
# The transformation layer converts raw EPICC extracts (loaded via
# metadata-driven ingestion) into analysis-ready datasets.
#
# This layer is intentionally modular. Each function performs one major
# transformation step so that:
#   - Data Team staff can debug intermediate objects
#   - each step can be tested independently
#   - the ETL pipeline is readable and maintainable
#
# The major transformation steps are:
#   1. transform_epicc_pathclient()
#        - joins client demographics and relocates to left side of tibble
#        - pivots Pathway Event form docsernos to columns
#        - retains only client demographics + enrollment metadata + docsernos
#
#   2. transform_epicc_referral_flow()
#        - joins Pathway Event forms (ref, ic, twow, etc.)
#        - joins SCD tables (active intake, active payor, active housing)
#
#   5. build_epicc_full_data()
#        - returns joined_referral_flow as a unified dataset
#
#   6. build_epicc_subsets() (optional)
#        - constructs reporting subsets, including:
#            * dismissed-within-fiscal-year (for outcomes)
#            * initiated-within-fiscal-year (for referral flow)
#
# All intermediate objects are returned as list elements so Data Team staff can
# inspect them interactively during development.
# ===

# ===
# Transform epicc_pathclient ----
#   - PathClient is the authoritative event timeline (enrollment, dismissal,
#       pathway events)
#   - Pivot to one row per enrollment
#   - Drop Pathway metadata columns
#   - Keep analytic fields (enrollment dates, dismissal, agency, etc.)
# ===
transform_epicc_pathclient <- function(
  epicc
) {
  # Load raw pathclient extract, which is duplicated by Pathway Event form rows.
  # Normalize event names from human readable event labels into canonical
  # column names that will become the *_docserno key. If new event types are
  # ever added for the program, they must be added here.
  df <- epicc$epicc_pathclient |>
    dplyr::mutate(
      event_key = dplyr::recode(
        pwy_event,
        "EPICC Referral" = "ref_docserno",
        "EPICC Initial Contact" = "ic_docserno",
        "EPICC 2 Week" = "twow_docserno",
        "EPICC 30 Day" = "thirtyd_docserno",
        "EPICC 3 Month" = "threem_docserno",
        "EPICC 6 Month" = "sixm_docserno"
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
    "agency_code_non_cfl",
    "agency_description_non_cfl",
    "agency_transformation_flag",
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
    dplyr::group_by(
      client_number,
      enrollment_starting_date
    ) |>
    dplyr::summarise(
      dplyr::across(
        dplyr::everything(),
        ~ dplyr::first(
          na.omit(
            .x
            )
          )
      ),
      .groups = "drop"
    )

  # Merge client demographics
  enrollment <- enrollment |>
    dplyr::left_join(
      dplyr::select(
        epicc$epicc_client,
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
  # event_docserno (ref_docserno, ic_docserno, etc.). There should only be one
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
      values_fn = ~ dplyr::first(
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
#   - Joins REF, IC, TWOW, THIRTYD, THREEM, SIXM, REENGAGE
#   - Prefixes all columns except tiedenrollment
#   - Joins SCD summation tables (intake, payor, housing) to ALL parent forms
#   - Collapses SCD summation tables to one active row per enrollment
# ===
transform_epicc_referral_flow <- function(
  epicc
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
  intake <- clean_form(
    epicc$epicc_active_intake,
    "intake_"
  ) |>
    dplyr::rename(
      parent_docserno = intake_parent_docserno
    ) |>
    dplyr::select(
      -client_number
    )
  payor <- clean_form(
    epicc$epicc_active_payor_source,
    "payor_"
  ) |>
    dplyr::rename(
      parent_docserno = payor_parent_docserno
    ) |>
    dplyr::select(
      -client_number
    )
  housing <- clean_form(
    epicc$epicc_active_housing,
    "housing_"
  ) |>
    dplyr::rename(
      parent_docserno = housing_parent_docserno
    ) |>
    dplyr::select(
      -client_number
    ) |> 
    dplyr::select(
      -tiedenrollment
    )

  # Drop docserno from parent event forms to avoid suffix collisions (.x/.y) due
  # to duplication when joining with pathclient. Pathclient is the authoritative
  # source of docserno values.
  ref <- clean_form(
    epicc$epicc_ref,
    "ref_"
  ) |>
    dplyr::select(
      -tidyselect::ends_with(
        "docserno"
      )
    )

  ic <- clean_form(
    epicc$epicc_ic,
    "ic_"
  ) |>
    dplyr::select(
      -tidyselect::ends_with(
        "docserno"
      )
    )

  twow <- clean_form(
    epicc$epicc_two_week,
    "twow_"
  ) |>
    dplyr::select(
      -tidyselect::ends_with(
        "docserno"
      )
    )

  thirtyd <- clean_form(
    epicc$epicc_thirty_day,
    "thirtyd_"
  ) |>
    dplyr::select(
      -tidyselect::ends_with(
        "docserno"
      )
    )

  threem <- clean_form(
    epicc$epicc_three_month,
    "threem_"
  ) |>
    dplyr::select(
      -tidyselect::ends_with(
        "docserno"
      )
    )

  sixm <- clean_form(
    epicc$epicc_six_month,
    "sixm_"
  ) |>
    dplyr::select(
      -tidyselect::ends_with(
        "docserno"
      )
    )

  reeng <- clean_form(
    epicc$epicc_reengagement,
    "reengage_"
  ) |>
    dplyr::select(
      -tidyselect::ends_with(
        "docserno"
      )
    )

  reeng_wide <- reeng |>
    mutate(
      reengage_period = case_when(
        reengage_follow_up_form_reengagement == "Two Week" ~ "two_week",
        reengage_follow_up_form_reengagement == "Thirty Day" ~ "thirty_day",
        .default = NA_character_
      )
    ) |> 
    group_by(
      reengage_period,
      tiedenrollment,
      client_number
    ) |> 
    arrange(
      desc(
        reengage_pathway_date
      )
    ) |> 
    summarize(
      reengage_status = first(
        na.omit(
          reengage_status_reengagement
        )
      ),
      .groups = "drop"
    ) |> 
    pivot_wider(
      names_from = reengage_period,
      values_from = reengage_status,
      names_prefix = "reengage_status_"
    )
  
  # Start with pivoted pathclient
  pc <- epicc$pathclient

  # Invariant: parent_map must contain exactly one row per (client_number,
  # tiedenrollment, parent_docserno). If this is violated, SCD collapse will
  # fail. The result is a long form tibble
  parent_map <- pc |>
    dplyr::select(
      client_number,
      tiedenrollment,
      ref_docserno,
      ic_docserno,
      twow_docserno,
      thirtyd_docserno,
      threem_docserno,
      sixm_docserno
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

  # Diagnostic: intake_one shows which active intake SCD row was selected for
  # each enrollment. Useful for debugging missing or stale SCD values.
  intake_one  <- collapse_scd(
    intake
  )

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


  # Start join sequence with joined "authoritative" pathclient
  joined <- pc |>
    # Join SCD "active" summaries once per enrollment
    dplyr::left_join(
      intake_one,
      by = c(
        "client_number",
        "tiedenrollment"
      )
    ) |>
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

    # Join event forms by enrollment
    dplyr::left_join(
      ref,
      by = join_by(
        "client_number",
        "tiedenrollment"
      )
    ) |>
    dplyr::left_join(
      ic,
      by = join_by(
        "client_number",
        "tiedenrollment"
      )
    ) |>
    dplyr::left_join(
      twow,
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
      reeng_wide,
      by = join_by(
        "client_number",
        "tiedenrollment"
      )
    )

  # Store the final joined referral flow table
  output <- list(
    scd = list(
      intake_one  = intake_one,
      payor_one   = payor_one,
      housing_one = housing_one
    ),
    parent_map = parent_map,
    transformed = list(
      ref = ref,
      ic = ic,
      twow = twow,
      thirtyd = thirtyd,
      threem = threem,
      sixm = sixm,
      reeng = reeng,
      reeng_wide = reeng_wide
    ),
    joined_referral_flow = joined
  )

  # Return the joined_referral_flow and scd
  return(
    output
  )

}

# ===
# 4. Extract final epicc_full_data ----
#   - Returns the final wide referral_flow table
#   - Maintained as a semantic wrapper to expose the final wide table as
#     epicc_full_data
# ===

extract_epicc_full_data <- function(
  referral_flow
) {
  referral_flow$joined_referral_flow
}

# ===
# 5. EPICC ETL entry point ----
#   - Loads analytic_fields metadata
#   - Ingests all EPICC extracts using metadata-driven loaders
#   - Applies EPICC-specific transformations (e.g., pivoting, referral_flow)
#   - Returns a named list of all EPICC data objects
#   - Does not write to disk or modify global env objects as the old ETL code
#       did.
# ===
run_epicc_etl <- function(
  analytic_fields,
  epicc_provider_placement,
  epicc_pathclient,
  epicc_pathclient_legacy_raw,
  epicc_pathway_docsernos,
  epicc_client,
  epicc_ref,
  epicc_ic,
  epicc_two_week,
  epicc_thirty_day,
  epicc_three_month,
  epicc_six_month,
  epicc_reengagement,
  epicc_active_intake,
  epicc_all_intake,
  epicc_active_payor_source,
  epicc_all_payor_source,
  epicc_active_housing,
  epicc_all_housing,
  epicc_case_notes,
  epicc_support_services_tracker,
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

  # analytic_fields is passed in by {targets} or default-loaded
  # No internal call to analytic_fields <- load_analytic_fields() is needed
  
  # print(
  #   "Columns from analytic_fields + field_name placeholder"
  # )
  # print(
  #   names(
  #     analytic_fields
  #   )
  # )

# ===
  # 1. Raw Ingestion
# ===
  epicc_raw <- list(
    epicc_client = epicc_client,
    epicc_provider_placement = epicc_provider_placement,
    epicc_pathclient = epicc_pathclient,
    epicc_pathclient_legacy = epicc_pathclient_legacy_raw,
    epicc_pathway_docsernos = epicc_pathway_docsernos,
    epicc_ref = epicc_ref,
    epicc_ic = epicc_ic,
    epicc_two_week = epicc_two_week,
    epicc_thirty_day = epicc_thirty_day,
    epicc_three_month = epicc_three_month,
    epicc_six_month = epicc_six_month,
    epicc_reengagement = epicc_reengagement,
    epicc_active_intake = epicc_active_intake,
    epicc_all_intake = epicc_all_intake,
    epicc_active_payor_source = epicc_active_payor_source,
    epicc_all_payor_source = epicc_all_payor_source,
    epicc_active_housing = epicc_active_housing,
    epicc_all_housing = epicc_all_housing,
    epicc_case_notes = epicc_case_notes,
    epicc_support_services_tracker = epicc_support_services_tracker
  )

# ===
  # 2. Transformations
# ===
  pathclient <- transform_epicc_pathclient(
    epicc_raw
  )
  # Promote pivoted pathclient to authoritative
  epicc_raw$pathclient <- pathclient$joined_pathclient

  referral_flow <- transform_epicc_referral_flow(
    epicc_raw
  )

  full <- extract_epicc_full_data(
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
    raw = epicc_raw,
    transform  = list(
      pathclient = pathclient,
      referral_flow = referral_flow
    ),
    epicc_full_data = full,
    subsets = subsets
  )
}
