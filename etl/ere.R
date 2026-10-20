# ===
# ERE ETL PIPELINE (Refactored, Metadata-Driven) ----
#
# This script implements a metadata-driven column typing ETL for ERE,
# returning a nested list of raw and transformed objects instead of writing to
# the global environment as has been done in the past. It replaces the legacy
# monolithic ETL child doc with a clean, testable, and maintainable workflow.
#
# ===
#
#  Core design principles:
#   - PathClient is the authoritative event timeline (one row per enrollment)
#   - TIEDENROLLMENT is the canonical episode join key
#   - Form tables (REF, HOSP_VISIT, IHNA, THREEM, SIXM, BHS) are
#       joined to PathClient by TIEDENROLLMENT
#   - SCD summation tables (payor, housing, client_needs) are joined by
#       PARENT_DOCSERNO to all possible parent forms (REF/IHNA/follow-ups)
#   - Active SCD summation tables are joined into ere_full_data (one row per
#       enrollment); “all” SCD summation tables remain long form for reporting
#   - All form columns are prefixed with form name (ref_, hosp_visit_, ihna_,
#       threem_, sixm_, bhs_, payor_, housing_, client_needs_)
#
#
# ===
#
# HOW THIS SCRIPT IS ORGANIZED
#
# 1. File paths
#      - All ERE extract paths are defined in `ere_paths`, using make_path()
#
# 2. Ingestion functions (load_ere_*)
#      - Each function loads one ERE extract using metadata from
#        analytic_fields (loaded via load_analytic_fields() in helpers.R)
#      - No column types are hard-coded; all typing is metadata-driven
#      - No renaming or cleanup should occur here
#
# 3. Transformation functions (transform_ere_*)
#      - Each function performs one major transformation step:
#          * transform_ere_pathclient()
#          * transform_ere_referral_flow()
#      - These functions join related extracts
#      - They return lists so Data Team staff can inspect intermediate objects
#      - ere-pathclient is pivoted to ensure one row per enrollment
#      - Event forms are cleaned to drop notes fields and *_code fields, prefix
#          all columns with form name, and preserve only client_number and
#          tiedenrollment as join keys
#      - SCD summation tables are cleaned to rename parent_docserno, prefix all
#          columns, and drop client_number from each. parent_docsernos are
#          joined to the first non-NA docserno from among the parent forms
#
# 4. Semantic wrapper function extract_ere_full_data() returns the final,
#      analysis-ready, wide ERE dataset (one row per enrollment). Subsetting
#      may be performed in the parent report projects using the build_subsets()
#      function in helpers.R.
#
# 5. Entry point
#      - run_ere_etl(...)
#      - Orchestrates ingestion → transformation → assembly
#      - Designed to be used as a {targets} target (e.g., ere_etl)
#      - Returns a nested list of all ERE objects; writing .rds files is
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
#   * ere <- run_ere_etl(ere_paths)
#
# Inspect raw ingestion tibbles:
#   * View(ere$raw$ere_client) # raw ere_client
#   * View(ere$raw$ere_pathclient) # raw ere_pathclient
#
# Inspect intermediate transformations:
#   * View(ere$transform$pathclient$joined_pathclient) # pivoted
#     ere_pathclient
#   * View(ere$transform$referral_flow$joined_referral_flow) # full joined
#     pathclient with pathway event tibbles and scd tables
#
# Inspect final full dataset:
#   * View(ere$ere_full_data) # wide, one row per enrollment
#
# This structure is intended to make debugging, onboarding, and unit testing
#   straightforward and to avoid reliance on global environment.
#
# However, if needed, one may assign objects to the global environment:
#   * ere_pathclient_raw <- ere$raw$ere_pathclient
#
# ===
#
# ABOUT ERE DATA STRUCTURE
#
# ERE enrollments are composed of multiple data sources:
#
#   * ProviderPlacement (program enrollment and dismissal - not joined)
#   * Client (demographics)
#   * PathClient (Pathway metadata bridge)
#   * Referral
#   * IHNA
#   * Follow-ups (three-month, six-month)
#   * Housing and Payor Source (active/all)
#   * Client Needs
#
# The transformation layer reconstructs this program life cycle for each
#   enrollment.
#
# ===
#
# REPORTING SUBSETS
#
# ERE reporting uses two primary fiscal-period subsets:
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
ere_paths <- list(
 ere_provider_placement = make_path(
  "FAMCare Q_ProviderPlacement_BHN/",
  "Q_ProviderPlacement_BHN.csv"
 ),
 ere_pathclient = make_path(
  "FAMCare ERE Extract/",
  "Q_ERE_PATHCLIENT_ENROLLMENTS.csv"
 ),
 ere_pathway_docsernos = make_path(
  "FAMCare ERE Extract/",
  "Q_ERE_PATHWAY_FORM_DOCSERNOS.csv"
 ),
 ere_client = make_path(
  "FAMCare ERE Extract/",
  "Q_ERE_CLIENT.csv"
 ),
 ere_ref = make_path(
  "FAMCare ERE Extract/",
  "Q_ERE_REFERRAL.csv"
 ),
 ere_ihna = make_path(
  "FAMCare ERE Extract/",
  "Q_ERE_IHNA.csv"
 ),
 ere_hosp_visit = make_path(
  "FAMCare ERE Extract/",
  "Q_ERE_HOSPITAL_VISIT_NOTE.csv"
 ),
 ere_three_month = make_path(
  "FAMCare ERE Extract/",
  "Q_ERE_THREE_MONTH.csv"
 ),
 ere_six_month = make_path(
  "FAMCare ERE Extract/",
  "Q_ERE_SIX_MONTH.csv"
 ),
 ere_bhs = make_path(
  "FAMCare ERE Extract/",
  "Q_ERE_BHS.csv"
 ),
 ere_active_housing = make_path(
  "FAMCare ERE Extract/",
  "Q_ERE_ACTIVE_HOUSING_STATUS.csv"
 ),
 ere_all_housing = make_path(
  "FAMCare ERE Extract/",
  "Q_ERE_ALL_HOUSING_STATUS.csv"
 ),
 ere_initial_latest_housing_status = make_path(
   "FAMCare ERE Extract/",
   "Q_ERE_INITIAL_LATEST_HOUSING_STATUS.csv"
 ),
 ere_active_payor_source = make_path(
  "FAMCare ERE Extract/",
  "Q_ERE_ACTIVE_PAYOR_SOURCE.csv"
 ),
 ere_all_payor_source = make_path(
  "FAMCare ERE Extract/",
  "Q_ERE_ALL_PAYOR_SOURCE.csv"
 ),
 ere_initial_latest_payor_source = make_path(
   "FAMCare ERE Extract/",
   "Q_ERE_INITIAL_LATEST_PAYOR_SOURCE.csv"
 ),
 ere_client_needs = make_path(
  "FAMCare ERE Extract/",
  "Q_ERE_CLIENT_NEEDS.csv"
 )
)

# ===
# 2. Ingestion/Loading Functions ----
#
# The loader functions were rendered obsolete when targets for each raw file 
# were added to _targets.R, so they have been removed. They were essentially
# dead code at this point. 
# ===

# ===
# 3. Transformation Layer Overview ----
#
# The transformation layer converts raw ere extracts (loaded via
# metadata-driven ingestion) into analysis-ready datasets.
#
# This layer is intentionally modular. Each function performs one major
# transformation step so that:
#   - Data Team staff can debug intermediate objects
#   - each step can be tested independently
#   - the ETL pipeline is readable and maintainable
#
# The major transformation steps are:
#   1. transform_ere_pathclient()
#        - joins client demographics and relocates to left side of tibble
#        - pivots Pathway Event form docsernos to columns
#        - retains only client demographics + enrollment metadata + docsernos
#
#   2. transform_ere_referral_flow()
#        - joins Pathway Event forms (ref, ihna, hosp_visit, etc.)
#        - joins SCD tables (active payor, active housing, client needs)
#
#   5. build_ere_full_data()
#        - returns joined_referral_flow as a unified dataset
#
#   6. build_ere_subsets() (optional)
#        - constructs reporting subsets, including:
#            * dismissed-within-fiscal-year (for outcomes)
#            * initiated-within-fiscal-year (for referral flow)
#
# All intermediate objects are returned as list elements so Data Team staff can
# inspect them interactively during development.
# ===

# ===
# Transform ere_pathclient ----
#   - PathClient is the authoritative event timeline (enrollment, dismissal,
#       pathway events)
#   - Pivot to one row per enrollment
#   - Drop Pathway metadata columns
#   - Keep analytic fields (enrollment dates, dismissal, agency, etc.)
# ===
transform_ere_pathclient <- function(
    ere
) {
  # Load raw pathclient extract, which is duplicated by Pathway Event form rows
  df <- ere$ere_pathclient |>
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
        "ERE Referral" = "ref_docserno",
        "ERE IHNA" = "ihna_docserno",
        "ERE Hospital Visit Note" = "hosp_visit_docserno",
        "ERE 3 Month" = "threem_docserno",
        "ERE 6 Month" = "sixm_docserno",
        "ERE Behavioral Health Service" = "bhs_docserno"
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
        ere$ere_client,
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
  # event_docserno (ref_docserno, ihna_docserno, etc.). There should only be one
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
#   - Joins ref, ihna, hosp_visit, threem, sixm, bhs
#   - Prefixes all columns except tiedenrollment and client_number
#   - Joins SCD summation tables (payor, housing, client needs) to ALL parent 
#       forms
#   - Collapses SCD summation tables to one active row per enrollment
# ===
transform_ere_referral_flow <- function(
    ere
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
  client_needs <- clean_form(
    ere$ere_client_needs,
    "client_needs_"
  ) |>
    dplyr::rename(
      parent_docserno = client_needs_parent_docserno
    ) |>
    dplyr::select(
      -client_number
    )

  # Drop docserno from parent event forms to avoid suffix collisions (.x/.y) due
  # to duplication when joining with pathclient. Pathclient is the authoritative
  # source of docserno values.
  ref <- clean_form(
    ere$ere_ref,
    "ref_"
  ) |>
    dplyr::select(
      -tidyselect::ends_with(
        "docserno"
      )
    )
  
  ihna <- clean_form(
    ere$ere_ihna,
    "ihna_"
  ) |>
    dplyr::select(
      -tidyselect::ends_with(
        "docserno"
      )
    )
  
  hosp_visit <- clean_form(
    ere$ere_hosp_visit,
    "hosp_visit_"
  ) |>
    dplyr::select(
      -tidyselect::ends_with(
        "docserno"
      )
    )
  
  threem <- clean_form(
    ere$ere_three_month,
    "threem_"
  ) |>
    dplyr::select(
      -tidyselect::ends_with(
        "docserno"
      )
    )
  
  sixm <- clean_form(
    ere$ere_six_month,
    "sixm_"
  ) |>
    dplyr::select(
      -tidyselect::ends_with(
        "docserno"
      )
    )
  
  bhs <- clean_form(
    ere$ere_bhs,
    "bhs_"
  ) |>
    dplyr::select(
      -tidyselect::ends_with(
        "docserno"
      )
    )
  
  payor <- clean_form(
    ere$ere_initial_latest_payor_source,
    "payor_"
  ) |> 
    dplyr::select(
      -tidyselect::ends_with(
        "docserno"
      )
    )
  
  housing <- clean_form(
    ere$ere_initial_latest_housing_status,
    "housing_"
  ) |> 
    dplyr::select(
      -tidyselect::ends_with(
        "docserno"
      )
    )
  
  # Start with pivoted pathclient
  pc <- ere$pathclient
  
  # Invariant: parent_map must contain exactly one row per (client_number,
  # tiedenrollment, parent_docserno). If this is violated, SCD collapse will
  # fail. The result is a long form tibble
  parent_map <- pc |>
    dplyr::select(
      client_number,
      tiedenrollment,
      ref_docserno,
      ihna_docserno,
      hosp_visit_docserno,
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
  
  # Diagnostic: client_needs_one shows which client needs SCD row was selected for
  # each enrollment. Useful for debugging missing or stale SCD values.
  client_needs_one  <- collapse_scd(
    client_needs
  )
  
  # Start join sequence with joined "authoritative" pathclient
  joined <- pc |>
    dplyr::left_join(
      client_needs_one,
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
      ihna,
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
    ) |> 
    dplyr::left_join(
      payor,
      by = join_by(
        "client_number",
        "tiedenrollment"
      )
    ) |> 
    dplyr::left_join(
      housing,
      by = join_by(
        "client_number",
        "tiedenrollment"
      )
    )
  
  # Store the final joined referral flow table
  output <- list(
    scd = list(
      client_needs_one = client_needs_one
    ),
    parent_map = parent_map,
    transformed = list(
      ref = ref,
      ihna = ihna,
      hosp_visit = hosp_visit,
      threem = threem,
      sixm = sixm,
      bhs = bhs,
      payor = payor,
      housing = housing
    ),
    joined_referral_flow = joined
  )
  
  # Return the joined_referral_flow and scd
  return(
    output
  )
  
}

# ===
# 4. Extract final ere_full_data ----
#   - Returns the final wide referral_flow table
#   - Maintained as a semantic wrapper to expose the final wide table as
#       ere_full_data
# ===

extract_ere_full_data <- function(
    referral_flow
) {
  referral_flow$joined_referral_flow
}

# ===
# 5. ere ETL entry point ----
#   - Loads analytic_fields metadata
#   - Ingests all ere extracts using metadata-driven loaders
#   - Applies ere-specific transformations (e.g., pivoting, referral_flow)
#   - Returns a named list of all ere data objects
#   - Does not write to disk or modify global env objects as the old ETL code
#       did.
# ===
run_ere_etl <- function(
    analytic_fields,
    ere_provider_placement,
    ere_pathclient,
    ere_pathway_docsernos,
    ere_client,
    ere_ref,
    ere_hosp_visit,
    ere_ihna,
    ere_three_month,
    ere_six_month,
    ere_bhs,
    ere_active_payor_source,
    ere_all_payor_source,
    ere_initial_latest_payor_source,
    ere_active_housing,
    ere_all_housing,
    ere_initial_latest_housing_status,
    ere_client_needs,
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
  
  # 1. Raw ingestion
  ere_raw <- list(
    ere_client = ere_client,
    ere_provider_placement = ere_provider_placement,
    ere_pathclient = ere_pathclient,
    ere_pathway_docsernos = ere_pathway_docsernos,
    ere_ref = ere_ref,
    ere_ihna = ere_ihna,
    ere_hosp_visit = ere_hosp_visit,
    ere_three_month = ere_three_month,
    ere_six_month = ere_six_month,
    ere_bhs = ere_bhs,
    ere_active_payor_source = ere_active_payor_source,
    ere_all_payor_source = ere_all_payor_source,
    ere_initial_latest_payor_source = ere_initial_latest_payor_source,
    ere_active_housing = ere_active_housing,
    ere_all_housing = ere_all_housing,
    ere_initial_latest_housing_status = ere_initial_latest_housing_status,
    ere_client_needs = ere_client_needs
  )
  
# ===
  # 2. Transformations
# ===
  pathclient <- transform_ere_pathclient(
    ere_raw
  )
  # Promote pivoted pathclient to authoritative
  ere_raw$pathclient <- pathclient$joined_pathclient
  
  referral_flow <- transform_ere_referral_flow(
    ere_raw
  )
  
  full <- extract_ere_full_data(
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
    raw = ere_raw,
    transform  = list(
      pathclient = pathclient,
      referral_flow = referral_flow
    ),
    ere_full_data = full,
    subsets = subsets
  )
}