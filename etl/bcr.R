# ===
# bcr ETL PIPELINE (Refactored, Metadata-Driven) ----
#
# This script implements a metadata-driven column typing ETL for bcr,
# returning a nested list of raw and transformed objects instead of writing to
# the global environment as has been done in the past. It replaces the legacy
# monolithic ETL child doc with a clean, testable, and maintainable workflow.
#
# ===
#
#  Core design principles:
#   - PathClient is the authoritative event timeline (one row per enrollment)
#   - TIEDENROLLMENT is the canonical episode join key
#   - Form tables (REF, IC, RP) are
#       joined to PathClient by TIEDENROLLMENT
#   - SCD summation tables (presconcerns, payor, housing) are joined by
#       PARENT_DOCSERNO to all possible parent forms (REF/IC/RP)
#   - Active SCD summation tables are joined into bcr_full_data (one row per
#       enrollment); “all” SCD summation tables remain long form for reporting
#   - All form columns are prefixed with form name (ref_, ic_, rp_,
#       events_, css_, pc_, payor_, housing_)
#
# ===
#
# HOW THIS SCRIPT IS ORGANIZED
#
# 1. File paths
#      - All bcr extract paths are defined in `bcr_paths`, using make_path()
#
# 2. Ingestion functions (load_bcr_*)
#      - Each function loads one bcr extract using metadata from
#        analytic_fields (loaded via load_analytic_fields() in helpers.R)
#      - No column types are hard-coded; all typing is metadata-driven
#      - No renaming or cleanup should occur here
#
# 3. Transformation functions (transform_bcr_*)
#      - Each function performs one major transformation step:
#          * transform_bcr_pathclient()
#          * transform_bcr_referral_flow()
#      - These functions join related extracts
#      - They return lists so Data Team staff can inspect intermediate objects
#      - bcr-pathclient is pivoted to ensure one row per enrollment
#      - Event forms are cleaned to drop notes fields and *_code fields, prefix
#          all columns with form name, and preserve only client_number and
#          tiedenrollment as join keys
#      - SCD summation tables are cleaned to rename parent_docserno, prefix all
#          columns, and drop client_number from each. parent_docsernos are
#          joined to the first non-NA docserno from among the parent forms
#
# 4. Semantic wrapper function extract_bcr_full_data() returns the final,
#      analysis-ready, wide bcr dataset (one row per enrollment). Subsetting
#      may be performed in the parent report projects using the build_subsets()
#      function in helpers.R.
#
# 5. Entry point
#      - run_bcr_etl(...)
#      - Orchestrates ingestion → transformation → assembly
#      - Designed to be used as a {targets} target (e.g., bcr_etl)
#      - Returns a nested list of all bcr objects; writing .rds files is
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
#   * bcr <- run_bcr_etl(bcr_paths)
#
# Inspect raw ingestion tibbles:
#   * View(bcr$raw$bcr_client) # raw bcr_client
#   * View(bcr$raw$bcr_pathclient) # raw bcr_pathclient
#
# Inspect intermediate transformations:
#   * View(bcr$transform$pathclient$joined_pathclient) # pivoted
#     bcr_pathclient
#   * View(bcr$transform$referral_flow$joined_referral_flow) # full joined
#     pathclient with pathway event tibbles and scd tables
#
# Inspect final full dataset:
#   * View(bcr$bcr_full_data) # wide, one row per enrollment
#
# This structure is intended to make debugging, onboarding, and unit testing
#   straightforward and to avoid reliance on global environment.
#
# However, if needed, one may assign objects to the global environment:
#   * bcr_pathclient_raw <- bcr$raw$bcr_pathclient
#
# ===
#
# ABOUT bcr DATA STRUCTURE
#
# bcr enrollments are composed of multiple data sources:
#
#   * ProviderPlacement (program enrollment and dismissal - not joined)
#   * Client (demographics)
#   * PathClient (Pathway metadata bridge)
#   * Referral
#   * IC (Initial Contact)
#   * RP (Referrals Placed)
#   * Events (non-client form)
#   * CSS (Client Counseling Sessions)
#   * Presenting Concerns, Housing (active/all), and Payor Source (active/all)
#
# The transformation layer reconstructs this program life cycle for each
#   enrollment.
#
# ===
#
# REPORTING SUBSETS
#
# bcr reporting uses two primary fiscal-period subsets:
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
# 0. Program-Specific Helper Functions ----
# ===

# Detect RP TYPE‑LEVEL referral fields (e.g., bh_ref_placed, housing_ref_placed)
# ===
# This function identifies which *type-level* referral fields exist in the
# raw referrals placed (RP) table. Type-level fields correspond to the
# "big buckets" of referral types (behavioral_health, housing, etc.).
#
# The master_lookup table defines which RP fields *should* exist. We intersect
# those with the actual RP columns to determine which type-level referral
# fields are present for this enrollment.
#
# Output:
#   placed_col  = the RP column name (e.g., "bh_ref_placed")
#   type_clean  = normalized type name (e.g., "bh")
bcr_detect_rp_types <- function(
    rp,
    master_lookup
    ) {
  
  rp <- rp |>
    janitor::clean_names()
  rp_cols <- names(
    rp
    )
  
  # Raw RP fields look like: bh_ref_placed, housing_ref_placed, etc.
  type_fields_raw <- master_lookup |>
    filter(
      master_table_name == "bcr_presenting_concerns",
      status == "Active"
      ) |>
    pull(
      rp_type_field_name
      )
  
  # Keep only those that exist in RP
  type_cols <- intersect(
    type_fields_raw,
    rp_cols
    )
  
  tibble(
    placed_col = type_cols,
    type_clean = sub(
      "_ref_placed$",
      "",
      type_cols
      )
  )
}

# Detect RP SUBTYPE‑LEVEL referral fields (e.g., counseling_ref_placed)
# ===
# RP contains many subtype-level referral fields (counseling, cmhc, furniture,
# primary care, etc.). These fields are prefixed in the cleaned RP, so we
# normalize column names by stripping "rp_" before detection.
#
# master_lookup defines the canonical list of subtype roots (rp_field_name).
# We convert those into expected RP column names ("<subtype>_ref_placed") and
# intersect with actual RP columns to determine which subtypes are present.
#
# Output:
#   placed_col      = "<subtype>_ref_placed"
#   subtype_clean   = "<subtype>"
#   agency_col      = "<subtype>_agency"
#   agency_desc_col = "<subtype>_agency_desc"
#   date_col        = "date_<subtype>_ref_placed"
#
# These columns are used later to extract agency names and referral dates.
bcr_detect_rp_subtypes <- function(
    rp,
    master_lookup
    ) {
  
  # Normalize RP column names
  rp <- rp |>
    janitor::clean_names()
  names(
    rp
    ) <- sub(
      "^rp_",
      "",
      names(
        rp
        )
      )
  rp_cols <- names(
    rp
    )
  
  # Metadata subtype fields
  subtype_fields <- master_lookup |>
    dplyr::filter(
      grepl(
        "^bcr_ref_placed_",
        master_table_name
        ),
      status == "Active"
    ) |>
    dplyr::pull(
      rp_field_name
      )
  
  # Expected RP columns
  expected_cols <- paste0(
    subtype_fields,
    "_ref_placed"
    )
  
  # Intersection with RP
  placed_cols <- intersect(
    expected_cols,
    rp_cols
    )
  
  subtype_clean <- sub(
    "_ref_placed$",
    "",
    placed_cols
    )
  
  tibble::tibble(
    placed_col = placed_cols,
    subtype_clean = subtype_clean,
    agency_col = paste0(
      subtype_clean,
      "_agency"
      ),
    agency_desc_col = paste0(
      subtype_clean,
      "_agency_desc"
      ),
    date_col = paste0(
      "date_",
      subtype_clean,
      "_ref_placed"
      )
  )
}

# Build TYPE‑LEVEL referral metadata map
# ===
# master_lookup and bcr_referral_type_map define the canonical referral types
# (Behavioral Health, Housing, Physical Health, etc.) and their RP field names.
#
# Steps:
#   1. Filter metadata to active type definitions.
#   2. Normalize referral_type into snake_case for consistency.
#   3. Construct placed_col = rp_type_field_name (e.g., "bh_ref_placed").
#   4. Detect which type-level fields actually exist in RP.
#   5. Join detected fields with metadata.
#
# Output includes:
#   placed_col      = RP column name
#   type_clean      = normalized type name
#   referral_type   = canonical snake_case type name
#   code, description, status
#
# This map drives the TYPE portion of rp_long.
bcr_build_rp_type_map <- function(
    bcr_referral_type_map,
    rp,
    master_lookup
) {
  
  bcr_referral_type_map <- bcr_referral_type_map |>
    filter(
      !is.na(
        rp_type_field_name
        ),
      rp_type_field_name != ""
      )
  
  type_map_norm <- bcr_referral_type_map |>
    mutate(
      type_clean = janitor::make_clean_names(
        referral_type
        ),
      placed_col = paste0(
        rp_type_field_name
        )  # raw RP column name
    )
  
  detected <- bcr_detect_rp_types(
    rp,
    master_lookup
    )
  
  detected |>
    left_join(
      type_map_norm,
      by = "placed_col"
      )
}

# Build SUBTYPE‑LEVEL referral metadata map
# ===
# Subtypes (counseling, cmhc, furniture, etc.) are defined in
# bcr_referral_subtype_map. RP contains corresponding fields, but they may be
# prefixed ("rp_") depending on how RP was cleaned. We normalize RP column
# names by stripping "rp_" before matching.
#
# Steps:
#   1. Normalize RP column names.
#   2. Identify subtype roots present in RP.
#   3. Filter metadata to only subtypes that actually exist in RP.
#   4. Build placed_col, agency_desc_col, date_col for each subtype.
#   5. Normalize referral_type to snake_case to match type_map.
#   6. Detect actual subtype fields in RP.
#   7. Join detected fields with metadata.
#
# Output includes:
#   placed_col      = "<subtype>_ref_placed"
#   subtype_clean   = "<subtype>"
#   agency_desc_col = "<subtype>_agency_desc"
#   date_col        = "date_<subtype>_ref_placed"
#   referral_type   = snake_case type name
#
# This map drives the SUBTYPE portion of rp_long.
bcr_build_rp_subtype_map <- function(
    bcr_referral_subtype_map,
    rp,
    master_lookup
) {
  
  # Normalize RP column names
  rp <- rp |>
    janitor::clean_names()
  names(
    rp
    ) <- sub(
      "^rp_",
      "",
      names(
        rp
        )
      )
  
  # Identify RP subtype roots
  rp_subtypes <- names(
    rp
    )[grepl("_ref_placed$", names(rp))] |>
    sub(
      "_ref_placed$",
      "",
      x = _
      )
  
  # Filter metadata to only RP subtypes that exist
  bcr_referral_subtype_map <- bcr_referral_subtype_map |>
    dplyr::filter(
      !is.na(
        rp_field_name
        ),
      rp_field_name != "",
      rp_field_name %in% rp_subtypes
    )
  
  # Build normalized metadata
  subtype_map_norm <- bcr_referral_subtype_map |>
    dplyr::mutate(
      placed_col = paste0(
        rp_field_name,
        "_ref_placed"
        ),
      agency_desc_col = paste0(
        rp_field_name,
        "_agency_desc"
        ),
      date_col = paste0(
        "date_",
        rp_field_name,
        "_ref_placed"
        )
    )
  
  subtype_map_norm <- subtype_map_norm |>
    mutate(
      referral_type = snakecase::to_snake_case(
        referral_type
        )
      )
  
  # Detect actual RP subtype columns
  detected <- bcr_detect_rp_subtypes(
    rp,
    master_lookup
    )
  
  # Join detected RP columns with metadata
  detected |>
    dplyr::left_join(
      subtype_map_norm,
      by = "placed_col"
      )
}

# Pivot RP TYPE + SUBTYPE referrals into long-form table
# ===
# rp_long is the unified long-form representation of all referrals placed.
# It merges:
#   - TYPE‑LEVEL referrals (behavioral_health, housing, etc.)
#   - SUBTYPE‑LEVEL referrals (counseling, cmhc, furniture, etc.)
#
# TYPE‑LEVEL:
#   - referral_subtype = NA
#   - agency_name and referral_date only exist for "other"
#   - referral_type comes from type_map (snake_case)
#
# SUBTYPE‑LEVEL:
#   - referral_subtype = "<subtype>"
#   - agency_name and referral_date extracted from RP using subtype_map
#   - referral_type normalized to snake_case
#
# Defensive extraction:
#   - agency_name and referral_date are only populated if the expected
#     columns exist in RP; otherwise NA is used.
#
# Output columns:
#   client_number, tiedenrollment
#   referral_type, referral_subtype
#   referral_code, referral_desc, referral_status
#   referral_placed
#   agency_name, referral_date
#   unmet_needs, unmet_needs_exp
#
# This table is the authoritative long-form representation of all referrals.
bcr_pivot_rp_long <- function(
    rp,
    type_map,
    subtype_map
) {
  # --- TYPE-LEVEL ---
  type_map_filtered <- type_map |>
    dplyr::filter(
      !is.na(
        referral_type
        )
      )
  
  rp_long_types <- purrr::map_dfr(
    seq_len(
      nrow(
        type_map_filtered
        )
      ),
    function(
    i
    ) {
      m <- type_map_filtered[i, ]
      key <- m$placed_col
      
      if (
        key == "other_ref_placed"
        ) {
        rp |>
          dplyr::mutate(
            referral_type = m$referral_type,
            referral_subtype = NA_character_,
            referral_code = NA_character_,
            referral_desc = NA_character_,
            referral_status = NA_character_,
            referral_placed = rp[[key]],
            agency_name = rp[["other_agency"]],
            referral_date = rp[["date_other_ref"]]
          ) |>
          dplyr::filter(
            referral_placed == 1
            )
      } else {
        rp |>
          dplyr::mutate(
            referral_type = m$referral_type,
            referral_subtype = NA_character_,
            referral_code = m$code,
            referral_desc = m$description,
            referral_status = NA_character_,
            referral_placed = rp[[key]],
            agency_name = rp[[paste0(key, "_agency_desc")]],
            referral_date = rp[[paste0("date_", key, "_ref_placed")]]
          ) |>
          dplyr::filter(
            referral_placed == 1
            )
      }
    }
  )
  
  # --- SUBTYPE-LEVEL ---
  rp_long_subtypes <- purrr::map_dfr(
    seq_len(
      nrow(
        subtype_map
        )
      ),
    function(
    i
    ) {
      m <- subtype_map[i, ] |>
        as.list()
      
      # Defensive extraction
      agency_col <- m$agency_desc_col
      date_col <- m$date_col
      
      agency_name <- if (
        !is.null(
          agency_col
          ) && length(
            agency_col
            ) == 1 && !is.na(
              agency_col
              ) && agency_col %in% names(
                rp
                )
        ) {
        rp[[agency_col]]
      } else {
        NA_character_
      }
      
      referral_date <- if (
        !is.null(
          date_col
          ) && length(
            date_col
            ) == 1 && !is.na(
              date_col
              ) && date_col %in% names(
                rp
                )
        ) {
        rp[[date_col]]
      } else {
        NA
      }
      
      rp |>
        dplyr::mutate(
          referral_type = m$referral_type,
          referral_subtype = m$subtype_clean,
          referral_code = m$code,
          referral_desc = m$description,
          referral_status = m$status,
          referral_placed = rp[[m$placed_col]],
          agency_name = agency_name,
          referral_date = referral_date
        ) |>
        dplyr::filter(
          referral_placed == 1
          ) |>
        dplyr::select(
          client_number,
          tiedenrollment,
          referral_type,
          referral_subtype,
          referral_code,
          referral_desc,
          referral_status,
          referral_placed,
          agency_name,
          referral_date,
          unmet_needs,
          unmet_needs_exp
        )
    }
  )
  
  dplyr::bind_rows(
    rp_long_types,
    rp_long_subtypes
    )
}

# Clean and prefix RP columns for joining into referral_flow
# ===
# RP is used in two forms:
#   1. rp_detect  = unprefixed (for detection + rp_long)
#   2. rp         = prefixed   (for joining into referral_flow)
#
# This function produces the prefixed version (rp):
#   - Prefixes *_ref_placed, *_agency, *_agency_desc, date_* fields with "rp_"
#   - Prefixes core fields (id, docserno, visit_date, etc.)
#   - Leaves join keys (client_number, tiedenrollment) unprefixed
#
# This ensures RP joins cleanly into referral_flow without name collisions.
bcr_clean_rp <- function(
    rp_raw
    ) {
  rp <- rp_raw |>
    janitor::clean_names()
  rp_cols <- names(
    rp
    )
  
  cols_to_prefix <- rp_cols[
    grepl(
      "_ref_placed$",
      rp_cols
      ) |
      grepl(
        "_agency$",
        rp_cols
        ) |
      grepl(
        "_agency_desc$",
        rp_cols
        ) |
      grepl(
        "^date_.*_ref_placed$",
        rp_cols
        ) |
      grepl(
        "^date_.*_ref$",
        rp_cols
        )
  ]
  
  new_names <- paste0(
    "rp_",
    cols_to_prefix
    )
  names(
    rp
    )[match(cols_to_prefix, names(rp))] <- new_names
  
  core_fields <- c(
    "id",
    "docserno",
    "visit_date",
    "visit_time",
    "userid",
    "event_name",
    # NOTE: no tiedenrollment, no client_number, no client_name
    "pathway_date",
    "bcr_type_ref_placed"
  )
  
  core_fields <- intersect(
    core_fields,
    names(
      rp
      )
    )
  names(
    rp
    )[match(core_fields, names(rp))] <- paste0(
      "rp_",
      core_fields
      )
  
  rp
}

# ===
# 1. List file paths for all data source files. ----
#   - Uses function make_path() from helpers.R.
# ===
bcr_paths <- list(
  bcr_provider_placement = make_path(
    "FAMCare Q_ProviderPlacement_BHN/",
    "Q_ProviderPlacement_BHN.csv"
  ),
  bcr_pathclient = make_path(
    "FAMCare BCR Extract/",
    "Q_BCR_PATHCLIENT_ENROLLMENTS.csv"
  ),
  bcr_pathway_docsernos = make_path(
    "FAMCare BCR Extract/",
    "Q_BCR_PATHWAY_FORM_DOCSERNOS.csv"
  ),
  bcr_client = make_path(
    "FAMCare BCR Extract/",
    "Q_BCR_CLIENT.csv"
  ),
  bcr_ref = make_path(
    "FAMCare BCR Extract/",
    "Q_BCR_REFERRAL.csv"
  ),
  bcr_ic = make_path(
    "FAMCare BCR Extract/",
    "Q_BCR_IC.csv"
  ),
  bcr_presenting_concerns = make_path(
    "FAMCare BCR Extract/",
    "Q_BCR_PRESENTING_CONCERNS.csv"
  ),
  bcr_referrals_placed = make_path(
    "FAMCare BCR Extract/",
    "Q_BCR_REF_PLACED.csv"
  ),
  bcr_events = make_path(
    "FAMCare BCR Extract/",
    "Q_BCR_EVENTS.csv"
  ),
  bcr_client_counseling_sessions = make_path(
    "FAMCare BCR Extract/",
    "Q_BCR_CLIENT_COUNSELING_SESSIONS.csv"
  ),
  bcr_active_payor_source = make_path(
    "FAMCare BCR Extract/",
    "Q_BCR_ACTIVE_PAYOR_SOURCE.csv"
  ),
  bcr_all_payor_source = make_path(
    "FAMCare BCR Extract/",
    "Q_BCR_ALL_PAYOR_SOURCE.csv"
  ),
  bcr_active_housing = make_path(
    "FAMCare BCR Extract/",
    "Q_BCR_ACTIVE_HOUSING_STATUS.csv"
  ),
  bcr_all_housing = make_path(
    "FAMCare BCR Extract/",
    "Q_BCR_ALL_HOUSING_STATUS.csv"
  )
)

# ===
# 2. Ingestion/Loading Functions ----
# ===

# ===
# Ingest bcr_client ----
#   - one row per client
# ===
load_bcr_client <- function(
  bcr_paths,
  analytic_fields
  ) {
  load_famcare_extract(
    path = bcr_paths$bcr_client,
    analytic_fields = analytic_fields
  )
}

# ===
# Ingest bcr_provider_placement ----
#   - one row per enrollment - available to supplement bcr_pathclient but not
#       joined
#   - Renames key fields
# ===
load_bcr_provider_placement <- function(
  bcr_paths,
  analytic_fields
  ) {
  load_famcare_extract(
    path = bcr_paths$bcr_provider_placement,
    analytic_fields = analytic_fields
  )
}

# ===
# Ingest bcr_pathclient ----
#   - Renames key fields
#   - Pivoting handled separately, so this is not one row per enrollment yet
# ===
load_bcr_pathclient <- function(
  bcr_paths,
  analytic_fields
  ) {
  load_famcare_extract(
    path = bcr_paths$bcr_pathclient,
    analytic_fields = analytic_fields
  )
}

# ===
# Ingest bcr_pathway_docsernos ----
#   - one row per Pathway Event form
# ===
load_bcr_pathway_docsernos <- function(
  bcr_paths,
  analytic_fields
  ) {
  load_famcare_extract(
    path = bcr_paths$bcr_pathway_docsernos,
    analytic_fields = analytic_fields
  )
}

# ===
# Ingest bcr_referral ----
#   - one row per referral for each enrollment
# ===
load_bcr_ref <- function(
  bcr_paths,
  analytic_fields
  ) {
  load_famcare_extract(
    path = bcr_paths$bcr_ref,
    analytic_fields = analytic_fields
  )
}

# ===
# Ingest bcr_initial_contact ----
#   - one row per initial contact for each enrollment
# ===
load_bcr_ic <- function(
  bcr_paths,
  analytic_fields
  ) {
  load_famcare_extract(
    path = bcr_paths$bcr_ic,
    analytic_fields = analytic_fields
  )
}

# ===
# Ingest bcr_referrals_placed ----
#   - one row per referrals placed for each enrollment
# ===
load_bcr_referrals_placed <- function(
  bcr_paths,
  analytic_fields
  ) {
  load_famcare_extract(
    path = bcr_paths$bcr_referrals_placed,
    analytic_fields = analytic_fields
  )
}

# ===
# Ingest bcr_presenting_concerns ----
#   - one row per presenting concerns for each enrollment
# ===
load_bcr_presenting_concerns <- function(
  bcr_paths,
  analytic_fields
  ) {
  load_famcare_extract(
    path = bcr_paths$bcr_presenting_concerns,
    analytic_fields = analytic_fields
  )
}

# ===
# Ingest bcr_events ----
#   - one row per event for each enrollment
# ===
load_bcr_events <- function(
  bcr_paths,
  analytic_fields
  ) {
  load_famcare_extract(
    path = bcr_paths$bcr_events,
    analytic_fields = analytic_fields
  )
}

# ===
# Ingest bcr_client_counseling_sessions ----
#   - multiple rows per client counseling session for each enrollment
# ===
load_bcr_client_counseling_sessions <- function(
  bcr_paths,
  analytic_fields
  ) {
  load_famcare_extract(
    path = bcr_paths$bcr_client_counseling_sessions,
    analytic_fields = analytic_fields
  )
}

# ===
# Ingest bcr_active_payor_source ----
#   - one row per active payor source per enrollment
#   - Renames key fields
# ===
load_bcr_active_payor_source <- function(
  bcr_paths,
  analytic_fields
  ) {
  load_famcare_extract(
    path = bcr_paths$bcr_active_payor_source,
    analytic_fields = analytic_fields
  )
}

# ===
# Ingest bcr_all_payor_source ----
#   - long form with one row per payor source record per enrollment, which
#       means that this duplicates on enrollments
# ===
load_bcr_all_payor_source <- function(
  bcr_paths,
  analytic_fields
  ) {
  load_famcare_extract(
    path = bcr_paths$bcr_all_payor_source,
    analytic_fields = analytic_fields
  )
}

# ===
# Ingest bcr_active_housing_status ----
#   - one row per active housing status per enrollment
# ===
load_bcr_active_housing <- function(
  bcr_paths,
  analytic_fields
  ) {
  load_famcare_extract(
    path = bcr_paths$bcr_active_housing,
    analytic_fields = analytic_fields
  )
}

# ===
# Ingest bcr_all_housing_status ----
#   - long form with one row per housing status record per enrollment, which
#       means that this duplicates on enrollments
# ===
load_bcr_all_housing <- function(
  bcr_paths,
  analytic_fields
  ) {
  load_famcare_extract(
    path = bcr_paths$bcr_all_housing,
    analytic_fields = analytic_fields
  )
}

# ===
# 3. Transformation Layer Overview ----
#
# The transformation layer converts raw BCR extracts (loaded via metadata-driven
# ingestion) into analysis-ready datasets.
#
# This layer is intentionally modular. Each function performs one major
# transformation step so that:
#   - Data Team staff can debug intermediate objects
#   - each step can be tested independently
#   - the ETL pipeline is readable and maintainable
#
# The major transformation steps are: 1. transform_bcr_pathclient()
#        - joins client demographics and relocates to left side of tibble
#        - pivots Pathway Event form docsernos to columns
#        - retains only client demographics + enrollment metadata + docsernos
#
# 2. transform_bcr_referral_flow()
#        - joins Pathway Event forms (ref, ic, rp)
#        - joins SCD tables (presenting concerns, active payor, active housing)
#
# 5. build_bcr_full_data()
#        - returns joined_referral_flow as a unified dataset
#
# 6. build_bcr_subsets() (optional)
#        - constructs reporting subsets, including:
#            * dismissed-within-fiscal-year (for outcomes)
#            * initiated-within-fiscal-year (for referral flow)
#
# All intermediate objects are returned as list elements so Data Team staff can
# inspect them interactively during development.
#
# All transformed tables are also returned as list elements to allow for
# troubleshooting.
# ===

## ===
## 3a. Transform bcr_pathclient ----
##   - PathClient is the authoritative event timeline (enrollment, dismissal,
##       pathway events)
##   - Pivot to one row per enrollment
##   - Drop Pathway metadata columns
##   - Keep analytic fields (enrollment dates, dismissal, agency, etc.)
## ===
transform_bcr_pathclient <- function(
  bcr
) {
  # Load raw pathclient extract, which is duplicated by Pathway Event form rows
  df <- bcr$bcr_pathclient |>
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
        "BCR Referral" = "ref_docserno",
        "BCR Initial Contact" = "ic_docserno",
         "BCR Referrals Placed" = "rp_docserno"
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

  enrollment <-  enrollment |>
    mutate(
      month_year_referral_full  = month_year_label_full(
        enrollment_starting_date
        ),
      month_year_referral_abbrev = month_year_label_abbrev(
        enrollment_starting_date
        )
    ) |>
    order_fiscal_year_labels(
      month_year_referral_full,
      enrollment_starting_date,
      "state"
      ) |>
    order_fiscal_year_labels(
      month_year_referral_abbrev,
      enrollment_starting_date,
      "state"
      )
  
  # Merge client demographics
  enrollment <- enrollment |>
    dplyr::left_join(
      dplyr::select(
        bcr$bcr_client,
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

## ===
## 3b. Transform referral flow ----
##   - Joins REF, IC, RP
##   - Prefixes all columns except tiedenrollment
##   - Joins SCD summation tables (presconcerns, payor, housing) to ALL parent forms
##   - Collapses SCD summation tables to one active row per enrollment
## ===
transform_bcr_referral_flow <- function(
    bcr,
    bcr_referral_type_map,
    bcr_referral_subtype_map,
    master_lookup
) {
  
  # Helper to prefix all columns except tiedenrollment and client_number and
  # drop metadata fields (notes, *_code with matching *_description) 
  clean_form <- function(
    df,
    prefix
    ) {
    
    code_cols <- names(
      df
      )[
        endsWith(
          names(
            df
            ),
          "_code"
          )
        ]
    desc_cols <- names(
      df
      )[
        endsWith(
          names(
            df
            ),
          "_description"
          )
        ]
    
    # Drop *_code fields when a *_description counterpart exists
    code_with_desc <- code_cols[
      sub(
        "_code$",
        "_description",
        code_cols
        ) %in% desc_cols
    ]
    
    df |>
      dplyr::select(
        -tidyselect::contains(
          "notes"
          ),
      ) |>
      dplyr::select(
        -tidyselect::all_of(
          code_with_desc
          )
      ) |>
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
  
  # --- SCD tables ---
  # Clean and prefix SCD tables; remove join keys so they can be collapsed
  payor <- clean_form(
    bcr$bcr_active_payor_source,
    "payor_"
    ) |>
    dplyr::rename(
      parent_docserno = payor_parent_docserno
      ) |>
    dplyr::select(
      -client_number,
      -tiedenrollment
      )
  
  housing <- clean_form(
    bcr$bcr_active_housing,
    "housing_"
    ) |>
    dplyr::rename(
      parent_docserno = housing_parent_docserno
      ) |>
    dplyr::select(
      -client_number
      )
  
  presconcerns <- clean_form(
    bcr$bcr_presenting_concerns,
    "pc_"
    ) |>
    dplyr::rename(
      parent_docserno = pc_parent_docserno
      ) |>
    dplyr::select(
      -client_number
      )
  
  # --- Event forms ---
  # REF and IC are cleaned and prefixed; drop docserno and client name fields
  ref <- clean_form(
    bcr$bcr_ref,
    "ref_"
    ) |>
    dplyr::select(
      -tidyselect::ends_with(
        "docserno"
        ),
      -ref_client_last,
      -ref_client_first
    )
  
  ic <- clean_form(
    bcr$bcr_ic,
    "ic_"
    ) |>
    dplyr::select(
      -tidyselect::ends_with(
        "docserno"
        ),
      -ic_client_last,
      -ic_client_first
    )
  
  # --- Referrals Placed (RP) ---
  rp_raw <- bcr$bcr_referrals_placed
  
  # rp_detect = unprefixed RP used for detection + type/subtype map building
  rp_detect <- rp_raw |>
    janitor::clean_names()
  
  # rp = prefixed RP used for joining into referral_flow
  rp <- bcr_clean_rp(
    rp_raw
    ) |>
    dplyr::select(
      -tidyselect::ends_with(
        "docserno"
        )
    )
  
  # --- CCS (long-form, NOT joined) ---
  # CCS is kept separate; pivoted wide only for reporting convenience
  ccs <- clean_form(
    bcr$bcr_client_counseling_sessions,
    "ccs_"
    ) |>
    dplyr::select(
      -tidyselect::ends_with(
        "docserno"
        )
      ) |>
    arrange(
      client_number,
      ccs_session_date
      ) |>
    group_by(
      client_number,
      tiedenrollment
      ) |>
    mutate(
      ccs_session_n = row_number()
      ) |>
    ungroup() |>
    tidyr::pivot_wider(
      id_cols = c(
        client_number,
        tiedenrollment
        ),
      names_from = ccs_session_n,
      values_from = ccs_session_date,
      names_glue = "ccs_session_{ccs_session_n}_date"
    ) |>
    mutate(
      ccs_total_sessions = rowSums(
        !is.na(
          dplyr::across(
            starts_with(
              "ccs_session_"
              )
          )
        )
      )
    )
  
  # --- Parent map ---
  # Maps each event form (REF, IC, RP) to its parent_docserno for SCD collapsing
  pc <- bcr$pathclient
  
  parent_map <- pc |>
    dplyr::select(
      client_number,
      tiedenrollment,
      ref_docserno,
      ic_docserno,
      rp_docserno
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
  
  # Collapse SCD tables to one active row per enrollment
  collapse_scd <- function(
    scd_tbl
    ) {
    parent_map |>
      dplyr::left_join(
        scd_tbl,
        by = "parent_docserno") |>
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
  
  payor_one <- collapse_scd(
    payor
    )
  housing_one <- collapse_scd(
    housing
    )
  presconcerns_one <- collapse_scd(
    presconcerns
    )
  
  # --- Join everything into wide referral_flow ---
  joined <- pc |>
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
      presconcerns_one,
      by = c(
        "client_number",
        "tiedenrollment"
        )
      ) |>
    dplyr::left_join(
      ref,
      by = c(
        "client_number",
        "tiedenrollment"
        )
      ) |>
    dplyr::left_join(
      ic,
      by = c(
        "client_number",
        "tiedenrollment"
        )
      ) |>
    dplyr::left_join(
      rp,
      by = c(
        "client_number",
        "tiedenrollment"
        )
      )
  
    # CCS intentionally NOT joined; remains long-form
    
    # dplyr::left_join(
    #   ccs,
    #   by = c(
    #     "client_number",
    #     "tiedenrollment"
    #     )
    #   )
  
  # --- TYPE & SUBTYPE maps ---
  
  # Build TYPE map using unprefixed RP (rp_detect)
  #   - Ensures placed_col matches raw RP column names
  #   - referral_type normalized to snake_case
  type_map <- bcr_build_rp_type_map(
    bcr_referral_type_map,
    rp_detect,
    master_lookup
  )
  
  # Build SUBTYPE map using unprefixed RP (rp_detect)
  #   - Ensures subtype roots match raw RP column names
  #   - referral_type normalized to snake_case
  subtype_map <- bcr_build_rp_subtype_map(
    bcr_referral_subtype_map,
    rp_detect,
    master_lookup
  )
  
  # Filter out subtype rows whose agency_desc_col does not exist in RP
  # (protects pivot from invalid column references)
  if (
    "agency_desc_col" %in% names(
      subtype_map
      )
    ) {
    subtype_map <- subtype_map |>
      dplyr::filter(
        agency_desc_col %in% names(
          rp_detect
          )
        )
  }
  
  # --- Pivot RP long using prefixed RP + maps ---
  # rp_long merges TYPE-level and SUBTYPE-level referrals into a unified
  # long-form table. TYPE rows have referral_subtype = NA; SUBTYPE rows
  # populate referral_subtype, agency_name, and referral_date.
  rp_long <- bcr_pivot_rp_long(
    rp_detect,
    type_map,
    subtype_map
  )
  
  # --- Output bundle ---
  output <- list(
    scd = list(
      presconcerns_one  = presconcerns_one,
      payor_one = payor_one,
      housing_one = housing_one
    ),
    parent_map = parent_map,
    transformed = list(
      ref = ref,
      ic = ic,
      rp = rp,
      type_map = type_map,
      subtype_map = subtype_map,
      rp_long = rp_long,
      ccs = ccs
    ),
    joined_referral_flow = joined
  )
  
  output
}

# ===
# 4. Extract final bcr_full_data ----
#   - Returns the final wide referral_flow table
#   - Maintained as a semantic wrapper to expose the final wide table as
#     bcr_full_data
# ===

extract_bcr_full_data <- function(
  referral_flow
  ) {
  referral_flow$joined_referral_flow
}

# ===
# 5. bcr ETL entry point ----
#   - Loads analytic_fields metadata
#   - Ingests all bcr extracts using metadata-driven loaders
#   - Applies bcr-specific transformations (e.g., pivoting, referral_flow)
#   - Returns a named list of all bcr data objects
#   - Does not write to disk or modify global env objects as the old ETL code
#       did.
# ===
run_bcr_etl <- function(
    analytic_fields,
    bcr_client,
    bcr_provider_placement,
    bcr_pathclient,
    bcr_pathway_docsernos,
    bcr_ref,
    bcr_ic,
    bcr_referrals_placed,
    bcr_presenting_concerns,
    bcr_events,
    bcr_client_counseling_sessions,
    bcr_active_payor_source,
    bcr_all_payor_source,
    bcr_active_housing,
    bcr_all_housing,
    bcr_referral_type_map,
    bcr_referral_subtype_map,
    master_lookup,
    start_date = NULL,
    end_date   = NULL,
    fiscal_system = c(
      "federal",
      "state"
      )
) {
  
  fiscal_system <- match.arg(
    fiscal_system
    )
  
  # 1. Raw bundle
  bcr_raw <- list(
    bcr_client = bcr_client,
    bcr_provider_placement = bcr_provider_placement,
    bcr_pathclient = bcr_pathclient,
    bcr_pathway_docsernos = bcr_pathway_docsernos,
    bcr_ref = bcr_ref,
    bcr_ic = bcr_ic,
    bcr_referrals_placed = bcr_referrals_placed,
    bcr_presenting_concerns = bcr_presenting_concerns,
    bcr_events = bcr_events,
    bcr_client_counseling_sessions = bcr_client_counseling_sessions,
    bcr_active_payor_source = bcr_active_payor_source,
    bcr_all_payor_source = bcr_all_payor_source,
    bcr_active_housing = bcr_active_housing,
    bcr_all_housing = bcr_all_housing
  )
  
  # 2. Pathclient transform
  pathclient <- transform_bcr_pathclient(
    bcr_raw
    )
  bcr_raw$pathclient <- pathclient$joined_pathclient
  
  # 3. Referral flow transform
  referral_flow <- transform_bcr_referral_flow(
    bcr_raw,
    bcr_referral_type_map,
    bcr_referral_subtype_map,
    master_lookup
  )
  
  # 4. Full data extract
  full <- extract_bcr_full_data(
    referral_flow
    )
  
  # 5. Optional subsets
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
  
  # 6. Return ETL object
  list(
    raw = bcr_raw,
    transform = list(
      pathclient = pathclient,
      referral_flow = referral_flow
    ),
    bcr_full_data = full,
    subsets = subsets
  )
}
