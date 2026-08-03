# ===
# helpers.R
# General-purpose helper functions used across ETL and reporting.
# ===

library(dplyr)
library(stringr)
library(lubridate)
library(rlang)

# ===
# 1. Logical helpers ----
# ===

`%nin%` <- function(
  x,
  y
) {
  !(
    x %in% y
  )
}

# This is intended to be used with select() to drop empty columns. This is
# redundant with fn drop_empty_cols, but it is used throughout etl files.
# Phase out in favor of drop_empty_cols. Retain not_all_na to avoid breaking
# existing code, but prefer drop_empty_cols in new code.
not_all_na <- function(
  x
) {
  !all(
    is.na(
      x
    )
  )
}

coalesce_na <- function(
  x,
  value
) {

  dplyr::coalesce(
    x,
    value
  )
}

# ===
# 2. String helpers ----
# ===

trim_ws <- function(
  x
) {
  stringr::str_trim(
    x,
    side = "both"
  )
}

str_empty_to_na <- function(
  x
) {
  ifelse(
    stringr::str_trim(
      x
    ) == "",
    NA,
    x
  )
}

str_to_title_case <- function(
  x
) {
  stringr::str_to_title(
    x
  )
}

# Optional: BHN-specific name cleaning
clean_names_bhn <- function(
  df
) {
  df |>
    dplyr::rename_with(
      ~ stringr::str_replace_all(
        .,
        "[^A-Za-z0-9_]",
        "_"
      )
    ) |>
    dplyr::rename_with(
      ~ stringr::str_replace_all(
        .,
        "__+",
        "_"
      )
    ) |>
    dplyr::rename_with(
      ~ stringr::str_to_lower(
        .
      )
    )
}

# ===
# 3. Numeric helpers ----
# ===

pct <- function(
  x,
  digits = 1
) {
  round(
    x * 100,
    digits
  )
}

safe_divide <- function(
  numerator,
  denominator,
  default = NA_real_
) {
  ifelse(
    denominator == 0 | is.na(
      denominator
    ),
    default,
    numerator / denominator
  )
}

round2 <- function(
  x,
  digits = 0
) {
  # Round half-up instead of banker's rounding
  posneg <- sign(
    x
  )
  z <- abs(
    x
  ) * 10^digits
  z <- z + 0.5
  z <- trunc(
    z
  )
  z <- z / 10^digits
  z * posneg
}

# ===
# 4. Date helpers (non-fiscal) ----
# ===

is_valid_date <- function(
  x
) {
  !is.na(
    lubridate::ymd(
      x
    )
  ) | !is.na(
    lubridate::mdy(
      x
    )
  )
}

parse_date_safe <- function(
  x
) {
  # Try ymd, then mdy
  y <- suppressWarnings(
    lubridate::ymd(
      x
    )
  )
  m <- suppressWarnings(
    lubridate::mdy(
      x
    )
  )
  dplyr::coalesce(
    y,
    m
  )
}

parse_datetime_safe <- function(
    x
    ) {
  suppressWarnings(
    lubridate::parse_date_time(
      x,
      orders = c(
        "Ymd HMS",
        "Ymd HM",
        "Ymd",
        "Y/m/d HMS",
        "Y/m/d HM",
        "Y/m/d",
        "mdY HMS",
        "mdY HM",
        "mdY",
        "m/d/Y HMS",
        "m/d/Y HM",
        "m/d/Y"
      ),
      tz = "UTC"
    )
  )
}

first_day <- function(
  date
) {
  lubridate::floor_date(
    date,
    "month"
  )
}

last_day <- function(
  date
) {
  lubridate::ceiling_date(
    date,
    "month"
  ) - lubridate::days(
    1
  )
}

age_at <- function(
  dob,
  ref_date = Sys.Date()
) {
  ifelse(
    is.na(
      dob
    ),
    NA_real_,
    floor(
      lubridate::interval(
        dob,
        ref_date
      ) / lubridate::years(
        1
      )
    )
  )
}

# ===
# 5. Data-frame helpers ----
# ===

drop_empty_rows <- function(
  df
) {
  df |>
    dplyr::filter(
      apply(
        .,
        1,
        not_all_na
      )
    )
}

drop_empty_cols <- function(
  df
) {
  df[, colSums(!is.na(df)) > 0, drop = FALSE]
}

rename_if_exists <- function(
  df,
  old,
  new
) {
  if (
    old %in% names(
      df
    )
  ) {
    df |>
      dplyr::rename(
        !!new := !!sym(
          old
        )
      )
  } else df
}

select_if_exists <- function(
  df,
  ...
) {
  cols <- quos(
    ...
  )
  cols <- cols[names(cols) %in% names(df)]
  df |>
    dplyr::select(
      !!!cols
    )
}

left_join_safe <- function(
  x,
  y,
  by
) {
  missing_keys <- dplyr::setdiff(
    by,
    names(
      x
    )
  )
  if (
    length(
      missing_keys
    ) > 0
  ) {
    warning(
      "Join keys missing from left table: ",
      paste(
        missing_keys,
        collapse = ", "
      )
    )
  }
  dplyr::left_join(
    x,
    y,
    by = by
  )
}

distinct_across <- function(
  df,
  ...
) {
  df |>
    dplyr::distinct(
      dplyr::across(
        c(
          ...
        )
      ),
      .keep_all = TRUE
    )
}

# ===
# 6. Factor and labeling helpers ----
# ===

ordered_quarter_factor <- function(
  q
) {
  factor(
    q,
    levels = paste0(
      "Q",
      1:4
    ),
    ordered = TRUE
   )
}

ordered_month_factor <- function(
  m
) {
  factor(
    m,
    levels = month.abb,
    ordered = TRUE
  )
}

label_yesno <- function(
  x
) {
  dplyr::case_when(
    x %in% c(
      1,
      TRUE,
      "1",
      "Y",
      "Yes"
    ) ~ "Yes",
    x %in% c(
      0,
      FALSE,
      "0",
      "N",
      "No"
    ) ~ "No",
    TRUE ~ NA_character_
  )
}

# ===
# 7. Column type metadata helpers ----
# ===

# Mapping table: SQL Server data types → readr col_types
sql_to_readr <- tibble::tribble(
  ~sql_type,       ~readr_type,
  "varchar",       "c",
  "nvarchar",      "c",
  "char",          "c",
  "text",          "c",
  "ntext",         "c",
  "int",           "n",
  "smallint",      "n",
  "tinyint",       "n",
  "decimal",       "n",
  "numeric",       "n",
  "float",         "n",
  "real",          "n",
  "bit",           "l",
  "date",          "D",
  "datetime",      "D",
  "datetime2",     "D",
  "smalldatetime", "D",
  "time",          "t",
  # Identifiers that must be read as character to avoid precision loss
  "bigint",        "c"
)

# Generate readr col_types string based on metadata
generate_col_types <- function(
  colnames,
  metadata,
  mapping = sql_to_readr
) {

  # 1. Join metadata to mapping table
  meta <- metadata |>
    dplyr::mutate(
      data_type = tolower(
        trimws(
          data_type
        )
      ),
      semantic_override = na_if(
        semantic_override, "NA"
      )
    ) %>%
    dplyr::left_join(
      mapping,
      by = c(
        "data_type" = "sql_type"
      )
    )

  # Fail fast if any data_type is unmapped
  bad <- meta |>
    dplyr::filter(
      is.na(
        readr_type
      )
    )

  if (
      nrow(
        bad
      ) > 0) {
    stop(
      "Unrecognized data_type values for fields: ",
      paste(
        bad$field_name,
        collapse = ", "
      ),
      "\nTheir data_type values were: ",
      paste(
        bad$data_type,
        collapse = ", "
      )
    )
  }

  # 2. Apply semantic overrides if present
  meta <- meta %>%
    dplyr::mutate(
      final_type = dplyr::coalesce(
        semantic_override,
        readr_type
      )
    )

  # fail fast if any column has no resolved type
  if (
    any(
      is.na(
        meta$final_type
      )
    )
  ) {
    bad <- meta$field_name[is.na(meta$final_type)]
    stop(
      "Unresolved column types for fields: ",
      paste(
        bad,
        collapse = ", "
      )
    )
  }


  # 3. Ensure all columns in the CSV have metadata
  missing <- setdiff(
    colnames,
    meta$field_name
  )
  if (
    length(
      missing
    ) > 0
  ) {
    stop(
      "Missing metadata for fields: ",
      paste(
        missing,
        collapse = ", "
      )
    )
  }

  # 4. Reorder metadata to match CSV column order
  meta <- meta |>
    dplyr::slice(
      match(
        colnames,
        field_name
      )
    )

  # 5. Collapse into a single col_types string
  paste0(
    meta$final_type,
    collapse = ""
  )
}

# ===
# 8. make_path helper ----
# ===
p_path <- "P:/DATA/Data Files/"

make_path <- function(
  ...,
  base = p_path
) {
  file.path(
    base,
    ...,
    fsep = "/"
  )
}

# ===
# 9. make_lut_path helper ----
# ===
p_lut_path <- "P:/DATA/LUTs/"

make_lut_path <- function(
    ...,
    base = p_lut_path
    ) {
  file.path(
    base,
    ...,
    fsep = "/"
    )
}

# ===
# 10. Latest-file make path helper when rolling datetimes are appended ----
# ===

# p_path is defined in make_path() above.

make_latest_file_path <- function(
  dir,
  pattern = "\\.(csv|xlsx|xls)$",
  base = p_path
) {
  full_dir <- file.path(
    base,
    dir,
    fsep = "/"
  )

  files <- list.files(
    path = full_dir,
    pattern = pattern,
    full.names = TRUE,
    ignore.case = TRUE
  )

  if (
    length(
      files
      ) == 0) {
    stop(
      "No files found in directory: ",
      full_dir,
      "\nPattern: ",
      pattern
    )
  }

  # Select newest by modification time
  latest <- files[which.max(file.info(files)$mtime)]
  latest
}

# ===
# 11. Read all files and bind function ----
# ===

# p_path is defined in make_path() above.

make_all_file_paths <- function(
    path,
    pattern = "\\.(csv|xlsx|xls)$",
    base = p_path
) {
  full_dir <- file.path(
    base,
    path,
    fsep = "/"
    )
  
  files <- list.files(
    path = full_dir,
    pattern = pattern,
    full.names = TRUE,
    ignore.case = TRUE
  )
  
  if (
    length(
      files
      ) == 0
    ) {
    stop(
      "No files found in directory: ",
      full_dir,
      "\nPattern: ",
      pattern
      )
  }
  
  files
}

# ===
# 12. Encoding detection function ----
# ===
detect_encoding <- function(
    path
    ) {
  raw <- readr::read_file_raw(
    path
    )
  enc <- stringi::stri_enc_detect(
    raw
    )[[1]]
  enc$Encoding[which.max(enc$Confidence)]
}

# ===
# 13. Normalize field name function ----
# ===
normalize_field_name <- function(
    x
  ) {
  x |>
    stringr::str_to_lower() |>
    stringr::str_replace_all(
      "[^a-z0-9]+",
      "_"
    ) |>
    stringr::str_replace_all(
      "_+",
      "_"
    ) |>
    stringr::str_replace(
      "^_|_$",
      ""
    )
}

# ===
# 14. Read FAMCare CSV wrapper ----
# ===
read_famcare_csv <- function(
  path,
  metadata,
  na = c(
    "",
    " "
    ),
  ...
) {

  # 1. Read header only (still uses read_csv() here because the header is safe)
  nms <- names(
    readr::read_delim(
      path,
      delim = metadata$delimiter[[1]],
      n_max = 0,
      show_col_types = FALSE
    )
  ) |>
    tolower()

  # 2. normalize field names so that the header names match the extracts
  nms <- normalize_field_name(
    nms
  )
  
  # 3. Generate governed col_types from metadata slice
  col_types <- generate_col_types(
    colnames = nms,
    metadata = metadata
  )

  # 4. Force all date columns to character
  col_types <- chartr(
    "D",
    "c",
    col_types
    )
  
  # 5a. Get delimiter from metadata (indicating the delimiter type in a file is
  # now a governed decision based on the value of delimiter column from
  # analytic_fields table.)
  delim <- metadata$delimiter[[1]]

  # 5b. Determine encoding
  fname <- basename(
    path
    )
  is_famcare <- grepl(
    "^Q_",
    fname,
    ignore.case = TRUE
    )
  
  encoding <- if (
    is_famcare
    ) {
    "Windows-1252"
  } else {
    detect_encoding(
      path
      )
  }

  # 6. Read full dataset with patched col_types. This uses read_delim(), which
  # includes a delim argument.
  df <- readr::read_delim(
    path,
    delim = delim,
    col_types = col_types,
    locale = readr::locale(
      encoding = encoding
      ),
    na = na,
    show_col_types = FALSE,
    ...
  )
  
  # Normalize all character columns to UTF-8
  df <- df |>
    dplyr::mutate(
      dplyr::across(
        where(
          is.character
          ),
        ~ iconv(
          .x,
          from = encoding,
          to = "UTF-8",
          sub = ""
          )
      )
    ) |>
    janitor::clean_names()
  
  df
  
}

# ===
# 15. Read FAMCare Excel wrapper ----
# ===
read_famcare_excel <- function(
  path,
  metadata
) {

  df <- readxl::read_excel(
    path
    )

  df <- df |>
    janitor::clean_names()

  df
}

# ===
# 16. Load analytic_fields metadata from governance workbook ----
#   - Reads Excel file defined by METADATA_GOVERNANCE_DIR
#   - Cleans column names
#   - Used by all program ETL scripts
# ===


load_analytic_fields <- function() {
  metadata_dir <- Sys.getenv(
    "METADATA_GOVERNANCE_DIR"
  )

  if (
    metadata_dir == ""
  ) {
    stop(
      "Environment variable METADATA_GOVERNANCE_DIR is not set. ",
      "Please define it in your .Renviron."
    )
  }

  path <- file.path(
    metadata_dir,
    "Metadata_Governance.xlsx"
  )

  analytic_fields <- readxl::read_excel(
    path,
    sheet = "analytic_fields"
  ) |>
    janitor::clean_names()

  # normalize field_name
  analytic_fields <- analytic_fields |>
    dplyr::mutate(
      field_name = normalize_field_name(
        view_field_name
      )
    )

  analytic_fields
}

# ===
# 17. Normalize CSV filename → asset_id used in analytic_fields ----
# Example:
#   Q_PROVIDERPLACEMENT_BHN.csv → q-providerplacement-bhn
# ===

extract_asset_id <- function(
    path,
    analytic_fields
    ) {
  
  filename <- basename(
    path
    )
  
  # 1. Check metadata patterns first
  if (
    "source_pattern" %in% names(
      analytic_fields
      )
    ) {
    # Step 1: remove NA and blank patterns BEFORE str_detect(), which requires a
    #   character vector
    patterns <- analytic_fields |> 
      dplyr::filter(
        !is.na(
          source_pattern
        ),
        source_pattern != "",
        !stringr::str_to_lower(
          source_pattern
          ) %in% c(
            "na"
            ),
        !is.na(
          asset_id
        ),
        asset_id != ""
      ) |> 
      dplyr::distinct(
        asset_id,
        source_pattern
      )
    
    # Step 2: now safely run str_detect(). Because stringr::regex() is not
    # vectorized, it will return only the first match. Given that there are
    # multiple rows to match, the next condition of if(nrow(matched) == 1) can
    # never be true if using only stringr::regex(). Therefore, use
    # purrr::map_lgl() to force one regex per row, one match per row, with no
    # vector recycling and no silent dropping resulting in NA on rows after the
    # first regex match.
    matched <- patterns |>
      dplyr::filter(
        purrr::map_lgl(
          source_pattern,
          ~ stringr::str_detect(
            filename,
            stringr::regex(
              .x,
              ignore_case = TRUE
              )
          )
        )
      )
    
    
    if (
      nrow(
        matched
        ) == 1
      ) {
      return(
        matched$asset_id
        )
    }
  }

  # 2. Fallback: normalize filename → asset_id
  basename(
    path
    ) |>
    tools::file_path_sans_ext() |>
    stringr::str_to_lower() |>
    stringr::str_replace_all(
      "_",
      "-"
      ) |>
    stringr::str_replace(
      "-\\d{4}.*$",
      ""
      ) |>
    trimws()

}

# ===
# 18. Slice analytic_fields for a specific asset_id ----
#   - Ensures each extract uses the correct metadata subset
# ===

slice_metadata <- function(
  analytic_fields,
  asset_id
) {
  analytic_fields |>
    dplyr::filter(
      !is.na(
        asset_id
      ),
      asset_id == !!asset_id
    ) |>
    dplyr::mutate(
      field_name = janitor::make_clean_names(
        view_field_name
      )
    )
}

# ===
# 19. Generic metadata-driven ingestion for any FAMCare extract ----
#   - Normalizes asset_id
#   - Slices metadata from <- table
#   - Reads CSV using read_famcare_csv()
#   - Applies metadata-driven renaming
# ===

load_famcare_extract <- function(
  path,
  analytic_fields
) {

  asset_id <- extract_asset_id(
    path,
    analytic_fields
  )

  metadata <- slice_metadata(
    analytic_fields,
    asset_id
  )

  # print("DEBUG: asset_id")
  # print(asset_id)

  # print("DEBUG: metadata slice")
  # print(metadata %>% select(asset_id, view_field_name))

  if (
    nrow(
      metadata
    ) == 0
  ) {
    stop(
      "No metadata found for asset_id: ",
      asset_id
    )
  }

  # a. Detect file extension ----
  ext <- tolower(
    tools::file_ext(
      path
    )
  )

  # b. Dispatch to appropriate reader for ingestion ----
  df <- switch(
    ext,
    "csv" = read_famcare_csv(
      path = path,
      metadata = metadata
    ),
    "xlsx" = read_famcare_excel(
      path = path,
      metadata = metadata
    ),
    "xls"  = read_famcare_excel(
      path = path,
      metadata = metadata
    ),
    stop(
      "Unsupported file extension: ",
      ext
    )
  )

  # c. Identify governed date columns from metadata in analytic_fields ----
  date_cols <- metadata |>
    dplyr::filter(
      data_type == "date"
    ) |>
    dplyr::pull(
      field_name
    ) |>
    tolower()

  # d. Apply safe date parsing to all governed date columns ----
  if (
    length(
      date_cols
    ) > 0
  ) {
    df <- df |>
      dplyr::mutate(
        dplyr::across(
          tidyselect::all_of(
            date_cols
          ),
          parse_date_safe
        )
      )
  }

  # e. Identify governed datetime columns from metadata in analytic_fields ----
  datetime_cols <- metadata |>
    dplyr::filter(
      data_type == "datetime"
      ) |>
    dplyr::pull(
      field_name
      ) |>
    tolower()
  
  # f. Apply safe datetime parsing to all governed datetime columns ----
  if (
    length(
      datetime_cols
      ) > 0
    ) {
    df <- df |>
      dplyr::mutate(
        dplyr::across(
          tidyselect::all_of(
            datetime_cols
            ),
          parse_datetime_safe
        )
      )
  }
  
  df
}

# ===
# 20. Metadata-driven ingestion for EXT ETO Excel extracts ----
#   - Uses analytic_fields
#   - Normalizes column headers first
#   - Applies governed data types
#   - Applies governed date/datetime parsing
#   - Performs NO FAMCare-specific transformations
# ===

load_ext_eto_extract <- function(
    path,
    analytic_fields
) {
  
  asset_id <- extract_asset_id(
    path,
    analytic_fields
  )
  
  metadata <- slice_metadata(
    analytic_fields,
    asset_id
  )
  
  if (
    nrow(
      metadata
      ) == 0
    ) {
    stop(
      "No metadata found for asset_id: ",
      asset_id
      )
  }
  
  # a. read header only, normalize names ---
  header <- names(
    readr::read_csv(
      path,
      n_max = 0,
      show_col_types = FALSE
    )
  )
  
  header_norm <- normalize_field_name(
    header
    )
  
  # b. Generate governed col_types using normalized header ---
  col_types <- generate_col_types(
    colnames = header_norm,
    metadata = metadata
  )
  
  # c. force all governed date/datetime columns to character ---
  col_types <- chartr(
    "D",
    "c",
    col_types
    )
  
  # d. Read full CSV using governed types ---
  df <- readr::read_csv(
    file = path,
    col_types = col_types
  )
  
  # e. Clean names (same as FAMCare asset ingestion) ---
  df <- df |> janitor::clean_names()
  
  # f. Governed date columns ---
  date_cols <- metadata |>
    dplyr::filter(
      data_type == "date"
      ) |>
    dplyr::pull(
      field_name
      ) |>
    tolower()
  
  if (
    length(
      date_cols
      ) > 0
    ) {
    df <- df |>
      dplyr::mutate(
        dplyr::across(
          tidyselect::all_of(
            date_cols
            ),
          parse_date_safe
        )
      )
  }
  
  # g. Governed datetime columns ---
  datetime_cols <- metadata |>
    dplyr::filter(
      data_type == "datetime"
      ) |>
    dplyr::pull(
      field_name
      ) |>
    tolower()
  
  if (
    length(
      datetime_cols
      ) > 0
    ) {
    df <- df |>
      dplyr::mutate(
        dplyr::across(
          tidyselect::all_of(
            datetime_cols
            ),
          parse_datetime_safe
        )
      )
  }
  
  df
}


# ===
# 21. Program-agnostic data subset function for use in parent projects ----
#   - Fiscal period-neutral (meaning federal or state fiscal systems)
# ===

build_subsets <- function(
  full_data,
  start_date,
  end_date,
  fiscal_system = c(
    "federal",
    "state"
  )
) {

  fiscal_system <- match.arg(
    fiscal_system
  )

  df <- full_data |>
    dplyr::mutate(
      in_period =
        enrollment_starting_date >= start_date &
        enrollment_starting_date <= end_date,

      dismissed_in_period =
        !is.na(
          enrollment_ending_date
        ) &
        enrollment_ending_date >= start_date &
        enrollment_ending_date <= end_date
    )
  
    list(
      initiated_within_period = dplyr::filter(
        df,
        in_period
      ),
      dismissed_within_period = dplyr::filter(
        df,
        dismissed_in_period
      )
    )
}

# ===
# 22. Complex Care data subset function for use in parent projects ----
#   - Fiscal period-neutral (meaning federal or state fiscal systems)
# ===
build_complex_care_subsets <- function(
    full_data,
    start_date,
    end_date,
    fiscal_system = c(
      "federal",
      "state"
    )
) {
  
  fiscal_system <- match.arg(
    fiscal_system
  )
  
  base <- build_subsets(
    full_data = full_data,
    start_date = start_date,
    end_date = end_date,
    fiscal_system = fiscal_system
  )
  
  df <- full_data
  
  still_active <- function(
    .data
  ) {
    is.na(
      .data$enrollment_ending_date
    ) | 
      .data$enrollment_ending_date >= end_date
  }
  
  list(
    # keep the base subsets
    initiated_within_period  = base$initiated_within_period,
    dismissed_within_period  = base$dismissed_within_period,
    
    # complex care-specific subsets
    active_selected_not_enrolled_within_period =
      df |>
      dplyr::filter(
        # Not enrolled OR enrolled after period
        is.na(
          benchmarks_enrollment_date
          ) |
          benchmarks_enrollment_date >= end_date,
        # Selected (roster added)
        !is.na(
          roster_added_cohort_date
          ),
        roster_added_cohort_date <= end_date,
        # Still active
        still_active(
          df
        )
      ),
    
    active_enrolled_not_admitted_within_period =
      df |>
      dplyr::filter(
        # Enrolled
        !is.na(
          benchmarks_enrollment_date
          ),
        # Not admitted OR admitted after period
        is.na(
          benchmarks_complete_admission_service_date
          ) |
          benchmarks_complete_admission_service_date >= end_date,
        # Still active
        still_active(
          df
        )
      ),
    
    active_admitted_no_tx_team_within_period =
      df |>
      dplyr::filter(
        # Admitted
        !is.na(
          benchmarks_complete_admission_service_date
          ),
        # No treatment team OR assigned after period
        is.na(
          benchmarks_treatment_team_assignment_date
          ) |
          benchmarks_treatment_team_assignment_date >= end_date,
        # Still active
        still_active(
          df
        )
      )
  )
}

# ===
# 23. Diagnostic helper functions ----
# ===

# Adds function to ensure that <program>_pathclient and <program>_full_data both
# have one row per (client_number, tiedenrollment)
check_enrollment_keys <- function(
  pathclient,
  full_data
) {

  pc_dupes <- pathclient |>
    dplyr::count(
      client_number,
      tiedenrollment
    ) |>
    dplyr::filter(
      n > 1
    )

  fd_dupes <- full_data |>
    dplyr::count(
      client_number,
      tiedenrollment
    ) |>
    dplyr::filter(
      n > 1
    )

  list(
    pathclient_duplicates = pc_dupes,
    full_data_duplicates = fd_dupes
  )
}

# Adds function to check whether every parent_docserno in active SCD summation
# tibbles matches at least one parent form docserno in the referral flow. This
# is a supplement to exception reports.
check_scd_parent_matches <- function(
  referral_flow,
  scd_tables,
  parent_form_cols
) {

  parent_forms <- referral_flow |>
    dplyr::select(
      all_of(
        parent_form_cols
      )
    ) |> 
    tidyr::pivot_longer(
      everything(),
      values_to = "docserno"
    ) |> 
    dplyr::filter(
      !is.na(
        docserno
      )
    ) |>
    dplyr::distinct(
      docserno
    )

  check_table <- function(
    df,
    name
  ) {
    df |>
      dplyr::select(
        parent_docserno
      ) |>
      dplyr::filter(
        !is.na(
          parent_docserno
        )
      ) |>
      dplyr::anti_join(
        parent_forms,
        by = c(
          "parent_docserno" = "docserno"
        )
      ) |>
      dplyr::mutate(
        source_table = name
      )
  }

  purrr::imap(
    scd_tables,
    check_table
  ) |> 
    list_rbind()
}

# A function that counts how many enrollments have each form completed.
summarise_form_completion <- function(
  full_data,
  form_prefixes
  ) {

  full_data |>
    dplyr::summarise(
      dplyr::across(
        tidyselect::starts_with(
          form_prefixes
        ),
        ~sum(
          !is.na(
            .x
          )
        ),
        .names = "n_{.col}"
      )
    )
}

# A function to show how many enrollments have active summation (payor source,
# housing status, intake status, etc.)
summarise_scd_coverage <- function(
    full,
    scd_prefixes
    ) {
  full |>
    summarise(
      across(
        starts_with(
          scd_prefixes
          ),
        ~ sum(
          !is.na(
            .x
            )
          ),
        .names = "n_{.col}"
      )
    )
}

# A function that ensures that all form columns are correctly prefixed.
check_column_prefixes <- function(
  full_data,
  allowed_prefixes
  ) {

  # allowed_prefixes <- c(
  #   "ref_",
  #   "ic_",
  #   "twow_",
  #   "thirtyd_",
  #   "threem_",
  #   "sixm_",
  #   "reengage_",
  #   "intake_",
  #   "payor_",
  #   "housing_",
  #   "pwy_",
  #   "enrollment_",
  #   "client_",
  #   "tiedenrollment"
  # )

  bad_cols <- names(
    full_data
    )[
    !purrr::map_lgl(
      names(
        full_data
        ),
      ~ any(
        startsWith(
          .x,
          allowed_prefixes
        )
      )
    )
  ]

  tibble::tibble(
    column = bad_cols
    )
}

# A function to ensure that no column names collide after prefixing.
check_no_duplicate_columns <- function(
  full_data
  ) {

  tibble::tibble(
    column = names(
      full_data
    )
  ) |>
    dplyr::count(
      column
    ) |>
    dplyr::filter(
      n > 1
    )
}

# A function to ensure that, if an SCD summation record is attached, the
# corresponding parent form exists. This supplements existing exception reports
# for orphan summation records.
check_parent_form_alignment <- function(
  full_data,
  scd_parent_cols,
  form_docserno_cols
  ) {

  purrr::map(
    scd_parent_cols,
    function(
      parent_col
      ) {
      tibble::tibble(
        parent_col = parent_col,
        n_without_parent =
          full_data |>
          dplyr::filter(
            !is.na(
              .data[[parent_col]]
              )
            ) |>
          dplyr::filter(
            dplyr::if_all(
              all_of(
                form_docserno_cols
                ),
              is.na
              )
          ) |>
          nrow()
      )
    }
  ) |> list_rbind()
}  

# ===
# 24. VPN / shared drive check ----
# ===

check_vpn <- function(
    path = "P:/DATA"
    ) {
  if (
    !dir.exists(
      path
      )
    ) {
    stop(
      "Shared drive not found at: ",
      path,
      "\nIs your VPN connected and the P: drive mounted?"
    )
  }
  invisible(
    TRUE
    )
}

# ===
# END OF MODULE
# ===
