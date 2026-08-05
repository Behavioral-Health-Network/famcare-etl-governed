# Function to list all master tables keyed by sheet name. Exclude sheet
# tables_changelog.
load_master_tables <- function(
  path = "P:/DATA/LUTs/FC_Master_Tables.xlsx"
) {
 sheets <- readxl::excel_sheets(
  path
  )
 sheets <- setdiff(
  sheets,
  "tables_changelog"
  )


 
 purrr::imap(
  purrr::set_names(
   sheets),
  ~ readxl::read_excel(
   path,
   sheet = .x
   ) |>
   janitor::clean_names() |>
   mutate(
    master_table_name = janitor::make_clean_names(
     .y
     )
    ) |> 
   mutate(
    across(
     everything(),
     as.character
     )
    )
 )
}

# Find all master tables into a unified lookup
bind_master_tables <- function(
  master_tables
  ) {
 purrr::list_rbind(
  master_tables
  )
}

# Normalize descriptions
normalize_subtype_names <- function(
  df
  ) {
 df |>
  mutate(
   subtype = janitor::make_clean_names(
    description
    )
  )
}

# Convenience wrapper for use in ETL scripts
get_master_lookup <- function(
  path = "P:/DATA/LUTs/FC_Master_Tables.xlsx"
) {
 master_tables <- load_master_tables(
  path
  )
 lookup <- bind_master_tables(
  master_tables
  )
 normalize_subtype_names(
  lookup
  )
}

