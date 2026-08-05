# ===
# setup.R
# Shared setup for FAMCare-Child-Documents ETL + Quarto reporting
# - Loads core libraries
# - Sets global options
# - Sources helper modules (helpers, fiscal_dates, cartography)
# ===

# ===
# 1. Core libraries used across modules ----
# ===

library(here)
library(kableExtra)
library(dplyr)
library(readr)
library(readxl)
library(tidyr)
library(tibble)
library(forcats)
library(stringr)
library(lubridate)
library(purrr)
library(ggplot2)
library(janitor)
library(sf)
library(tigris)

# ===
# 2. Global options ----
# ===

options(
  tigris_use_cache = TRUE,
  tigris_class = "sf"
)

# ===
# 3. Source internal helper modules ----
# 
#    Assume this file lives in etl/ and helpers live in ../R/
# ===

# Determine ETL repo root depending on execution context, which will allow for
# the root directory to be properly sourced within either the parent project's
# working directory or within the famcare-etl-governed directory during
# development. The parent project is expected to define `etl_dir` in its global
# environment before sourcing this setup.R file, which will be used here to set
# the root directory for sourcing helper modules.
get_etl_root <- function() {
  if (
    exists(
      "etl_dir"
    )
  ) {
    # Sourced from a parent project
    etl_dir
  } else {
    # Sourced from within famcare-etl-governed during development
    here::here()
  }
}

root <- get_etl_root()

source(
  file.path(
    root,
    "R",
    "helpers.R"
  )
)
source(
  file.path(
    root,
    "R",
    "fiscal_dates.R"
  )
)
source(
  file.path(
    root,
    "R",
    "cartography.R"
  )
)

source(
  file.path(
    root,
    "R",
    "ccsr.R"
  )
)

source(
  file.path(
    root,
    "R",
    "pipeline_diagnostics.R"
  )
)

source(
  file.path(
    root,
    "R",
    "master_tables.R"
  )
)

# ===
# END OF SETUP
# ===
