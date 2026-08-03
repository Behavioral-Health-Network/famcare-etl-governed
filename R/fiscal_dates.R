# ===
# fiscal_dates.R ----
# Centralized fiscal-year and quarter utilities for all programs.
# Supports:
#   - Federal Fiscal Year (Oct 1 – Sep 30)
#   - State Fiscal Year   (Jul 1 – Jun 30)
#   - Optional lag windows
#   - Rolling comparisons
#   - Quarter/FY labels
#   - Same-period-prior-year logic
# ===

library(lubridate)
library(dplyr)
library(stringr)
library(purrr)

# ===
# 1. Generic helpers ----
# ===

# Generic fiscal quarter
fy_quarter <- function(
  date,
  fiscal_start_month
) {
  lubridate::quarter(
    date,
    fiscal_start = fiscal_start_month
  )
}

# Generic fiscal year (4-digit)
fy_year <- function(
  date,
  fiscal_start_month
) {
  as.integer(
    lubridate::quarter(
      date,
      with_year = TRUE,
      fiscal_start = fiscal_start_month
    )
  )
}

# Generic quarter start
fy_quarter_start <- function(
    date,
    fiscal_start_month
  ) {
  # Determine the fiscal quarter number
  q <- lubridate::quarter(
    date,
    fiscal_start = fiscal_start_month
  )
  
  # Determine the fiscal year
  fy <- fy_year(
    date,
    fiscal_start_month
  )
  
  # Compute the first month of the quarter
  start_month <- (
    (
      q - 1
    ) * 3 + fiscal_start_month - 1
  ) %% 12 + 1
  
  # Compute the year of that month
  start_year <- if (
    start_month < fiscal_start_month
  ) fy else fy - 1
  
  as.Date(
    sprintf(
      "%04d-%02d-01",
      start_year,
      start_month
    )
  )
}

# Generic quarter end
fy_quarter_end <- function(
    date,
    fiscal_start_month
  ) {
  start <- fy_quarter_start(
    date,
    fiscal_start_month
  )
  start %m+% months(
    3
  ) - days(
      1
    )
}

# Generic FY start
fy_start <- function(
  date,
  fiscal_start_month
) {
  fy <- fy_year(
    date,
    fiscal_start_month
  )
  start_year <- fy - 1
  as.Date(
    sprintf(
      "%04d-%02d-01",
      start_year,
      fiscal_start_month
    )
  )
}

# Generic FY end
fy_end <- function(
  date,
  fiscal_start_month
) {
  fy <- fy_year(
    date,
    fiscal_start_month
  )
  end_month <- fiscal_start_month - 1
  if (
    end_month == 0
  ) end_month <- 12
  end_year <- ifelse(
    end_month == 12,
    fy - 1,
    fy
  )
  lubridate::ceiling_date(
    as.Date(
      sprintf(
        "%04d-%02d-01",
        end_year,
        end_month
      )
    ),
    "month"
  ) - lubridate::days(
    1
  )
}

# Generic quarter label
fy_quarter_label <- function(
  date,
  fiscal_start_month
) {
  q <- fy_quarter(
    date,
    fiscal_start_month
  )
  fy <- fy_year(
    date,
    fiscal_start_month
  )
  paste0(
    "Q",
    q,
    " FY",
    fy
  )
}

# Generic same-period-prior-year
same_period_prior_fy <- function(
  date,
  fiscal_start_month
) {
  prior <- date %m-% years(
    1
  )
  fy_quarter_label(
    prior,
    fiscal_start_month
  )
}

# ===
# 2. Federal Fiscal Year (Oct 1 – Sep 30) ----
# ===

federal_fy_quarter <- function(
  date
) {
  fy_quarter(
    date,
    10
  )
}

federal_fy_year <- function(
  date
) {
  fy_year(
    date,
    10
  )
}

federal_quarter_start <- function(
  date
) {
  fy_quarter_start(
    date,
    10
  )
}

federal_quarter_end <- function(
  date
) {
  fy_quarter_end(
    date,
    10
  )
}

federal_fy_start <- function(
  date
) {
  fy_start(
    date,
    10
  )
}

federal_fy_end <- function(
  date
) {
  fy_end(
    date,
    10
  )
}

federal_fy_label <- function(
  date
) {
  fy_quarter_label(
    date,
    10
  )
}

federal_same_period_prior <- function(
  date
) {
  same_period_prior_fy(
    date,
    10
  )
}

# ===
# 3. State Fiscal Year (Jul 1 – Jun 30) ----
# ===

state_fy_quarter <- function(
  date
) {
  fy_quarter(
    date,
    7
  )
}

state_fy_year <- function(
  date
) {
  fy_year(
    date,
    7
  )
}

state_quarter_start <- function(
  date
) {
  fy_quarter_start(
    date,
    7
  )
}

state_quarter_end <- function(
  date
) {
  fy_quarter_end(
    date,
    7
  )
}

state_fy_start <- function(
  date
) {
  fy_start(
    date,
    7
  )
}

state_fy_end <- function(
  date
) {
  fy_end(
    date,
    7
  )
}

state_fy_label <- function(
  date
) {
  fy_quarter_label(
    date,
    7
  )
}

state_same_period_prior <- function(
  date
) {
  same_period_prior_fy(
    date,
    7
  )
}

# ===
# 4. Lag functions (optional) ----
# ===

# Lag by N months (e.g., 3-month lag)
lag_months <- function(
  date,
  n = 3
) {
  date %m-% months(
    n
  )
}

# Lag by one fiscal quarter
lag_quarter <- function(
  date,
  fiscal_system = c(
    "state",
    "federal"
  )
) {
  fiscal_system <- match.arg(
    fiscal_system
  )

  if (
    fiscal_system == "state"
  ) {
    start <- state_quarter_start(
      date
    )
    return(
      start %m-% months(
        3
      )
    )
  }

  if (
    fiscal_system == "federal"
  ) {
    start <- federal_quarter_start(
      date
    )
    return(
      start %m-% months(
        3
      )
    )
  }
}

# ===
# 5. Rolling comparison helpers ----
# ===

# Rolling N fiscal quarters
rolling_quarters <- function(
  date,
  n = 3,
  fiscal_system = c(
    "state",
    "federal"
  )
) {
  fiscal_system <- match.arg(
    fiscal_system
  )

  if (
    fiscal_system == "state"
  ) {
    starts <- purrr::map(
      0:(
        n - 1
      ),
      ~ state_quarter_start(
        date
      ) %m-% months(
        3 * .x
      )
    )
    return(
      starts
    )
  }

  if (
    fiscal_system == "federal"
  ) {
    starts <- purrr::map(
      0:(
        n - 1
      ),
      ~ federal_quarter_start(
        date
      ) %m-% months(
        3 * .x
      )
    )
    return(
      starts
    )
  }
}

# Rolling N fiscal years
rolling_fy_periods <- function(
  date,
  n = 3,
  fiscal_system = c(
    "state",
    "federal"
  )
) {
  fiscal_system <- match.arg(
    fiscal_system
  )

  if (
    fiscal_system == "state"
  ) {
    years <- purrr::map(
      0:(
        n - 1
      ),
      ~ state_fy_year(
        date
      ) - .x
    )
    return(
      years
    )
  }

  if (
    fiscal_system == "federal"
  ) {
    years <- purrr::map(
      0:(
        n - 1
      ),
      ~ federal_fy_year(
        date
      ) - .x
    )
    return(
      years
    )
  }
}

# ===
# 6. Month-year labels (useful for reporting) ----
# ===

month_year_label_full <- function(
  date
) {
  paste0(
    lubridate::month(
      date,
      label = TRUE,
      abbr = FALSE
    ),
    " ",
    lubridate::year(
      date
    )
  )
}

month_year_label_abbrev <- function(
    date
) {
  paste0(
    lubridate::month(
      date,
      label = TRUE,
      abbr = TRUE
    ),
    " ",
    lubridate::year(
      date
    )
  )
}

fiscal_year_month_index <- function(
    date,
    fiscal_system = c(
      "state",
      "federal"
      )
    ) {
  fiscal_system <- match.arg(
    fiscal_system
    )
  
  m <- lubridate::month(
    date
    )
  
  if (
    fiscal_system == "state"
    ) {
    # July = 1, … June = 12
    (
      (
        m - 7
        ) %% 12
      ) + 1
  } else {
    # federal: October = 1, … September = 12
    (
      (
        m - 10
        ) %% 12
      ) + 1
  }
}

order_fiscal_year_labels <- function(
    df,
    label_col,
    date_col,
    fiscal_system = c(
      "state",
      "federal"
      )
    ) {
  fiscal_system <- match.arg(
    fiscal_system
    )
  
  df |>
    dplyr::mutate(
      fiscal_month_index = fiscal_year_month_index(
        {{ date_col }},
        fiscal_system
        )
    ) |>
    dplyr::arrange(
      fiscal_month_index
      ) |>
    dplyr::mutate(
      {{ label_col }} := factor(
        {{ label_col }},
        levels = unique(
          {{ label_col }}
          )
      )
    ) |>
    dplyr::select(
      -fiscal_month_index
      )
}

# ===
# 7. General reporting period helper ----
# ===

reporting_period <- function(
  date = Sys.Date(),
  system = c(
    "state",
    "federal"
  ),
  period = c(
    "month",
    "quarter",
    "year"
  )
) {
  system <- match.arg(
    system
  )
  period <- match.arg(
    period
  )

  # MONTH ---------------------------------------------------------------
  if (
    period == "month"
  ) {
    start <- lubridate::floor_date(
      date,
      "month"
    )
    end <- lubridate::ceiling_date(
      date,
      "month"
    ) - lubridate::days(
      1
    )
    return(
      list(
        start = start,
        end = end
      )
    )
  }

  # QUARTER -------------------------------------------------------------
  if (
    period == "quarter"
  ) {
    if (
      system == "state"
    ) {
      start <- state_quarter_start(
        date
      )
      end   <- state_quarter_end(
        date
      )
    } else {
      start <- federal_quarter_start(
        date
      )
      end   <- federal_quarter_end(
        date
      )
    }
    return(
      list(
        start = start,
        end = end
      )
    )
  }

  # YEAR (full fiscal year) --------------------------------------------
  if (
    period == "year"
  ) {
    if (
      system == "state"
    ) {
      start <- state_fy_start(
        date
      )
      end   <- state_fy_end(
        date
      )
    } else {
      start <- federal_fy_start(
        date
      )
      end   <- federal_fy_end(
        date
      )
    }
    return(
      list(
        start = start,
        end = end
      )
    )
  }
}

# ===
# END OF MODULE
# ===
