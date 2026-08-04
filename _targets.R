# ===
# famcare-etl-governed: Unified ETL Pipeline ----
# ===

library(targets)
library(tarchetypes)

# ===
# Load shared setup and program ETL scripts ----
# ===
source("etl/setup.R")          # loads helpers.R, fiscal_dates.R, cartography.R
source("etl/epicc.R")          # epicc_paths, run_epicc_etl()
source("etl/bcr.R")            # bcr_paths, run_bcr_etl()
source("etl/ere.R")            # ere_paths, run_ere_etl()
source("etl/yere.R")           # yere_paths, run_yere_etl()
source("etl/complex_care.R")   # complex_care_paths, run_complex_care_etl()

# ===
# Global options ----
# ===
targets::tar_option_set(
 packages = c(
  "dplyr",
  "readr",
  "purrr",
  "tidyr",
  "lubridate",
  "stringr"
 )
)

# ===
# PIPELINE ----
# ===

list(

 # ===
 # VPN Check ----
 # ===
 
 targets::tar_target(
  vpn_check,
  { 
   check_vpn(
    "P:/DATA"
   );
   TRUE
   }
 ),

 # ===
 # Metadata ----
 # ===
 
 targets::tar_target(
  metadata_workbook,
  file.path(
    Sys.getenv(
      "METADATA_GOVERNANCE_DIR"
      ),
    "Metadata_Governance.xlsx"
    ),
  format = "file"
 ),
 
 # 1. Extract analytic_fields sheet from Excel into an in-memory tibble.
 #   Force readxl::read_excel() to apply the correct col_types and not guess so
 #   it does not class source_pattern as logical due to blank rows. Add
 #   additional col_types in order as new columns are added.
 targets::tar_target(
   analytic_fields_raw,
   readxl::read_excel(
     metadata_workbook,
     sheet = "analytic_fields",
     col_types = c(
       "text",  # program_scope
       "text",  # program
       "text",  # asset_id
       "text",  # source_pattern
       "text",  # delimiter (",", "|", "\t", ";" as relevant for source file)
       "text",  # view_field_name
       "text",  # description
       "text",  # variable_name
       "text",  # defined_in_r_object
       "text",  # data_type
       "text",  # semantic_override
       "logical", # is_derived_field
       "logical", # is_description_field
       "logical", # is_pivot_field
       "text",    # pivot_parent
       "text"     # audit_notes
     )
   )
 ),

 # 2. Write that tibble to a persistent CSV inside _targets/
 targets::tar_target(
  analytic_fields_csv,
  {
   csv_path <- file.path(
     "_targets",
     "metadata",
     "analytic_fields.csv"
     )
   
   dir.create(
     dirname(
       csv_path
       ),
     recursive = TRUE,
     showWarnings = FALSE
     )
   
   readr::write_csv(
     analytic_fields_raw,
     csv_path
     )
   
   csv_path
  },
  format = "file",
  cue = targets::tar_cue(
    file = TRUE
    )
 ),
 
 # 3. Read the CSV back in with explicit col_types
 targets::tar_target(
  analytic_fields,
  readr::read_delim(
   analytic_fields_csv,
   delim = ",",
   col_types = readr::cols(
     .default = readr::col_character(),
     is_derived_field = readr::col_logical(),
      is_description_field = readr::col_logical(),
      is_pivot_field = readr::col_logical()
   ),
   escape_backslash = FALSE,
   escape_double = FALSE,
   trim_ws = TRUE
  ) |>
   janitor::clean_names() |>
   dplyr::mutate(
    field_name = janitor::make_clean_names(
      view_field_name
      )
   )
 ),
 
 # ===
 # Cartography targets ----
 # ===
 
 targets::tar_target(
  cartography_bundle,
  {
    vpn_check
    build_cartography_bundle()
  },
 ),
 
 # ===
 # CCSR diagnosis LUT targets ----
 # ===
 
 targets::tar_target(
  ccsr_dx_lut,
  {
    vpn_check
    build_ccsr_dx_lut()
  }
 ),
 
 # ===
 # PCC notification facilities LUT target ----
 # ===
 
 tar_target(
   complex_care_ext_pcc_facility_lut_file,
   {
     vpn_check
     return(
       complex_care_paths$complex_care_ext_pcc_facility_lut
       )
   },
   format = "file"
 ),
 
 # ===
 # Add extract FILE targets ----
 # ===
 
 ## BCR ----
 
 targets::tar_target(
   bcr_provider_placement_file,
   {
     vpn_check
     bcr_paths$bcr_provider_placement
   },
   format = "file"
 ),
 
 targets::tar_target(
   bcr_pathclient_file,
   {
     vpn_check
     bcr_paths$bcr_pathclient
   },
   format = "file"
 ),
 
 targets::tar_target(
   bcr_pathway_docsernos_file,
   {
     vpn_check
     bcr_paths$bcr_pathway_docsernos
   },
   format = "file"
 ),
 
 targets::tar_target(
   bcr_client_file,
   {
     vpn_check
     bcr_paths$bcr_client
   },
   format = "file"
 ),
 
 targets::tar_target(
   bcr_ref_file,
   {
     vpn_check
     bcr_paths$bcr_ref
   },
   format = "file"
 ),
 
 targets::tar_target(
   bcr_ic_file,
   {
     vpn_check
     bcr_paths$bcr_ic
   },
   format = "file"
 ),
 
 targets::tar_target(
   bcr_referrals_placed_file,
   {
     vpn_check
     bcr_paths$bcr_referrals_placed
   },
   format = "file"
 ),
 
 targets::tar_target(
   bcr_presenting_concerns_file,
   {
     vpn_check
     bcr_paths$bcr_presenting_concerns
   },
   format = "file"
 ),
 
 targets::tar_target(
   bcr_events_file,
   {
     vpn_check
     bcr_paths$bcr_events
   },
   format = "file"
 ),
 
 targets::tar_target(
   bcr_client_counseling_sessions_file,
   {
     vpn_check
     bcr_paths$bcr_client_counseling_sessions
   },
   format = "file"
 ),
 
 targets::tar_target(
   bcr_active_payor_source_file,
   {
     vpn_check
     bcr_paths$bcr_active_payor_source
   },
   format = "file"
 ),
 
 targets::tar_target(
   bcr_all_payor_source_file,
   {
     vpn_check
     bcr_paths$bcr_all_payor_source
   },
   format = "file"
 ),
 
 targets::tar_target(
   bcr_active_housing_file,
   {
     vpn_check
     bcr_paths$bcr_active_housing
   },
   format = "file"
 ),
 
 targets::tar_target(
   bcr_all_housing_file,
   {
     vpn_check
     bcr_paths$bcr_all_housing
   },
   format = "file"
 ),
 
 ## Complex Care ----
 
 targets::tar_target(
  complex_care_provider_placement_file,
  {
    vpn_check
    complex_care_paths$complex_care_provider_placement
  },
  format = "file"
 ),
 
 targets::tar_target(
  complex_care_pathclient_file,
  {
    vpn_check
    complex_care_paths$complex_care_pathclient
  },
  format = "file"
 ),
 
 targets::tar_target(
  complex_care_pathway_docsernos_file,
  {
    vpn_check
    complex_care_paths$complex_care_pathway_docsernos
  },
  format = "file"
 ),
 
 targets::tar_target(
  complex_care_client_file,
  {
    vpn_check
    complex_care_paths$complex_care_client
  },
  format = "file"
 ),
 
 targets::tar_target(
  complex_care_roster_file,
  {
    vpn_check
    complex_care_paths$complex_care_roster
  },
  format = "file"
 ),
 
 targets::tar_target(
  complex_care_clinical_notes_file,
  {
    vpn_check
    complex_care_paths$complex_care_clinical_notes
  },
  format = "file"
 ),
 
 targets::tar_target(
  complex_care_mercy_beacn_benchmarks_file,
  {
    vpn_check
    complex_care_paths$complex_care_mercy_beacn_benchmarks
  },
  format = "file"
 ),
 
 targets::tar_target(
  complex_care_pfp_discharge_file,
  {
    vpn_check
    complex_care_paths$complex_care_pfp_discharge
  },
  format = "file"
 ),

 targets::tar_target(
  complex_care_quality_of_life_file,
  {
    vpn_check
    complex_care_paths$complex_care_quality_of_life
  },
  format = "file"
 ),

 targets::tar_target(
  complex_care_shelter_beds_file,
  {
    vpn_check
    complex_care_paths$complex_care_shelter_beds
  },
  format = "file"
 ),

 targets::tar_target(
  complex_care_active_payor_source_file,
  {
    vpn_check
    complex_care_paths$complex_care_active_payor_source
  },
  format = "file"
 ),
 
 targets::tar_target(
  complex_care_all_payor_source_file,
  {
    vpn_check
    complex_care_paths$complex_care_all_payor_source
  },
  format = "file"
 ),
 
 targets::tar_target(
  complex_care_active_housing_file,
  {
    vpn_check
    complex_care_paths$complex_care_active_housing
  },
  format = "file"
 ),
 
 targets::tar_target(
  complex_care_all_housing_file,
  {
    vpn_check
    complex_care_paths$complex_care_all_housing
  },
  format = "file"
 ),

 targets::tar_target(
  complex_care_ext_mercy_utilization_file,
  {
    vpn_check
    complex_care_paths$complex_care_ext_mercy_utilization
  },
  format = "file"
),

 targets::tar_target(
   complex_care_ext_atd_notifications_file,
   {
     vpn_check
     complex_care_paths$complex_care_ext_atd_notifications
   },
   format = "file"
),

targets::tar_target(
  complex_care_ext_pfp_service_history_file,
  {
    vpn_check
    complex_care_paths$complex_care_ext_pfp_service_history
  },
  format = "file"
),

targets::tar_target(
  complex_care_ext_pfp_metrics_legacy_file,
  {
    vpn_check
    complex_care_paths$complex_care_ext_pfp_metrics_legacy
  },
  format = "file"
),

targets::tar_target(
  complex_care_ext_eto_roster_file,
  {
    vpn_check
    complex_care_paths$complex_care_ext_eto_roster
  },
  format = "file"
),

 ## EPICC ----
 
 targets::tar_target(
  epicc_provider_placement_file,
  {
    vpn_check
     epicc_paths$epicc_provider_placement
  },
  format = "file"
 ),
 
 targets::tar_target(
  epicc_pathclient_file,
  {
    vpn_check
    epicc_paths$epicc_pathclient
  },
  format = "file"
 ),
 
 targets::tar_target(
  epicc_pathway_docsernos_file,
  {
    vpn_check
    epicc_paths$epicc_pathway_docsernos
  },
  format = "file"
 ),
 
 targets::tar_target(
  epicc_client_file,
  {
    vpn_check
    epicc_paths$epicc_client
  },
  format = "file"
 ),
 
 targets::tar_target(
  epicc_ref_file,
  {
    vpn_check
    epicc_paths$epicc_ref
  },
  format = "file"
 ),
 
 targets::tar_target(
  epicc_ic_file,
  {
    vpn_check
    epicc_paths$epicc_ic
  },
  format = "file"
 ),
 
 targets::tar_target(
  epicc_two_week_file,
  {
    vpn_check
    epicc_paths$epicc_two_week
  },
  format = "file"
 ),
 
 targets::tar_target(
  epicc_thirty_day_file,
  {
    vpn_check
    epicc_paths$epicc_thirty_day
  },
  format = "file"
 ),
 
 targets::tar_target(
  epicc_three_month_file,
  {
    vpn_check
    epicc_paths$epicc_three_month
  },
  format = "file"
 ),
 
 targets::tar_target(
  epicc_six_month_file,
  {
    vpn_check
    epicc_paths$epicc_six_month
  },
  format = "file"
 ),
 
 targets::tar_target(
  epicc_reengagement_file,
  {
    vpn_check
    epicc_paths$epicc_reengagement
  },
  format = "file"
 ),
 
 targets::tar_target(
  epicc_active_intake_file,
  {
    vpn_check
    epicc_paths$epicc_active_intake
  },
  format = "file"
 ),
 
 targets::tar_target(
  epicc_all_intake_file,
  {
    vpn_check
    epicc_paths$epicc_all_intake
  },
  format = "file"
 ),
 
 targets::tar_target(
  epicc_active_payor_source_file,
  {
    vpn_check
    epicc_paths$epicc_active_payor_source
  },
  format = "file"
 ),
 
 targets::tar_target(
  epicc_all_payor_source_file,
  {
    vpn_check
    epicc_paths$epicc_all_payor_source
  },
  format = "file"
 ),
 
 targets::tar_target(
  epicc_active_housing_file,
  {
    vpn_check
    epicc_paths$epicc_active_housing
  },
  format = "file"
 ),
 
 targets::tar_target(
  epicc_all_housing_file,
  {
    vpn_check
    epicc_paths$epicc_all_housing
  },
  format = "file"
 ),
 
 targets::tar_target(
  epicc_case_notes_file,
  {
    vpn_check
    epicc_paths$epicc_case_notes
  },
  format = "file"
 ),
 
 targets::tar_target(
  epicc_support_services_tracker_file,
  {
    vpn_check
    epicc_paths$epicc_support_services_tracker
  },
  format = "file"
 ),
 
 ## ERE ----
 
 targets::tar_target(
  ere_provider_placement_file,
  {
    vpn_check
    ere_paths$ere_provider_placement
  },
  format = "file"
 ),
 
 targets::tar_target(
  ere_pathclient_file,
  {
    vpn_check
    ere_paths$ere_pathclient
  },
  format = "file"
 ),
 
 targets::tar_target(
  ere_pathway_docsernos_file,
  {
    vpn_check
    ere_paths$ere_pathway_docsernos
  },
  format = "file"
 ),
 
 targets::tar_target(
  ere_client_file,
  {
    vpn_check
    ere_paths$ere_client
  },
  format = "file"
 ),
 
 targets::tar_target(
  ere_ref_file,
  {
    vpn_check
    ere_paths$ere_ref
  },
  format = "file"
 ),
 
 targets::tar_target(
  ere_hosp_visit_file,
  {
    vpn_check
    ere_paths$ere_hosp_visit
  },
  format = "file"
 ),
 
 targets::tar_target(
  ere_ihna_file,
  {
    vpn_check
    ere_paths$ere_ihna
  },
  format = "file"
 ),
 
 targets::tar_target(
  ere_three_month_file,
  {
    vpn_check
    ere_paths$ere_three_month
  },
  format = "file"
 ),
 
 targets::tar_target(
  ere_six_month_file,
  {
    vpn_check
    ere_paths$ere_six_month
  },
  format = "file"
 ),
 
 targets::tar_target(
  ere_bhs_file,
  {
    vpn_check
    ere_paths$ere_bhs
  },
  format = "file"
 ),
 
 targets::tar_target(
  ere_active_payor_source_file,
  {
    vpn_check
    ere_paths$ere_active_payor_source
  },
  format = "file"
 ),
 
 targets::tar_target(
  ere_all_payor_source_file,
  {
    vpn_check
    ere_paths$ere_all_payor_source
  },
  format = "file"
 ),
 
 targets::tar_target(
  ere_active_housing_file,
  {
    vpn_check
    ere_paths$ere_active_housing
  },
  format = "file"
 ),
 
 targets::tar_target(
  ere_all_housing_file,
  {
    vpn_check
    ere_paths$ere_all_housing
  },
  format = "file"
 ),
 
 targets::tar_target(
  ere_client_needs_file,
  {
    vpn_check
    ere_paths$ere_client_needs
  },
  format = "file"
 ),
 
 ## YERE ----
 
 targets::tar_target(
  yere_provider_placement_file,
  {
    vpn_check
    yere_paths$yere_provider_placement
  },
  format = "file"
 ),
 
 targets::tar_target(
  yere_pathclient_file,
  {
    vpn_check
    yere_paths$yere_pathclient
  },
  format = "file"
 ),
 
 targets::tar_target(
  yere_pathway_docsernos_file,
  {
    vpn_check
    yere_paths$yere_pathway_docsernos
  },
  format = "file"
 ),
 
 targets::tar_target(
  yere_client_file,
  {
    vpn_check
    yere_paths$yere_client
  },
  format = "file"
 ),
 
 targets::tar_target(
  yere_ref_file,
  {
    vpn_check
    yere_paths$yere_ref
  },
  format = "file"
 ),
 
 targets::tar_target(
  yere_hosp_visit_file,
  {
    vpn_check
    yere_paths$yere_hosp_visit
  },
  format = "file"
 ),
 
 targets::tar_target(
  yere_ia_file,
  {
    vpn_check
    yere_paths$yere_ia
  },
  format = "file"
 ),
 
 targets::tar_target(
  yere_thirty_day_file,
  {
    vpn_check
    yere_paths$yere_thirty_day
  },
  format = "file"
 ),
 
 targets::tar_target(
  yere_three_month_file,
  {
    vpn_check
    yere_paths$yere_three_month
  },
  format = "file"
 ),
 
 targets::tar_target(
  yere_six_month_file,
  {
    vpn_check
    yere_paths$yere_six_month
  },
  format = "file"
 ),
 
 targets::tar_target(
  yere_bhs_file,
  {
    vpn_check
    yere_paths$yere_bhs
  },
  format = "file"
 ),
 
 targets::tar_target(
  yere_active_payor_source_file,
  {
    vpn_check
    yere_paths$yere_active_payor_source
  },
  format = "file"
 ),
 
 targets::tar_target(
  yere_all_payor_source_file,
  {
    vpn_check
    yere_paths$yere_all_payor_source
  },
  format = "file"
 ),
 
 targets::tar_target(
  yere_active_housing_file,
  {
    vpn_check
    yere_paths$yere_active_housing
  },
  format = "file"
 ),
 
 targets::tar_target(
  yere_all_housing_file,
  {
    vpn_check
    yere_paths$yere_all_housing
  },
  format = "file"
 ),
 
 targets::tar_target(
  yere_client_needs_file,
  {
    vpn_check
    yere_paths$yere_client_needs
  },
  format = "file"
 ),
 
 targets::tar_target(
  yere_caregiver_needs_file,
 {
    vpn_check
    yere_paths$yere_caregiver_needs
  },
  format = "file"
 ),

 targets::tar_target(
   yere_client_family_needs_file,
   {
     vpn_check
     yere_paths$yere_client_family_needs
   },
   format = "file"
 ),
 
 # ===
 # Adds RAW-read targets that depend on file targets ----
 # ===
 
 ## BCR ----
 
 targets::tar_target(
   bcr_provider_placement_raw,
   load_famcare_extract(
     path = bcr_provider_placement_file,
     analytic_fields = analytic_fields
   )
 ),
 
 targets::tar_target(
   bcr_pathclient_raw,
   load_famcare_extract(
     path = bcr_pathclient_file,
     analytic_fields = analytic_fields
   )
 ),
 
 targets::tar_target(
   bcr_pathway_docsernos_raw,
   load_famcare_extract(
     path = bcr_pathway_docsernos_file,
     analytic_fields = analytic_fields
   )
 ),
 
 targets::tar_target(
   bcr_client_raw,
   load_famcare_extract(
     path = bcr_client_file,
     analytic_fields = analytic_fields
   )
 ),
 
 targets::tar_target(
   bcr_ref_raw,
   load_famcare_extract(
     path = bcr_ref_file,
     analytic_fields = analytic_fields
   )
 ),
 
 targets::tar_target(
   bcr_ic_raw,
   load_famcare_extract(
     path = bcr_ic_file,
     analytic_fields = analytic_fields
   )
 ),
 
 targets::tar_target(
   bcr_referrals_placed_raw,
   load_famcare_extract(
     path = bcr_referrals_placed_file,
     analytic_fields = analytic_fields
   )
 ),
 
 targets::tar_target(
   bcr_presenting_concerns_raw,
   load_famcare_extract(
     path = bcr_presenting_concerns_file,
     analytic_fields = analytic_fields
   )
 ),
 
 targets::tar_target(
   bcr_events_raw,
   load_famcare_extract(
     path = bcr_events_file,
     analytic_fields = analytic_fields
   )
 ),
 
 targets::tar_target(
   bcr_client_counseling_sessions_raw,
   load_famcare_extract(
     path = bcr_client_counseling_sessions_file,
     analytic_fields = analytic_fields
   )
 ),
 
 targets::tar_target(
   bcr_active_payor_source_raw,
   load_famcare_extract(
     path = bcr_active_payor_source_file,
     analytic_fields = analytic_fields
   )
 ),
 
 targets::tar_target(
   bcr_all_payor_source_raw,
   load_famcare_extract(
     path = bcr_all_payor_source_file,
     analytic_fields = analytic_fields
   )
 ),
 
 targets::tar_target(
   bcr_active_housing_raw,
   load_famcare_extract(
     path = bcr_active_housing_file,
     analytic_fields = analytic_fields
   )
 ),
 
 targets::tar_target(
   bcr_all_housing_raw,
   load_famcare_extract(
     path = bcr_all_housing_file,
     analytic_fields = analytic_fields
   )
 ),
 
 ## Complex Care ----
 
 targets::tar_target(
  complex_care_provider_placement_raw,
  load_famcare_extract(
   path = complex_care_provider_placement_file,
   analytic_fields = analytic_fields
  )
 ),
 
 targets::tar_target(
  complex_care_pathclient_raw,
  load_famcare_extract(
   path = complex_care_pathclient_file,
   analytic_fields = analytic_fields
  )
 ),
 
 targets::tar_target(
  complex_care_pathway_docsernos_raw,
  load_famcare_extract(
   path = complex_care_pathway_docsernos_file,
   analytic_fields = analytic_fields
  )
 ),
 
 targets::tar_target(
  complex_care_client_raw,
  load_famcare_extract(
   path = complex_care_client_file,
   analytic_fields = analytic_fields
  )
 ),
 
 targets::tar_target(
  complex_care_roster_raw,
  load_famcare_extract(
   path = complex_care_roster_file,
   analytic_fields = analytic_fields
  )
 ),
 
 targets::tar_target(
  complex_care_clinical_notes_raw,
  load_famcare_extract(
   path = complex_care_clinical_notes_file,
   analytic_fields = analytic_fields
  )
 ),
 
 targets::tar_target(
  complex_care_mercy_beacn_benchmarks_raw,
  load_famcare_extract(
   path = complex_care_mercy_beacn_benchmarks_file,
   analytic_fields = analytic_fields
  )
 ),
 
 targets::tar_target(
  complex_care_pfp_discharge_raw,
  load_famcare_extract(
   path = complex_care_pfp_discharge_file,
   analytic_fields = analytic_fields
  )
 ),
 
 targets::tar_target(
  complex_care_quality_of_life_raw,
  load_famcare_extract(
   path = complex_care_quality_of_life_file,
   analytic_fields = analytic_fields
  )
 ),

 targets::tar_target(
  complex_care_shelter_beds_raw,
  load_famcare_extract(
   path = complex_care_shelter_beds_file,
   analytic_fields = analytic_fields
  )
 ),

 targets::tar_target(
  complex_care_active_payor_source_raw,
  load_famcare_extract(
   path = complex_care_active_payor_source_file,
   analytic_fields = analytic_fields
  )
 ),
 
 targets::tar_target(
  complex_care_all_payor_source_raw,
  load_famcare_extract(
   path = complex_care_all_payor_source_file,
   analytic_fields = analytic_fields
  )
 ),
 
 targets::tar_target(
  complex_care_active_housing_raw,
  load_famcare_extract(
   path = complex_care_active_housing_file,
   analytic_fields = analytic_fields
  )
 ),
 
 targets::tar_target(
  complex_care_all_housing_raw,
  load_famcare_extract(
   path = complex_care_all_housing_file,
   analytic_fields = analytic_fields
  )
 ),

# supplies the entire complex_care_paths because path isn't an argument in
# load_complex_care_ext_mercy_utilization.
targets::tar_target(
  complex_care_ext_mercy_utilization_raw,
  load_complex_care_ext_mercy_utilization(
    complex_care_paths = complex_care_paths,
    analytic_fields = analytic_fields
  )
),

 targets::tar_target(
   complex_care_ext_atd_notifications_raw,
   load_complex_care_ext_atd_notifications(
     complex_care_paths,
     analytic_fields
   )
 ),
 
targets::tar_target(
  complex_care_ext_pfp_service_history_raw,
  load_famcare_extract(
    path = complex_care_ext_pfp_service_history_file,
    analytic_fields = analytic_fields
  )
),

# Target to build the alert watchlist
targets::tar_target(
  complex_care_alert_watchlist_raw,
  transform_complex_care_alert_watchlist(
    complex_care_etl$complex_care_full_data
    )
),

targets::tar_target(
  complex_care_ext_pfp_metrics_legacy_raw,
  load_ext_eto_extract(
    complex_care_ext_pfp_metrics_legacy_file,
    analytic_fields
  )
),

targets::tar_target(
  complex_care_ext_pcc_facility_lut_raw,
  load_complex_care_ext_pcc_facility_lut(
    complex_care_ext_pcc_facility_lut_file
  )
),

targets::tar_target(
  complex_care_ext_eto_roster_raw,
  load_complex_care_ext_eto_roster(
    complex_care_ext_eto_roster_file,
    analytic_fields
  )
),

 ## EPICC ----
 
 targets::tar_target(
  epicc_provider_placement_raw,
  load_famcare_extract(
   path = epicc_provider_placement_file,
   analytic_fields = analytic_fields
  )
 ),
 
 targets::tar_target(
  epicc_pathclient_raw,
  load_famcare_extract(
   path = epicc_pathclient_file,
   analytic_fields = analytic_fields
  )
 ),
 
 targets::tar_target(
  epicc_pathway_docsernos_raw,
  load_famcare_extract(
   path = epicc_pathway_docsernos_file,
   analytic_fields = analytic_fields
  )
 ),
 
 targets::tar_target(
  epicc_client_raw,
  load_famcare_extract(
   path = epicc_client_file,
   analytic_fields = analytic_fields
  )
 ),
 
 targets::tar_target(
  epicc_ref_raw,
  load_famcare_extract(
   path = epicc_ref_file,
   analytic_fields = analytic_fields
  )
 ),
 
 targets::tar_target(
  epicc_ic_raw,
  load_famcare_extract(
   path = epicc_ic_file,
   analytic_fields = analytic_fields
  )
 ),
 
 targets::tar_target(
  epicc_two_week_raw,
  load_famcare_extract(
   path = epicc_two_week_file,
   analytic_fields = analytic_fields
  )
 ),
 
 targets::tar_target(
  epicc_thirty_day_raw,
  load_famcare_extract(
   path = epicc_thirty_day_file,
   analytic_fields = analytic_fields
  )
 ),
 
 targets::tar_target(
  epicc_three_month_raw,
  load_famcare_extract(
   path = epicc_three_month_file,
   analytic_fields = analytic_fields
  )
 ),
 
 targets::tar_target(
  epicc_six_month_raw,
  load_famcare_extract(
   path = epicc_six_month_file,
   analytic_fields = analytic_fields
  )
 ),
 
 targets::tar_target(
  epicc_reengagement_raw,
  load_famcare_extract(
   path = epicc_reengagement_file,
   analytic_fields = analytic_fields
  )
 ),
 
 targets::tar_target(
  epicc_active_intake_raw,
  load_famcare_extract(
   path = epicc_active_intake_file,
   analytic_fields = analytic_fields
  )
 ),
 
 targets::tar_target(
  epicc_all_intake_raw,
  load_famcare_extract(
   path = epicc_all_intake_file,
   analytic_fields = analytic_fields
  )
 ),
 
 targets::tar_target(
  epicc_active_payor_source_raw,
  load_famcare_extract(
   path = epicc_active_payor_source_file,
   analytic_fields = analytic_fields
  )
 ),
 
 targets::tar_target(
  epicc_all_payor_source_raw,
  load_famcare_extract(
   path = epicc_all_payor_source_file,
   analytic_fields = analytic_fields
  )
 ),
 
 targets::tar_target(
  epicc_active_housing_raw,
  load_famcare_extract(
   path = epicc_active_housing_file,
   analytic_fields = analytic_fields
  )
 ),
 
 targets::tar_target(
  epicc_all_housing_raw,
  load_famcare_extract(
   path = epicc_all_housing_file,
   analytic_fields = analytic_fields
  )
 ),
 
 targets::tar_target(
  epicc_case_notes_raw,
  load_famcare_extract(
   path = epicc_case_notes_file,
   analytic_fields = analytic_fields
  )
 ),
 
 targets::tar_target(
  epicc_support_services_tracker_raw,
  load_famcare_extract(
   path = epicc_support_services_tracker_file,
   analytic_fields = analytic_fields
  )
 ),
 
 ## ERE ----
 
 targets::tar_target(
  ere_provider_placement_raw,
  load_famcare_extract(
   path = ere_provider_placement_file,
   analytic_fields = analytic_fields
  )
 ),
 
 targets::tar_target(
  ere_pathclient_raw,
  load_famcare_extract(
   path = ere_pathclient_file,
   analytic_fields = analytic_fields
  )
 ),
 
 targets::tar_target(
  ere_pathway_docsernos_raw,
  load_famcare_extract(
   path = ere_pathway_docsernos_file,
   analytic_fields = analytic_fields
  )
 ),
 
 targets::tar_target(
  ere_client_raw,
  load_famcare_extract(
   path = ere_client_file,
   analytic_fields = analytic_fields
  )
 ),
 
 targets::tar_target(
  ere_ref_raw,
  load_famcare_extract(
   path = ere_ref_file,
   analytic_fields = analytic_fields
  )
 ),
 
 targets::tar_target(
  ere_hosp_visit_raw,
  load_famcare_extract(
   path = ere_hosp_visit_file,
   analytic_fields = analytic_fields
  )
 ),
 
 targets::tar_target(
  ere_ihna_raw,
  load_famcare_extract(
   path = ere_ihna_file,
   analytic_fields = analytic_fields
  )
 ),
 
 targets::tar_target(
  ere_three_month_raw,
  load_famcare_extract(
   path = ere_three_month_file,
   analytic_fields = analytic_fields
  )
 ),
 
 targets::tar_target(
  ere_six_month_raw,
  load_famcare_extract(
   path = ere_six_month_file,
   analytic_fields = analytic_fields
  )
 ),
 
 targets::tar_target(
  ere_bhs_raw,
  load_famcare_extract(
   path = ere_bhs_file,
   analytic_fields = analytic_fields
  )
 ),
 
 targets::tar_target(
  ere_active_payor_source_raw,
  load_famcare_extract(
   path = ere_active_payor_source_file,
   analytic_fields = analytic_fields
  )
 ),
 
 targets::tar_target(
  ere_all_payor_source_raw,
  load_famcare_extract(
   path = ere_all_payor_source_file,
   analytic_fields = analytic_fields
  )
 ),
 
 targets::tar_target(
  ere_active_housing_raw,
  load_famcare_extract(
   path = ere_active_housing_file,
   analytic_fields = analytic_fields
  )
 ),
 
 targets::tar_target(
  ere_all_housing_raw,
  load_famcare_extract(
   path = ere_all_housing_file,
   analytic_fields = analytic_fields
  )
 ),
 
 targets::tar_target(
  ere_client_needs_raw,
  load_famcare_extract(
   path = ere_client_needs_file,
   analytic_fields = analytic_fields
  )
 ),
 
 ## YERE ----
 
 targets::tar_target(
  yere_provider_placement_raw,
  load_famcare_extract(
   path = yere_provider_placement_file,
   analytic_fields = analytic_fields
  )
 ),
 
 targets::tar_target(
  yere_pathclient_raw,
  load_famcare_extract(
   path = yere_pathclient_file,
   analytic_fields = analytic_fields
  )
 ),
 
 targets::tar_target(
  yere_pathway_docsernos_raw,
  load_famcare_extract(
   path = yere_pathway_docsernos_file,
   analytic_fields = analytic_fields
  )
 ),
 
 targets::tar_target(
  yere_client_raw,
  load_famcare_extract(
   path = yere_client_file,
   analytic_fields = analytic_fields
  )
 ),
 
 targets::tar_target(
  yere_ref_raw,
  load_famcare_extract(
   path = yere_ref_file,
   analytic_fields = analytic_fields
  )
 ),
 
 targets::tar_target(
  yere_hosp_visit_raw,
  load_famcare_extract(
   path = yere_hosp_visit_file,
   analytic_fields = analytic_fields
  )
 ),
 
 targets::tar_target(
  yere_ia_raw,
  load_famcare_extract(
   path = yere_ia_file,
   analytic_fields = analytic_fields
  )
 ),
 
 targets::tar_target(
  yere_thirty_day_raw,
  load_famcare_extract(
   path = yere_thirty_day_file,
   analytic_fields = analytic_fields
  )
 ),
 
 targets::tar_target(
  yere_three_month_raw,
  load_famcare_extract(
   path = yere_three_month_file,
   analytic_fields = analytic_fields
  )
 ),
 
 targets::tar_target(
  yere_six_month_raw,
  load_famcare_extract(
   path = yere_six_month_file,
   analytic_fields = analytic_fields
  )
 ),
 
 targets::tar_target(
  yere_bhs_raw,
  load_famcare_extract(
   path = yere_bhs_file,
   analytic_fields = analytic_fields
  )
 ),
 
 targets::tar_target(
  yere_active_payor_source_raw,
  load_famcare_extract(
   path = yere_active_payor_source_file,
   analytic_fields = analytic_fields
  )
 ),
 
 targets::tar_target(
  yere_all_payor_source_raw,
  load_famcare_extract(
   path = yere_all_payor_source_file,
   analytic_fields = analytic_fields
  )
 ),
 
 targets::tar_target(
  yere_active_housing_raw,
  load_famcare_extract(
   path = yere_active_housing_file,
   analytic_fields = analytic_fields
  )
 ),
 
 targets::tar_target(
  yere_all_housing_raw,
  load_famcare_extract(
   path = yere_all_housing_file,
   analytic_fields = analytic_fields
  )
 ),
 
  targets::tar_target(
    yere_client_needs_raw,
    load_famcare_extract(
     path = yere_client_needs_file,
     analytic_fields = analytic_fields
    )
   ),
 
  targets::tar_target(
    yere_caregiver_needs_raw,
    load_famcare_extract(
     path = yere_caregiver_needs_file,
     analytic_fields = analytic_fields
    )
   ),

  targets::tar_target(
    yere_client_family_needs_raw,
    load_famcare_extract(
      path = yere_client_family_needs_file,
      analytic_fields = analytic_fields
    )
  ),
 
  # ===
  # Standalone EXT Asset Transformation Targets ----
  # ===
  
  ## BCR ----
  
  ## Complex Care ----
  targets::tar_target(
    complex_care_ext_mercy_utilization_transformed,
    transform_complex_care_ext_mercy_utilization(
      df_raw = complex_care_ext_mercy_utilization_raw,
      complex_care_paths = complex_care_paths,
      ccsr_dx_lut = ccsr_dx_lut
    )
  ),
  
  targets::tar_target(
    complex_care_ext_pfp_metrics_legacy,
    complex_care_ext_pfp_metrics_legacy_raw
  ),

  # PCC visit facility LUT target for downstream use
  targets::tar_target(
    complex_care_ext_pcc_facility_lut,
    complex_care_ext_pcc_facility_lut_raw
  ),
  
  
  targets::tar_target(
    complex_care_ext_eto_roster,
    complex_care_ext_eto_roster_raw
  ),
  
  ## EPICC ----
  
  ## ERE ----
  
  ## YERE ----

  # ===
  # Referral Flow Targets (required by diagnostics) ----
  # ===
  
  ## BCR ----
  
  targets::tar_target(
    bcr_referral_flow,
    bcr_etl$transform$referral_flow$joined_referral_flow
  ),
  
  ## Complex Care ----
  
  targets::tar_target(
    complex_care_referral_flow,
    complex_care_etl$transform$referral_flow$joined_referral_flow
  ),
  
  ## EPICC ----
  
  targets::tar_target(
    epicc_referral_flow,
    epicc_etl$transform$referral_flow$joined_referral_flow
  ),
  
  ## ERE ----
  
  targets::tar_target(
    ere_referral_flow,
    ere_etl$transform$referral_flow$joined_referral_flow
  ),
  
  ## YERE ----
  
  targets::tar_target(
    yere_referral_flow,
    yere_etl$transform$referral_flow$joined_referral_flow
  ),

 # ===
 # Program ETL branches (each returns a structured list) ----
 # ===

 ## BCR ----
 
 targets::tar_target(
   bcr_etl,
   run_bcr_etl(
     analytic_fields = analytic_fields,
     bcr_provider_placement = bcr_provider_placement_raw,
     bcr_pathclient = bcr_pathclient_raw,
     bcr_pathway_docsernos = bcr_pathway_docsernos_raw,
     bcr_client = bcr_client_raw,
     bcr_ref = bcr_ref_raw,
     bcr_ic = bcr_ic_raw,
     bcr_referrals_placed = bcr_referrals_placed_raw,
     bcr_presenting_concerns = bcr_presenting_concerns_raw,
     bcr_events = bcr_events_raw,
     bcr_client_counseling_sessions = bcr_client_counseling_sessions_raw,
     bcr_active_payor_source = bcr_active_payor_source_raw,
     bcr_all_payor_source = bcr_all_payor_source_raw,
     bcr_active_housing = bcr_active_housing_raw,
     bcr_all_housing = bcr_all_housing_raw,
     bcr_referral_subtype_map = bcr_referral_subtype_map
   )
 ),

 ## Complex Care ----

 targets::tar_target(
  complex_care_etl,
  run_complex_care_etl(
    analytic_fields = analytic_fields,
    complex_care_provider_placement = complex_care_provider_placement_raw,
    complex_care_pathclient = complex_care_pathclient_raw,
    complex_care_pathway_docsernos = complex_care_pathway_docsernos_raw,
    complex_care_client = complex_care_client_raw,
    complex_care_roster = complex_care_roster_raw,
    complex_care_clinical_notes = complex_care_clinical_notes_raw,
    complex_care_mercy_beacn_benchmarks = 
      complex_care_mercy_beacn_benchmarks_raw,
    complex_care_pfp_discharge = complex_care_pfp_discharge_raw,
    complex_care_quality_of_life = complex_care_quality_of_life_raw,
    complex_care_shelter_beds = complex_care_shelter_beds_raw,
    complex_care_active_payor_source = complex_care_active_payor_source_raw,
    complex_care_all_payor_source = complex_care_all_payor_source_raw,
    complex_care_active_housing = complex_care_active_housing_raw,
    complex_care_all_housing = complex_care_all_housing_raw,
    complex_care_ext_mercy_utilization =
      complex_care_ext_mercy_utilization_raw,
    complex_care_ext_mercy_utilization_transformed =
      complex_care_ext_mercy_utilization_transformed,
    complex_care_ext_atd_notifications = complex_care_ext_atd_notifications_raw,
    complex_care_ext_pfp_service_history = 
      complex_care_ext_pfp_service_history_raw,
    complex_care_ext_pfp_metrics_legacy = complex_care_ext_pfp_metrics_legacy,
    complex_care_ext_pcc_facility_lut = complex_care_ext_pcc_facility_lut,
    complex_care_ext_eto_roster = complex_care_ext_eto_roster
  )
 ),
 
 ## EPICC ----

 targets::tar_target(
  epicc_etl,
  run_epicc_etl(
   analytic_fields = analytic_fields,
   epicc_provider_placement = epicc_provider_placement_raw,
   epicc_pathclient = epicc_pathclient_raw,
   epicc_pathway_docsernos = epicc_pathway_docsernos_raw,
   epicc_client = epicc_client_raw,
   epicc_ref = epicc_ref_raw,
   epicc_ic = epicc_ic_raw,
   epicc_two_week = epicc_two_week_raw,
   epicc_thirty_day = epicc_thirty_day_raw,
   epicc_three_month = epicc_three_month_raw,
   epicc_six_month = epicc_six_month_raw,
   epicc_reengagement = epicc_reengagement_raw,
   epicc_active_intake = epicc_active_intake_raw,
   epicc_all_intake = epicc_all_intake_raw,
   epicc_active_payor_source = epicc_active_payor_source_raw,
   epicc_all_payor_source = epicc_all_payor_source_raw,
   epicc_active_housing = epicc_active_housing_raw,
   epicc_all_housing = epicc_all_housing_raw,
   epicc_case_notes = epicc_case_notes_raw,
   epicc_support_services_tracker = epicc_support_services_tracker_raw
  )
 ),
 
 ## ERE ----

 targets::tar_target(
  ere_etl,
  run_ere_etl(
   analytic_fields = analytic_fields,
   ere_provider_placement = ere_provider_placement_raw,
   ere_pathclient = ere_pathclient_raw,
   ere_pathway_docsernos = ere_pathway_docsernos_raw,
   ere_client = ere_client_raw,
   ere_ref = ere_ref_raw,
   ere_hosp_visit = ere_hosp_visit_raw,
   ere_ihna = ere_ihna_raw,
   ere_three_month = ere_three_month_raw,
   ere_six_month = ere_six_month_raw,
   ere_bhs = ere_bhs_raw,
   ere_active_payor_source = ere_active_payor_source_raw,
   ere_all_payor_source = ere_all_payor_source_raw,
   ere_active_housing = ere_active_housing_raw,
   ere_all_housing = ere_all_housing_raw,
   ere_client_needs = ere_client_needs_raw
  )
 ),
 
  ## YERE ----

 targets::tar_target(
  yere_etl,
  run_yere_etl(
    analytic_fields = analytic_fields,
    yere_provider_placement = yere_provider_placement_raw,
    yere_pathclient = yere_pathclient_raw,
    yere_pathway_docsernos = yere_pathway_docsernos_raw,
    yere_client = yere_client_raw,
    yere_ref = yere_ref_raw,
    yere_hosp_visit = yere_hosp_visit_raw,
    yere_ia = yere_ia_raw,
    yere_thirty_day = yere_thirty_day_raw,
    yere_three_month = yere_three_month_raw,
    yere_six_month = yere_six_month_raw,
    yere_bhs = yere_bhs_raw,
    yere_active_payor_source = yere_active_payor_source_raw,
    yere_all_payor_source = yere_all_payor_source_raw,
    yere_active_housing = yere_active_housing_raw,
    yere_all_housing = yere_all_housing_raw,
    yere_client_needs = yere_client_needs_raw,
    yere_caregiver_needs = yere_caregiver_needs_raw,
    yere_client_family_needs = yere_client_family_needs_raw
    )
  ),
 
 # ===
 # FULL_DATA table convenience targets ----
 # ===

 ## BCR ----

 targets::tar_target(
  bcr_full_data,
  bcr_etl$bcr_full_data
 ),
 
 ## Complex Care ----
 
 targets::tar_target(
  complex_care_full_data,
  complex_care_etl$complex_care_full_data
 ),
 
 ## EPICC ----

  targets::tar_target(
  epicc_full_data,
  epicc_etl$epicc_full_data
  ),
 
 ## ERE ----

  targets::tar_target(
  ere_full_data,
  ere_etl$ere_full_data
  ),
 
 ## YERE ----

 targets::tar_target(
  yere_full_data,
  yere_etl$yere_full_data
  ),

 # ===
 # BHN-wide: fact table + program assets (unjoined) ----
 # ===
 
 bhn_wide_paths <- list(
  bhn_wide_pathclient = make_path(
   "FAMCare BHNWIDE Extract/",
   "Q_BHNWIDE_PATHCLIENT_ENROLLMENTS.csv"
   )
 ),
 
 targets::tar_target(
  bhn_wide_pathclient_file,
  bhn_wide_paths$bhn_wide_pathclient,
  format = "file"
 ),
 
 targets::tar_target(
  bhn_wide_pathclient_raw,
  load_famcare_extract(
   bhn_wide_pathclient_file,
   analytic_fields
   )
 ),
 
 targets::tar_target(
  bhn_wide_full_data,
  list(
   bhn_wide_pathclient = bhn_wide_pathclient_raw,
   bcr_raw = bcr_etl$raw,
   complex_care_raw = complex_care_etl$raw,
   epicc_raw = epicc_etl$raw,
   ere_raw = ere_etl$raw,
   yere_raw = yere_etl$raw
  )
 ),

# ===
# File WRITE Outputs (csv|xlsx) ----
# ===

# Write the PCC alerting watchlist to enterprise server
targets::tar_target(
  complex_care_alert_watchlist_server_csv,
  {
    out_path <- paste0(
      "P:/DATA/Data Files/Collective Medical Uploads/BHN_CM_Watchlist_",
      format(
        Sys.Date(),
        "%Y%m%d"
        ),
      ".csv"
    )
    
    readr::write_csv(
      complex_care_alert_watchlist_raw,
      file = out_path,
      na = ""
    )
    
    out_path
  },
  format = "file"
),

# Write the PCC alerting watchlist to Complex Care SharePoint folder
targets::tar_target(
  complex_care_alert_watchlist_sharepoint_csv,
  {
    out_path <- paste0(
      "C:/Users/",
      Sys.info()[7],
      "/Behavioral Health Network of Greater St. Louis/",
      "BHN - Documents/Complex Care/Data & Evaluation/HIDI Analysis/",
      "BHN_CM_Watchlist_",
      format(
        Sys.Date(),
        "%Y%m%d"
        ),
      ".csv"
    )
    
    readr::write_csv(
      complex_care_alert_watchlist_raw,
      file = out_path,
      na = ""
    )
    
    out_path
  },
  format = "file"
),

# ===
# Cached outputs (RDS) ----
# ===
 
## Cartography Bundle ----

targets::tar_target(
  cartography_county_two_rds,
  {
    dir.create(
      "data_intermediate/cartography",
      recursive = TRUE,
      showWarnings = FALSE
    )
    saveRDS(
      cartography_bundle$county_two,
      "data_intermediate/cartography/county_two.rds"
    )
    "data_intermediate/cartography/county_two.rds"
  },
  format = "file"
),

targets::tar_target(
  cartography_county_seven_rds,
  {
    dir.create(
      "data_intermediate/cartography",
      recursive = TRUE,
      showWarnings = FALSE
    )
    saveRDS(
      cartography_bundle$county_seven,
      "data_intermediate/cartography/county_seven.rds"
    )
    "data_intermediate/cartography/county_seven.rds"
  },
  format = "file"
),

targets::tar_target(
  cartography_zcta_fips_rds,
  {
    dir.create(
      "data_intermediate/cartography",
      recursive = TRUE,
      showWarnings = FALSE
    )
    saveRDS(
      cartography_bundle$zcta_fips,
      "data_intermediate/cartography/zcta_fips.rds"
    )
    "data_intermediate/cartography/zcta_fips.rds"
  },
  format = "file"
),

targets::tar_target(
  cartography_north_zip_codes_rds,
  {
    dir.create(
      "data_intermediate/cartography",
      recursive = TRUE,
      showWarnings = FALSE
    )
    saveRDS(
      cartography_bundle$north_zip_codes,
      "data_intermediate/cartography/north_zip_codes.rds"
    )
    "data_intermediate/cartography/north_zip_codes.rds"
  },
  format = "file"
),

## CCSR DX LUT ----

targets::tar_target(
  ccsr_dx_lut_rds,
  {
    dir.create(
      "data_intermediate/ccsr",
      recursive = TRUE,
      showWarnings = FALSE
    )
    saveRDS(
      ccsr_dx_lut,
      "data_intermediate/ccsr/ccsr_dx_lut.rds"
    )
    "data_intermediate/ccsr/ccsr_dx_lut.rds"
  },
  format = "file"
),

## FAMCare Master Tables LUT RDS ----

# These are rds outputs so they will be avaialble to downstream dependencies,
# but they are not defined as file targets because these targets concern
# metadata.

targets::tar_target(
  master_tables,
  load_master_tables(
    "P:/DATA/LUTs/FC_Master_Tables.xlsx"
    ),
  format = "rds"
),

targets::tar_target(
  master_lookup,
  get_master_lookup(
    "P:/DATA/LUTs/FC_Master_Tables.xlsx"
    ),
  format = "rds"
),

### BCR Master Table LUTs RDS ----
  
tar_target(
  bcr_referral_subtype_map,
  {
    bcr_referral_type_map <- tibble::tibble(
      master_table_name = c(
        "bcr_ref_placed_bh_subtype",
        "bcr_ref_placed_housing_subtype",
        "bcr_ref_placed_phys_health_sub",
        "bcr_ref_placed_social_subtype"
      ),
      referral_type = c(
        "behavioral_health",
        "housing",
        "physical_health",
        "social_services"
      )
    )
    
    master_lookup %>%
      dplyr::filter(
        master_table_name %in% bcr_referral_type_map$master_table_name
        ) %>%
      dplyr::left_join(
        bcr_referral_type_map,
        by = "master_table_name"
        ) %>%
      dplyr::select(
        master_table_name,
        referral_type,
        code,
        description,
        status,
        subtype
      )
  },
  format = "rds"
),

 ## BCR ETL RDS ----

 targets::tar_target(
  bcr_etl_rds,
  { dir.create(
   "data_intermediate/etl/bcr",
   recursive = TRUE,
   showWarnings = FALSE
  ) 
   saveRDS(
    bcr_etl,
    "data_intermediate/etl/bcr/bcr_etl.rds"
   )
   "data_intermediate/etl/bcr/bcr_etl.rds" },
  format = "file"
 ),

 ## Complex Care ETL RDS ----

 targets::tar_target(
  complex_care_etl_rds,
  { dir.create(
   "data_intermediate/etl/complex_care",
   recursive = TRUE,
   showWarnings = FALSE
  )
   saveRDS(
    complex_care_etl,
    "data_intermediate/etl/complex_care/complex_care_etl.rds"
   )
   "data_intermediate/etl/complex_care/complex_care_etl.rds" },
  format = "file"
 ),

 ## EPICC ETL RDS----

 targets::tar_target(
  epicc_etl_rds,
  {
   dir.create(
    "data_intermediate/etl/epicc",
    recursive = TRUE,
    showWarnings = FALSE
   )
   saveRDS(
    epicc_etl,
    "data_intermediate/etl/epicc/epicc_etl.rds"
   )
   "data_intermediate/etl/epicc/epicc_etl.rds"
  },
  format = "file"
 ),

 ## ERE ETL RDS ----

 targets::tar_target(
  ere_etl_rds,
  { dir.create(
   "data_intermediate/etl/ere",
   recursive = TRUE,
   showWarnings = FALSE
  ) 
   saveRDS(
    ere_etl,
    "data_intermediate/etl/ere/ere_etl.rds"
   )
   "data_intermediate/etl/ere/ere_etl.rds" },
  format = "file"
 ),

 ## YERE ETL RDS ----

 targets::tar_target(
  yere_etl_rds,
  { dir.create(
   "data_intermediate/etl/yere",
   recursive = TRUE,
   showWarnings = FALSE
  ) 
   saveRDS(
    yere_etl,
    "data_intermediate/etl/yere/yere_etl.rds"
   )
   "data_intermediate/etl/yere/yere_etl.rds" },
  format = "file"
 ),
 
 ## BHN-Wide ETL RDS ----
 
targets::tar_target(
  bhn_wide_rds,
  {
    out <- "data_intermediate/etl/bhn_wide/bhn_wide_etl.rds"
    dir.create(
      dirname(
        out
        ),
      recursive = TRUE,
      showWarnings = FALSE
      )
    saveRDS(
      bhn_wide_full_data,
      out
      )
    out
  },
  format = "file",
  deployment = "main"
  )

)
