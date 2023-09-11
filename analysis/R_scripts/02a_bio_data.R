# Author: Kevin See
# Purpose: create tag lists to feed to PTAGIS query
# Created: 8/15/2023
# Last Modified: 9/8/2023
# Notes:

#-----------------------------------------------------------------
# load needed libraries
library(PITcleanr)
library(tidyverse)
library(readxl)
library(lubridate)
library(janitor)
library(magrittr)
library(writexl)
library(here)

#-----------------------------------------------------------------
# which spawn years are we interested in?
# spwn_yrs <- 2021:2023
#
# # read in tagging and recapture data from Priest for those years
# sthd_tags <-
#   tibble(spawn_year = spwn_yrs) |>
#   mutate(tag_data = map(spawn_year,
#                         .f = function(yr) {
#                           read_csv(here("analysis/data/raw_data",
#                                         "tagging_recapture",
#                                         paste0("Priest Tagging Detail ",
#                                                yr, ".csv")),
#                                    show_col_types = FALSE) |>
#                             clean_names() |>
#                             mutate(across(contains("_date_"),
#                                           mdy)) |>
#                             add_column(type = "mark",
#                                        .before = 0) |>
#                             bind_rows(read_csv(here("analysis/data/raw_data",
#                                                     "tagging_recapture",
#                                                     paste0("Priest Recapture Detail ",
#                                                            yr, ".csv")),
#                                                show_col_types = FALSE) |>
#                                         clean_names() |>
#                                         mutate(across(contains("_date_"),
#                                                       mdy)) |>
#                                         add_column(type = "recap",
#                                                    .before = 0))
#                         })) |>
#   unnest(tag_data)

#-----------------------------------------------------------------
# one file for 2011 - 2023

# uses tagging detail report and recapture detail report
# sthd_tags <-
#   read_csv(here("analysis/data/raw_data",
#                 "tagging_recapture",
#                 "Priest Tagging Detail 2011-2023.csv"),
#            show_col_types = FALSE) |>
#   clean_names() |>
#   mutate(across(contains("_date_"),
#                 mdy)) |>
#   add_column(type = "mark",
#              .before = 0) |>
#   bind_rows(read_csv(here("analysis/data/raw_data",
#                           "tagging_recapture",
#                           "Priest Recapture Detail 2011-2023.csv"),
#                      show_col_types = FALSE) |>
#               clean_names() |>
#               mutate(across(contains("_date_"),
#                             mdy)) |>
#               add_column(type = "recap",
#                          .before = 0)) |>
#   mutate(spawn_year = if_else(type == "mark",
#                               if_else(month(release_date_mmddyyyy) < 7,
#                                       year(release_date_mmddyyyy),
#                                       year(release_date_mmddyyyy) + 1),
#                               if_else(month(recap_date_mmddyyyy) < 7,
#                                       year(recap_date_mmddyyyy),
#                                       year(recap_date_mmddyyyy) + 1))) |>
#   relocate(spawn_year,
#            .before = 0)

# # pull out MRR data about all steelhead tags
# bio_df <-
#   sthd_tags |>
#   mutate(ptagis_file_nm = if_else(type == "mark",
#                                   mark_file_name,
#                                   recap_file_name)) |>
#   select(spawn_year,
#          ptagis_file_nm) |>
#   distinct() |>
#   arrange(spawn_year,
#           ptagis_file_nm) |>
#   # slice(1:6) |>
#   mutate(tag_file = map(ptagis_file_nm,
#                         .f = function(x) {
#                           out <-
#                             tryCatch(queryMRRDataFile(x),
#                                      error =
#                                        function(cond) {
#                                          message(paste("Error with file", x))
#                                          message("Error message:")
#                                          message(cond)
#                                          return(NULL)
#                                        },
#                                      warning =
#                                        function(cond) {
#                                          message(paste("Warning with file", x))
#                                          message("Warning message:")
#                                          message(cond)
#                                          return(NULL)
#                                        })
#                           if("spawn_year" %in% names(out)) {
#                             out <- out |>
#                               select(-spawn_year)
#                           }
#
#                           return(out)
#                         })) |>
#   unnest(tag_file) |>
#   # fix problems with one PTAGIS file
#   mutate(
#     across(
#       event_date,
#       ~ if_else(ptagis_file_nm == "TLM-2011-165-PRD.xml",
#                 release_date,
#                 .))) |>
#   mutate(year = if_else(month(event_date) < 7,
#                               year(event_date),
#                               year(event_date) + 1)) |>
#   relocate(year,
#            .before = 0) |>
#   distinct() |>
#   filter(str_detect(species_run_rear_type, "^3")) |>
#   arrange(year,
#           event_date)
#
#
# bio_df |>
#   filter(pit_tag %in% pit_tag[duplicated(pit_tag)]) |>
#   arrange(pit_tag, event_date) |>
#   select(year,
#          ptagis_file_nm,
#          pit_tag,
#          event_date,
#          event_type) |>
#   slice(11:20)
#


#-----------------------------------------------------------------
# uses a complete tag history of marks/recaptures at PRD or PRDLD1
sthd_tags <- read_csv(here("analysis/data/raw_data",
                           "tagging_recapture",
                           "Priest Rapids Mark_Recapture 2011-2023.csv"),
                      show_col_types = F) |>
  clean_names() |>
  mutate(across(contains("_date_mm"),
                mdy),
         across(contains("date_time"),
                mdy_hms)) |>
  # fix problems with one PTAGIS file
  mutate(
    across(
      event_date_mmddyyyy,
      ~ if_else(event_file_name == "TLM-2011-165-PRD.xml",
                event_release_date_mmddyyyy ,
                .)),
    across(
      event_date_time_value,
      ~ if_else(event_file_name == "TLM-2011-165-PRD.xml",
                event_release_date_time_value,
                .))) |>
  mutate(spawn_year = if_else(month(event_date_mmddyyyy) < 6,
                              year(event_date_mmddyyyy),
                              year(event_date_mmddyyyy) + 1))

# remove tags that are spawning in a future year
sthd_tags <-
  sthd_tags |>
  filter(spawn_year <= year(today()))

# pull out MRR data about all steelhead tags
bio_df <-
  sthd_tags |>
  select(spawn_year,
         event_file_name) |>
  distinct() |>
  arrange(spawn_year,
          event_file_name) |>
  # slice(1:6) |>
  mutate(tag_file = map(event_file_name,
                        .f = function(x) {
                          out <-
                            tryCatch(queryMRRDataFile(x),
                                     error =
                                       function(cond) {
                                         message(paste("Error with file", x))
                                         message("Error message:")
                                         message(cond)
                                         return(NULL)
                                       },
                                     warning =
                                       function(cond) {
                                         message(paste("Warning with file", x))
                                         message("Warning message:")
                                         message(cond)
                                         return(NULL)
                                       })
                          if("spawn_year" %in% names(out)) {
                            out <- out |>
                              select(-spawn_year)
                          }

                          return(out)
                        })) |>
  unnest(tag_file) |>
  # fix problems with one PTAGIS file
  mutate(
    across(
      event_date,
      ~ if_else(event_file_name == "TLM-2011-165-PRD.xml",
                release_date,
                .))) |>
  mutate(year = if_else(month(event_date) < 6,
                        year(event_date),
                        year(event_date) + 1)) |>
  relocate(year,
           .before = 0) |>
  distinct() |>
  filter(str_detect(species_run_rear_type, "^3"),
         str_detect(pit_tag, "\\.\\.\\.", negate = T)) |>
  arrange(year,
          event_date)


bio_df |>
  group_by(spawn_year) |>
  summarize(n_mrr_tags = n_distinct(pit_tag),
            n_2tag = sum(!is.na(second_pit_tag)),
            .groups = "drop") |>
  full_join(sthd_tags |>
              group_by(spawn_year) |>
              summarize(n_sthd_tags = n_distinct(tag_code),
                        .groups = "drop")) |>
  mutate(diff = n_mrr_tags - n_sthd_tags)

yr = 2015
bio_df |>
  filter(spawn_year == yr) |>
  anti_join(sthd_tags |>
              filter(spawn_year == yr) |>
              select(pit_tag = tag_code)) |>
  as.data.frame()


bio_df |>
  filter(pit_tag %in% pit_tag[duplicated(pit_tag)]) |>
  arrange(pit_tag, event_date) |>
  select(year,
         event_file_name,
         pit_tag,
         event_date,
         event_type) |>
  filter(year == 2023)

#-----------------------------------------------------------------
tabyl(bio_df,
      year,
      species_run_rear_type) |>
  adorn_totals(where = "both")


#-----------------------------------------------------------------
# add age and final origin data from scales
scale_age_file <- "T:/DFW-Team FP Upper Columbia Escapement - General/UC_Sthd/inputs/Bio Data/PIT Tag PRD Scale Ages Run Years 2022 to present.xlsx"

scale_age_df <-
  tibble(sheet_nm = excel_sheets(scale_age_file)) |>
  mutate(scale_data = map(sheet_nm,
                          .f = function(x) {
                            read_excel(scale_age_file,
                                       sheet = x) |>
                              clean_names()
                          })) |>
  unnest(scale_data) |>
  filter(spawn_year %in% bio_df$spawn_year) |>
  mutate(across(age_scales,
                str_to_title),
         across(age_scales,
                ~ if_else(nchar(.) > 10 & !is.na(as.numeric(.)),
                          as.character(round(as.numeric(.), 1)),
                          .)))

scale_age_df |>
  filter(age_scales == "Unreadable") |>
  filter(primary_pit_tag %in% primary_pit_tag[duplicated(primary_pit_tag)]) |>
  arrange(spawn_year,
          primary_pit_tag)

scale_age_df |>
  filter(!(primary_pit_tag %in% primary_pit_tag[duplicated(primary_pit_tag)] &
             age_scales == "Unreadable")) |>
  filter(primary_pit_tag %in% primary_pit_tag[duplicated(primary_pit_tag)]) |>
  arrange(spawn_year,
          primary_pit_tag)

setdiff(unique(scale_age_df$ptagis_file_name), unique(bio_df$event_file_name))
setdiff(unique(bio_df$event_file_name[bio_df$spawn_year %in% unique(scale_age_df$spawn_year)]),
        unique(scale_age_df$ptagis_file_name))

scale_age_df |>
  filter(primary_pit_tag %in% bio_df$pit_tag) |>
  filter(!ptagis_file_name %in% unique(bio_df$event_file_name)) |>
  select(spawn_year,
         pit_tag = primary_pit_tag,
         ptagis_file_name) |>
  left_join(bio_df |>
              select(spawn_year,
                     pit_tag,
                     event_file_name) |>
              distinct())



# which tags don't have scale data associated with them?
bio_df |>
  filter(spawn_year %in% unique(scale_age_df$spawn_year)) |>
  full_join(scale_age_df |>
              filter(!(primary_pit_tag %in% primary_pit_tag[duplicated(primary_pit_tag)] &
                         age_scales == "Unreadable")) |>
              group_by(primary_pit_tag) |>
              filter(!is.na(origin_scales)) |>
              slice(1) |>
              ungroup() |>
              mutate(scale_age_data = T) |>
              select(-c(sheet_nm,
                        run_year,
                        ptagis_file_name)),
            by = join_by(spawn_year,
                         pit_tag == primary_pit_tag)) |>
  filter(is.na(scale_age_data)) %>%
  arrange(event_date,
          pit_tag) |>
  select(spawn_year,
         pit_tag,
         event_date,
         event_type,
         contains("comments"),
         species_run_rear_type,
         age_scales,
         origin_field,
         origin_scales) |>
  write_csv(here("outgoing/other/missing_scale_data.csv"))
  # mutate(event_month = month(event_date,
  #                            label = T)) |>
  # tabyl(spawn_year, event_month) |>
  # adorn_totals(where = "both")

#-----------------------------------------------------------------
# save as Excel file
#-----------------------------------------------------------------
bio_df %>%
  split(list(.$year)) %>%
  write_xlsx(path = here('analysis/data/derived_data',
                         'PRA_Sthd_BioData_v2.xlsx'))

#-----------------------------------------------------------------
# for tag lists
#-----------------------------------------------------------------
# put bounds around years
min_yr = min(bio_df$year)
max_yr = max(bio_df$year)


# pull out PIT tag numbers by spawn year
tag_list <-
  bio_df |>
  select(year,
         contains("pit_tag")) |>
  pivot_longer(cols = contains("pit_tag"),
               names_to = "source",
               values_to = "tag_code") %>%
  filter(!is.na(tag_code)) |>
  select(-source) |>
  distinct() |>
  nest(.by = year)

# save tags to upload to PTAGIS
# # all years
# for(yr in tag_list$year) {
#   tag_list |>
#     filter(year == yr) |>
#     pull(data) |>
#     extract2(1) |>
#     write_delim(file = here('analysis/data/raw_data/tag_lists',
#                             paste0('test_UC_Sthd_Tags_', yr, '.txt')),
#                 delim = '\n',
#                 col_names = F)
# }

# just write the latest year
tag_list |>
  filter(year == max_yr) |>
  pull(data) |>
  extract2(1) |>
  write_delim(file = here('analysis/data/raw_data/tag_lists',
                          paste0('UC_Sthd_Tags_', max_yr, '.txt')),
              delim = '\n',
              col_names = F)

#-----------------------------------------------------------------
# save biological data for later
write_rds(bio_df,
          file = here('analysis/data/derived_data',
                      paste0('Bio_Data_', min_yr, '_', max_yr, '.rds')))

#-----------------------------------------------------------------
# decode conditional comments
cond_comm_codes <- read_csv(
  paste0("T:/DFW-Team FP Upper Columbia Escapement - General/",
         "UC_Sthd/inputs/PTAGIS/",
         "Glossary_ConditionalComment_ValidationCodes.csv")) |>
  clean_names()

bio_df |>
  filter(year == 2023) |>
  select(pit_tag,
         conditional_comments) |>
  # sample_n(1000) |>
  mutate(tmp = map(conditional_comments,
                   .f = function(x) {
                     str_split(x, "[:space:]") |>
                       unlist() |>
                       as_tibble() |>
                       rename(code = value)
                   })) |>
  select(-conditional_comments) |>
  unnest(tmp) |>
  left_join(cond_comm_codes |>
              select(-definition))

bio_df |>
  filter(year == 2023) |>
  select(pit_tag,
         conditional_comments,
         sex,
         adipose_clip,
         contains("cwt")) |>
  filter(is.na(cwt_s),
         str_detect(conditional_comments, "CW"))
