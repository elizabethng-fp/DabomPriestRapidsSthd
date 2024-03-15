# Author: Kevin See
# Purpose: compile age data from multiple sources and create a standard formatted file
# Created: 3/14/2024
# Last Modified: 3/14/2024
# Notes:

#-----------------------------------------------------------------
# load needed libraries
library(PITcleanr)
library(tidyverse)
library(readxl)
library(lubridate)
library(janitor)
library(writexl)
library(here)

#-----------------------------------------------------------------
# 2011 - 2019
# read in older ages from WDFW database
wdfw_ages <-
  read_excel(here("analysis/data/raw_data/WDFW",
                  "UCME_STHD_PRD_RunYears_2010-2018_3.12.24.xlsx")) |>
  clean_names() |>
  mutate(spawn_year = year(survey_date) + 1) |>
  rename(pit_tag = primary_tag,
         second_pit_tag = secondary_tag,
         event_date = survey_date) |>
  filter(pit_tag != "-") |>
  mutate(species_run_rear_type = paste0(recode(species_final,
                                               "ST" = "3"),
                                        recode(run_final,
                                               "SU" = "2"),
                                        origin),
         scale_id = paste0(str_pad(scale_card,
                                   3,
                                   side = "left",
                                   pad = "0"),
                           "-",
                           str_pad(scale_cell,
                                   2,
                                   side = "left",
                                   pad = "0")),
         across(age_scales,
                ~ str_replace(., "^r", "R"))) |>
  arrange(spawn_year,
          event_date,
          pit_tag) |>
  select(primary_pit_tag = pit_tag,
         second_pit_tag,
         spawn_year,
         scale_card,
         scale_cell,
         # scale_id,
         age_scales,
         origin_scales,
         origin_final,
         final_age_source) |>
  pivot_longer(contains("pit"),
               names_to = "source",
               values_to = "pit_tag") |>
  filter(!is.na(pit_tag)) |>
  select(-source) |>
  mutate(run_year = spawn_year - 1) |>
  relocate(primary_pit_tag = pit_tag,
           run_year,
           .before = 0)

tabyl(wdfw_ages,
      spawn_year,
      final_age_source)

#-----------------------------------------------------------------
# 2020 - 2021
# use data sent directly from Denny Snyder at BioAnalysts

# 2020
ages_2020 <-
  read_csv(here('analysis/data/raw_data/WDFW',
                'NBD-2019-189-PRD 1.csv'),
           show_col_types = F) %>%
  janitor::clean_names() |>
  mutate(run_year = spawn_year - 1) |>
  select(primary_pit_tag = pit_tag,
         second_pit_tag,
         run_year,
         spawn_year,
         scale_id,
         age_scales = scale_age,
         origin_scales = scale_origin,
         origin_field = field_origin,
         origin_final = final_origin) |>
  mutate(scale_card = str_split(scale_id, "-", simplify = T)[,1],
         scale_cell = str_split(scale_id, "-", simplify = T)[,2],
         across(scale_cell,
                as.numeric)) |>
  relocate(scale_card,
           scale_cell,
           .before = scale_id) |>
  mutate(final_age_source = if_else(!is.na(age_scales),
                                    "SC",
                                    NA_character_))

# 2021
ages_2021 <-
  read_csv(here('analysis/data/raw_data/WDFW',
                'CME-2020-192-PRD correct columns for Kevin final 2-10-21.csv'),
           show_col_types = F) |>
  janitor::clean_names() |>
  select(primary_pit_tag = pit_tag,
         second_pit_tag,
         run_year = migration_year,
         spawn_year,
         scale_id,
         age_scales = scale_age,
         origin_scales = scale_origin,
         origin_field = field_origin,
         origin_final = final_origin) |>
  distinct() |>
  mutate(spawn_year = run_year + 1) |>
  mutate(scale_card = str_split(scale_id, "-", simplify = T)[,1],
         scale_cell = str_split(scale_id, "-", simplify = T)[,2],
         across(scale_cell,
                as.numeric)) |>
  relocate(scale_card,
           scale_cell,
           .before = scale_id) |>
  mutate(final_age_source = if_else(!is.na(age_scales),
                                    "SC",
                                    NA_character_))

# ages_2020 |>
#   bind_rows(ages_2021) |>
#   filter(!is.na(second_pit_tag))
#
#
# denny_ages <-
#   ages_2020 |>
#   bind_rows(ages_2021) |>
#   select(-second_pit_tag,
#          -scale_id)
#
#
# intersect(names(wdfw_ages),
#           names(denny_ages))
# setdiff(names(wdfw_ages),
#         names(denny_ages))
# setdiff(names(denny_ages),
#         names(wdfw_ages))

#-----------------------------------------------------------------
# 2022 -
# read in file from Teams
recent_age_file <- "T:/DFW-Team FP Upper Columbia Escapement - General/UC_Sthd/inputs/Bio Data/PIT Tag PRD Scale Ages Run Years 2022 to present.xlsx"

recent_ages <-
  tibble(sheet_nm = excel_sheets(recent_age_file)) |>
  mutate(scale_data = map(sheet_nm,
                          .f = function(x) {
                            read_excel(recent_age_file,
                                       sheet = x) |>
                              clean_names() |>
                              mutate(spawn_year = str_remove(x, "^SY"),
                                     across(spawn_year,
                                            as.numeric),
                                     run_year = spawn_year - 1)
                          },
                          .progress = T)) |>
  unnest(scale_data) |>
  mutate(scale_cell = str_split(scale_card, "-", simplify = T)[,2],
         across(scale_cell,
                as.numeric),
         across(scale_card,
                ~ str_remove(., "-\\w+"))) |>
  filter(!is.na(primary_pit_tag)) |>
  mutate(final_age_source = if_else(!is.na(age_scales),
                                    "SC",
                                    NA_character_))

#-----------------------------------------------------------------
# put together
all_ages <-
  wdfw_ages |>
  bind_rows(ages_2020,
            ages_2021,
            recent_ages) |>
  # select(any_of(names(wdfw_ages)))
  select(any_of(union(names(wdfw_ages),
                      names(ages_2020)))) |>
  select(-second_pit_tag) |>
  mutate(across(scale_card,
                ~ str_pad(.,
                          width = 3,
                          side = "left",
                          pad = "0"))) |>
  relocate(scale_id,
           .after = scale_cell) |>
  relocate(origin_field,
           .before = origin_final) |>
  # fix a few typo things in the ages
  mutate(across(age_scales,
                str_to_upper),
         across(age_scales,
                ~ recode(.,
                         "R" = "R.0",
                         "R." = "R.0",
                         "R.`" = "R.0")),
         across(age_scales,
                ~ str_remove(., "^W"))) |>
  mutate(
         across(age_scales,
                ~ if_else(nchar(.) > 10 & !is.na(as.numeric(.)),
                          as.character(round(as.numeric(.), 1)),
                          .)))


#-----------------------------------------------------------------
# add PTAGIS file name
# uses a complete tag history of marks/recaptures at PRD or PRDLD1
sthd_tags <- read_csv(here("analysis/data/raw_data",
                           "tagging_recapture",
                           "PriestRapids_Mark_Recapture_2011_2024.csv"),
                      show_col_types = F) |>
  clean_names() |>
  mutate(across(contains("_date_mm"),
                mdy),
         across(contains("date_time"),
                mdy_hms)) |>
  mutate(spawn_year = if_else(month(event_date_mmddyyyy) < 6,
                              year(event_date_mmddyyyy),
                              year(event_date_mmddyyyy) + 1))

# # missing some tags, presumeably because they're listed as second tags in PTAGIS
# all_ages |>
#   anti_join(sthd_tags |>
#               select(spawn_year,
#                      pit_tag = tag_code,
#                      ptagis_file_name = event_file_name),
#             by = join_by(spawn_year,
#                          primary_pit_tag == pit_tag))


# pull out MRR data about all steelhead tags
tagging_df <-
  sthd_tags |>
  select(spawn_year,
         event_file_name) |>
  distinct() |>
  arrange(spawn_year,
          event_file_name) |>
  # filter out one tagging file that is not a WDFW project
  filter(event_file_name != "CLD14105.WA1") |>
  mutate(tag_file = map(event_file_name,
                        .f = function(x) {
                          out <-
                            tryCatch(PITcleanr::queryMRRDataFile(x),
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
                        },
                        .progress = T)) |>
  unnest(tag_file) |>
  filter(life_stage == "Adult") |>
  distinct()

# choose a singular PTAGIS file name to associate with each tag
ptagis_file_df <-
  tagging_df |>
  select(spawn_year,
         pit_tag,
         second_pit_tag,
         event_type,
         release_date,
         ptagis_file_name = event_file_name) |>
  nest(.by = c(spawn_year,
               pit_tag)) |>
  mutate(keep_data = map(data,
                         .f = function(x) {
                           if(sum(x$event_type == "Mark") > 0) {
                             keep_row <-
                               x |>
                               filter(event_type == "Mark")
                           } else {
                             keep_row = x
                           }

                           if(nrow(keep_row) > 1) {
                             keep_row <-
                               keep_row |>
                               filter(release_date == min(release_date))
                           }
                           if(nrow(keep_row) > 1) {
                             keep_row <-
                               keep_row |>
                               slice(1)
                           }
                           return(keep_row)
                         },
                         .progress = TRUE)) |>
  unnest(keep_data) |>
  select(-c(data,
            event_type,
            release_date)) |>
  pivot_longer(contains("pit"),
               names_to = "source",
               values_to = "pit_tag") |>
  filter(!is.na(pit_tag)) |>
  select(-source) |>
  distinct()


# any tags with age data missing a PTAGIS file name?
all_ages |>
  anti_join(ptagis_file_df,
            by = join_by(spawn_year,
                         primary_pit_tag == pit_tag))

# fix a few tags that were listed with the wrong spawn year
switch_spwn_yrs <-
  all_ages |>
  anti_join(ptagis_file_df,
            by = join_by(spawn_year,
                         primary_pit_tag == pit_tag)) |>
  select(pit_tag = primary_pit_tag) |>
  left_join(tagging_df |>
              select(pit_tag,
                     spawn_year,
                     release_date))

all_ages <-
  all_ages |>
  left_join(switch_spwn_yrs |>
              rename(spawn_year_v2 = spawn_year),
            by = join_by(primary_pit_tag == pit_tag)) |>
  mutate(across(run_year,
                ~if_else(!is.na(spawn_year_v2),
                         spawn_year_v2 - 1,
                         .)),
         across(spawn_year,
                ~ if_else(!is.na(spawn_year_v2),
                          spawn_year_v2,
                          .))) |>
  select(-spawn_year_v2,
         -release_date)

#-----------------------------------------
all_ages <-
  all_ages |>
  left_join(ptagis_file_df,
            by = join_by(spawn_year,
                         primary_pit_tag == pit_tag))

tabyl(all_ages,
      spawn_year)

#-----------------------------------------
age_list <-
  all_ages |>
  split(f = ~ spawn_year) |>
  rlang::set_names(nm = function(x) {
    paste0("SY", x)
  })

write_xlsx(age_list,
           paste("T:/DFW-Team FP Upper Columbia Escapement - General/UC_Sthd/inputs/Bio Data/PIT Tag PRD Scale Ages Spawn Years", min(all_ages$spawn_year), "to present.xlsx"))


