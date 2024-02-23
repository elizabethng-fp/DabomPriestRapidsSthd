# Author: Kevin See
# Purpose: compare PTAGIS MRR biological data to older data compiled across multiple years
# Created: 12/20/2023
# Last Modified: 1/22/2024
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
# read in old biological data

# transcription code errors
transcribe_err <-
  read_excel(here("T:/DFW-Team FP Upper Columbia Escapement - General/UC_Sthd/inputs/BioData_Vs_PTagis_TagAdjustments",
                  "STHD Adjustments 2011-2022.xlsx"),
             "Transcription Errors",
             skip = 1) |>
  clean_names() |>
  rename(spawn_year = x1,
         comment = x4)

dbl_tag <-
  read_excel(here("T:/DFW-Team FP Upper Columbia Escapement - General/UC_Sthd/inputs/BioData_Vs_PTagis_TagAdjustments",
                  "STHD Adjustments 2011-2022.xlsx"),
             "Undocumented Double Tags in P4",
             skip = 1) |>
  clean_names()

old_bio <-
  read_csv(here("T:/DFW-Team FP Upper Columbia Escapement - General/UC_Sthd/inputs/Bio Data",
                "Old_bio_data_2011-2022.csv"),
           show_col_types = F) |>
  rename(spawn_year = year) |>
  mutate(run_year = spawn_year - 1,
         sheet_nm = paste0("SY", spawn_year)) |>
  select(sheet_nm,
         # primary_pit_tag = tag_code,
         pit_tag = tag_code,
         second_pit_tag = tag_other,
         run_year,
         spawn_year,
         event_date = trap_date,
         sex_old = sex,
         ad_clip_old = ad_clip,
         cwt_old = cwt,
         age_scales = age,
         fork_length_old = fork_length,
         origin_old = origin) |>
  mutate(across(sex_old,
                ~ recode(.,
                         "F" = "Female",
                         "M" = "Male")),
         across(fork_length_old,
                ~ if_else(. < 100,
                          . * 10,
                          .))) |>
  mutate(across(c(ad_clip_old,
                  cwt_old),
                ~ if_else(is.na(.), F, T))) |>
  # fix some transcription errors with PIT tag codes
  left_join(transcribe_err |>
              select(-comment),
            by = join_by(spawn_year,
                         pit_tag == ucm_e_db_wrong)) |>
  mutate(across(pit_tag,
                ~ if_else(!is.na(p4_db_correct),
                          p4_db_correct,
                          .))) |>
  select(-p4_db_correct) |>
  arrange(spawn_year,
          pit_tag,
          event_date)


#-----------------------------------------------------------------
# read in data from file that comes from WDFW database
wdfw_db <-
  read_excel(here("analysis/data/raw_data/WDFW",
                  "UCM&E_STHD_PRD_RunYears_2010-2018.xlsx")) |>
  clean_names() |>
  mutate(spawn_year = year(survey_date) + 1) |>
  rename(pit_tag = primary_tag,
         second_pit_tag = secondary_tag,
         event_date = survey_date,
         sex = sex_final,
         origin = origin_final) |>
  filter(pit_tag != "-") |>
  mutate(across(fork_length,
                ~ if_else(. < 100,
                          . * 10,
                          .)),
         cwt = if_else(is.na(cwt_sn) & is.na(cwt_do) & is.na(cwt_ad),
                       F, T),
         across(ad_clip,
                ~ if_else(!is.na(.),
                          T, F)),
         species_run_rear_type = paste0(recode(species_final,
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
         across(sex,
                ~ recode(.,
                         "M" = "Male",
                         "F" = "Female")),
         across(age_scales,
                ~ str_replace(., "^r", "R"))) |>
  arrange(spawn_year,
          event_date,
          pit_tag)

#-----------------------------------------------------------------
# uses a complete tag history of marks/recaptures at PRD or PRDLD1
sthd_tags <- read_csv(here("analysis/data/raw_data",
                           "tagging_recapture",
                           "PriestRapids_Mark_Recapture_2011_2023.csv"),
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
  filter(spawn_year < year(today()))

# pull out MRR data about all steelhead tags
tagging_df <-
  sthd_tags |>
  select(spawn_year,
         event_file_name) |>
  distinct() |>
  # filter out one tagging file that is not a WDFW project
  filter(event_file_name != "CLD14105.WA1") |>
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
          event_date) |>
  mutate(across(poh,
                as.numeric)) |>
  # assign sex based on conditional comments
  mutate(sex = case_when(str_detect(conditional_comments, "FE") ~ "Female",
                         str_detect(conditional_comments, "MA") ~ "Male",
                         .default = NA_character_)) |>
  # determine CWT and ad-clip status from conditional comments
  mutate(cwt = if_else(str_detect(conditional_comments, "CP") |
                         str_detect(conditional_comments, "CW"),
                       T, F),
         ad_clip = case_when(str_detect(conditional_comments, "AD") ~ T,
                             str_detect(conditional_comments, "AI") ~ F,
                             .default = NA)) |>
  # correct any lengths that were mistakenly entered as cm, convert to mm
  mutate(across(length,
                ~ if_else(. < 100,
                          . * 10,
                          .)))

# some second PIT tags only appear in text comments
second_tags <-
  tagging_df |>
  filter(str_detect(text_comments, coll("same as", ignore_case = T))) |>
  mutate(text_code = str_split(text_comments, coll("same as", ignore_case = T), simplify = T)[,2],
         across(text_code,
                ~ str_remove(., "\\#")),
         across(text_code,
                ~ str_remove(., "SD$")),
         across(text_code,
                str_squish),
         across(text_code,
                ~ str_remove(., " ")),
         across(text_code,
                str_to_upper),
         across(text_code,
                ~ if_else(str_detect(text_code, "^3", negate = T),
                          paste0("3D9.", .),
                          .))) |>
  filter(second_pit_tag != text_code) |>
  select(spawn_year,
         pit_tag,
         second_pit_tag,
         text_comments,
         text_code)

second_tags
# the two tags left were recorded incorrectly in the comments (neither of those codes exist in PTAGIS).
# The second tag recorded in PTAGIS now is correct

# fish with second tag in comments, is that recorded in WDFW database?
second_tags |>
  left_join(wdfw_db |>
              select(spawn_year,
                     pit_tag,
                     second_pit_tag_wdfw = second_pit_tag)) |>
  filter(is.na(second_pit_tag_wdfw))



# tagging_df <-
#   tagging_df |>
#   left_join(second_tags |>
#               select(spawn_year,
#                      pit_tag,
#                      text_code)) |>
#   mutate(across(second_pit_tag,
#                 ~ if_else(is.na(.) & !is.na(text_code),
#                           text_code,
#                           .))) |>
#   select(-text_code)

# pull out certain columns to use going forward
bio_df <-
  tagging_df |>
  mutate(origin = str_extract(species_run_rear_type, "[:alpha:]$")) |>
  select(spawn_year,
         event_file_name,
         pit_tag,
         second_pit_tag,
         event_date,
         event_type,
         species_run_rear_type,
         origin,
         sex,
         cwt,
         ad_clip,
         fork_length = length,
         poh,
         conditional_comments,
         text_comments,
         release_date,
         scale_id)

names(wdfw_db)[!names(wdfw_db) %in% names(bio_df)]
names(bio_df)[!names(bio_df) %in% names(wdfw_db)]

intersect(names(wdfw_db), names(bio_df))

bio_df |>
  select(spawn_year,
         event_file_name,
         pit_tag,
         event_type,
         event_date,
         release_date) |>
  mutate(across(ends_with("date"),
                ~ floor_date(., unit = "days"))) |>
  left_join(sthd_tags |>
              select(event_file_name,
                     pit_tag = tag_code,
                     event_type = event_type_name,
                     event_date_mmddyyyy,
                     event_release_date_mmddyyyy)) |>
  arrange(spawn_year,
          event_file_name,
          pit_tag,
          event_date) |>
  filter(event_date != event_date_mmddyyyy |
           release_date != event_release_date_mmddyyyy) #|>
  group_by(spawn_year) |>
  summarize(n_records = n_distinct(event_file_name))


#------------------------------------------------------------
# any tags appear more than once with different SRRs?
srr_comp <-
  bio_df |>
  filter(pit_tag %in% pit_tag[duplicated(pit_tag)]) |>
  group_by(pit_tag) |>
  mutate(n_srr = n_distinct(species_run_rear_type)) |>
  ungroup() |>
  filter(n_srr > 1) |>
  arrange(pit_tag, spawn_year) |>
  select(spawn_year,
         event_file_name,
         pit_tag,
         event_date,
         event_type,
         conditional_comments,
         SRR = species_run_rear_type)

srr_comp


#-----------------------------------------------------------------
# read in recent age and final origin data from scales
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
  filter(spawn_year %in% unique(bio_df$spawn_year)) |>
  mutate(across(age_scales,
                str_to_title),
         across(age_scales,
                ~ if_else(nchar(.) > 10 & !is.na(as.numeric(.)),
                          as.character(round(as.numeric(.), 1)),
                          .))) |>
  mutate(scale_id = if_else(!is.na(scale_card),
                            scale_card,
                            scale_cell)) |>
  relocate(scale_id,
           .after = "scale_cell") |>
  select(-scale_card,
         -scale_cell) |>
  distinct()

# filter out rows for duplicated tags that have "Unreadable" ages
scale_age_df <-
  scale_age_df |>
  unite(sy_pit,
        spawn_year, primary_pit_tag,
        remove = F) |>
  filter(!(sy_pit %in% sy_pit[duplicated(sy_pit)] &
             age_scales == "Unreadable")) |>
  # for one tag with multiple ages, choose W1.2 (Mike Hughes said so)
  filter(!(primary_pit_tag == "3DD.003D552F68" &
             age_scales == "R.2")) |>
  select(-sy_pit)


# compile older age data into format similar to what's currently being provided
old_age_df <-
  old_bio |>
  select(spawn_year,
         run_year,
         pit_tag,
         age_scales) |>
  distinct() |>
  group_by(spawn_year,
           pit_tag) |>
  mutate(id = 1:n()) |>
  ungroup() |>
  pivot_wider(names_from = id,
              values_from = age_scales) |>
  mutate(age = case_when(!is.na(`1`) & is.na(`2`) ~ `1`,
                         !is.na(`2`) & is.na(`1`) ~ `2`,
                         `2` == "NS" ~ `1`,
                         .default = NA_character_),
         sheet_nm = paste0("SY", spawn_year)) |>
  select(sheet_nm,
         primary_pit_tag = pit_tag,
         run_year,
         spawn_year,
         age_scales = age) |>
  # add MRR PTAGIS file name
  left_join(bio_df |>
              select(spawn_year,
                     event_file_name,
                     primary_pit_tag = pit_tag) |>
              distinct() |>
              group_by(spawn_year,
                       primary_pit_tag) |>
              summarize(ptagis_file_name = case_when(sum(str_detect(event_file_name, "xml$")) > 0 ~
                                                       event_file_name[str_detect(event_file_name, "xml$")][1],
                                                     .default = event_file_name[1]),
                        .groups = "drop"),
            by = join_by(primary_pit_tag, spawn_year)) |>
  add_column(scale_card = NA_character_,
             scale_cell = NA_character_,
             .after = 'spawn_year') |>
  add_column(origin_field = NA_character_,
             origin_scales = NA_character_,
             .after = 'age_scales')

old_age_df |>
  unite(id, spawn_year, primary_pit_tag) |>
  filter(id %in% id[duplicated(id)])


# one row per spawn year / tag?
old_age_df |>
  select(spawn_year,
         primary_pit_tag) |>
  distinct() |>
  nrow() |>
  identical(nrow(old_age_df))

# how many missing ages?
old_age_df |>
  mutate(missing_age = if_else(is.na(age_scales), T, F)) |>
  tabyl(spawn_year, missing_age) |>
  adorn_totals("both")


# which tags from bio_df aren't in old bio data?
bio_df |>
  filter(spawn_year %in% union(unique(old_age_df$spawn_year),
                               unique(scale_age_df$spawn_year))) |>
  left_join(old_age_df |>
              filter(!sheet_nm %in% unique(scale_age_df$sheet_nm)) |>
              bind_rows(scale_age_df) |>
              select(spawn_year,
                     pit_tag = primary_pit_tag,
                     age_scales) |>
              mutate(age_data_exists_v1 = T),
            by = join_by(spawn_year,
                         pit_tag)) |>
  left_join(old_age_df |>
              filter(!sheet_nm %in% unique(scale_age_df$sheet_nm)) |>
              bind_rows(scale_age_df) |>
              select(spawn_year,
                     second_pit_tag = primary_pit_tag,
                     age_scales_v2 = age_scales) |>
              mutate(age_data_exists_v2 = T),
            by = join_by(spawn_year,
                         second_pit_tag)) |>
  mutate(across(starts_with("age_data_exists"),
                ~ replace_na(., F))) |>
  mutate(age_data_exists = if_else(age_data_exists_v1 | age_data_exists_v2, T, F),
         age_scales = if_else(is.na(age_scales) & !is.na(age_scales_v2),
                              age_scales_v2,
                              age_scales)) |>
  filter(!age_data_exists) |>
  arrange(event_date,
          pit_tag) |>
  select(spawn_year,
         pit_tag,
         second_pit_tag,
         species_run_rear_type,
         event_date,
         event_type,
         contains("comments"),
         age_scales) |>
  # filter(!is.na(second_pit_tag)) |>
  tabyl(spawn_year) |> adorn_totals() |> adorn_pct_formatting()
  # write_csv(here("outgoing/other/missing_scale_data.csv"))

# save as Excel file, one tab per spawn year
age_list <-
  old_age_df |>
  nest(data = -sheet_nm) |>
  filter(!sheet_nm %in% unique(scale_age_df$sheet_nm)) |>
  bind_rows(scale_age_df |>
              nest(data = -sheet_nm)) |>
  mutate(my_list = map2(sheet_nm,
                        data,
                        .f = function(x, y) {
                          res = list(y)
                          names(res) = x
                          return(res)
                        }))

age_file = vector("list", nrow(age_list))
for(i in 1:nrow(age_list)) {
  age_file[[i]] = age_list$data[i][[1]]
  names(age_file)[i] = age_list$sheet_nm[i]
}

write_xlsx(age_file,
           paste("T:/DFW-Team FP Upper Columbia Escapement - General/UC_Sthd/inputs/Bio Data/PIT Tag PRD Scale Ages Spawn Years", min(old_age_df$spawn_year), "to present.xlsx"))


age_df <-
  age_file |>
  map_df(.f = identity,
         .id = "group") |>
  rename(pit_tag = primary_pit_tag) |>
  select(-group)




#-----------------------------------------------------------------
# primary PTAGIS file name for each tag
ptagis_file_df <-
  bio_df |>
  select(spawn_year,
         event_file_name,
         pit_tag) |>
  distinct() |>
  group_by(spawn_year,
           pit_tag) |>
  summarize(ptagis_file_name = case_when(sum(str_detect(event_file_name, "xml$")) > 0 ~
                                           event_file_name[str_detect(event_file_name, "xml$")][1],
                                         .default = event_file_name[1]),
            .groups = "drop")


bio_comp <-
  bio_df |>
  filter(spawn_year %in% unique(wdfw_db$spawn_year)) |>
  mutate(origin = str_extract(species_run_rear_type, "[:alpha:]")) |>
  select(spawn_year,
         pit_tag,
         event_date,
         release_date,
         event_type,
         SRR = species_run_rear_type,
         origin,
         sex,
         fork_length,
         cwt,
         ad_clip,
         event_file_name,
         conditional_comments) |>
  # left_join(ptagis_file_df,
  #           by = join_by(spawn_year, pit_tag)) |>
  # filter(event_file_name == ptagis_file_name) |>
  arrange(event_date,
          pit_tag) |>
  mutate(across(event_date,
                ~ floor_date(., unit = "days")))

# add age data
bio_comp <-
  bio_comp |>
  left_join(age_df |>
              select(spawn_year,
                     pit_tag,
                     age_scales)) |>
  # fix a few typo things in the ages
  mutate(across(age_scales,
                ~ recode(.,
                         "R" = "R.0",
                         "R." = "R.0")),
         across(age_scales,
                ~ str_remove(., "^w")))



# # keep first tag / spawn year combination
# bio_comp <-
#   bio_comp |>
#   unite(sy_tag,
#         spawn_year,
#         pit_tag,
#         remove = F) |>
#   # filter(sy_tag %in% sy_tag[duplicated(sy_tag)]) |>
#   group_by(sy_tag) |>
#   slice(1) |>
#   ungroup() |>
#   select(-sy_tag)



# # how many tags are missing fork_lengths in PTAGIS?
# # Any of those have fork_lengths in older data?
# length_comp <-
#   bio_comp |>
#   filter(is.na(fork_length)) |>
#   select(spawn_year,
#          event_date,
#          event_type,
#          pit_tag,
#          SRR,
#          # second_pit_tag,
#          fork_length,
#          event_file_name) |>
#   distinct() |>
#   left_join(old_bio |>
#               select(spawn_year,
#                      event_date,
#                      pit_tag,
#                      fork_length_old) |>
#               distinct(),
#             by = join_by(spawn_year,
#                          event_date,
#                          pit_tag)) |>
#   left_join(wdfw_db |>
#               select(spawn_year,
#                      event_date,
#                      pit_tag,
#                      fork_length_wdfw = fork_length) |>
#               distinct(),
#             by = join_by(spawn_year,
#                          event_date,
#                          pit_tag)) |>
#   mutate(missing_fork_length = if_else(is.na(fork_length_wdfw), T, F))
#
# length_comp |>
#   tabyl(spawn_year, missing_fork_length) |>
#   adorn_totals("both")
#
# length_comp |>
#   filter(missing_fork_length) |>
#   select(spawn_year,
#          pit_tag) |>
#   distinct() |>
#   left_join(bio_comp |>
#               select(spawn_year,
#                      pit_tag,
#                      event_date,
#                      fork_length)) |>
#   group_by(spawn_year,
#            pit_tag) |>
#   mutate(has_fl = if_else(sum(!is.na(fork_length)) > 0, T, F)) |>
#   filter(!has_fl)
#
# # how many tags are missing sex in PTAGIS?
# # Any of those have sex in older data?
# sex_comp <-
#   bio_comp |>
#   filter(is.na(sex)) |>
#   select(spawn_year,
#          event_date,
#          pit_tag,
#          SRR,
#          conditional_comments,
#          sex,
#          event_file_name) |>
#   distinct() |>
#   left_join(old_bio |>
#               select(spawn_year,
#                      event_date,
#                      pit_tag,
#                      sex_old) |>
#               distinct()) |>
#   left_join(wdfw_db |>
#               select(spawn_year,
#                      event_date,
#                      pit_tag,
#                      sex_wdfw = sex) |>
#               distinct()) |>
#   mutate(missing_sex = if_else(is.na(sex_wdfw), T, F))
#
# sex_comp |>
#   tabyl(spawn_year, missing_sex) |>
#   adorn_totals("both")
#
# # how many tags are missing CWT in PTAGIS? None
# bio_comp |>
#   filter(is.na(cwt))
#
# # how many tags are missing ad-clip status in PTAGIS?
# # Any of those have ad-clip status in older data?
# ad_clip_comp <-
#   bio_comp |>
#   filter(is.na(ad_clip)) |>
#   select(spawn_year,
#          pit_tag,
#          SRR,
#          conditional_comments,
#          ad_clip,
#          event_file_name) |>
#   distinct() |>
#   left_join(old_bio |>
#               select(spawn_year,
#                      pit_tag,
#                      ad_clip_old) |>
#               distinct(),
#             relationship = "many-to-many") |>
#   left_join(wdfw_db |>
#               select(spawn_year,
#                      pit_tag,
#                      ad_clip_wdfw = ad_clip) |>
#               distinct()) |>
#   mutate(missing_ad_clip_data = if_else(is.na(ad_clip_wdfw), T, F)) |>
#   arrange(pit_tag,
#           spawn_year)
#
# ad_clip_comp |>
#   tabyl(spawn_year, missing_ad_clip_data) |>
#   adorn_totals("both")

#-----------------------------------------------------------------
# missing data in PTAGIS
checkMissing <- function(var) {
  if(! var %in% names(bio_df)) {
    warning(paste(var, "not in columns of bio_df.\n"))
    stop()
  }
  bio_df |>
    filter(is.na(.data[[var]])) |>
    select(spawn_year, pit_tag) |>
    distinct() |>
    left_join(bio_df |>
                filter(!is.na(.data[[var]])),
              by = join_by(spawn_year, pit_tag)) |>
    filter(is.na(event_date)) |>
    select(spawn_year, pit_tag) |>
    distinct() |>
    left_join(bio_df |>
                select(spawn_year,
                       pit_tag,
                       event_file_name,
                       event_date,
                       release_date,
                       conditional_comments,
                       {{ var }}),
              by = join_by(spawn_year, pit_tag)) |>
    left_join(wdfw_db |>
                select(spawn_year,
                       pit_tag,
                       {{ var }}) |>
                rename(wdfw_value = {{ var }}),
              by = join_by(spawn_year, pit_tag))
}

na_origin = checkMissing("origin")
na_srr = checkMissing("species_run_rear_type")
na_length = checkMissing("fork_length")
na_sex = checkMissing("sex")
na_ad_clip = checkMissing("ad_clip")
na_cwt = checkMissing("cwt")
# na_age = checkMissing("age_scales")

#-----------------------------------------------------------------
# any missing tags from particular spawn years?
miss_tags <-
  bio_comp |>
  select(spawn_year,
         # event_file_name,
         pit_tag) |>
  distinct() |>
  mutate(record_ptagis = T) |>
  full_join(wdfw_db |>
              select(spawn_year,
                     pit_tag) |>
              distinct() |>
              mutate(record_wdfw = T)) |>
  mutate(across(starts_with("record_"),
                ~ replace_na(., F))) |>
  filter(!record_ptagis |
           !record_wdfw) |>
  mutate(missing_source = case_when(record_ptagis ~ "WDFW",
                                    record_wdfw ~ "PTAGIS",
                                    .default = NA_character_))

miss_tags |>
  tabyl(spawn_year,
        missing_source) |>
  adorn_totals("both")



#-----------------------------------------------------------------
# compare new and old values for some fields

# dates
date_comp <-
  bio_comp |>
  filter(spawn_year %in% unique(wdfw_db$spawn_year)) |>
  select(spawn_year,
         event_file_name,
         event_date,
         release_date,
         event_type,
         pit_tag) |>
  distinct() |>
  mutate(across(ends_with("date"),
                ~ floor_date(., unit = "days"))) |>
  mutate(record_ptagis = T) |>
  pivot_longer(cols = ends_with("date"),
               names_to = "type",
               values_to = "date") |>
  mutate(across(type,
                ~ str_remove(., "_date$"))) |>
  nest(ptagis_dates = c(type, date)) |>
  full_join(wdfw_db |>
              select(spawn_year,
                     event_date,
                     pit_tag,
                     event_date) |>
              distinct() |>
              mutate(record_wdfw = T) |>
              pivot_longer(cols = ends_with("date"),
                           names_to = "type",
                           values_to = "date") |>
              mutate(across(type,
                            ~ str_remove(., "_date$"))) |>
              nest(wdfw_dates = c(type, date))) |>
  left_join(old_bio |>
              select(spawn_year,
                     event_date,
                     pit_tag,
                     event_date) |>
              distinct() |>
              mutate(record_old = T) |>
              pivot_longer(cols = ends_with("date"),
                           names_to = "type",
                           values_to = "date") |>
              mutate(across(type,
                            ~ str_remove(., "_date$"))) |>
              nest(old_dates = c(type, date))) |>
  mutate(across(starts_with("record_"),
                ~ replace_na(., F)))

date_comp |>
  anti_join(miss_tags |>
              select(spawn_year,
                     pit_tag)) |>
  tabyl(record_ptagis,
        record_wdfw)




mismatch_dates <-
  date_comp |>
  filter(record_wdfw,
         record_ptagis) |>
  mutate(shared_dates = map2_lgl(ptagis_dates,
                                 wdfw_dates,
                                 .f = function(x, y) {
                                   if_else(sum(x$date %in% y$date) > 0 |
                                             sum(y$date %in% x$date) > 0,
                                           T, F)
                                 })) |>
  filter(! shared_dates)

mismatch_dates |>
  tabyl(event_file_name,
        spawn_year)

mismatch_dates |>
  rename(ptagis = ptagis_dates,
         wdfw = wdfw_dates,
         old = old_dates) |>
  select(-starts_with("record"),
         -shared_dates) |>
  slice(50) |>
  unnest(c(ptagis, wdfw, old),
         names_sep = "_") |>
  as.data.frame()

date_issues <-
  mismatch_dates |>
  rename(ptagis = ptagis_dates,
         wdfw = wdfw_dates,
         old = old_dates) |>
  select(-starts_with("record"),
         -shared_dates) |>
  unnest(c(ptagis, wdfw, old),
         names_sep = "_")

date_issues |>
  group_by(event_file_name) |>
  mutate(n_events = sum(ptagis_type == "event"),
         n_event_dates = n_distinct(ptagis_date[ptagis_type == "event"])) |>
  ungroup() |>
  filter(!(n_event_dates == 1 & ptagis_type == "event")) |>
  tabyl(ptagis_type)


# character type fields
chr_comp <-
  bio_comp |>
  select(spawn_year,
         # event_file_name,
         # event_type,
         pit_tag,
         SRR,
         origin,
         sex,
         age_scales) |>
  distinct() |>
  mutate(source = "ptagis") |>
  pivot_longer(cols = -c(spawn_year:pit_tag,
                         source),
               names_to = "field",
               values_to = "value") |>
  bind_rows(wdfw_db |>
              select(spawn_year,
                     pit_tag,
                     SRR = species_run_rear_type,
                     origin,
                     sex,
                     age_scales) |>
              distinct() |>
              mutate(source = "wdfw") |>
              pivot_longer(cols = -c(spawn_year:pit_tag,
                                     source),
                           names_to = "field",
                           values_to = "value")) |>
  pivot_wider(names_from = source,
              values_from = value) |>
  unnest(c(ptagis, wdfw)) |>
  group_by(spawn_year,
           pit_tag,
           field) |>
  mutate(n_records = n(),
         n_agree = sum(ptagis == wdfw),
         n_disagree = sum(ptagis != wdfw)) |>
  ungroup() |>
  filter(n_disagree > 0) #|>
  # left_join(old_bio |>
  #             select(spawn_year,
  #                    pit_tag,
  #                    origin = origin_old,
  #                    sex = sex_old,
  #                    age_scales) |>
  #             distinct() |>
  #             pivot_longer(cols = -c(spawn_year:pit_tag),
  #                          names_to = "type",
  #                          values_to = "old"))


chr_comp |>
  select(-starts_with("n_"))

# logical type fields
lgl_comp <-
  bio_comp |>
  filter(spawn_year %in% unique(wdfw_db$spawn_year)) |>
  select(spawn_year,
         # event_file_name,
         # event_type,
         pit_tag,
         cwt,
         ad_clip) |>
  distinct() |>
  mutate(source = "ptagis") |>
  pivot_longer(cols = -c(spawn_year:pit_tag,
                         source),
               names_to = "field",
               values_to = "value") |>
  bind_rows(wdfw_db |>
              select(spawn_year,
                     pit_tag,
                     cwt,
                     ad_clip) |>
              distinct() |>
              mutate(source = "wdfw") |>
              pivot_longer(cols = -c(spawn_year:pit_tag,
                                     source),
                           names_to = "field",
                           values_to = "value")) |>
  pivot_wider(names_from = source,
              values_from = value) |>
  unnest(c(ptagis, wdfw)) |>
  group_by(spawn_year,
           pit_tag,
           field) |>
  mutate(n_records = n(),
         n_agree = sum(ptagis == wdfw, na.rm = T),
         n_disagree = sum(ptagis != wdfw, na.rm = T)) |>
  ungroup() |>
  filter(n_disagree > 0) |>
  left_join(bio_comp |>
              select(spawn_year,
                     # event_file_name,
                     # event_type,
                     SRR,
                     pit_tag) |>
              distinct()) |>
  relocate(SRR,
           .after = pit_tag)
# left_join(old_bio |>
#             select(spawn_year,
#                    pit_tag,
#                    origin = origin_old,
#                    sex = sex_old,
#                    age_scales) |>
#             distinct() |>
#             pivot_longer(cols = -c(spawn_year:pit_tag),
#                          names_to = "type",
#                          values_to = "old"))

lgl_comp |>
  tabyl(ptagis,
        wdfw,
        field)

lgl_comp |>
  tabyl(spawn_year,
        field) |>
  adorn_totals("both")

lgl_comp |>
  filter(field == "ad_clip") |>
  select(-starts_with("n_"))

lgl_comp |>
  filter(field == "cwt") |>
  select(-starts_with("n_")) |>
  tabyl(spawn_year)

# length
length_comp <-
  bio_comp |>
  filter(spawn_year %in% unique(wdfw_db$spawn_year)) |>
  select(spawn_year,
         pit_tag,
         fork_length) |>
  distinct() |>
  mutate(record_ptagis = T) |>
  nest(ptagis = fork_length) |>
  full_join(wdfw_db |>
              select(spawn_year,
                     pit_tag,
                     fork_length) |>
              distinct() |>
              mutate(record_wdfw = T) |>
              nest(wdfw= fork_length)) |>
  mutate(across(starts_with("record_"),
                ~ replace_na(., F)))

length_comp |>
  tabyl(record_ptagis,
        record_wdfw)


mismatch_length <-
  length_comp |>
  filter(record_ptagis,
         record_wdfw) |>
  mutate(shared_lengths = map2_lgl(ptagis,
                                 wdfw,
                                 .f = function(x, y) {
                                   if_else(sum(x$fork_length %in% y$fork_length) > 0 |
                                             sum(y$fork_length %in% x$fork_length) > 0,
                                           T, F)
                                 })) |>
  unnest(c(ptagis,
           wdfw),
         names_sep = "_") |>
  arrange(spawn_year,
          pit_tag,
          ptagis_fork_length) |>
  filter(ptagis_fork_length != wdfw_fork_length)
  # filter(! shared_lengths)


length_comp |>
  filter(!record_ptagis)


alt_lengths <-
  mismatch_length |>
  select(spawn_year,
         pit_tag) |>
  left_join(bio_comp |>
              select(spawn_year,
                     event_file_name,
                     event_date,
                     release_date,
                     pit_tag,
                     ptagis_fl = fork_length) |>
              mutate(across(ends_with("date"),
                            ~ floor_date(., unit = "days")))) |>
  arrange(spawn_year,
          pit_tag,
          release_date) |>
  group_by(spawn_year,
           pit_tag) |>
  mutate(rec_num = 1:n()) |>
  ungroup() |>
  pivot_wider(names_from = rec_num,
              values_from = c(ptagis_fl,
                              event_file_name,
                              ends_with("date")),
              names_glue = "{.value}_{rec_num}") |>
  left_join(mismatch_length |>
              select(spawn_year,
                     pit_tag) |>
              left_join(wdfw_db |>
                          select(spawn_year,
                                 wdfw_date = event_date,
                                 pit_tag,
                                 wdfw_fl = fork_length)) |>
              arrange(spawn_year,
                      pit_tag,
                      wdfw_date) |>
              group_by(spawn_year,
                       pit_tag) |>
              mutate(rec_num = 1:n()) |>
              ungroup() |>
              pivot_wider(names_from = rec_num,
                          values_from = c(wdfw_fl,
                                          wdfw_date),
                          names_glue = "{.value}_{rec_num}")) |>
  mutate(adjust_wdfw = if_else(ptagis_fl_1 == ptagis_fl_2 &
                                 wdfw_fl_1 != wdfw_fl_2 |
                                 (is.na(ptagis_fl_1) | is.na(ptagis_fl_2)),
                               T, F),
         adjust_ptagis = if_else(ptagis_fl_1 != ptagis_fl_2 &
                                   wdfw_fl_1 == wdfw_fl_2,
                                 T, F))

#-------------------------------------------------------
# save results
list(
  NA_length = na_length,
  NA_sex = na_sex,
  NA_ad_clip = na_ad_clip,
  missing_tags = miss_tags,
  date_mismatch = date_issues,
  sex_mismatch = chr_comp |>
    select(-starts_with("n_")) |>
    filter(field == "sex"),
  origin_mismatch = chr_comp |>
    select(-starts_with("n_")) |>
    filter(field %in% c("origin",
                        "SRR")),
  ad_clip_mismatch = lgl_comp |>
    filter(field == "ad_clip") |>
    select(-ends_with("agree")),
  cwt_mismatch = lgl_comp |>
    filter(field == "cwt") |>
    select(-ends_with("agree")),
  length_mismatch = alt_lengths) |>
  write_xlsx(here("outgoing/other/QAQC_PTAGIS_vs_WDFWdatabase.xlsx"))

list("PTAGIS" = bio_df,
     "WDFW" = wdfw_db,
     "Old" = old_bio) |>
  write_xlsx(here("outgoing/other/BioData_Comparison.xlsx"))
