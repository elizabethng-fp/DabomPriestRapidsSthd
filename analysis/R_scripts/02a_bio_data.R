# Author: Kevin See
# Purpose: create tag lists to feed to PTAGIS query
# Created: 8/15/2023
# Last Modified: 12/20/2023
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
  filter(spawn_year <= year(today()))

# pull out MRR data about all steelhead tags
tagging_df <-
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
                             .default = NA))

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
  select(spawn_year,
         pit_tag,
         second_pit_tag,
         text_comments,
         text_code)

tagging_df <-
  tagging_df |>
  left_join(second_tags |>
              select(spawn_year,
                     pit_tag,
                     text_code)) |>
  mutate(across(second_pit_tag,
                ~ if_else(is.na(.) & !is.na(text_code),
                          text_code,
                          .))) |>
  select(-text_code)

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
         length,
         poh,
         conditional_comments,
         text_comments,
         release_date,
         scale_id)


bio_df |>
  tabyl(spawn_year, ad_clip)
bio_df |>
  tabyl(spawn_year, cwt)

tabyl(bio_df,
      spawn_year,
      species_run_rear_type) |>
  adorn_totals(where = "both")


#-----------------------------------------------------------------
# add age and final origin data from scales
# scale_age_file <- "T:/DFW-Team FP Upper Columbia Escapement - General/UC_Sthd/inputs/Bio Data/PIT Tag PRD Scale Ages Run Years 2022 to present.xlsx"

scale_age_file <- "T:/DFW-Team FP Upper Columbia Escapement - General/UC_Sthd/inputs/Bio Data/PIT Tag PRD Scale Ages Spawn Years 2011 to present.xlsx"

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

# which PIT tags are duplciated?
# and what are the ages associated with those?
scale_age_df |>
  unite(sy_pit,
        spawn_year, primary_pit_tag,
        remove = F) |>
  filter(sy_pit %in% sy_pit[duplicated(sy_pit)]) |>
  arrange(primary_pit_tag,
          spawn_year) |>
  select(spawn_year,
         primary_pit_tag,
         age_scales) |>
  group_by(spawn_year,
           primary_pit_tag) |>
  mutate(n_rec = 1:n()) |>
  ungroup() |>
  pivot_wider(names_from = n_rec,
              values_from = age_scales)# |>
  filter(`1` != "Unreadable",
         `2` != "Unreadable")

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

# any more duplicated SY / tags?
scale_age_df |>
  unite(sy_pit,
        spawn_year, primary_pit_tag,
        remove = F) |>
  filter(sy_pit %in% sy_pit[duplicated(sy_pit)])

# differences in PTAGIS file names
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
  left_join(scale_age_df |>
              select(spawn_year,
                     pit_tag = primary_pit_tag,
                     age_scales) |>
              mutate(age_data_exists_v1 = T),
            by = join_by(spawn_year,
                         pit_tag)) |>
  left_join(scale_age_df |>
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
         species_run_rear_type,
         event_date,
         event_type,
         contains("comments"),
         age_scales) |>
  # write_csv(here("outgoing/other/missing_scale_data.csv"))
  mutate(event_month = month(event_date,
                             label = T)) |>
  tabyl(spawn_year, event_month) |>
  adorn_totals(where = "both")

# looking for mismatches between scale origins and SRR calls
# These should probably be fixed in PTAGIS?
bio_df |>
  left_join(scale_age_df |>
              select(spawn_year,
                     pit_tag = primary_pit_tag,
                     age_scales,
                     origin_field,
                     origin_scales),
            by = join_by(spawn_year,
                         pit_tag)) |>
  select(spawn_year,
         event_date,
         pit_tag,
         SRR = species_run_rear_type,
         contains("comments"),
         starts_with("origin")) |>
  filter(spawn_year %in% unique(scale_age_df$spawn_year),
         origin != origin_scales) #|>
  write_csv(here("outgoing/other",
                 "mismatch_origin.csv"))



#-----------------------------------------
# add scale data to bio_df
bio_df <-
  bio_df |>
  left_join(scale_age_df |>
              select(spawn_year,
                     pit_tag = primary_pit_tag,
                     # scale_id,
                     age_scales),
            by = join_by(spawn_year,
                         pit_tag))

#-----------------------------------------------------------------
# reduce to one row per tag / spawn year
bio_df |>
  ungroup() |>
  unite(sy_pit,
        spawn_year, pit_tag,
        remove = F) |>
  summarize(n_row = n(),
            n_records = n_distinct(sy_pit),
            n_dups = sum(duplicated(sy_pit)))

# look at duplicated records briefly
bio_df |>
  unite(sy_pit,
        spawn_year, pit_tag,
        remove = F) |>
  filter(sy_pit %in% sy_pit[duplicated(sy_pit)]) |>
  arrange(sy_pit) |>
  select(spawn_year,
         pit_tag,
         event_file_name,
         event_date,
         event_type,
         srr = species_run_rear_type,
         sex,
         cwt,
         ad_clip,
         length,
         conditional_comments)

# group bio data by a few variables for the sections below
bio_df <-
  bio_df |>
  # group_by(spawn_year,
  #          pit_tag,
  #          species_run_rear_type,
  #          origin)
  group_by(spawn_year,
           pit_tag)



# primary PTAGIS file name for each tag, along with date and event type
ptagis_file_df <-
  bio_df |>
  arrange(spawn_year,
          pit_tag,
          event_date) |>
  summarize(ptagis_file_name = case_when(sum(str_detect(event_file_name, "xml$")) > 0 ~
                                           event_file_name[str_detect(event_file_name, "xml$")][1],
                                        sum(str_detect(event_file_name, "xml$")) == 0 ~
                                          event_file_name[event_date == min(event_date)][1],
                                         .default = event_file_name[1]),
            across(c(event_date,
                     event_type,
                     release_date),
                   ~ .[event_file_name == ptagis_file_name][1]),
            .groups = "drop") |>
  rename(event_file_name = ptagis_file_name)


# bio_df |>
#   inner_join(ptagis_file_df) |>
#   unite(sy_pit,
#         spawn_year, pit_tag,
#         remove = F) |>
#   filter(sy_pit %in% sy_pit[duplicated(sy_pit)]) |>
#   arrange(sy_pit)

# get any available second pit tag
second_tags <-
  bio_df |>
  select(-second_pit_tag) |>
  left_join(bio_df |>
              filter(!is.na(second_pit_tag)) |>
              select(second_pit_tag)) |>
  select(second_pit_tag) |>
  distinct()


# if sex is available, pull it out
sex_calls <-
  bio_df |>
  reframe(across(sex,
                 ~ case_when(sum(!is.na(.)) == 1 ~ unique(.[!is.na(.)]),
                             sum(!is.na(.)) > 1 ~ unique(.[event_date == min(event_date)]),
                             sum(!is.na(.)) == 0 ~ NA_character_,
                             .default = NA_character_))) #|>
  # unite(sy_pit,
  #       spawn_year, pit_tag,
  #       remove = F) |>
  # filter(sy_pit %in% sy_pit[duplicated(sy_pit)]) #|>
  # left_join(bio_df) |> as.data.frame()


# if CWT or ad-clip is available, pull it out
cwt_calls <-
  bio_df |>
  reframe(across(cwt,
                 ~ case_when(sum(!is.na(.)) == 1 ~ unique(.[!is.na(.)]),
                             sum(!is.na(.)) > 1 ~ unique(.[event_date == min(event_date)]),
                             sum(!is.na(.)) == 0 ~ NA,
                             .default = NA))) #|>
  # unite(sy_pit,
  #       spawn_year, pit_tag,
  #       remove = F) |>
  # filter(sy_pit %in% sy_pit[duplicated(sy_pit)])

# some years all ad_clip is NA, which drops that year from the results
ad_clip_calls <-
  bio_df |>
  reframe(across(ad_clip,
                 ~ case_when(sum(!is.na(.)) == 1 ~ unique(.[!is.na(.)]),
                             sum(!is.na(.)) > 1 ~ unique(.[event_date == min(event_date)]),
                             sum(!is.na(.)) == 0 ~ NA,
                             .default = NA))) #|>
  # unite(sy_pit,
  #       spawn_year, pit_tag,
  #       remove = F) |>
  # filter(sy_pit %in% sy_pit[duplicated(sy_pit)])

# for length and poh, use the maximum observed lengths
length_max <-
  bio_df |>
  summarize(across(c(length,
                     poh),
                   ~ max(., na.rm = T) |>
                     suppressWarnings())) |>
  mutate(across(c(length,
                  poh),
                ~ if_else(. == -Inf, NA_real_, .)))

# this version says when multiple lengths exist, use the one associated with the primary file name
length_calls <-
  bio_df |>
  left_join(ptagis_file_df |>
              rename(file_nm = event_file_name,
                     type = event_type) |>
              select(-event_date)) |>
  reframe(across(length,
                 ~ case_when(sum(!is.na(.)) == 1 ~ unique(.[!is.na(.)]),
                             sum(!is.na(.)) > 1 ~ unique(.[event_file_name == file_nm & event_type == type]),
                             sum(!is.na(.)) == 0 ~ NA_real_,
                             .default = NA_real_))) |>
  distinct()

length_calls |>
  full_join(length_max |>
              rename(max_length = length)) |>
  filter(length != max_length) |>
  select(-length) |>
  left_join(bio_df |>
              select(spawn_year,
                     pit_tag,
                     length,
                     event_date,
                     event_type))


# poh is mostly missing, but this grabs the few tags we have it for
poh_calls <-
  bio_df |>
  left_join(ptagis_file_df |>
              rename(file_nm = event_file_name,
                     type = event_type) |>
              select(-event_date)) |>
  reframe(across(poh,
                 ~ case_when(sum(!is.na(.)) == 1 ~ unique(.[!is.na(.)]),
                             sum(!is.na(.)) > 1 ~ unique(.[event_file_name == file_nm & event_type == type]),
                             sum(!is.na(.)) == 0 ~ NA_real_,
                             .default = NA_real_)))

poh_calls |>
  full_join(length_max |>
              rename(max_poh = poh)) |>
  filter(poh != max_poh)


# consolidate all conditional comments
all_cond_comments <-
  bio_df |>
  select(conditional_comments) |>
  mutate(tmp = map(conditional_comments,
                   .f = function(x) {
                     str_split(x, "[:space:]") |>
                       unlist() |>
                       as_tibble() |>
                       rename(code = value)
                   })) |>
  select(-conditional_comments) |>
  unnest(tmp) |>
  distinct() |>
  summarize(conditional_comments = paste(code, collapse = " "))

# consolidate all text comments
all_text_comments <-
  bio_df |>
  select(text_comments) |>
  distinct() |>
  summarize(across(text_comments,
                   ~ paste(., collapse = ". "))) |>
  mutate(across(text_comments,
                ~ if_else(. == "NA",
                          NA_character_,
                          .)))

# pick out SRR associated with ptagis file name
srr_calls <-
  bio_df |>
  group_by(spawn_year,
           pit_tag,
           species_run_rear_type) |>
  summarize(across(event_date,
                   min)) |>
  distinct() |>
  arrange(spawn_year,
          pit_tag,
          event_date) |>
  left_join(ptagis_file_df) |>
  filter(!is.na(event_file_name)) |>
  select(spawn_year,
         pit_tag,
         species_run_rear_type) |>
  mutate(origin = str_extract(species_run_rear_type, "[:alpha:]$"))

# srr_calls |>
#   unite(sy_pit,
#         spawn_year, pit_tag,
#         remove = F) |>
#   filter(sy_pit %in% sy_pit[duplicated(sy_pit)])


# put everything together with one row per spawn year / tag combo
new_bio <-
  ptagis_file_df |>
  full_join(second_tags,
            by = join_by(spawn_year,
                         pit_tag)) |>
  full_join(srr_calls,
            by = join_by(spawn_year,
                         pit_tag)) |>
  full_join(sex_calls,
            by = join_by(spawn_year,
                         pit_tag)) |>
  full_join(cwt_calls,
            by = join_by(spawn_year,
                         pit_tag)) |>
  full_join(ad_clip_calls,
            by = join_by(spawn_year,
                         pit_tag)) |>
  full_join(length_max,
            by = join_by(spawn_year,
                         pit_tag)) |>
  full_join(all_cond_comments,
            by = join_by(spawn_year,
                         pit_tag)) |>
  full_join(all_text_comments,
            by = join_by(spawn_year,
                         pit_tag)) |>
  left_join(scale_age_df |>
              select(spawn_year,
                     pit_tag = primary_pit_tag,
                     age_scales),
            by = join_by(spawn_year,
                         pit_tag))


names(bio_df)[!names(bio_df) %in% names(new_bio)]

bio_df |>
  ungroup() |>
  unite(sy_pit,
        spawn_year, pit_tag,
        remove = F) |>
  summarize(n_row = n(),
            n_records = n_distinct(sy_pit),
            n_dups = sum(duplicated(sy_pit))) |>
  add_column(source = "original",
             .before = 0) |>
  bind_rows(new_bio |>
              unite(sy_pit,
                    spawn_year, pit_tag,
                    remove = F) |>
              summarize(n_row = n(),
                        n_records = n_distinct(sy_pit),
                        n_dups = sum(duplicated(sy_pit))) |>
              add_column(source = "updated",
                         .before = 0))

bio_df = new_bio

#-----------------------------------------------------------------
# save as Excel file
#-----------------------------------------------------------------
bio_df %>%
  split(list(.$spawn_year)) %>%
  write_xlsx(path = here('analysis/data/derived_data',
                         'PRA_Sthd_BioData.xlsx'))

#-----------------------------------------------------------------
# for tag lists
#-----------------------------------------------------------------
# put bounds around years
min_yr = min(bio_df$spawn_year)
max_yr = max(bio_df$spawn_year)


# pull out PIT tag numbers by spawn year
tag_list <-
  bio_df |>
  select(spawn_year,
         contains("pit_tag")) |>
  pivot_longer(cols = contains("pit_tag"),
               names_to = "source",
               values_to = "tag_code") %>%
  filter(!is.na(tag_code)) |>
  select(-source) |>
  distinct() |>
  nest(.by = spawn_year)

# save tags to upload to PTAGIS
# all years
# for(yr in tag_list$spawn_year) {
#   tag_list |>
#     filter(spawn_year == yr) |>
#     pull(data) |>
#     extract2(1) |>
#     write_delim(file = here('analysis/data/raw_data/tag_lists',
#                             paste0('UC_Sthd_Tags_', yr, '.txt')),
#                 delim = '\n',
#                 col_names = F)
# }

# just write the latest year
tag_list |>
  filter(spawn_year == max_yr) |>
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

save(tagging_df,
     bio_df,
     file = here('analysis/data/derived_data',
                 paste0('Bio_Tag_Data_', min_yr, '_', max_yr, '.rda')))

#-----------------------------------------------------------------
# decode conditional comments
cond_comm_codes <- read_csv(
  paste0("T:/DFW-Team FP Upper Columbia Escapement - General/",
         "UC_Sthd/inputs/PTAGIS/",
         "Glossary_ConditionalComment_ValidationCodes.csv")) |>
  clean_names()

bio_df |>
  filter(spawn_year == 2023) |>
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

