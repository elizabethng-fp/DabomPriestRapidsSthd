# Author: Kevin See
# Purpose: clean PTAGIS data with PITcleanr
# Created: 4/27/20
# Last Modified: 9/8/23
# Notes:

# if needed, install development version of packages
# devtools::install_github("KevinSee/PITcleanr@develop")
# devtools::install_github("KevinSee/DABOM@develop")

#-----------------------------------------------------------------
# load needed libraries
library(PITcleanr)
library(tidyverse)
library(lubridate)
library(janitor)
library(readxl)
library(magrittr)
library(here)

#-----------------------------------------------------------------
# load configuration and site_df data
load(here('analysis/data/derived_data/site_config.rda'))

# which spawn year are we dealing with?
yr = 2023

# for(yr in 2011:2020) {

# load and file biological data
# bio_df = read_rds(here('analysis/data/derived_data/Bio_Data_2011_2022.rds')) %>%
#   filter(year == yr)

bio_df = read_rds(here('analysis/data/derived_data/Bio_Data_2011_2023.rds')) %>%
  filter(year == yr) |>
  rename(tag_code = pit_tag)

# bio_df <-
#   read_excel(here('analysis/data/derived_data',
#                   'PRA_Sthd_BioData_v2.xlsx'),
#              sheet = as.character(yr)) |>
#   clean_names() |>
#   rename(tag_code = pit_tag)


# any double-tagged fish?
dbl_tag = bio_df %>%
  # filter(!is.na(tag_other))
  filter(!is.na(second_pit_tag))


#-----------------------------------------------------------------
# start date is June 1 of previous year
start_date = paste0(yr - 1, '0601')
# when is the last possible observation date?
max_obs_date = paste0(yr, "0531")

# get raw observations from PTAGIS
# These come from running a saved query on the list of tags to be used
ptagis_file = here("analysis/data/raw_data/PTAGIS",
                   paste0("UC_Sthd_", yr, "_CTH.csv"))

# recode the PTAGIS observations of double tagged fish so that the tag code matches the TagID (not TagOther)
ptagis_obs = readCTH(ptagis_file)

# add some observations from Colockum (CLK), a temporary antenna that only operated in some years
if(yr %in% c(2015, 2018) ) {
  clk_obs = read_csv(here('analysis/data/raw_data/WDFW/CLK_observations.csv')) %>%
    rename(tag_code = `Tag Code`) %>%
    mutate(event_type_name = "Observation",
           event_site_code_value = 'CLK',
           antenna_id = 'A1',
           antenna_group_configuration_value = 100,
           cth_count = 1) %>%
    mutate(time = if_else(is.na(time),
                          hms::hms(seconds = 0,
                                   minutes = 0,
                                   hours = 12),
                          time)) %>%
    mutate(event_date_time_value = paste(date, time)) %>%
    mutate(across(event_date_time_value,
                  lubridate::mdy_hms)) %>%
    filter(year(event_date_time_value) == yr) %>%
    select(-date, -time) %>%
    # get the origin info for these fish
    left_join(ptagis_obs %>%
                select(tag_code,
                       mark_species_name,
                       mark_rear_type_name) %>%
                distinct())

  ptagis_obs %<>%
    bind_rows(clk_obs)

  rm(clk_obs)
}

# any orphaned or disowned tags?
qcTagHistory(ptagis_obs,
             "PTAGIS",
             ignore_event_vs_release = T)

# deal with double tagged fish, assign all observations to one tag
# if(nrow(dbl_tag) > 0) {
#   ptagis_obs %<>%
#     left_join(dbl_tag %>%
#                 mutate(fish_id = 1:n()) %>%
#                 select(fish_id,
#                        starts_with("tag")) %>%
#                 mutate(use_tag = tag_code) %>%
#                 pivot_longer(cols = starts_with("tag"),
#                              names_to = "source",
#                              values_to = "tag_code") %>%
#                 select(tag_code, use_tag),
#               by = "tag_code") %>%
#     rowwise() %>%
#     mutate(tag_code = if_else(!is.na(use_tag),
#                               use_tag,
#                               tag_code)) %>%
#     ungroup() %>%
#     select(-use_tag) %>%
#     distinct()
# }

# deal with double tagged fish, assign all observations to one tag
if(nrow(dbl_tag) > 0) {
  ptagis_obs <-
    ptagis_obs |>
    left_join(dbl_tag %>%
                mutate(fish_id = 1:n()) %>%
                select(fish_id,
                       contains("pit_tag")) %>%
                mutate(use_tag = pit_tag) %>%
                pivot_longer(cols = contains("pit_tag"),
                             names_to = "source",
                             values_to = "tag_code") %>%
                select(tag_code, use_tag) |>
                filter(tag_code != use_tag),
              by = "tag_code") %>%
    rowwise() %>%
    mutate(tag_code = if_else(!is.na(use_tag) &
                                !event_site_code_value %in% c("DISOWN",
                                                              "ORPHAN"),
                              use_tag,
                              tag_code)) %>%
    ungroup() %>%
    select(-use_tag) %>%
    distinct()
}

# compress and process those observations with PITcleanr
prepped_ch = PITcleanr::prepWrapper(cth_file = ptagis_obs,
                                    file_type = "PTAGIS",
                                    configuration = configuration,
                                    parent_child = parent_child %>%
                                      addParentChildNodes(configuration = configuration),
                                    min_obs_date = start_date,
                                    max_obs_date = max_obs_date,
                                    ignore_event_vs_release = F,
                                    filter_orphan_disown_tags = FALSE,
                                    add_tag_detects = T,
                                    save_file = T,
                                    file_name = here('outgoing/PITcleanr', paste0('UC_Steelhead_', yr, '.xlsx')))


# save some stuff
save(parent_child, configuration, start_date, bio_df, prepped_ch,
     file = here('analysis/data/derived_data/PITcleanr',
                 paste0('UC_Steelhead_', yr, '.rda')))


# rm(start_date, bio_df, prepped_ch,
#    ptagis_file,
#    ptagis_obs,
#    dbl_tag)
# }

#-------------------------------------------
# NEXT STEPS
#-------------------------------------------
# open that Excel file, and filter on the column user_keep_obs, looking for blanks. Fill in each row with TRUE or FALSE, depending on whether that observation should be kept or not. The column auto_keep_obs provides a suggestion, but the biologist's best expert judgment should be used based on detection dates, detection locations before and after, etc.

load(here('analysis/data/derived_data/PITcleanr',
          paste0('UC_Steelhead_', yr, '.rda')))

# wdfw_df = read_excel(here('analysis/data/derived_data/WDFW',
#                           paste0('UC_Steelhead_', yr, '.xlsx')))

wdfw_df <-
  read_excel(paste0("T:/DFW-Team FP Upper Columbia Escapement - General/UC_Sthd/inputs/PITcleanr/PITcleanr Final/",
                    "UC_Steelhead_",
                    yr,
                    ".xlsx")) |>
  mutate(across(c(duration,
                  travel_time),
                ~ as.difftime(., units = "mins"))) |>
  select(all_of(names(prepped_ch)))

identical(n_distinct(prepped_ch$tag_code),
          n_distinct(wdfw_df$tag_code))

nrow(wdfw_df)
nrow(prepped_ch)


# update with new node codes
if(sum(str_detect(wdfw_df$node, "B0$")) > 0) {
  wdfw_df <-
    wdfw_df |>
    mutate(across(c(node,
                    path,
                    tag_detects),
                  ~ str_replace_all(., "B0", "_D")),
           across(c(node,
                    path,
                    tag_detects),
                  ~ str_replace_all(., "A0", "_U")),
           across(c(node),
                  ~ str_replace_all(., "^S_U", "SA0")),
           across(c(tag_detects,
                    path),
                  ~ str_replace_all(., " S_U", " SA0")))
}

# what rows are in prepped_ch but not in wdfw_df?
prepped_ch |>
  select(tag_code:min_det,
         -n_dets) |>
  anti_join(wdfw_df)

# vice versa?
wdfw_df |>
  select(tag_code:min_det,
         -n_dets) |>
  anti_join(prepped_ch)



prepped_ch |>
  select(tag_code:min_det,
         -n_dets) |>
  anti_join(wdfw_df) |>
  select(tag_code) |>
  distinct() |>
  slice(1) |>
  left_join(prepped_ch |>
              select(tag_code:max_det,
                     auto_keep_obs,
                     user_keep_obs,
                     path)) |>
  left_join(wdfw_df |>
              select(tag_code:max_det,
                     wdfw_keep_obs = user_keep_obs)) |>
  select(-path,
         -max_det) #|>
  # tail(10)
  # select(-user_keep_obs)


# add a few rows back, and correct the user_keep_obs field

if(yr == 2022) {
  wdfw_df <-
    prepped_ch |>
    select(-user_keep_obs) |>
    full_join(wdfw_df |>
                select(tag_code:max_det,
                       user_keep_obs)) |>
    mutate(
      across(user_keep_obs,
             ~ if_else(tag_code == "3DD.003DA28953",
                       if_else(node %in% c("PRA", "PRO_D", "PRO_U"),
                               T, F),
                       .)),
      across(user_keep_obs,
             ~ if_else(tag_code == "3DD.003DA28960",
                       T,
                       .)),
      across(user_keep_obs,
             ~ if_else(tag_code == "3DD.003DA28A0F",
                       if_else(node %in% c("OBF", "OMF_D"),
                               T, .),
                       .))) |>
    filter(!is.na(auto_keep_obs))

} else if(yr == 2023) {

  wdfw_df <-
    prepped_ch |>
    select(-user_keep_obs) |>
    full_join(wdfw_df |>
                select(tag_code:max_det,
                       user_keep_obs)) |>
    mutate(
      across(user_keep_obs,
             ~ if_else(tag_code %in% c("3DD.003BDDBF78",
                                       "3DD.003DA28B7A",
                                       "3DD.003DA28CB1",
                                       "3DD.003DA28D38",
                                       "3DD.003DA28EE6",
                                       "3DD.003DA29002"),
                       if_else(min_det > ymd(max_obs_date),
                               F, .),
                       .)),
      across(user_keep_obs,
             ~ if_else(tag_code %in% c("3DD.003DA28C1F",
                                       "3DD.003DA28C62",
                                       "3DD.003DA28D42",
                                       "3DD.003DA28EB0",
                                       "3DD.003DA28BD1"),
                       auto_keep_obs, .))) |>
    filter(!is.na(auto_keep_obs))
}

tabyl(wdfw_df,
      auto_keep_obs,
      user_keep_obs)


filter_obs = wdfw_df %>%
  mutate(user_keep_obs = if_else(is.na(user_keep_obs),
                                 auto_keep_obs,
                                 user_keep_obs)) %>%
  filter(user_keep_obs)

# construct all valid paths
all_paths = buildPaths(addParentChildNodes(parent_child,
                                           configuration))

tag_path = summarizeTagData(filter_obs,
                            bio_df %>%
                              group_by(tag_code = pit_tag) %>%
                              slice(1) %>%
                              ungroup()) %>%
  select(tag_code, final_node) %>%
  distinct() %>%
  left_join(all_paths,
            by = join_by(final_node == end_loc)) %>%
  rename(tag_path = path)

# check if any user definied keep_obs lead to invalid paths
error_tags = filter_obs %>%
  left_join(tag_path) %>%
  rowwise() %>%
  mutate(node_in_path = str_detect(tag_path, node)) %>%
  ungroup() %>%
  filter(!node_in_path) %>%
  select(tag_code) %>%
  distinct()

nrow(error_tags)
error_tags %>%
  # left_join(wdfw_df) %>%
  left_join(filter_obs) |>
  as.data.frame()




prepped_ch = prepped_ch %>%
  select(-user_keep_obs) %>%
  left_join(wdfw_df %>%
              select(tag_code:max_det,
                     user_keep_obs))

save(parent_child, configuration, start_date, bio_df, prepped_ch,
     file = here('analysis/data/derived_data/PITcleanr',
                 paste0('UC_Steelhead_', yr, '.rda')))


#-----------------------------------------------------------------
# tag summaries
#-----------------------------------------------------------------
# use auto_keep_obs for the moment
tag_summ = summarizeTagData(prepped_ch |>
                              mutate(across(user_keep_obs,
                                            ~ if_else(is.na(.),
                                                      auto_keep_obs,
                                                      .))),
                            bio_df %>%
                              rename(tag_code = pit_tag) |>
                              group_by(tag_code) %>%
                              slice(1) %>%
                              ungroup())

# any duplicated tags?
sum(duplicated(tag_summ$tag_code))
tag_summ %>%
  filter(tag_code %in% tag_code[duplicated(tag_code)]) %>%
  as.data.frame()

# where are tags assigned?
janitor::tabyl(tag_summ,
               final_node,
               origin) %>%
  janitor::adorn_totals("both") |>
  arrange(desc(Total))


# preliminary estimate of node efficiency
node_eff = prepped_ch %>%
  mutate(across(user_keep_obs,
                ~ if_else(is.na(.),
                          auto_keep_obs,
                          .))) |>
  filter(user_keep_obs) %>%
  estNodeEff(node_order = buildNodeOrder(addParentChildNodes(parent_child, configuration)))

node_eff %>%
  filter(tags_at_node > 0,
         eff_est < 1)

node_eff %>%
  filter(tags_at_node > 0) |>
  # arrange(desc(eff_se))
  arrange(eff_est)


#-----------------------------------------------------------------
# examine some of the output
#-----------------------------------------------------------------
# which tags have "strange" capture histories?
prepped_ch %>%
  summarise(n_tags = n_distinct(tag_code),
            n_weird = n_distinct(tag_code[direction == "unknown"]),
            n_fix = n_distinct(tag_code[is.na(user_keep_obs)]),
            prop_weird = n_weird / n_tags,
            prop_fix = n_fix / n_tags)

# look at which branch each tag was assigned to for spawning
brnch_df = buildNodeOrder(addParentChildNodes(parent_child, configuration)) %>%
  separate(col = path,
           into = paste("step", 1:max(.$node_order), sep = "_"),
           remove = F) %>%
  mutate(branch_nm = if_else(node == "PRA",
                             "Start",
                             if_else(grepl('LWE', path) | node %in% c("CLK"),
                                     "Wenatchee",
                                     if_else(grepl("ENL", path),
                                             "Entiat",
                                             if_else(grepl("LMR", path),
                                                     "Methow",
                                                     if_else(grepl("OKL", path) | node %in% c("FST"),
                                                             "Okanogan",
                                                             if_else(step_2 != "RIA" & !is.na(step_2),
                                                                     "Downstream",
                                                                     "Mainstem"))))))) %>%
  select(-starts_with("step"))

tag_summ %<>%
  left_join(brnch_df %>%
              select(final_node = node,
                     branch_nm))

# how many tags in each branch?
tag_summ %>%
  janitor::tabyl(branch_nm,
                 origin) %>%
  janitor::adorn_totals("both") |>
  # janitor::adorn_pct_formatting() %>%
  arrange(desc(Total))

# age comp in each branch, by sex
tag_summ %>%
  filter(!is.na(age_scales)) %>%
  ggplot(aes(x = branch_nm,
             fill = as.ordered(age_scales))) +
  geom_bar(position = position_fill()) +
  facet_wrap(~ sex) +
  theme_bw() +
  theme(axis.text.x = element_text(angle = 45,
                                   hjust = 1)) +
  labs(x = "Branch",
       y = "Percent of Tags",
       fill = "Age")


# look at run timing between branches
tag_summ %>%
  ggplot(aes(x = start_date,
             color = branch_nm,
             fill = branch_nm)) +
  geom_density(alpha = 0.2) +
  theme_bw() +
  scale_color_brewer(palette = 'Set1',
                     name = "Branch") +
  scale_fill_brewer(palette = 'Set1',
                    name = "Branch") +
  labs(x = "Trap Date at Priest Rapids")
