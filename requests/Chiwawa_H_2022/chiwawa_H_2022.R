# Author: Kevin See
# Purpose: examine hatchery tags detected in Chiwawa in early 2022
# Created: 8/18/2023
# Last Modified: 8/18/2023
# Notes:

#-----------------------------------------------------------------
# load needed libraries
library(PITcleanr)
library(tidyverse)
library(readxl)
library(lubridate)
library(janitor)
library(here)

#-----------------------------------------------------------------
# started with query of complete tag histories of tags detected on CHL or CHU from March 1 - June 30, 2022
df <- read_csv(here("requests/Chiwawa_H_2022",
                    "Chiwawa_cth.csv"),
               show_col_types = FALSE) |>
  clean_names() |>
  mutate(
    across(
      ends_with("time_value"),
      ~ mdy_hms(.)
    ),
    across(
      ends_with("mmddyyyy"),
      ~ mdy(.)
    )
  ) |>
  mutate(time_fr_mark = difftime(event_date_time_value, mark_date_mmddyyyy, units = "weeks"),
         across(time_fr_mark,
                ~ as.numeric(.)))

# pull out appropriate tags and save them in text file
df |>
  filter(!(mark_site_code_value %in% c("CHIP", "CHIWAT") & time_fr_mark < 30),
         mark_rear_type_name == "Hatchery Reared") |>
  select(tag_code) |>
  distinct() |>
  write_delim(file = here("requests/Chiwawa_H_2022",
                          "Chiwawa_sthd_H_tags_2022.txt"),
              delim = '\n',
              col_names = F)

# compare with Katy Shelby's PTAGIS queries
katy_df <- read_excel(here("requests/Chiwawa_H_2022",
                           "Complete Tag History- Tag List CHL SY2022.xlsx")) |>
  clean_names()

katy_df |>
  select(tag_code,
         mark_rear_type_name) |>
  distinct() |>
  tabyl(mark_rear_type_name)

katy_df |>
  select(tag_code,
         mark_rear_type_name) |>
  distinct() |>
  full_join(cth_df |>
              select(tag_code,
                     mark_rear_type_name_v2 = mark_rear_type_name) |>
              distinct()) |>
  filter(is.na(mark_rear_type_name))

# query PTAGIS for complete tag histories of these tags
cth_df <- readCTH(here("requests/Chiwawa_H_2022",
                       "PITcleanr Query.csv"))



# build configuration file
config <- buildConfig(node_assign = "site")

# compress detections
comp_df <- cth_df |>
  compress(configuration = config,
           units = "days") |>
  left_join(config |>
              select(node,
                     site_name) |>
              distinct() |>
              group_by(node) |>
              slice(1) |>
              ungroup()) |>
  relocate(site_name,
           .after = node) |>
  left_join(cth_df |>
              select(tag_code,
                     mark_rear_type_name) |>
              distinct() |>
              mutate(origin = if_else(mark_rear_type_name == "Hatchery Reared",
                                      "H",
                                      "W")) |>
              select(tag_code,
                     origin)) |>
  relocate(origin,
           .after = tag_code) |>
  group_by(tag_code) |>
  mutate(mark_site = node[event_type_name == "Mark"]) |>
  relocate(mark_site,
           .after = origin) |>
  ungroup() |>
  left_join(cth_df |>
              group_by(tag_code) |>
              summarize(chiwawa_det = min(event_date_time_value[event_site_code_value %in% c("CHL", "CHU")]),
                        .groups = "drop"))

#-----------------------------------------------------------------
comp_df |>
  filter(node %in% c("CHL", "CHU")) |>
  filter(n_dets < 300) |>
  tabyl(node) |>
  adorn_totals()

comp_df |>
  select(tag_code, origin, mark_site) |>
  distinct() |>
  tabyl(mark_site) |>
  adorn_totals()

extractTagObs(comp_df) |>
  tabyl(tag_detects) |>
  adorn_pct_formatting()

extractTagObs(comp_df) |>
  mutate(seen_CHL = str_detect(tag_detects, "CHL"),
         seen_CHU = str_detect(tag_detects, "CHU")) |>
  # tabyl(seen_CHL,
  #       seen_CHU)
  left_join(katy_df |>
              select(tag_code) |>
              mutate(katy_tag = T) |>
              distinct()) |>
  mutate(across(katy_tag,
                ~ replace_na(., F))) |>
  tabyl(seen_CHU,
        katy_tag)

comp_df |>
  select(tag_code,
         origin,
         mark_site) |>
  # site = node,
  # site_name,
  # event_type_name,
  # first_event = min_det,
  # last_event = max_det) |>
  distinct() |>
  left_join(extractTagObs(comp_df)) |>
  filter(str_detect(tag_detects, "CHL")) |>
  tabyl(mark_site)



comp_df |>
  select(tag_code,
         origin,
         mark_site,
         site = node,
         site_name,
         event_type_name,
         first_event = min_det,
         last_event = max_det) |>
  left_join(extractTagObs(comp_df)) |>
  write_csv(here("requests",
                 "Chiwawa_Sthd_H_2022.csv"))

extractTagObs(comp_df) |>
  filter(str_detect(tag_detects, "BO1")) |>
  select(tag_code) |>
  left_join(comp_df) |>
  select(-mark_site)

comp_df |>
  filter(mark_site == "TUM") |>
  extractTagObs()

comp_df |>
  select(tag_code,
         origin,
         mark_site,
         site = node,
         site_name,
         event_type_name,
         first_event = min_det,
         last_event = max_det) |>
  left_join(extractTagObs(comp_df)) |>
  filter(str_detect(tag_detects, "CHL$"),
         mark_site == "CHIP",
         str_detect(tag_detects, "TUF"))

"3DD.0077C16DCF"

# one tag divided by detection rate of CHL
mu = 1 / 0.414
# uncertainty
se = 1 * 0.229 / 0.414^2

# number of juvenile tagged fish
tibble(est = mu,
       se = se) |>
  mutate(low_ci = qnorm(0.025, mu, se),
         upp_ci = qnorm(0.975, mu, se)) |>
  mutate(across(low_ci,
                ~ if_else(. < 0, 0, .)))

# in release year 2019, total releases
rel <- 216666

pit_tagged <- 31200

chw_tag_rate <- pit_tagged / rel
# assume a CV for the tag rate
tag_cv = 0.15


# number of juv tagged fish
n_det_tags = 1

mu = (n_det_tags / 0.414) / chw_tag_rate

se = msm::deltamethod(~ (n_det_tags / x1) / x2,
                      mean = c(0.414,
                               chw_tag_rate),
                      cov = diag(c(0.229,
                                   chw_tag_rate * tag_cv)^2))

# number of total hatchery fish
tibble(est = mu,
       se = se) |>
  mutate(low_ci = qnorm(0.025, mu, se),
         upp_ci = qnorm(0.975, mu, se)) |>
  mutate(across(low_ci,
                ~ if_else(. < 0, 0, .)))



?pbinom

tibble(n_tags = seq(1, 10),
       prob_det = 0.414) |>
  mutate(prob_obs_zero = dbinom(0, n_tags, prob_det))

crossing(prob_det = seq(0.364, 0.464, length.out = 3),
         n_tags = seq(1,10)) |>
  mutate(prob_obs_zero = dbinom(0, n_tags, prob_det)) |>
  filter(prob_obs_zero <= 0.05)

estBetaParams <- function(mu, var) {
  alpha <- ((1 - mu) / var - 1 / mu) * mu ^ 2
  beta <- alpha * (1 / mu - 1)
  return(params = list(alpha = alpha, beta = beta))
}

beta_params <- estBetaParams(0.414, 0.229^2) |>
  enframe() |>
  unnest(value) |>
  pivot_wider()

beta_params <- estBetaParams(0.859, 0.135^2) |>
  enframe() |>
  unnest(value) |>
  pivot_wider()




beta_params$alpha / (beta_params$alpha + beta_params$beta)



tibble(mu = 0.414,
       sd = 0.229) |>
  mutate(beta_params = map2(mu, sd,
                            function(x, y) {
                              estBetaParams(x, y^2) |>
                                enframe() |>
                                unnest(value) |>
                                pivot_wider()
                            })) |>
  unnest(beta_params)

#------------------------------------------------------------------
library(VGAM)

# load configuration and site_df data
load(here('analysis/data/derived_data',
          'site_config.rda'))

tag_rate_df <-
  read_excel(paste0("T:/DFW-Team FP Upper Columbia Escapement - General/UC_Sthd/Estimates/",
                    "UC_STHD_Model_Output.xlsx"),
             "Priest Rapids Escapement") |>
  clean_names() |>
  select(run_year,
         spawn_year,
         origin,
         priest_tag_rate = tag_rate)

all_escp <-
  read_excel(paste0("T:/DFW-Team FP Upper Columbia Escapement - General/UC_Sthd/Estimates/",
                    "UC_STHD_Model_Output.xlsx"),
             "Run Escp All Locations") |>
  clean_names()

zero_escp <-
  all_escp  |>
  filter(spawn_year == 2022) |>
  filter(estimate == 0,
         se == 0) |>
  select(run_year:location)


det_probs <-
  read_excel(paste0("T:/DFW-Team FP Upper Columbia Escapement - General/UC_Sthd/Estimates/",
                    "UC_STHD_Model_Output.xlsx"),
             "Detection Probability") |>
  clean_names() |>
  mutate(location = str_remove(node, "B0$"),
         across(location,
                ~ str_remove(., "A0$"))) |>
  filter(n_tags > 0,
         estimate < 1) |>
  group_by(spawn_year,
           location) |>
  mutate(n_nodes = n_distinct(node)) |>
  ungroup() |>
  inner_join(zero_escp) |>
  group_by(location) |>
  mutate(total_tags = sum(n_tags)) |>
  ungroup() |>
  filter(total_tags > 0) |>
  # filter(n_tags > 0) |>
  arrange(location,
          origin,
          node) |>
  group_by(run_year,
           spawn_year,
           location,
           total_tags,
           origin) |>
  mutate(mu = if_else(n_nodes == 1,
                      estimate,
                      1 - ((1 - estimate[1]) * (1 - estimate[2]))),
         se_v2 = if_else(n_nodes == 1,
                         se,
                         msm::deltamethod(~ 1 - ((1 - x1) * (1 - x2)),
                                          mean = c(estimate[1],
                                                   estimate[2]),
                                          cov = diag(c(se[1], se[2])^2)))) |>
  select(run_year,
         spawn_year,
         location,
         total_tags,
         origin,
         mu,
         se = se_v2) |>
  ungroup() |>
  distinct() |>
  mutate(beta_params = map2(mu, se,
                            function(x, y) {
                              estBetaParams(x, y^2) |>
                                enframe() |>
                                unnest(value) |>
                                pivot_wider()
                            })) |>
  unnest(beta_params) |>
  crossing(n_tags = seq(0, 20)) |>
  mutate(prob_obs_zero = dbetabinom.ab(0,
                                       n_tags,
                                       shape1 = alpha,
                                       shape2 = beta))

upp_ci_df <-
  det_probs |>
  group_by(run_year,
           spawn_year,
           location,
           origin) |>
  filter(n_tags == min(n_tags[prob_obs_zero < 0.05])) |>
  ungroup() |>
  left_join(tag_rate_df) |>
  mutate(n_fish = n_tags / priest_tag_rate) |>
  left_join(parent_child |>
              select(dwnstrm_site = parent,
                     location = child)) |>
  mutate(across(dwnstrm_site,
                ~ paste0(., "_bb"))) |>
  left_join(all_escp |>
              filter(spawn_year == 2022) |>
              select(origin,
                     dwnstrm_site = location,
                     dwnstrm_bb = estimate,
                     dwn_lci = lci,
                     dwn_uci = uci)) |>
  rowwise() |>
  mutate(upp_ci = min(n_fish, dwnstrm_bb)) |>
  ungroup() |>
  rename(prob_det = mu,
         prob_se = se)

upp_ci_df

upp_ci_df |>
  left_join(configuration |>
              select(location = site_code,
                     site_name) |>
              distinct()) |>
  relocate(site_name,
           .after = location)

upp_ci_df |>
  filter(!is.na(dwnstrm_bb)) |>
  clean_names("title") |>
  write_csv(here("requests/Chiwawa_H_2022",
                 "Zero_CI.csv"))

