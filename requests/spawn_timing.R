# Author: Kevin See
# Purpose: examine spawn timing in the tributaries
# Created: 8/30/2023
# Last Modified: 8/30/2023
# Notes:

#-----------------------------------------------------------------
# load needed libraries
library(tidyverse)
library(here)
library(sroem)
library(janitor)

#-----------------------------------------------------------------
# construct a df of lowest sites in each trib
low_sites <-
  tibble(trib = c("Chumstick",
                  "Mission",
                  "Peshastin",
                  "Icicle",
                  "Chiwaukum",
                  "Chiwawa",
                  "Nason"),
         low_site = c("CHM",
                      "MCL",
                      "PES",
                      "ICL",
                      "CHW",
                      "CHL",
                      "NAL"),
         upp_site = c("CHM",
                      "MCL",
                      "PEU",
                      "ICM",
                      "CHW",
                      "CHU",
                      "NAU"))

spawn_years = c(2014:2022)

df <-
  tibble(sy = spawn_years) |>
  mutate(pit_det = map(sy,
                       .f = function(yr) {
                         load(here("analysis/data/derived_data/model_fits",
                                   paste0('PRA_DABOM_Steelhead_', yr,'.rda')))
                         return(filter_obs)
                       }),
         tag_summ = map(sy,
                       .f = function(yr) {
                         query_dabom_results(dabom_dam_nm = "RockIsland",
                                             query_year = yr,
                                             result_type = "tag_summ")
                       })) |>
  mutate(trib_tags = map(tag_summ,
                         .f = function(x) {
                           wen_tags <-
                             x |>
                             # pull out fish in the Wenatchee
                             filter(str_detect(path, " LWE")) |>
                             mutate(trib = NA_character_)

                           for(i in 1:nrow(low_sites)) {
                             wen_tags$trib[str_detect(wen_tags$path, paste0(" ", low_sites$low_site[i]))] = low_sites$trib[i]
                           }
                           wen_tags |>
                             select(tag_code, origin, trib) %>%
                             return()
                         }),
         first_det = map2(trib_tags,
                     pit_det,
                     .f = function(x, y) {
                       x |>
                         filter(!is.na(trib)) |>
                         # tabyl(trib, origin)
                         left_join(y) |>
                         left_join(low_sites) |>
                         relocate(low_site,
                                  upp_site,
                                  .after = trib) |>
                         rowwise() |>
                         filter(str_detect(node, low_site) |
                                  str_detect(node, upp_site)) |>
                         group_by(tag_code,
                                  origin,
                                  trib) |>
                         summarize(across(min_det,
                                          min),
                                   .groups = "drop") |>
                         filter(month(min_det) <= 6) |>
                         mutate(doy = yday(min_det))
                     }))

ecdf_df <-
  df |>
  select(sy,
         first_det) |>
  unnest(first_det) |>
  nest(first_date = -c(sy,
                       trib,
                       origin)) |>
  mutate(ecdf = map(first_date,
                    .f = function(x) ecdf(x$doy)),
         ecdf_tab = map(ecdf,
                        .f = function(x) {
                          x |>
                            summary() |>
                            as.matrix() |>
                            as_tibble(rownames = "percentile") |>
                            rename(doy = V1)
                        }))

plot_yr = 2022

plot_df <-
  ecdf_df |>
  filter(sy == plot_yr) |>
  mutate(across(sy,
                as_factor)) |>
  select(-starts_with("ecdf")) |>
  unnest(first_date) |>
  mutate(plot_date = ymd(paste(year(today()), "0101")) + days(doy - 1))

samp_size = plot_df |>
  group_by(origin,
           trib) |>
  summarize(n_tags = n_distinct(tag_code),
            .groups = "drop")

day_offset = 8
text_size = 3

plot_df |>
  ggplot(aes(x = min_det,
             color = origin)) +
  scale_color_brewer(palette = "Set1",
                     name = "Origin") +
  stat_ecdf(geom = "step") +
  # geom_text(data = samp_size |>
  #             filter(origin == "W"),
  #           aes(label = paste("n = ", n_tags)),
  #           x = min(plot_df$min_det) + days(day_offset),
  #           y = 0.9,
  #           size = text_size,
  #           show.legend = F) +
  # geom_text(data = samp_size |>
  #             filter(origin == "H"),
  #           aes(label = paste("n = ", n_tags)),
  #           x = min(plot_df$min_det) + days(day_offset),
  #           y = 0.8,
  #           size = text_size,
  #           show.legend = F) +
geom_text(data = samp_size |>
            filter(origin == "W"),
          aes(label = paste("n = ", n_tags)),
          x = max(plot_df$min_det) - days(day_offset),
          y = 0.2,
          size = text_size,
          show.legend = F) +
  geom_text(data = samp_size |>
              filter(origin == "H"),
            aes(label = paste("n = ", n_tags)),
            x = max(plot_df$min_det) - days(day_offset),
            y = 0.1,
            size = text_size,
            show.legend = F) +
  facet_wrap(~ trib,
             scales = "free_y") +
  theme_bw() +
  theme(legend.position = "bottom",
        axis.text.x = element_text(angle = 45,
                                   hjust = 1)) +
  labs(x = "First Detection",
       y = "ECDF",
       title = paste0("SY", plot_yr))

ggsave(filename = here(paste0("requests/spawn_timing_", plot_yr, ".pdf")),
       width = 7,
       height = 7)



ecdf_df |>
  select(sy:trib,
         ecdf_tab) |>
  unnest(ecdf_tab)


p_list <-
  ecdf_df |>
  mutate(across(sy,
                as_factor)) |>
  select(-starts_with("ecdf")) |>
  unnest(first_date) |>
  mutate(plot_date = ymd(paste(year(today()), "0101")) + days(doy - 1)) |>
  nest(plot_data = -origin) |>
  mutate(plots = map(plot_data,
                     .f = function(df) {
                       df |>
                         ggplot(aes(x = plot_date,
                                    color = sy)) +
                         # scale_color_brewer(palette = "Set3",
                         #                    name = "SY") +
                         scale_color_discrete_sequential(palette = "Viridis",
                                                         name = "SY") +
                         stat_ecdf(geom = "step") +
                         facet_wrap(~ trib,
                                    scales = "free_y",
                                    nrow = 2) +
                         theme_bw() +
                         theme(legend.position = "bottom",
                               axis.text.x = element_text(angle = 45,
                                                          hjust = 1)) +
                         labs(x = "First Detection",
                              y = "ECDF")
                     })) |>
  pull(plots)

ggpubr::ggarrange(plotlist = p_list,
                  labels = c("W",
                             "H"),
                  nrow = 2,
                  ncol = 1,
                  common.legend = T,
                  legend = "right")
