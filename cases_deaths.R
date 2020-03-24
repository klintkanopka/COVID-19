#!/usr/bin/env Rscript

library(tidyverse)

# Shelter in place order dates
bay_shelter <- as.Date("2020-03-16")
ca_shelter <- as.Date("2020-03-19")

# Read data
bay <- read_csv("data/bay_area_agg.csv")
ca  <- read_csv("data/california_agg.csv")
sc  <- read_csv("data/santa_clara_agg.csv")

df <- 
  bind_rows("Bay Area" = bay, 
            "All of CA" = ca, 
            "Santa Clara County" = sc,
            .id = "region") %>% 
  select(region, date, cases, deaths) %>% 
  pivot_longer(c(cases, deaths), names_to = "var", values_to = "count") %>% 
  mutate(group = paste(region, var, sep = ", "))

colors <- c("#003f5c", "#bc5090", "#ffa600") 
ltys <- c("solid", "dashed") 
          
ggplot(data = df, 
       aes(x = date, y = count, color = group, lty = group)) +
  geom_line() +
  geom_segment(x = bay_shelter, xend = bay_shelter,
               y = 0, yend = .92*max(df$count),
               lty = "dotted", color = "black") +
  annotate("text", x = bay_shelter - 6, y = 0.95*max(df$count), label = "Bay Shelter in Place") +
  geom_segment(x = ca_shelter, xend = ca_shelter, 
               y = 0, yend = .97*max(df$count), 
               lty = "dotted", color = "black") +
  annotate("text", x = ca_shelter - 6, y = max(df$count), label = "CA Shelter in Place") +
  theme_minimal() +
  labs(title = "COVID Cases & Deaths Jan 22 through Present", 
       x = "Date", 
       y = "Count", 
       color = "", 
       lty = "", 
       caption = paste("Graph last updated at", Sys.time())) +
  scale_colour_manual(values= rep(colors, each = length(ltys))) +   
  scale_linetype_manual(values=rep(ltys, length(unique(df$region)))) 

ggsave("cases_deaths.png")
