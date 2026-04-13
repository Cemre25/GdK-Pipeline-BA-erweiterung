library(shiny)
library(leaflet)
library(ggplot2)
library(dplyr)
library(lubridate)
library(shinydashboard)
library(plotly)
library(leaflet.extras)
library(tidyr)
library(stringr)
library(shinyBS)
library(sf)
library(data.table)
library(viridis)

# Sozialindex einlesen
sozialindex <- fread("data/sozialindex.csv", sep = ";", encoding = "UTF-8") %>%
  mutate(bzrID = str_pad(as.character(bzrID), width = 6, pad = "0"))

# Gießdaten mit LOR-Infos einlesen
df_merged_mit_lor_sum <- st_read("data/df_merged_mit_lor_und_sum.geojson") %>%
  mutate(bzr_id = str_pad(as.character(bzr_id), width = 6, pad = "0"))

# LOR-Geometrie aus Berlin-Portal laden
lor_url <- "https://gdi.berlin.de/services/wfs/lor_2019?service=WFS&version=1.1.0&request=GetFeature&typeName=lor_2019:b_lor_bzr_2019"

lors <- st_read(lor_url) %>%
  st_transform(4326)

# Tabelle mit bzr_id und Bezirk erstellen
bezirk_lor_map <- lors %>%
  st_drop_geometry() %>%
  select(bzr_id, bez) %>%
  distinct()

# Join: Gießdaten mit Sozialindex
df_joined <- df_merged_mit_lor_sum %>%
  left_join(sozialindex, by = c("bzr_id" = "bzrID"))

# Join: Bezirk hinzufügen
df_joined <- df_joined %>%
  left_join(bezirk_lor_map, by = "bzr_id")

# Export ohne Geometrie
df_export <- df_joined %>%
  st_drop_geometry()

df_export <- df_joined %>%
  st_drop_geometry() %>%
  select(-bzrNAME, -V1) 

# CSV speichern
write.csv2(df_export, file = "data/sozialindex_mit_Gesamtbewässerung.csv", sep = ";")


df_merged_mit_lor_sum_clean %>% n_distinct(df_merged_mit_lor_sum_clean$bzr_name)

lors %>% n_distinct(lors$bzr_name)

sozialindex %>% n_distinct(sozialindex$bzrName)

