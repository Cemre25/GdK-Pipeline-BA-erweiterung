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
library(leafgl)
library(memoise)
library(purrr)

# Konstanten & Funktionen
lor_url <- "https://gdi.berlin.de/services/wfs/lor_2019?service=WFS&version=1.1.0&request=GetFeature&typeName=lor_2019:b_lor_bzr_2019"

bezirksflaechen <- data.frame(
  bezirk = c("Mitte", "Friedrichshain-Kreuzberg", "Pankow", "Charlottenburg-Wilmersdorf",
             "Spandau", "Steglitz-Zehlendorf", "Tempelhof-Schöneberg", "Neukölln",
             "Treptow-Köpenick", "Marzahn-Hellersdorf", "Lichtenberg", "Reinickendorf"),
  flaeche_ha = c(3.940, 2.040, 10.322, 6.469, 9.188, 10.256, 5.305, 4.493, 16.773, 6.182, 5.212, 8.932)
)
