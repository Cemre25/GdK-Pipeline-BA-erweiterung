library(dplyr)

df_merged_sum_distanz_umkreis_pump_ok_lor <- fread("data/df_merged_sum_mit_distanzen_mit_umkreis_gesamter_Baumbestand_nur_Pumpen_ok_lor.csv",sep = ";", encoding = "UTF-8")
df_merged <- fread("data/df_merged_final.csv", sep = ";", encoding = "UTF-8")

df_export <- df_merged_sum_distanz_umkreis_pump_ok_lor %>%
  st_drop_geometry() %>%
  select(-gml_id, -X, -V1) 

glimpse(df_merged_sum_distanz_umkreis_pump_ok_lor)
glimpse(df_merged)


# Schritt 1: Auswahl der relevanten Spalten
df_gml <- df_merged %>%
  select(gisid, gml_id)

# Schritt 2: Merge
df_merged_final <- df_merged_sum_distanz_umkreis_pump_ok_lor %>%
  st_drop_geometry() %>%
  select(-gml_id) %>%  # falls du es bereits entfernt hast, optional
  left_join(df_gml, by = "gisid")

glimpse(df_merged_final)

write.csv2(df_merged_final, file = "data/df_merged_sum_mit_distanzen_mit_umkreis_gesamter_Baumbestand_nur_Pumpen_ok_lor.csv")
