df_merged_mit_lor_sum <- st_read("data/df_merged_mit_lor_und_sum.geojson")

df_merged_mit_lor_sum_clean <- df_merged_mit_lor_sum %>%
  select(-bzr_name.y) %>%       
  rename(bzr_name = bzr_name.x) 

st_write(df_merged_mit_lor_sum_clean, "data/df_merged_mit_lor_und_sum.geojson", driver = "GEOJSON", delete_dsn = TRUE)