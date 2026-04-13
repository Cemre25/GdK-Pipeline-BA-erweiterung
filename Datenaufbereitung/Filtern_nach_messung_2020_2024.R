# Daten einlesen
df_recent_daily <- read.csv("../Wetterdaten/recent/produkt_klima_tag_20230923_20250325_00433.csv", 
                            sep = ";", stringsAsFactors = FALSE)
df_recent_monthly <- read.csv("../Wetterdaten/recent/produkt_klima_monat_20230801_20250228_00433.csv", 
                              sep = ";", stringsAsFactors = FALSE)
df_historical_daily <- read.csv("../Wetterdaten/historical/produkt_klima_tag_19480101_20231231_00433.csv", 
                                sep = ";", stringsAsFactors = FALSE)
df_historical_monthly <- read.csv("../Wetterdaten/historical/produkt_klima_monat_19380101_20231231_00433.csv", 
                                  sep = ";", stringsAsFactors = FALSE)

# Datum in das richtige Format umwandeln
df_recent_daily$MESS_DATUM <- as.Date(as.character(df_recent_daily$MESS_DATUM), 
                                      format="%Y%m%d")
df_historical_daily$MESS_DATUM <- as.Date(as.character(df_historical_daily$MESS_DATUM), 
                                          format="%Y%m%d")

df_recent_monthly$MESS_DATUM_BEGINN <- as.Date(as.character(df_recent_monthly$MESS_DATUM_BEGINN), 
                                               format="%Y%m%d")
df_recent_monthly$MESS_DATUM_ENDE <- as.Date(as.character(df_recent_monthly$MESS_DATUM_ENDE), 
                                             format="%Y%m%d")

df_historical_monthly$MESS_DATUM_BEGINN <- as.Date(as.character(df_historical_monthly$MESS_DATUM_BEGINN), 
                                                   format="%Y%m%d")
df_historical_monthly$MESS_DATUM_ENDE <- as.Date(as.character(df_historical_monthly$MESS_DATUM_ENDE), 
                                                 format="%Y%m%d")


# Daten zwischen 2020 und 2024 filtern
df_recent_daily_filtered <- subset(df_recent_daily, 
                                   MESS_DATUM >= as.Date("2020-01-01") & MESS_DATUM <= as.Date("2024-12-31"))
df_recent_monthly_filtered <- subset(df_recent_monthly, 
                                     MESS_DATUM_BEGINN >= as.Date("2020-01-01") & MESS_DATUM_BEGINN <= as.Date("2024-12-31"))
df_historical_daily_filtered <- subset(df_historical_daily, 
                                       MESS_DATUM >= as.Date("2020-01-01") & MESS_DATUM <= as.Date("2024-12-31"))
df_historical_monthly_filtered <- subset(df_historical_monthly, 
                                         MESS_DATUM_BEGINN >= as.Date("2020-01-01") & MESS_DATUM_BEGINN <= as.Date("2024-12-31"))



df_daily_combined <- rbind(df_recent_daily_filtered, df_historical_daily_filtered)
df_monthly_combined <- rbind(df_recent_monthly_filtered, df_historical_monthly_filtered)

write.csv2(df_daily_combined, "../Wetterdaten/Filtered_Wetterdaten/combined_daily_daten_2020_2024.csv", 
           row.names = FALSE)
write.csv2(df_monthly_combined, "../Wetterdaten/Filtered_Wetterdaten/combined_monthly_daten_2020_2024.csv", 
           row.names = FALSE)

