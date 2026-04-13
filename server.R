# Server-Logik
server <- function(input, output, session) {
  
  

  sozialindex_mit_Gesamtbewasserung_agg <- reactive({
    fread("data/sozialindex_mit_Gesamtbewässerung.csv", sep = ";", encoding = "UTF-8") %>%
      mutate(
        gesamt_bewaesserung_lor = as.numeric(gsub(",", ".", gesamt_bewaesserung_lor)),
        GESIx_2022 = as.numeric(gsub(",", ".", GESIx_2022))
      )
  })
  
  kpi <- reactive({
    fread("data/KPI.csv", sep = ";", encoding = "UTF-8")
  })
  
  lor <- reactive({
    st_read(lor_url) %>%
      select(bzr_id, bzr_name, geom) %>%
      st_simplify(preserveTopology = TRUE, dTolerance = 0.001) %>%
      st_transform(4326)
  })
  
  df_wetter_monatlich <- reactive({
    read.csv("data/combined_monthly_daten_2020_2024_minimal.csv", sep = ";", stringsAsFactors = FALSE, fileEncoding = "UTF-8") %>%
      mutate(
        # Datum korrekt parsen (aus "01.08.2023" wird Date-Objekt)
        date = as.Date(MESS_DATUM_BEGINN, format = "%d.%m.%Y"),
        year = year(date),
        month = month(date),
        
        # Numerische Spalten umwandeln (Komma als Dezimaltrenner)
        MO_RR = as.numeric(gsub(",", ".", MO_RR)),  # Niederschlag (mm)
        MO_TT = as.numeric(gsub(",", ".", MO_TT))   # Temperatur (°C)
      ) %>%
      # -999 durch NA ersetzen (fehlende Werte)
      mutate(
        MO_RR = ifelse(MO_RR == -999, NA, MO_RR),
        MO_TT = ifelse(MO_TT == -999, NA, MO_TT)
      ) %>%
      select(date, year, month, niederschlag = MO_RR, temp_avg = MO_TT)  # Umbenennen der Spalten
    
  })
    
  
  df_merged <- reactive({
    fread("data/df_merged_final.csv", sep = ";", encoding = "UTF-8") %>%
      mutate(
        pflanzjahr = as.numeric(pflanzjahr),
        bewaesserungsmenge_in_liter = as.numeric(bewaesserungsmenge_in_liter),
        baumalter = ifelse(!is.na(pflanzjahr), year(Sys.Date()) - pflanzjahr, NA)
      )
  })
  
  df_merged_mit_lor_sum <- reactive({
    st_read("data/df_merged_mit_lor_und_sum.geojson")
  })
  
  df_merged_unique <- reactive({
    df_merged() %>%
      distinct(gisid, .keep_all = TRUE)
  })
  
  pumpen_mit_bezirk <- reactive({
    df <- st_read("data/pumpen_mit_bezirk_minimal.geojson")
    df$label_text <- ifelse(!is.na(df$id), as.character(df$id), "Unbekannt")
    df
  })
  
  pumpen_mit_lor <- reactive({
    st_read("data/pumpen_mit_lor.geojson")
  })
  
  
  df_merged_sum_distanz_umkreis_pump_ok_lor <- reactive({
    df <- fread("data/df_merged_sum_mit_distanzen_mit_umkreis_gesamter_Baumbestand_nur_Pumpen_ok_lor.csv", sep = ";", encoding = "UTF-8")
    
    df <- df %>%
      mutate(
        lng = as.numeric(str_replace(lng, ",", ".")),
        lat = as.numeric(str_replace(lat, ",", ".")),
        durchschnitts_intervall = as.numeric(str_replace(durchschnitts_intervall, ",", "."))
      ) %>%
      drop_na(lat, lng)
    
    df
  })
  
  df_merged_sum_distanz_umkreis_pump_ok_lor_clean <- reactive({
    df_merged_sum_distanz_umkreis_pump_ok_lor() %>% drop_na(timestamp)
  })
  
  df_merged_clean <- reactive({
    df_merged() %>% drop_na(bezirk, timestamp, gattung_deutsch)
  })
  
  sozialindex <- reactive({
    fread("data/sozialindex.csv", sep = ";", encoding = "UTF-8")
  })
  
  bezirksgrenzen <- reactive({
    st_read("data/bezirksgrenzen.geojson")
  })
  
  
  df_merged_sum_distanz_umkreis_pump_ok_lor_with_fl <- reactive({
    df_merged_sum_distanz_umkreis_pump_ok_lor() %>%
      left_join(bezirksflaechen, by = "bezirk")
  })
  
  baumanzahl_pro_bezirk <- reactive({
    df_merged_sum_distanz_umkreis_pump_ok_lor() %>%
      group_by(bezirk) %>%
      summarise(baumanzahl = n_distinct(gisid))
  })
  
  baum_dichte <- reactive({
    baumanzahl_pro_bezirk() %>%
      left_join(bezirksflaechen, by = "bezirk") %>%
      mutate(baeume_pro_ha = baumanzahl / flaeche_ha)
  })
  
  cleaned_data <- reactive({
    req(df_merged_sum_distanz_umkreis_pump_ok_lor()) 
    df <- df_merged_sum_distanz_umkreis_pump_ok_lor()
    
    # Überprüfe, ob df ein Dataframe ist
    if (!is.data.frame(df)) {
      stop("Daten sind kein Dataframe. Klasse: ", class(df))
    }
    
    df %>%
      mutate(
        timestamp = as.Date(timestamp),
        monat = month(timestamp),
        saison = case_when(
          monat %in% c(12, 1, 2) ~ "Winter",
          monat %in% c(3, 4, 5) ~ "Frühling",
          monat %in% c(6, 7, 8) ~ "Sommer",
          monat %in% c(9, 10, 11) ~ "Herbst",
          TRUE ~ "Unbekannt"
        )
      )
  })
  
  cleaned_data_light <- reactive({
    cleaned_data() %>%
      select(gisid, gesamt_bewaesserung, durchschnitts_intervall, gattung_deutsch,
             art_dtsch, hausnr, strname, bezirk, bzr_name, lng, lat, timestamp) %>%
      mutate(
        year = year(timestamp),
        month = month(timestamp)
      )
  })
  
  wetter_monat <- reactive({
    df_wetter_monatlich() %>%
      select(year, month, niederschlag, temp_avg)
  })
  
  gisid_check <- reactive({
    df_merged() %>%
      group_by(gisid) %>%
      filter(n() > 1) %>%
      summarise(across(everything(), ~ n_distinct(.))) %>%
      filter(if_any(everything(), ~ . > 1))
  })
  
  merged_for_rating_base <- reactive({
    cleaned_data_light() %>%
      left_join(wetter_monat(), by = c("year", "month"), relationship = "many-to-many") %>%
      left_join(df_merged_unique() %>% select(gisid, baumalter), by = "gisid")
  })
  
  merged_for_rating <- reactive({
    merged_for_rating_base() %>%
      mutate(
        gesamt_bewaesserung_rating = case_when(
          is.na(niederschlag) | is.na(temp_avg) ~ "Unbekannt",
          baumalter < 10 & niederschlag < 30 & temp_avg > 25 ~ "Hoch",
          baumalter < 30 & niederschlag < 60 & temp_avg > 20 ~ "Mittel",
          TRUE ~ "Niedrig"
        )
      )
  })
 
  # Einwohnerdaten + Gießmengen pro Bezirk kombinieren
  einwohnerGiessm <- reactive({
    fread("data/Einwohner_Giessmenge_joined.csv", sep = ";", encoding = "UTF-8") })

  einwohner <- reactive({
    fread("data/GesamteEinwohnerzahlNachBezirk.csv", sep = ";", encoding = "UTF-8") })
  
  einwohnerGiessm2020_24 <- reactive({
    fread("data/Einwohner_Giessmenge_joined_2020_2024.csv", sep = ";", encoding = "UTF-8") })
  
  
  
  # observe({
  #   print(head(cor_data()))
  #   # print(head(giessmenge_pro_bezirk()))
  #   browser()  # hält die App hier an
  # })

 
 
 
  # observe({
  #   req(input$start_year)
  #   req(input$bezirk)
  #   req(input$stats_baumvt_year)
  #   req(input$pie_bezirk)
  #   req(input$map_bezirk)
  #   req(input$map_year)
  #   req(input$map_baumgattung)
  #   req(input$map_lor)
  #   req(input$bar_bezirk)
  #   req(input$trend_bezirk_pj)
  #   req(input$trend_year_ts)
  #   req(input$trend_bezirk)
  #   req(input$trend_baumgattung)
  #   req(input$trend_year)
  #   req(input$selected_bezirk)
  #   
  #   
  #   #updateSelectInput(session, "start_year", choices = c("2020-2024", "Baumbestand Stand 2025", sort(unique(na.omit(year(df_merged()$timestamp))))), selected = isolate(input$start_year))
  #   
  #  # updateSelectInput(session, "bezirk", choices = c("Alle", unique(df_merged_clean()$bezirk)), selected = isolate(input$bezirk))
  #   # updateSelectInput(session, "stats_baumvt_year", choices = c("2020-2024", "Baumbestand Stand 2025", sort(unique(na.omit(year(df_merged()$timestamp))))), selected = isolate(input$stats_baumvt_year))
  #   
  #   #updateSelectInput(session, "pie_bezirk", choices = c("Alle", unique(df_merged_clean()$bezirk)), selected = isolate(input$pie_bezirk))
  #   #updateSelectInput(session, "map_bezirk", choices = c("Alle", unique(cleaned_data()$bezirk)), selected = isolate(input$map_bezirk))
  #   #updateSelectInput(session, "map_year", choices = c("2020-2024", unique(year(cleaned_data()$timestamp))), selected = isolate(input$map_year))
  #   #updateSelectInput(session, "map_baumgattung", choices = c("Alle", unique(cleaned_data()$gattung_deutsch)), selected = isolate(input$map_baumgattung))
  #   #updateSelectInput(session, "map_lor", choices = c("Alle", sort(unique(df_merged_sum_distanz_umkreis_pump_ok_lor()$bzr_name))), selected = isolate(input$map_lor))
  #   
  #   #updateSelectInput(session, "bar_bezirk", choices = c("Alle", unique(df_merged_clean()$bezirk)), selected = isolate(input$bar_bezirk))
  #   #updateSelectInput(session, "trend_bezirk_pj", choices = c("Alle", unique(df_merged_clean()$bezirk)), selected = isolate(input$trend_bezirk_pj))
  #   # updateSelectInput(session, "trend_year_ts", choices = unique(year(df_merged_sum_distanz_umkreis_pump_ok_lor_clean()$timestamp)), selected = isolate(input$trend_year_ts))
  #   # updateSelectInput(session, "trend_bezirk", choices = c("Alle", unique(df_merged_clean()$bezirk)), selected = isolate(input$trend_bezirk))
  #   # updateSelectInput(session, "trend_baumgattung", choices = c("Alle", unique(df_merged()$gattung_deutsch)), selected = isolate(input$trend_baumgattung))
  #   updateSliderInput(session, "trend_year", min = min(df_merged()$pflanzjahr, na.rm = TRUE),
  #                     max = max(df_merged()$pflanzjahr, na.rm = TRUE),
  #                     value = c(min(df_merged()$pflanzjahr, na.rm = TRUE),
  #                               max(df_merged()$pflanzjahr, na.rm = TRUE)),
  #   )
  #   
  #   updateSelectInput(session, "selected_bezirk", choices = sort(unique(sozialindex_mit_Gesamtbewasserung_agg()$bez)), selected = isolate(input$selected_bezirk))
  # })
  
  
  
  observe({
    req(df_merged())
    req(df_merged_clean())
    req(cleaned_data())
    req(df_merged_sum_distanz_umkreis_pump_ok_lor())
    req(df_merged_sum_distanz_umkreis_pump_ok_lor_clean())
    req(sozialindex_mit_Gesamtbewasserung_agg())
    
    jahr_choices <- c("2020-2024", "Baumbestand Stand 2025", sort(unique(na.omit(year(df_merged()$timestamp)))))
    updateSelectInput(session, "start_year", choices = jahr_choices, selected = "Baumbestand Stand 2025")
    
    bezirk_choices <- c("Alle",  unique(df_merged_clean()$bezirk))
    updateSelectInput(session, "bezirk", choices = bezirk_choices, selected = "Alle")
    updateSelectInput(session, "stats_baumvt_year", choices = jahr_choices, selected = "Baumbestand Stand 2025")
    
    bezirk_choices_cleaned  <- c("Alle",  unique(cleaned_data()$bezirk))
    jahr_choices_cleaned <- c("2020-2024", "Baumbestand Stand 2025", sort(unique(na.omit(year(cleaned_data()$timestamp)))))
    baumgattung_choices <- c("Alle", unique(cleaned_data()$gattung_deutsch))
    lor_choices <- c("Alle", sort(unique(df_merged_sum_distanz_umkreis_pump_ok_lor()$bzr_name)))

    updateSelectInput(session, "pie_bezirk", choices = bezirk_choices, selected = "Alle")
    updateSelectInput(session, "map_bezirk", choices = bezirk_choices_cleaned, selected = "Alle" )
    updateSelectInput(session, "map_year", choices = jahr_choices_cleaned, selected = "2020-2024")
    updateSelectInput(session, "map_baumgattung", choices = baumgattung_choices, selected = "Alle")
    updateSelectInput(session, "map_lor", choices = lor_choices, selected = "Alle")

    jahr_choices_df <- c(2024, sort(unique(year(df_merged_sum_distanz_umkreis_pump_ok_lor_clean()$timestamp))))
    baumgattung_choices_merged <- c("Alle", unique(df_merged()$gattung_deutsch))
    # pflanzjahr_choices <- c("Alle", min = min(df_merged()$pflanzjahr, na.rm = TRUE),
    #                   max = max(df_merged()$pflanzjahr, na.rm = TRUE),
    #                   value = c(min(df_merged()$pflanzjahr, na.rm = TRUE),
    #                             max(df_merged()$pflanzjahr, na.rm = TRUE)))
    bezirk_choices_sozialindex <- c( sort(unique(sozialindex_mit_Gesamtbewasserung_agg()$bez)))

    updateSelectInput(session, "bar_bezirk", choices = bezirk_choices, selected ="Alle")
    updateSelectInput(session, "trend_bezirk_pj", choices = bezirk_choices, selected = "Alle")
    updateSelectInput(session, "trend_year_ts", choices = jahr_choices_df, selected = "2024" )
    updateSelectInput(session, "trend_bezirk", choices = bezirk_choices, selected = "Alle")
    updateSelectInput(session, "trend_baumgattung", choices= baumgattung_choices_merged, selected = "Alle")
    updateSliderInput(
      session, "trend_year",
      min = 1800,
      max = max(df_merged()$pflanzjahr, na.rm = TRUE),
      value = c(min(df_merged()$pflanzjahr, na.rm = TRUE),
                max(df_merged()$pflanzjahr, na.rm = TRUE))
    )
    
    updateSelectInput(session, "selected_bezirk", choices = bezirk_choices_sozialindex, selected = "01 - Mitte")
    
    updateRadioButtons(session, "water_mode", selected = "bd")

  })
  
  
  
  # observeEvent(input$info_boxes, {
  #   showModal(modalDialog(
  #     title = "Erklärung:",
  #     HTML("
  #     <strong>Was wird angezeigt?</strong><br><br>
  #     <strong>Box 1</strong><br>
  #     Zeigt die <strong>Gesamtmenge an Wasser</strong>, die im gewählten Zeitraum und Bezirk zum Gießen verwendet wurde, automatisch umgerechnet in passende Einheiten (Liter, m³, ML).<br><br>
  # 
  #     <strong>Box 2</strong><br>
  #     Der <strong>Durchschnitt</strong> an Wassermenge pro gegossenen Baum also: wie viel Wasser bekam ein Baum im Schnitt.<br><br>
  # 
  #     <strong>Box 3</strong><br>
  #     Dieses Diagramm zeigt die <strong>Anzahl Bäume pro Hektar</strong> in den Berliner Bezirken – also die <strong>Baumdichte</strong> bezogen auf die Fläche jedes Bezirks. Dadurch lässt sich erkennen, wie dicht bepflanzt ein Bezirk im Verhältnis zu seiner Größe ist.<br>
  # 
  #     <strong>Datenquelle</strong><br>
  #       <ul>
  #         <li><a href="https://daten.berlin.de/datensaetze/baumbestand-berlin-wfs-48ad3a23">Baumbestandsdaten</a></li>
  #         <li><a href="https://www.govdata.de/suche/daten/giess-den-kiez-nutzungsdaten">Gießdaten des Projekts 'Gieß den Kiez'</a></li>
  #       </ul>
  #   "),
  #     easyClose = TRUE,
  #     footer = modalButton("Schließen")
  #   ))
  # })
  # 
  # 
  # # observeEvent(input$info_avg_water, {
  # #   showModal(modalDialog(
  # #     title = "Erklärung:",
  # #     HTML("<b>Was wird angezeigt?</b><br>
  # #   Der <strong>Durchschnitt</strong> an Wassermenge pro gegossenen Baum, also: wie viel Wasser bekam ein Baum im Schnitt."),
  # #     easyClose = TRUE,
  # #     footer = modalButton("Schließen")
  # #   ))
  # # })
  # # 
  # # observeEvent(input$info_tree_distribution, {
  # #   showModal(modalDialog(
  # #     title = "Erklärung:",
  # #     HTML("<b>Was wird angezeigt?</b><br>
  # #   Dieses Diagramm zeigt die <strong>Anzahl Bäume pro Hektar</strong> in den Berliner Bezirken, also die <strong>Baumdichte</strong> bezogen auf die Fläche jedes Bezirks. Dadurch lässt sich erkennen, wie dicht bepflanzt ein Bezirk im Verhältnis zu seiner Größe ist.<br><br>
  # #   <b>Wie Filter wirken:</b>
  # #   <ul>
  # #     <li><strong>Jahr-Auswahl:</strong> Zeigt entweder alle gepflanzten Bäume („Baumbestand Stand 2025“) oder nur tatsächlich bewässerte Bäume („2020–2024“ oder ein bestimmtes Jahr).</li>
  # #     <li><strong>Bezirk:</strong> Eingrenzung auf einzelne Bezirke möglich.</li>
  # #   </ul>"),
  # #     easyClose = TRUE,
  # #     footer = modalButton("Schließen")
  # #   ))
  # # })
  # 
  # observeEvent(input$info_hist_bewaesserung_pro_bezirk, {
  #   showModal(modalDialog(
  #     title = "Erklärung:",
  #     HTML("<b>Was wird angezeigt?</b><br>
  #     Ein Balkendiagramm mit der <strong>Gesamtwassermenge</strong>, die pro Bezirk zum Gießen verwendet wurde. Die Werte sind automatisch in passende Einheiten (Liter, m³ oder ML) umgerechnet.<br>
  #   
  #      <strong>Datenquelle</strong><br>
  #       <ul>
  #         <li><a href="https://daten.berlin.de/datensaetze/baumbestand-berlin-wfs-48ad3a23">Baumbestandsdaten</a></li>
  #         <li><a href="https://www.govdata.de/suche/daten/giess-den-kiez-nutzungsdaten">Gießdaten des Projekts 'Gieß den Kiez'</a></li>
  #       </ul>
  #   "),
  #     easyClose = TRUE,
  #     footer = modalButton("Schließen")
  #   ))
  # })
  # 
  # observeEvent(input$info_hist_bewaesserung_verhaeltnis, {
  #   showModal(modalDialog(
  #     title = "Erklärung:",
  #     HTML("<b>Was wird angezeigt?</b><br>
  #   Wie viel Wasser im Durchschnitt <strong>pro Baum</strong> verwendet wurde, getrennt nach Bezirken. Das hilft, Bezirke mit intensiver oder sparsamer Bewässerung zu erkennen.<br>
  #   
  #    <strong>Datenquelle</strong><br>
  #       <ul>
  #         <li><a href="https://daten.berlin.de/datensaetze/baumbestand-berlin-wfs-48ad3a23">Baumbestandsdaten</a></li>
  #         <li><a href="https://www.govdata.de/suche/daten/giess-den-kiez-nutzungsdaten">Gießdaten des Projekts 'Gieß den Kiez'</a></li>
  #       </ul>
  #   "),
  #     easyClose = TRUE,
  #     footer = modalButton("Schließen")
  #   ))
  # })
  # 
  # observeEvent(input$info_trend_water, {
  #   showModal(modalDialog(
  #     title = "Erklärung:",
  #     HTML("<b>Was wird angezeigt?</b><br>
  #   Dieses Diagramm zeigt, wie viel Wasser für Bäume <strong>verschiedener Pflanzjahre</strong> verwendet wurde. Es zeigt z. B., ob jüngere Bäume häufiger gegossen wurden.<br><br>
  #   <b>Wie Filter wirken:</b>
  #   <ul>
  #     <li><strong>Jahr-Spanne:</strong> Auswahl bestimmter Pflanzjahre.</li>
  #     <li><strong>Bezirk:</strong> Fokus auf einzelne Stadtteile möglich.</li>
  #   </ul><br>
  #   
  #   <strong>Datenquelle</strong><br>
  #       <ul>
  #         <li><a href="https://daten.berlin.de/datensaetze/baumbestand-berlin-wfs-48ad3a23">Baumbestandsdaten</a></li>
  #         <li><a href="https://www.govdata.de/suche/daten/giess-den-kiez-nutzungsdaten">Gießdaten des Projekts 'Gieß den Kiez'</a></li>
  #       </ul>
  # 
  #   "),
  #     easyClose = TRUE,
  #     footer = modalButton("Schließen")
  #   ))
  # })
  # 
  # observeEvent(input$info_trend_water_ts, {
  #   showModal(modalDialog(
  #     title = "Erklärung:",
  #     HTML("<b>Was wird angezeigt:</b><br>
  #   Die Entwicklung von <strong>Bewässerungsmenge, Temperatur und Niederschlag</strong> im zeitlichen Verlauf monatlich oder jährlich.<br><br>
  #   <b>Wie Filter wirken:</b>
  #   <ul>
  #     <li><strong>Jahr/Modus:</strong> Auswahl eines bestimmten Jahres oder Modus (monatlich/jährlich).</li>
  #     <li><strong>Bezirk & Baumart:</strong> Eingrenzung nach Region oder Art möglich.</li>
  #   </ul><br>
  #   
  #   <strong>Datenquelle</strong><br>
  #   <ul>
  #     <li><a href="https://daten.berlin.de/datensaetze/baumbestand-berlin-wfs-48ad3a23">Baumbestandsdaten</a></li>
  #     <li><a href="https://www.govdata.de/suche/daten/giess-den-kiez-nutzungsdaten">Gießdaten des Projekts 'Gieß den Kiez'</a></li>
  #     <li><a href="https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/">Wetterdaten</a></li>
  #   </ul>"),
  #     easyClose = TRUE,
  #     footer = modalButton("Schließen")
  #   ))
  # })
  # 
  # observeEvent(input$info_tree_pie_chart, {
  #   showModal(modalDialog(
  #     title = "Erklärung:",
  #     HTML("<b>Was wird angezeigt?</b><br>
  #   Ein Kreisdiagramm der <strong>Top 10 Baumarten</strong> mit ihrem Anteil an gegossenen Bäumen, also, welche Arten besonders häufig bewässert wurden.<br><br>
  #   <b>Wie Filter wirken:</b>
  #   <ul><li>Optional Eingrenzung auf einen Bezirk.</li></ul><br>
  #   
  #  <strong>Datenquelle</strong><br>
  #     <ul>
  #       <li><a href="https://daten.berlin.de/datensaetze/baumbestand-berlin-wfs-48ad3a23">Baumbestandsdaten</a></li>
  #       <li><a href="https://www.govdata.de/suche/daten/giess-den-kiez-nutzungsdaten">Gießdaten des Projekts 'Gieß den Kiez'</a></li>
  #     </ul>"),
  #     easyClose = TRUE,
  #     footer = modalButton("Schließen")
  #   ))
  # })
  # 
  # observeEvent(input$info_map, {
  #   showModal(modalDialog(
  #     title = "Erklärung:",
  #     HTML("<b>Was wird angezeigt?</b><br>
  #   Eine interaktive <strong>Karte von Berlin</strong> mit allen Bäumen als Punkte und ihrer Bewässerungshistorie. Ab einer bestimmten Zoomstufe werden auch Pumpen eingeblendet.<br><br>
  #   <b>Wie Filter wirken:</b>
  #   <ul>
  #     <li><strong>Bezirk, Baumart, Jahr, Saison:</strong> bestimmen die Auswahl der Bäume.</li>
  #     <li><strong>Kartenbereich:</strong> Nur sichtbare Bäume werden geladen.</li>
  #     <li><strong>Zoom-Level:</strong> Pumpen werden erst ab einer bestimmten Zoomstufe angezeigt.</li>
  #   </ul><br>
  #   
  #   <strong>Datenquelle</strong><br>
  #   <ul>
  #     <li><a href="https://daten.berlin.de/datensaetze/baumbestand-berlin-wfs-48ad3a23">Baumbestandsdaten</a></li>
  #     <li><a href="https://www.govdata.de/suche/daten/giess-den-kiez-nutzungsdaten">Gießdaten des Projekts 'Gieß den Kiez'</a></li>
  #     <li><a href="https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/">Wetterdaten</a></li>
  #   </ul>
  #   "),
  #     easyClose = TRUE,
  #     footer = modalButton("Schließen")
  #   ))
  # })
  # 
  # observeEvent(input$info_balken_plot, {
  #   showModal(modalDialog(
  #     title = "Erklärung:",
  #     HTML("<b>Was wird angezeigt?</b><br>
  #   Ein gruppiertes Balkendiagramm mit:
  #   <ul>
  #     <li><strong>Blau:</strong> Gesamtbewässerung pro Bezirk</li>
  #     <li><strong>Orange:</strong> Anzahl der funktionierenden Pumpen</li>
  #   </ul>
  #   <b>Hinweis:</b> Die beiden Größen sind aufeinander skaliert, damit sie gemeinsam dargestellt werden können.<br>
  #   
  #   <strong>Datenquelle</strong><br>
  #   <ul>
  #     <li><a href="https://daten.berlin.de/datensaetze/baumbestand-berlin-wfs-48ad3a23">Baumbestandsdaten</a></li>
  #     <li><a href="https://www.govdata.de/suche/daten/giess-den-kiez-nutzungsdaten">Gießdaten des Projekts 'Gieß den Kiez'</a></li>
  #     <li><a href="https://overpass-turbo.eu/">Pumpendaten</a></li>
  #   </ul>
  #   "),
  #     easyClose = TRUE,
  #     footer = modalButton("Schließen")
  #   ))
  # })
  # 
  # observeEvent(input$info_karte_giessverhalten, {
  #   showModal(modalDialog(
  #     title = "Erklärung:",
  #     HTML("<b>Was wird angezeigt?</b><br>
  #   Eine Karte mit Bezirken (grobe Ebene) oder LORs (feinere Gebietseinheiten), eingefärbt nach ihrer <strong>Gesamtbewässerung</strong>. Pumpenstandorte werden je nach Zoomstufe eingeblendet.<br>
  #   
  #   <strong>Datenquelle</strong><br>
  #   <ul>
  #     <li><a href="https://daten.berlin.de/datensaetze/baumbestand-berlin-wfs-48ad3a23">Baumbestandsdaten</a></li>
  #     <li><a href="https://www.govdata.de/suche/daten/giess-den-kiez-nutzungsdaten">Gießdaten des Projekts 'Gieß den Kiez'</a></li>
  #     <li><a href="https://overpass-turbo.eu/">Pumpendaten</a></li>
  #     <li><a href=" https://www.berlin.de/sen/sbw/stadtdaten/stadtwissen/sozialraumorientierte-planungsgrundlagen/lebensweltlich-orientierte-raeume">LOR-Bereiche</a>/<li>
  #   </ul>"),
  #     easyClose = TRUE,
  #     footer = modalButton("Schließen")
  #   ))
  # })
  # 
  # observeEvent(input$info_hist_Top_10_best_verhaeltnis_baum, {
  #   showModal(modalDialog(
  #     title = "Erklärung:",
  #     HTML("<b>Was wird angezeigt?</b><br>
  #   Die zehn Straßen, bei denen im Verhältnis pro Baum <strong>am meisten oder am wenigsten Wasser</strong> verwendet wurde.<br><br>
  #   <b>Wie Filter wirken:</b>
  #   <ul>
  #     <li><strong>Bezirk:</strong> Auswahl über <code>bar_bezirk</code>.</li>
  #     <li>Nur gegossene Bäume mit zugeordneter Straße werden berücksichtigt.</li>
  #   </ul><br>
  #    <strong>Datenquelle</strong><br>
  #   <ul>
  #     <li><a href="https://daten.berlin.de/datensaetze/baumbestand-berlin-wfs-48ad3a23">Baumbestandsdaten</a></li>
  #     <li><a href="https://www.govdata.de/suche/daten/giess-den-kiez-nutzungsdaten">Gießdaten des Projekts 'Gieß den Kiez'</a></li>
  #   </ul>"),
  #     easyClose = TRUE,
  #     footer = modalButton("Schließen")
  #   ))
  # })
  # 
  # observeEvent(input$info_pumpenkategorien_plot_pump_ok_nur_straße, {
  #   showModal(modalDialog(
  #     title = "Erklärung:",
  #     HTML("<b>Was wird angezeigt?</b><br>
  #   Straßen werden in Kategorien unterteilt nach der Anzahl von Pumpen im Umkreis (100 m):<br>
  #   <ul>
  #     <li><strong>Keine Pumpe</strong></li>
  #     <li><strong>Eine Pumpe</strong></li>
  #     <li><strong>Mehrere Pumpen</strong></li>
  #   </ul>
  #   Für jede Kategorie wird z. B. die <strong>durchschnittliche Gießmenge</strong> oder <strong>Anzahl der Bäume</strong> angezeigt.<br><br>
  #   <b>Wie Filter wirken:</b>
  #   <ul>
  #     <li>Beruht auf <code>pumpen_im_umkreis_100m</code> je Baumstandort.</li>
  #     <li>Bezirk filterbar über <code>bar_bezirk</code>.</li>
  #   </ul><br>
  #    <strong>Datenquelle</strong><br>
  #   <ul>
  #     <li><a href="https://daten.berlin.de/datensaetze/baumbestand-berlin-wfs-48ad3a23">Baumbestandsdaten</a></li>
  #     <li><a href="https://www.govdata.de/suche/daten/giess-den-kiez-nutzungsdaten">Gießdaten des Projekts 'Gieß den Kiez'</a></li>
  #     <li><a href="https://overpass-turbo.eu/">Pumpendaten</a></li>
  #   </ul>
  #   "),
  #     easyClose = TRUE,
  #     footer = modalButton("Schließen")
  #   ))
  # })
  # 
  # observeEvent(input$hist_bewaesserung_pro_lor, {
  #   showModal(modalDialog(
  #     title = "Erklärung:",
  #     HTML("<b>Was wird angezeigt?</b><br>
  #   Eine <strong>feinräumige Darstellung</strong>: Zeigt die gesamte Wassermenge, die in jedem <strong>LOR-Gebiet</strong> (Lebensweltlich orientierter Raum) verwendet wurde.<br><br>
  #   <b>Wie Filter wirken:</b>
  #   <ul>
  #     <li>Berücksichtigt Bezirks- und Jahresauswahl.</li>
  #     <li>Basiert auf <code>df_merged_mit_lor_sum</code>.</li>
  #   </ul><br>
  #    <strong>Datenquelle</strong><br>
  #   <ul>
  #     <li><a href="https://www.govdata.de/suche/daten/giess-den-kiez-nutzungsdaten">Gießdaten des Projekts 'Gieß den Kiez'</a></li>
  #     <li><a href=" https://www.berlin.de/sen/sbw/stadtdaten/stadtwissen/sozialraumorientierte-planungsgrundlagen/lebensweltlich-orientierte-raeume">LOR-Bereiche</a>/<li>
  #   </ul>
  #   
  #   "),
  #     easyClose = TRUE,
  #     footer = modalButton("Schließen")
  #   ))
  # })
  
  
  




  observeEvent(input$info_boxes, {
  showModal(modalDialog(
    title = "Erklärung:",
    HTML(paste0("
      <strong>Was wird angezeigt?</strong><br><br>
      <strong>Box 1</strong><br>
            Die Grafik zeigt die <strong>Gesamtzahl der Bäume</strong> in <strong>Berlin</strong>. Wird ein <strong>spezifisches Jahr</strong> ausgewählt, werden ausschließlich die Bäume angezeigt, die in diesem <strong>Jahr tatsächlich bewässert</strong> wurden.<br><br>

      <strong>Box 2</strong><br>
      Der <strong>Durchschnitt</strong> an Wassermenge pro gegossenen Baum also: wie viel Wasser bekam ein Baum im Schnitt.<br><br>

      <strong>Box 3</strong><br>
      Zeigt die <strong>Gesamtmenge an Wasser</strong>, die im gewählten Zeitraum und Bezirk zum Gießen verwendet wurde, automatisch umgerechnet in passende Einheiten (Liter, m³, ML).<br><br>

      <strong>Datenquelle</strong><br>
      <ul>
        <li><a href='https://daten.berlin.de/datensaetze/baumbestand-berlin-wfs-48ad3a23'>Baumbestandsdaten</a></li>
        <li><a href='https://www.govdata.de/suche/daten/giess-den-kiez-nutzungsdaten'>Gießdaten des Projekts 'Gieß den Kiez'</a></li>
      </ul>
    ")),
    easyClose = TRUE,
    footer = modalButton("Schließen")
  ))
})
  
observeEvent(input$info_tree_distribution, {
  showModal(modalDialog(
    title = "Erklärung:",
    HTML("<b>Was wird angezeigt?</b><br>
    Dieses Diagramm zeigt die <strong>Anzahl Bäume pro Hektar</strong> in den Berliner Bezirken, also die <strong>Baumdichte</strong> bezogen auf die Fläche jedes Bezirks. Dadurch lässt sich erkennen, wie dicht bepflanzt ein Bezirk im Verhältnis zu seiner Größe ist.<br><br>
    <b>Wie Filter wirken:</b>
    <ul>
      <li><strong>Jahr-Auswahl:</strong> Zeigt entweder alle gepflanzten Bäume („Baumbestand Stand 2025“) oder nur tatsächlich bewässerte Bäume („2020–2024“ oder ein bestimmtes Jahr).</li>
      <li><strong>Bezirk:</strong> Eingrenzung auf einzelne Bezirke möglich.</li>
    </ul><br>
    
    <strong>Datenquelle</strong><br>
      <ul>
        <li><a href='https://daten.berlin.de/datensaetze/baumbestand-berlin-wfs-48ad3a23'>Baumbestandsdaten</a></li>
        <li><a href='https://www.govdata.de/suche/daten/giess-den-kiez-nutzungsdaten'>Gießdaten des Projekts 'Gieß den Kiez'</a></li>
      </ul>     
         "),
      easyClose = TRUE,
      footer = modalButton("Schließen")
  ))
})

observeEvent(input$info_hist_bewaesserung_pro_bezirk, {
  showModal(modalDialog(
    title = "Erklärung:",
    HTML(paste0("
      <b>Was wird angezeigt?</b><br>
      Ein Balkendiagramm mit der <strong>Gesamtwassermenge</strong>, die pro Bezirk zum Gießen verwendet wurde. Die Werte sind automatisch in passende Einheiten (Liter, m³ oder ML) umgerechnet.<br><br>
    
      <strong>Datenquelle</strong><br>
      <ul>
        <li><a href='https://daten.berlin.de/datensaetze/baumbestand-berlin-wfs-48ad3a23'>Baumbestandsdaten</a></li>
        <li><a href='https://www.govdata.de/suche/daten/giess-den-kiez-nutzungsdaten'>Gießdaten des Projekts 'Gieß den Kiez'</a></li>
      </ul>
    ")),
    easyClose = TRUE,
    footer = modalButton("Schließen")
  ))
})

observeEvent(input$info_hist_bewaesserung_verhaeltnis, {
  showModal(modalDialog(
    title = "Erklärung:",
    HTML(paste0("
      <b>Was wird angezeigt?</b><br>
      Wie viel Wasser im Durchschnitt <strong>pro Baum</strong> verwendet wurde, getrennt nach Bezirken. Das hilft, Bezirke mit intensiver oder sparsamer Bewässerung zu erkennen.<br><br>
    
      <strong>Datenquelle</strong><br>
      <ul>
        <li><a href='https://daten.berlin.de/datensaetze/baumbestand-berlin-wfs-48ad3a23'>Baumbestandsdaten</a></li>
        <li><a href='https://www.govdata.de/suche/daten/giess-den-kiez-nutzungsdaten'>Gießdaten des Projekts 'Gieß den Kiez'</a></li>
      </ul>
    ")),
    easyClose = TRUE,
    footer = modalButton("Schließen")
  ))
})

observeEvent(input$info_trend_water, {
  showModal(modalDialog(
    title = "Erklärung:",
    HTML(paste0("
      <b>Was wird angezeigt?</b><br>
      Dieses Diagramm zeigt, wie viel Wasser für Bäume <strong>verschiedener Pflanzjahre</strong> verwendet wurde. Es zeigt z. B., ob jüngere Bäume häufiger gegossen wurden.<br><br>
      <b>Wie Filter wirken:</b>
      <ul>
        <li><strong>Jahr-Spanne:</strong> Auswahl bestimmter Pflanzjahre.</li>
        <li><strong>Bezirk:</strong> Fokus auf einzelne Stadtteile möglich.</li>
      </ul><br>
    
      <strong>Datenquelle</strong><br>
      <ul>
        <li><a href='https://daten.berlin.de/datensaetze/baumbestand-berlin-wfs-48ad3a23'>Baumbestandsdaten</a></li>
        <li><a href='https://www.govdata.de/suche/daten/giess-den-kiez-nutzungsdaten'>Gießdaten des Projekts 'Gieß den Kiez'</a></li>
      </ul>
    ")),
    easyClose = TRUE,
    footer = modalButton("Schließen")
  ))
})

observeEvent(input$info_trend_water_ts, {
  showModal(modalDialog(
    title = "Erklärung:",
    HTML(paste0("
      <b>Was wird angezeigt:</b><br>
      Die Entwicklung von <strong>Bewässerungsmenge, Temperatur und Niederschlag</strong> im zeitlichen Verlauf monatlich oder jährlich.<br><br>
      <b>Wie Filter wirken:</b>
      <ul>
        <li><strong>Jahr/Modus:</strong> Auswahl eines bestimmten Jahres oder Modus (monatlich/jährlich).</li>
        <li><strong>Bezirk & Baumart:</strong> Eingrenzung nach Region oder Art möglich.</li>
      </ul><br>
    
      <strong>Datenquelle</strong><br>
      <ul>
        <li><a href='https://daten.berlin.de/datensaetze/baumbestand-berlin-wfs-48ad3a23'>Baumbestandsdaten</a></li>
        <li><a href='https://www.govdata.de/suche/daten/giess-den-kiez-nutzungsdaten'>Gießdaten des Projekts 'Gieß den Kiez'</a></li>
        <li><a href='https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/'>Wetterdaten</a></li>
      </ul>
    ")),
    easyClose = TRUE,
    footer = modalButton("Schließen")
  ))
})

observeEvent(input$info_tree_pie_chart, {
  showModal(modalDialog(
    title = "Erklärung:",
    HTML(paste0("
      <b>Was wird angezeigt?</b><br>
      Ein Kreisdiagramm der <strong>Top 10 Baumarten</strong> mit ihrem Anteil an gegossenen Bäumen, also, welche Arten besonders häufig bewässert wurden.<br><br>
      <b>Wie Filter wirken:</b>
      <ul><li>Optional Eingrenzung auf einen Bezirk.</li></ul><br>
    
      <strong>Datenquelle</strong><br>
      <ul>
        <li><a href='https://daten.berlin.de/datensaetze/baumbestand-berlin-wfs-48ad3a23'>Baumbestandsdaten</a></li>
        <li><a href='https://www.govdata.de/suche/daten/giess-den-kiez-nutzungsdaten'>Gießdaten des Projekts 'Gieß den Kiez'</a></li>
      </ul>
    ")),
    easyClose = TRUE,
    footer = modalButton("Schließen")
  ))
})

observeEvent(input$info_map, {
  showModal(modalDialog(
    title = "Erklärung:",
    HTML(paste0("
      <b>Was wird angezeigt?</b><br>
      Eine interaktive <strong>Karte von Berlin</strong> mit allen Bäumen als Punkte und ihrer Bewässerungshistorie. Ab einer bestimmten Zoomstufe werden auch Pumpen eingeblendet.<br><br>
      <b>Wie Filter wirken:</b>
      <ul>
        <li><strong>Bezirk, Baumart, Jahr, Saison:</strong> bestimmen die Auswahl der Bäume.</li>
        <li><strong>Kartenbereich:</strong> Nur sichtbare Bäume werden geladen.</li>
        <li><strong>Zoom-Level:</strong> Pumpen werden erst ab einer bestimmten Zoomstufe angezeigt.</li>
      </ul><br>
    
      <strong>Datenquelle</strong><br>
      <ul>
        <li><a href='https://daten.berlin.de/datensaetze/baumbestand-berlin-wfs-48ad3a23'>Baumbestandsdaten</a></li>
        <li><a href='https://www.govdata.de/suche/daten/giess-den-kiez-nutzungsdaten'>Gießdaten des Projekts 'Gieß den Kiez'</a></li>
        <li><a href='https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/'>Wetterdaten</a></li>
      </ul>
    ")),
    easyClose = TRUE,
    footer = modalButton("Schließen")
  ))
})

observeEvent(input$info_balken_plot, {
  showModal(modalDialog(
    title = "Erklärung:",
    HTML(paste0("
      <b>Was wird angezeigt?</b><br>
      Ein gruppiertes Balkendiagramm mit:
      <ul>
        <li><strong>Blau:</strong> Gesamtbewässerung pro Bezirk</li>
        <li><strong>Orange:</strong> Anzahl der funktionierenden Pumpen</li>
      </ul>
      <b>Hinweis:</b> Die beiden Größen sind aufeinander skaliert, damit sie gemeinsam dargestellt werden können.<br><br>
    
      <strong>Datenquelle</strong><br>
      <ul>
        <li><a href='https://daten.berlin.de/datensaetze/baumbestand-berlin-wfs-48ad3a23'>Baumbestandsdaten</a></li>
        <li><a href='https://www.govdata.de/suche/daten/giess-den-kiez-nutzungsdaten'>Gießdaten des Projekts 'Gieß den Kiez'</a></li>
        <li><a href='https://overpass-turbo.eu/'>Pumpendaten</a></li>
      </ul>
    ")),
    easyClose = TRUE,
    footer = modalButton("Schließen")
  ))
})

observeEvent(input$info_karte_giessverhalten, {
  showModal(modalDialog(
    title = "Erklärung:",
    HTML(paste0("
      <b>Was wird angezeigt?</b><br>
      Eine Karte mit Bezirken (grobe Ebene) oder LORs (feinere Gebietseinheiten), eingefärbt nach ihrer <strong>Gesamtbewässerung</strong>. Pumpenstandorte werden je nach Zoomstufe eingeblendet.<br><br>
    
      <strong>Datenquelle</strong><br>
      <ul>
        <li><a href='https://daten.berlin.de/datensaetze/baumbestand-berlin-wfs-48ad3a23'>Baumbestandsdaten</a></li>
        <li><a href='https://www.govdata.de/suche/daten/giess-den-kiez-nutzungsdaten'>Gießdaten des Projekts 'Gieß den Kiez'</a></li>
        <li><a href='https://overpass-turbo.eu/'>Pumpendaten</a></li>
        <li><a href='https://www.berlin.de/sen/sbw/stadtdaten/stadtwissen/sozialraumorientierte-planungsgrundlagen/lebensweltlich-orientierte-raeume'>LOR-Bereiche</a></li>
      </ul>
    ")),
    easyClose = TRUE,
    footer = modalButton("Schließen")
  ))
})

observeEvent(input$info_hist_Top_10_best_verhaeltnis_baum, {
  showModal(modalDialog(
    title = "Erklärung:",
    HTML(paste0("
      <b>Was wird angezeigt?</b><br>
      Die zehn Straßen, bei denen im Verhältnis pro Baum <strong>am meisten oder am wenigsten Wasser</strong> verwendet wurde.<br><br>
      <b>Wie Filter wirken:</b>
      <ul>
        <li><strong>Bezirk:</strong> Auswahl über <code>bar_bezirk</code>.</li>
        <li>Nur gegossene Bäume mit zugeordneter Straße werden berücksichtigt.</li>
      </ul><br>
      <strong>Datenquelle</strong><br>
      <ul>
        <li><a href='https://daten.berlin.de/datensaetze/baumbestand-berlin-wfs-48ad3a23'>Baumbestandsdaten</a></li>
        <li><a href='https://www.govdata.de/suche/daten/giess-den-kiez-nutzungsdaten'>Gießdaten des Projekts 'Gieß den Kiez'</a></li>
      </ul>
    ")),
    easyClose = TRUE,
    footer = modalButton("Schließen")
  ))
})

observeEvent(input$info_pumpenkategorien_plot_pump_ok_nur_straße, {
  showModal(modalDialog(
    title = "Erklärung:",
    HTML(paste0("
      <b>Was wird angezeigt?</b><br>
      Straßen werden in Kategorien unterteilt nach der Anzahl von Pumpen im Umkreis (100 m):<br>
      <ul>
        <li><strong>Keine Pumpe</strong></li>
        <li><strong>Eine Pumpe</strong></li>
        <li><strong>Mehrere Pumpen</strong></li>
      </ul>
      Für jede Kategorie wird z. B. die <strong>durchschnittliche Gießmenge</strong> oder <strong>Anzahl der Bäume</strong> angezeigt.<br><br>
      <b>Wie Filter wirken:</b>
      <ul>
        <li>Beruht auf <code>pumpen_im_umkreis_100m</code> je Baumstandort.</li>
        <li>Bezirk filterbar über <code>bar_bezirk</code>.</li>
      </ul><br>
      <strong>Datenquelle</strong><br>
      <ul>
        <li><a href='https://daten.berlin.de/datensaetze/baumbestand-berlin-wfs-48ad3a23'>Baumbestandsdaten</a></li>
        <li><a href='https://www.govdata.de/suche/daten/giess-den-kiez-nutzungsdaten'>Gießdaten des Projekts 'Gieß den Kiez'</a></li>
        <li><a href='https://overpass-turbo.eu/'>Pumpendaten</a></li>
      </ul>
    ")),
    easyClose = TRUE,
    footer = modalButton("Schließen")
  ))
})

observeEvent(input$info_hist_bewaesserung_pro_lor, {
  showModal(modalDialog(
    title = "Erklärung:",
    HTML(paste0("
      <b>Was wird angezeigt?</b><br>
      Eine <strong>feinräumige Darstellung</strong>: Zeigt die gesamte Wassermenge, die in jedem <strong>LOR-Gebiet</strong> (Lebensweltlich orientierter Raum) verwendet wurde.<br><br>
      <b>Wie Filter wirken:</b>
      <ul>
        <li>Berücksichtigt Bezirks- und Jahresauswahl.</li>
        <li>Basiert auf <code>df_merged_mit_lor_sum</code>.</li>
      </ul><br>
      
      <strong>Definition</strong><br>
      LOR (lebensweltlich orientierten Räume) sind räumlich aufgeteilte Bereiche die seit 2007 als Grundlage für Planung, Progrnose und Beobachtung demografischer und sozialer Entwicklungen in Berlin. <br>
      
      <br><strong>Datenquelle</strong><br>
      <ul>
        <li><a href='https://www.govdata.de/suche/daten/giess-den-kiez-nutzungsdaten'>Gießdaten des Projekts 'Gieß den Kiez'</a></li>
        <li><a href='https://www.berlin.de/sen/sbw/stadtdaten/stadtwissen/sozialraumorientierte-planingsgrundlagen/lebensweltlich-orientierte-raeume'>LOR-Bereiche</a></li>
      </ul>
    ")),
    easyClose = TRUE,
    footer = modalButton("Schließen")
  ))
})
  
  
  
  
  
  
  
  
  
  
  
  
  convert_units <- function(liters) {
    if (liters >= 1e6) {
      return(list(value = round(liters / 1e6, 2), unit = "ML"))
    } else if (liters >= 1e3) {
      return(list(value = round(liters / 1e3, 2), unit = "m³"))
    } else {
      return(list(value = round(liters, 2), unit = "L"))
    }
  }
  
  # Funktion zum Umrechnen von Vektoren
  convert_unit_vector <- function(liters_vector) {
    sapply(liters_vector, function(liters) {
      conversion_result <- convert_units(liters)
      return(list(value = conversion_result$value, unit = conversion_result$unit))
    })
  }
  
  full_unit <- function(unit) {
    if(length(unit) == 1) { 
      switch(unit,
             "ML" = "Mega Liter", 
             "L" = "Liter", 
             "m³" = "Kubikmeter",  
             "kL" = "Kilo Liter",
             unit)  # Default 
    } else {
      return("Unknown unit")  
    }
  }
  
  filteredData <- function(filter_bezirk = TRUE) {
    # Ensure required inputs are available
    req(input$start_year)
    
    df <- df_merged() %>%
      mutate(year = year(timestamp))
    
    selected_years <- tryCatch({
      if ("Baumbestand Stand 2025" %in% input$start_year) {
        2020:2024
      } else if ("2020-2024" %in% input$start_year) {
        2020:2024
      } else {
        as.numeric(input$start_year)
      }
    }, warning = function(w) numeric(0), error = function(e) numeric(0))
    
    # Apply year filtering
    df <- df %>%
      filter(
        ("Baumbestand Stand 2025" %in% input$start_year & 
           (is.na(timestamp) | year %in% 2020:2024)) |
          ("2020-2024" %in% input$start_year & 
             !is.na(timestamp) & year %in% 2020:2024) |
          (any(!input$start_year %in% c("2020-2024", "Baumbestand Stand 2025")) & 
             year %in% tryCatch(as.numeric(input$start_year), warning = function(e) numeric(0)))
           
      )
    
    # Apply bezirk filtering only if requested and input exists
    if (filter_bezirk) {
      if (!is.null(input$bezirk)) {
        df <- df %>%
          filter(input$bezirk == "Alle" | bezirk %in% input$bezirk)
      }
    }
    
    return(df)
  }
  
  
  
  # mapFilteredData <- reactive({
  #   
  #   # Standardmäßig Jahre 2020-2024 setzen
  #   selected_years <- if (is.null(input$map_year) || input$map_year == "2020-2024") {
  #     2020:2024  
  #   } else {
  #     input$map_year
  #   }
  #   
  #   # Jahreszeiten basierend auf Monaten bestimmen
  #   df_merged() %>%
  #     mutate(timestamp = ymd_hms(timestamp),
  #            year = year(timestamp),
  #            month = month(timestamp),
  #            saison = case_when(
  #              month %in% c(12, 1, 2) ~ "Winter",
  #              month %in% c(3, 4, 5)  ~ "Frühling",
  #              month %in% c(6, 7, 8)  ~ "Sommer",
  #              month %in% c(9, 10, 11) ~ "Herbst"
  #            )) %>%
  #     filter(
  #       (input$map_bezirk == "Alle" | bezirk %in% input$map_bezirk) &
  #         (year %in% selected_years) &
  #         (input$map_saison == "Alle" | saison %in% input$map_saison) &
  #         (input$map_baumgattung == "Alle" | gattung_deutsch %in% input$map_baumgattung)
  #     ) %>%
  #     group_by(pitid, lat, lng, gattung_deutsch, pflanzjahr) %>% 
  #     summarise(
  #       bewaesserungsmenge_in_liter = sum(bewaesserungsmenge_in_liter, na.rm = TRUE),
  #       print(bewaesserungsmenge_in_liter),
  #       durchschnitts_intervall = mean(durchschnitts_intervall, na.rm = TRUE)
  #     ) %>%
  #     ungroup()
  # })
  
  barFilteredData <- reactive({
    df_merged_sum_distanz_umkreis_pump_ok_lor() %>%
      drop_na(strname) %>%
      filter(input$bar_bezirk == "Alle" | bezirk %in% input$bar_bezirk)
  })
  
  filteredBaumgattung <- reactive({
    df_merged() %>%
      filter(
        (input$gattung_deutsch == "Alle" | gattung_deutsch %in% input$gattung_deutsch)
      )
  })
  
  output$total_trees <- renderValueBox({
    valueBox(
      formatC(n_distinct(df_merged()$gisid), format = "d", big.mark = "."),
      "Gesamtzahl der Bäume",
      icon = icon("tree"),
      color = "green"
    )
  })
  
  output$total_tree_watered <- renderValueBox({
    valueBox(
      formatC(n_distinct(filteredData()$gisid), format = "d", big.mark = "."),
      "Gesamtzahl der gegossenen Bäume",
      icon = icon("tree"),
      color = "green"
    )
  })
  
  # Dynamische Auswahl: welche Box zeigen?
  output$dynamic_tree_box <- renderUI({
    if (identical(input$start_year, "Baumbestand Stand 2025") || (length(input$start_year) == 1 && input$start_year == "Baumbestand Stand 2025")) {
      valueBoxOutput("total_trees")
    } else {
      valueBoxOutput("total_tree_watered")
    }
  })
  
  
  output$tree_distribution <- renderPlotly({
    req(input$stats_baumvt_year)
    
    df_filtered <- df_merged() %>%
      mutate(timestamp = as.Date(timestamp)) %>%
      filter(
        case_when(
          "Baumbestand Stand 2025" %in% input$stats_baumvt_year ~ 
            is.na(timestamp) | lubridate::year(timestamp) %in% 2020:2024,
          "2020-2024" %in% input$stats_baumvt_year ~ 
            !is.na(timestamp) & lubridate::year(timestamp) %in% 2020:2024,
          TRUE ~  # Alle anderen Fälle (einzelnes Jahr)
            !is.na(timestamp) & 
            lubridate::year(timestamp) %in% 
            suppressWarnings(as.numeric(input$stats_baumvt_year))
        )
      )
    
    
    x_axis_title <- if ("Baumbestand Stand 2025" %in% input$stats_baumvt_year) {
      "Bäume pro Hektar (gesamt)"
    } else {
      "Gegossene Bäume pro Hektar"
    }
    
    baumanzahl_filtered <- df_filtered %>%
      group_by(bezirk) %>%
      summarise(tree_count = n_distinct(gisid), .groups = "drop")
    
    baum_dichte_filtered <- baum_dichte() %>%
      filter(bezirk %in% baumanzahl_filtered$bezirk) %>%
      left_join(baumanzahl_filtered, by = "bezirk") %>%
      mutate(tree_count_per_ha = tree_count / flaeche_ha)
    
    plot <- ggplot(baum_dichte_filtered, aes(x = reorder(bezirk, tree_count_per_ha), y = tree_count_per_ha, fill = baeume_pro_ha, text = paste0(
      "Bezirk: ", bezirk, "<br>",
      "Bäume pro ha aktuell: ", round(tree_count_per_ha, 1), "<br>",
      "Baumbestand pro ha (Basis): ", round(baeume_pro_ha, 1)
    ))) +
      geom_bar(stat = "identity", position = "dodge") +
      scale_fill_viridis_c(option = "cividis", name = "Baumdichte (Bäume/ha)") +
      coord_flip() +
      labs(title = "Baumverteilung im Verhältnis zur Bezirkfläche", x = "Bezirk", y = x_axis_title) +
      theme_minimal() +
      theme(legend.position = "right")
    
    ggplotly(plot, tooltip = "text")
  })
  
  output$population_distribution <- renderPlotly({
    req(input$stats_baumvt_year)
    req(einwohnerGiessm2020_24())
    
    # --- 1) Daten laden ---
    df <- einwohnerGiessm2020_24() %>%
      mutate(
        jahr = as.numeric(jahr),
        einwohner = as.numeric(einwohner),
        giessmenge = as.numeric(giessmenge)
      )
    
    # --- 2) Filter wie beim Baumplot ---
    df_filtered <- df %>%
      filter(
        case_when(
          "Baumbestand Stand 2025" %in% input$stats_baumvt_year ~ jahr %in% 2020:2024,
          "2020-2024" %in% input$stats_baumvt_year ~ jahr %in% 2020:2024,
          TRUE ~ jahr %in% suppressWarnings(as.numeric(input$stats_baumvt_year))
        )
      )
    
    # --- 3) Aggregation nach Bezirk ---
    df_agg <- df_filtered %>%
      group_by(bezirk) %>%
      summarise(
        einwohner = mean(einwohner, na.rm = TRUE),
        giessmenge_total = sum(giessmenge, na.rm = TRUE),
        .groups = "drop"
      ) %>%
      left_join(bezirksflaechen, by = "bezirk") %>%
      mutate(
        einwohner_pro_ha = einwohner / flaeche_ha,
        giessmenge_pro_einwohner = giessmenge_total / einwohner
      )
    
    # --- 4) Dynamischer Achsentitel ---
    y_axis_title <- if ("Baumbestand Stand 2025" %in% input$stats_baumvt_year) {
      "Einwohner pro Hektar (2020–2024)"
    } else {
      "Einwohner pro Hektar"
    }
    
    # --- 5) Plot erstellen ---
    plot <- ggplot(
      df_agg,
      aes(
        x = reorder(bezirk, einwohner_pro_ha),
        y = einwohner_pro_ha,
        fill = giessmenge_pro_einwohner,
        text = paste0(
          "Bezirk: ", bezirk, "<br>",
          "Einwohner (Ø): ", format(round(einwohner, 0), big.mark = "."), "<br>",
          "Einwohner/ha: ", round(einwohner_pro_ha, 1), "<br>",
          "Gießmenge gesamt (L): ", format(round(giessmenge_total, 0), big.mark = "."), "<br>",
          "Gießmenge pro Einwohner (L): ", round(giessmenge_pro_einwohner, 2)
        )
      )
    ) +
      geom_bar(stat = "identity") +
      scale_fill_viridis_c(option = "cividis", name = "Gießmenge pro Einwohner (L)") +
      coord_flip() +
      labs(
        title = "Bevölkerungsdichte & Bewässerungsintensität nach Bezirk",
        x = "Bezirk",
        y = y_axis_title
      ) +
      theme_minimal() +
      theme(legend.position = "right")
    
    ggplotly(plot, tooltip = "text")
  })
  
  
  
  
  
  
  
  output$total_water <- renderValueBox({
    # Umrechnung des Werts und Ermittlung der Einheit
    conversion_result <- convert_units(sum(filteredData()$bewaesserungsmenge_in_liter, na.rm = TRUE))
    
    # Der umgerechnete Wert und die Einheit
    converted_value <- conversion_result$value
    unit <- conversion_result$unit
    
    valueBox(
      paste(format(converted_value, decimal.mark = ",", big.mark = "."), unit),
      paste("Gesamtbewässerung (", full_unit(unit), ")", sep=""),  
      icon = icon("tint"),
      color = "blue"
    )
  })
  
  output$avg_water <- renderValueBox({
    valueBox(
      formatC(mean(filteredData()$bewaesserungsmenge_in_liter, na.rm = TRUE), digits = 2),
      "Durchschnittliche Bewässerung pro gegossenen Baum (Liter)",
      icon = icon("chart-line"),
      color = "aqua"
    )
  })
  
  # # Interaktive Karte (openStreetMap) mit Leaflet rendern
  # output$map <- renderLeaflet({
  #   leaflet(mapFilteredData()) %>%
  #     addTiles() %>% # OpenStreetMap-Hintergrundkarte hinzufügen
  #     addCircleMarkers(
  #       ~lng, ~lat, 
  #       color = ~color_palette(pmin(ifelse(is.na(bewaesserungsmenge_in_liter), 0, bewaesserungsmenge_in_liter), 1200)),
  #       popup = ~paste("Baumart:", gattung_deutsch, 
  #                      "<br>Pflanzjahr:", pflanzjahr, 
  #                      "<br>Bewässerung:", bewaesserungsmenge_in_liter, "Liter",
  #                      "<br>Ø Bewässerungsintervall:", 
  #                      ifelse(is.infinite(durchschnitts_intervall), "Keine Daten", 
  #                             paste(round(durchschnitts_intervall, 1), "Tage"))
  #       )
  #     ) %>%
  #     addLegend(
  #       position = "bottomright",
  #       pal = color_palette,
  #       values = c(0, 1200),
  #       title = "Bewässerungsmenge (Liter)",
  #       opacity = 1
  #     )
  # })
  
  output$hist_bewaesserung_pro_bezirk <- renderPlotly({
    df_agg <- df_merged() %>%
      drop_na(bewaesserungsmenge_in_liter) %>%
      group_by(bezirk) %>%
      summarise(total_water = sum(bewaesserungsmenge_in_liter, na.rm = TRUE)) %>%
      ungroup() 
    
    df_agg <- df_agg %>%
      mutate(
        converted = purrr::map(total_water, convert_units),  
        value = sapply(converted, `[[`, "value"),  
        unit = sapply(converted, `[[`, "unit"),
        tooltip_text = paste0(
          "Bezirk: ", bezirk, "<br>",
          "Gesamtmenge: ", round(value, 1), " ", unit
        ) 
      )
    
     p <- ggplot(df_agg, aes(x = bezirk, y = value, fill = bezirk, text = tooltip_text)) +
      geom_bar(stat = "identity", color = "white", alpha = 0.7) +
      labs(
        title = "Bewässerung pro Bezirk",
        x = "Bezirke in Berlin",
        y = paste("Gesamte Bewässerungsmenge (", unique(df_agg$unit), ")", sep = "")
      ) +
      theme_light() +
      theme(legend.position = "bottom",
            axis.text.x = element_text(angle = 55, hjust = 1)) +
      scale_fill_viridis_d(option = "D", name = "Bezirk") 
     
     ggplotly(p, tooltip = "text")
  })
  
  
  
  output$hist_bewaesserung_verhaeltnis <- renderPlotly({
    plot <- df_merged() %>%
      group_by(bezirk) %>%
      summarise(
        total_water = sum(bewaesserungsmenge_in_liter, na.rm = TRUE),
        tree_count = n_distinct(gisid)  # Anzahl eindeutiger Bäume pro Bezirk
      ) %>%
      mutate(water_sum_trees = total_water / tree_count) %>%  # Verhältnis berechnen
      ggplot(aes(x = reorder(bezirk, water_sum_trees), y = water_sum_trees, fill = bezirk)) +
      geom_bar(stat = "identity") +
      coord_flip() +
      labs(
        title = "Verhältnis Bewässerung / Anzahl Bäume nach Bezirk", 
        x = "Bezirk", 
        y = "Bewässerung pro Baum (Liter)"
      ) +
      theme_minimal() +
      scale_fill_viridis_d(option = "D") +
      theme(legend.position = "none")
    
    ggplotly(plot, tooltip = "y")  # Hover-Tooltip für `water_per_tree`
  })
  
  
  # output$avg_water_district <- renderPlot({
  #   filteredData() %>%
  #     group_by(bezirk) %>%
  #     summarize(avg_water = mean(bewaesserungsmenge_in_liter, na.rm = TRUE)) %>%
  #     ggplot(aes(x = reorder(bezirk, avg_water), y = avg_water)) +
  #     geom_bar(stat = "identity", fill = "darkblue") +
  #     coord_flip() +
  #     theme_minimal() +
  #     labs(title = "Durchschnittliche Bewässerung nach Bezirk", x = "Bezirk", y = "Durchschnittliche Bewässerungsmenge")
  # })
  
  output$trend_water <- renderPlotly({
    plot <- df_merged() %>%
      filter(input$trend_bezirk_pj == "Alle" | bezirk %in% input$trend_bezirk_pj) %>%
      filter(pflanzjahr >= input$trend_year[1] & pflanzjahr <= input$trend_year[2]) %>%
      group_by(pflanzjahr) %>%
      summarize(total_water = sum(bewaesserungsmenge_in_liter, na.rm = TRUE)) %>%
      ggplot(aes(x = pflanzjahr, y = total_water)) +
      geom_line(color = "blue") +
      geom_point(aes(text = paste("Summe:", total_water, "l")),size = 0.2, color = "blue") + 
      theme_minimal() +
      labs(title = "Trend der Bewässerung je nach Pflanzjahr", x = "Pflanzjahr", y = "Gesamtbewässerung (Liter)")
    
    ggplotly(plot, tooltip = "text") # Aktiviert den Hover-Effekt für "text"
  })
  
  output$tree_pie_chart <- renderPlotly({
    
    #  nach Bezirk filtern
    df_filtered <- df_merged()
    if (!is.null(input$pie_bezirk) && !"Alle" %in% input$pie_bezirk) {
      df_filtered <- df_filtered %>%
        filter(bezirk %in% input$pie_bezirk)
    }
    
    # Einzigartige Bäume mit Art extrahieren
    baeume_einzigartig <- df_filtered %>%
      filter(!is.na(art_dtsch)) %>%
      distinct(gisid, art_dtsch)
    
    
    #  Gegossene Bäume  extrahieren
    baeume_gegossen <- df_filtered %>%
      filter(!is.na(art_dtsch) & !is.na(bewaesserungsmenge_in_liter)) %>%
      distinct(gisid, art_dtsch)
    
    #  Anzahl pro Art (gesamt & gegossen)
    art_ratio_df <- baeume_einzigartig %>%
      group_by(art_dtsch) %>%
      summarise(gesamt = n()) %>%
      left_join(
        baeume_gegossen %>%
          group_by(art_dtsch) %>%
          summarise(gegossen = n()),
        by = "art_dtsch"
      ) %>%
      mutate(
        gegossen = replace_na(gegossen, 0),
        anteil_gegossen = gegossen / gesamt
      ) %>%
      arrange(desc(gesamt)) %>%
      slice_max(order_by = gesamt, n = 10)  # Top 10 häufigste Arten
    
    # Pie Chart
    fig <- plot_ly(
      art_ratio_df,
      labels = ~art_dtsch,
      values = ~anteil_gegossen,
      type = 'pie',
      textinfo = 'label+percent',
      hoverinfo = 'label+percent+value',
      marker = list(colors = viridis::viridis(10)) 
    ) %>%
      layout(
        title = list(
          text = "Anteil gegossener Bäume pro Baumart (Top 10)",
          font = list(size = 16)
        )
      )
    
    fig
  })
  
  
  # # Top 10 und Bottom 10 Straßen auswählen
  # top_streets <- reactive({
  #   barFilteredData() %>%
  #     group_by(strname) %>%
  #     summarise(total_water = sum(bewaesserungsmenge_in_liter, na.rm = TRUE)) %>%
  #     arrange(desc(total_water)) %>%
  #     head(10)  # Top 10 auswählen
  # })
  # 
  # bottom_streets <- reactive({
  #   barFilteredData()%>%
  #     group_by(strname) %>%
  #     summarise(total_water = sum(bewaesserungsmenge_in_liter, na.rm = TRUE)) %>%
  #     arrange(desc(total_water)) %>%
  #     tail(10)  
  # })
  
  
  # output$hist_Top_10_best <- renderPlot({
  #   df_top <- top_streets()
  #   ggplot(df_top, aes(x = reorder(strname, total_water), y = total_water, fill = strname)) +
  #     geom_bar(stat = "identity") +
  #     coord_flip() +
  #     labs(title = "Top 10 Straßen mit höchster Bewässerung", x = "Straße", y = "Gesamte Bewässerung (Liter)") +
  #     scale_fill_discrete(name = "Straßennamen") +  # Ändert den Titel der Legende
  #     theme_minimal()
  # })
  # 
  # output$hist_Top_10_worst <- renderPlot({
  #   df_bottom <- bottom_streets() 
  #   ggplot(df_bottom, aes(x = reorder(strname, total_water), y = total_water, fill = strname)) +
  #     geom_bar(stat = "identity") +
  #     coord_flip() +
  #     labs(title = "Bottom 10 Straßen mit geringster Bewässerung", x = "Straße", y = "Gesamte Bewässerung (Liter)") +
  #     scale_fill_discrete(name = "Straßennamen") +  # Ändert den Titel der Legende
  #     theme_minimal() 
  # })
  
  streetWaterRatio <- reactive({
    barFilteredData() %>%
      filter(!is.na(gesamt_bewaesserung)) %>%
      group_by(strname) %>%
      summarise(
        total_water = sum(gesamt_bewaesserung, na.rm = TRUE),
        tree_count = n_distinct(gisid),
        water_per_tree = total_water / tree_count
      ) %>%
      filter(tree_count > 0) %>%
      arrange(desc(water_per_tree))
  })
  
  
  output$hist_Top_10_best_verhaeltnis_baum <- renderPlotly({
    df <- streetWaterRatio() %>% head(10)
    
    p <- ggplot(df, aes(
      x = reorder(strname, water_per_tree),
      y = water_per_tree,
      fill = strname,
      text = paste0(
        "Straße: ", strname, "<br>",
        "Bäume: ", tree_count, "<br>",
        "Gesamtbewässerung: ", total_water, " L<br>",
        "Liter pro Baum: ", round(water_per_tree, 1)
      )
    )) +
      geom_bar(stat = "identity") +
      coord_flip() +
      labs(
        title = "Top 10 Straßen nach Verhältnis: Bewässerung pro Baum",
        x = "Straße",
        y = "Gesamtbewässerung / Baum (Liter)"
      ) +
      scale_fill_viridis_d(name = "Straßen", option = "D") +  # Farbenblind-freundlich
      theme_minimal()
    
    ggplotly(p, tooltip = "text")
  })
  
  
  output$hist_Top_10_worst_verhaeltnis_baum <- renderPlotly({
    df <- streetWaterRatio() %>% filter(water_per_tree > 0) %>% tail(10)
    
    p <- ggplot(df, aes(
      x = reorder(strname, water_per_tree),
      y = water_per_tree,
      fill = strname,
      text = paste0(
        "Straße: ", strname, "<br>",
        "Bäume: ", tree_count, "<br>",
        "Gesamtbewässerung: ", total_water, " L<br>",
        "Liter pro Baum: ", round(water_per_tree, 1)
      )
    )) +
      geom_bar(stat = "identity") +
      coord_flip() +
      labs(
        title = "Bottom 10 Straßen nach Verhältnis: Bewässerung pro Baum",
        x = "Straße",
        y = "Gesamtbewässerung / Baum (Liter)"
      ) +
      scale_fill_viridis_d(name = "Straßen", option = "D") +
      theme_minimal()
    
    ggplotly(p, tooltip = "text")
  })
  
  
    
  
  filtered_trend_data <- reactive({
    df_base <- df_merged_sum_distanz_umkreis_pump_ok_lor_clean() %>%
      mutate(timestamp = ymd_hms(timestamp),
             year = year(timestamp),
             month = month(timestamp)) %>%
      filter(
        (input$trend_mode == "month" & year == input$trend_year_ts) |
          (input$trend_mode == "year" & year >= 2020 & year <= 2024),
        (input$trend_bezirk == "Alle" | bezirk %in% input$trend_bezirk),
        (input$trend_baumgattung == "Alle" | gattung_deutsch %in% input$trend_baumgattung)
      )
    
    df_water <- df_base %>%
      group_by(year, month) %>%
      summarise(total_water = sum(gesamt_bewaesserung), .groups = "drop")
    
    df_combined <- df_water %>%
      left_join(df_wetter_monatlich(), by = c("year", "month"))
    
    # Anzeige je nach Modus
    if (input$trend_mode == "month") {
      df_combined <- df_combined %>%
        filter(year == input$trend_year_ts) %>%
        mutate(label = factor(month.abb[month], levels = month.abb))
    } else {
      
      df_combined <- df_combined %>%
        group_by(year) %>%
        summarise(
          total_water = sum(total_water, na.rm = TRUE),
          niederschlag = sum(niederschlag, na.rm = TRUE),
          temp_avg = mean(temp_avg, na.rm = TRUE)
        ) %>%
        mutate(label = as.character(year))
    }
    
    return(df_combined)
  })
  
  
  
  output$trend_water_ts <- renderPlotly({
    df <- filtered_trend_data()
    
    plot_ly(df, x = ~label) %>%
      add_lines(y = ~total_water, name = "Bewässerung (Liter)", line = list(color = "blue", dash = "solid"), yaxis = "y") %>%
      add_lines(y = ~niederschlag, name = "Niederschlag (mm)", line = list(color = "green", dash = "dash"), yaxis = "y2") %>%
      add_lines(y = ~temp_avg, name = "Temperatur (°C)", line = list(color = "red", dash = "dot"), yaxis = "y3") %>%
      layout(
        title = "Bewässerung, Temperatur und Niederschlag",
        xaxis = list(
          title = "Jahr",
          type = "category",
          # Erzwinge angezeigten Bereich mit Puffer:
          range = c(-0.5, length(unique(df$label)) - 0.5),  # 0.5 Einheiten Puffer links/rechts
          autorange = FALSE  # Verhindere automatische Anpassung
        ),
        yaxis = list(title = "Bewässerung (Liter)", side = "left"),
        yaxis2 = list(title = "Niederschlag (mm)", overlaying = "y", side = "right", showgrid = FALSE),
        yaxis3 = list(title = "Temperatur (°C)", overlaying = "y", side = "right", position = 1.1, showgrid = FALSE),
        legend = list(
          x = 1.2,   # Horizontale Position (1 = ganz rechts)
          y = 0.95,  # Vertikale Position (1 = oben)
          xanchor = "left",  # Ankerpunkt der Legende
          bgcolor = "rgba(255,255,255,0.8)",  # Hintergrund mit Transparenz
          bordercolor = "#CCC",
          borderwidth = 1
        ),
        margin = list(r = 150)  # Rechten Rand vergrößern für Legende
      )
  })
  
  
  # Anzahl der Bäume, Gesamtbewässerung und durchschnittliche Bewässerung pro Bezirk berechnen
  baum_bewaesserung_daten <- reactive({
    df_merged() %>%
      group_by(bezirk) %>%
      summarise(
        baum_anzahl = n(),
        gesamt_bewaesserung = sum(bewaesserungsmenge_in_liter, na.rm = TRUE),
        durchschnittliche_bewaesserung = mean(bewaesserungsmenge_in_liter, na.rm = TRUE)
      ) %>%
      mutate(baum_anzahl_pro_durchschnitt = durchschnittliche_bewaesserung / baum_anzahl ) %>%
      arrange(desc(baum_anzahl_pro_durchschnitt))
    
  })
    
    
  # Daten umwandeln für gruppiertes Balkendiagramm
  baum_bewaesserung_long <- reactive({
    req(baum_bewaesserung_daten())
    
    baum_bewaesserung_daten() %>%
      pivot_longer(
        cols = c(baum_anzahl_pro_durchschnitt), 
        names_to = "Kategorie", 
        values_to = "Wert"
      )
  })
  
  # # Gruppiertes Balkendiagramm erstellen
  # output$bar_water_vh <- renderPlot({
  #   ggplot(baum_bewaesserung_long, aes(x = reorder(bezirk, -Wert), y = Wert, fill = Kategorie)) +
  #     geom_bar(stat = "identity", position = "dodge") +
  #     coord_flip() +
  #     theme_minimal() +
  #     labs(
  #       x = "Bezirk",
  #       y = "Verhältnis (Durchschnittliche Bewässerung / Bäume)",
  #       fill = "Kategorie"
  #     ) +
  #     scale_fill_manual(values = c("baum_anzahl_pro_durchschnitt" = "orange")) +
  #     theme(text = element_text(size = 12))
  # })
  
  # Bewässerung pro Bezirk (2020-2024)
  observeEvent(input$info_btn_hbpb, {
    if (input$water_mode == "avg") {
      showModal(modalDialog(
        title = "Erklärung: Durchschnittliche Bewässerung pro Bezirk",
        p("Dieses Diagramm zeigt, wie viel Wasser im Durchschnitt in jedem Bezirk über den Zeitraum 2020-2024 verbraucht wurde."),
        easyClose = TRUE,
        footer = NULL
      ))
    } else if (input$water_mode == "ratio") {
      showModal(modalDialog(
        title = "Erklärung: Verhältnis Bewässerung zu Baumanzahl",
        p("Dieses Diagramm stellt die Gesamtmenge an Wasser pro Bezirk ins Verhältnis zur Anzahl der gegossenen Bäume."),
        easyClose = TRUE,
        footer = NULL
      ))
    }
  })
  
  
  # Ranking Top 
  observeEvent(input$info_btn_top, {
    if (input$water_mode_Top == "avg") {
      showModal(modalDialog(
        title = "Erklärung: Top 10 Straßen mit höchster Bewässerung",
        p("Dieses Diagramm zeigt die zehn Straßen mit der höchsten Gesamtbewässerung im Zeitraum 2020-2024."),
        easyClose = TRUE,
        footer = NULL
      ))
    } else if (input$water_mode_Top == "ratio") {
      showModal(modalDialog(
        title = "Erklärung: Top 10 Straßen mit höchster Bewässerung (Verhältnis)",
        p("Hier wird die gesamte Bewässerungsmenge pro Straße ins Verhältnis zur Anzahl der gegossenen Bäume gesetzt."),
        easyClose = TRUE,
        footer = NULL
      ))
    }
  })
  
  
  # Ranking Bottom 
  observeEvent(input$info_btn_bottom, {
    if (input$water_mode_Bottom == "avg") {
      showModal(modalDialog(
        title = "Erklärung: Bottom 10 Straßen mit geringster Bewässerung",
        p("Dieses Diagramm zeigt die zehn Straßen mit der geringsten Gesamtbewässerung im Zeitraum 2020-2024."),
        easyClose = TRUE,
        footer = NULL
      ))
    } else if (input$water_mode_Bottom == "ratio") {
      showModal(modalDialog(
        title = "Erklärung: Bottom 10 Straßen mit geringster Bewässerung (Verhältnis)",
        p("Hier wird die gesamte Bewässerungsmenge pro Straße ins Verhältnis zur Anzahl der gegossenen Bäume gesetzt."),
        easyClose = TRUE,
        footer = NULL
      ))
    }
  })
  
  
  
  filtered_data_map <- reactive({
    data <- merged_for_rating()
    
    if (!is.null(input$map_bezirk) && !("Alle" %in% input$map_bezirk)) {
      data <- data %>% filter(bezirk %in% input$map_bezirk)
    }

    if (!is.null(input$map_lor) && !("Alle" %in% input$map_lor)) {
      data <- data %>% filter(bzr_name %in% input$map_lor)
    }

    if (!is.null(input$map_baumgattung) && !("Alle" %in% input$map_baumgattung)) {
      data <- data %>% filter(gattung_deutsch %in% input$map_baumgattung)
    }

    if (!is.null(input$map_year) && !("2020-2024" %in% input$map_year)) {
      data$timestamp <- as.Date(data$timestamp)
      data <- data %>%
        filter(lubridate::year(timestamp) %in% as.numeric(input$map_year))
    }

    if (!is.null(input$map_saison) && !("Alle" %in% input$map_saison)) {
      data <- data %>% filter(saison %in% input$map_saison)
    }

    # Nur Bäume im sichtbaren Kartenausschnitt
    bounds <- input$map_bounds
    if (!is.null(bounds)) {
      data <- data %>%
        filter(
          lat >= bounds$south & lat <= bounds$north,
          lng >= bounds$west  & lng <= bounds$east
        )
    }
    
    return(data)
  })
  
  observeEvent(input$map_bezirk, {
    if (!is.null(input$map_bezirk) && !("Alle" %in% input$map_bezirk)) {
      # Nur LORs, die im gewählten Bezirk liegen
      lor_choices <- cleaned_data_light() %>%
        filter(bezirk %in% input$map_bezirk) %>%
        pull(bzr_name) %>%
        unique() %>%
        sort()
    } else {
      # Alle anzeigen
      lor_choices <- sort(unique(merged_for_rating()$bzr_name))
    }

    updateSelectInput(session, "map_lor", choices = c("Alle", lor_choices), selected = "Alle")
  })
  
  # Icon vorbereiten
  icon_pumpe <- makeIcon(
    iconUrl = "icons/water-pump-icon-14.jpg",
    iconWidth = 15,
    iconHeight = 15
  )
  
  # Erst-Render mit initialen Daten
  output$map <- renderLeaflet({
    init_data <- merged_for_rating()
    pal <- colorNumeric(
      palette = colorRampPalette(c("#FFA500", "#0000FF"))(100),
      domain = range(pmin(init_data$gesamt_bewaesserung, 2500), na.rm = TRUE),
      na.color = "#CCCCCC"
    )
    
    leaflet(data = init_data) %>%
      addTiles() %>%
      setView(lng = 13.4050, lat = 52.5200, zoom = 13) %>%  # Berlin Zentrum
      addCircleMarkers(
        lng = ~lng,
        lat = ~lat,
        radius = 4,
        stroke = FALSE,
        fillOpacity = 0.7,
        color = ~pal(pmin(gesamt_bewaesserung, 2500)),
        popup = ~paste0(
          "<strong>gisid: </strong>", gisid, "<br>",
          "<strong>Baumart: </strong>", art_dtsch, "<br>",
          "<strong>Gattung: </strong>", gattung_deutsch, "<br>",
          "<strong>Standort: </strong>", strname, " ", hausnr, "<br>",
          "<strong>Gesamtbewässerung: </strong>", round(as.numeric(gesamt_bewaesserung), 1), " Liter", "<br>",
          "Ø <strong>Bewässerungsintervall: </strong>",
          ifelse(is.infinite(durchschnitts_intervall), "Keine Daten",
                 paste(round(durchschnitts_intervall, 1), " Tage")), "<br><br>"
        )
      ) %>%
      addLayersControl(
        overlayGroups = c("Pumpen"),
        options = layersControlOptions(collapsed = FALSE)
      ) %>%
      addLegend(
        position = "bottomright",
        pal = pal,
        values = pmin(init_data$gesamt_bewaesserung, 2500),
        title = "Gesamtbewässerung (Liter)",
        labFormat = labelFormat(suffix = " L", digits = 0),
        opacity = 1
      ) %>%
      hideGroup("Pumpen")
  })
  
  # Daten aktualisieren per Proxy, nur wenn vorhanden
  observeEvent(filtered_data_map(), {
    data <- filtered_data_map()
    req(nrow(data) > 0)
    
    pal <- colorNumeric(
      palette = colorRampPalette(c("#FFA500", "#0000FF"))(100),
      domain = range(pmin(data$gesamt_bewaesserung, 2500), na.rm = TRUE),
      na.color = "#CCCCCC"
    )
    
    leafletProxy("map", data = data) %>%
      clearMarkers() %>%
      clearShapes() %>%
      addCircleMarkers(
        lng = ~lng,
        lat = ~lat,
        radius = 4,
        stroke = FALSE,
        fillOpacity = 0.7,
        color = ~pal(pmin(gesamt_bewaesserung, 2500)),
        popup = ~paste0(
          "<strong>Baumart: </strong>", art_dtsch, "<br>",
          "<strong>Gattung: </strong>", gattung_deutsch, "<br>",
          "<strong>Standort: </strong>", strname, " ", hausnr, "<br>",
          "<strong>Gesamtbewässerung: </strong>", round(as.numeric(gesamt_bewaesserung), 1), " Liter", "<br>",
          "Ø <strong>Bewässerungsintervall: </strong>",
          ifelse(is.infinite(durchschnitts_intervall), "Keine Daten",
                 paste(round(durchschnitts_intervall, 1), " Tage")), "<br><br>",
          "<strong>Bewässerungsrating: </strong>", gesamt_bewaesserung_rating, "<br><br>"
        )
      )
  })
  
  # Zoomlevel-gesteuerte Pumpenanzeige + Layer-Control-Status
  observe({
    req(input$map_zoom)
    zoom_level <- input$map_zoom
    # Überprüfen, ob der Pumpen-Layer aktiv ist (über Layer-Control)
    pumpen_layer_aktiv <- is.null(input$map_groups) || "Pumpen" %in% input$map_groups
    
    leafletProxy("map") %>% 
      clearGroup("Pumpen") %>% 
      {
        if (zoom_level >= 14 && pumpen_layer_aktiv) {
          addMarkers(., data = pumpen_mit_bezirk(), icon = icon_pumpe, group = "Pumpen")
        } else {
          .
        }
      }
  })
  
  
  
  # Nur funktionierende Pumpen berücksichtigen
  pumpen_mit_bezirk_ok <- reactive({
    req(pumpen_mit_bezirk())
    pumpen_mit_bezirk() %>%
      filter(pump.status == "ok") %>%
      rename(bezirk = Gemeinde_name)
  })
  
  # Pumpenanzahl je Bezirk zählen
  pumpen_pro_bezirk <- reactive({
    req(pumpen_mit_bezirk_ok())
    pumpen_mit_bezirk_ok() %>%
      st_drop_geometry() %>%
      group_by(bezirk) %>%
      summarise(pumpenanzahl = n(), .groups = "drop")
  })
  
  # Mit Fläche verknüpfen
  pumpendichte <- reactive({
    req(pumpen_pro_bezirk(), bezirksflaechen)
    pumpen_pro_bezirk() %>%
      left_join(bezirksflaechen, by = "bezirk") %>%
      mutate(pumpen_pro_ha = pumpenanzahl / flaeche_ha)
  })
  
  
  giess_pumpen_dichte_df <- reactive({
    req(df_merged_sum_distanz_umkreis_pump_ok_lor(), pumpendichte())
    
    df_merged_sum_distanz_umkreis_pump_ok_lor() %>%
      group_by(bezirk) %>%
      summarise(
        gesamt_bewaesserung = sum(bewaesserungsmenge_in_liter, na.rm = TRUE),
        durchschnittl_intervall = mean(durchschnitts_intervall[is.finite(durchschnitts_intervall)], na.rm = TRUE),
        .groups = "drop"
      ) %>%
      left_join(pumpendichte(), by = "bezirk")
  })
  
  
  anzahl_pumpen_pro_bezirk <- reactive({
    req(pumpen_mit_bezirk_ok())
    pumpen_mit_bezirk_ok() %>%
      st_drop_geometry() %>%
      group_by(bezirk) %>%
      summarise(anzahl_pumpen = n(), .groups = "drop")
  })
  
  
  giess_pumpen_dichte_df_pump <- reactive({
      req(giess_pumpen_dichte_df())
      giess_pumpen_dichte_df() %>%
        left_join(anzahl_pumpen_pro_bezirk(), by = "bezirk")
    })
  
  # 'bezirksgrenzen' und 'giess_pumpen_dichte' verbinden
  bezirke_karte <- reactive({
    req(bezirksgrenzen(), giess_pumpen_dichte_df_pump())
    bezirksgrenzen() %>%
      left_join(giess_pumpen_dichte_df_pump(), by = c("Gemeinde_name" = "bezirk"))
  })
  
  # Bezirkszentren berechnen
  bezirkszentren <- reactive({
    req(bezirksgrenzen(), pumpen_pro_bezirk())
    st_centroid(bezirksgrenzen()) %>%
      rename(bezirk = Gemeinde_name) %>%
      left_join(pumpen_pro_bezirk(), by = "bezirk")
  })
  
  # output$karte_giessverhalten <- renderLeaflet({
  #   
  #   # Farbpalette: Blauverlauf
  #   pal <- colorNumeric(palette = "Blues", domain = bezirke_karte$gesamt_bewaesserung)
  #   
  #   leaflet() %>%
  #     addProviderTiles(providers$CartoDB.Positron) %>%
  #     
  #     # Bezirke einfärben
  #     addPolygons(
  #       data = bezirke_karte,
  #       fillColor = ~pal(gesamt_bewaesserung),
  #       fillOpacity = 0.7,
  #       weight = 1,
  #       color = "white",
  #       highlightOptions = highlightOptions(
  #         weight = 2,
  #         color = "#666",
  #         fillOpacity = 0.9,
  #         bringToFront = FALSE
  #       ),
  #       label = ~paste0(Gemeinde_name, ": ", round(gesamt_bewaesserung), " Liter"),
  #       labelOptions = labelOptions(
  #         style = list("font-weight" = "normal", padding = "3px 8px"),
  #         direction = "auto"
  #       )
  #     ) %>%
  #     
  #     # Pumpen-Punkte (an Bezirkszentren) mit Größen nach Anzahl
  #     addCircleMarkers(
  #       data = bezirkszentren,
  #       radius = ~sqrt(anzahl_pumpen) * 2,  
  #       color = "blue",
  #       fillColor = "white",
  #       fillOpacity = 0.9,
  #       stroke = TRUE,
  #       weight = 1,
  #       label = ~paste0(bezirk, ": ", anzahl_pumpen, " Pumpen"),
  #       labelOptions = labelOptions(
  #         style = list("font-weight" = "bold", padding = "3px 8px"),
  #         direction = "auto"
  #       )
  #     ) %>%
  #     addLegend(
  #       "bottomright", pal = pal, values = bezirke_karte$gesamt_bewaesserung,
  #       title = "Gesamtbewässerung (Liter)",
  #       opacity = 1
  #     )
  # })
  
  
  # # Pumpen pro LOR zählen
  # pumpen_pro_lor <- reactive({
  #   pumpen_mit_lor() %>%
  #     st_drop_geometry() %>%
  #     group_by(bzr_id) %>%
  #     summarise(pumpen_anzahl_lor = n()) %>%
  #     ungroup()
  # })
  # 
  # 
  # # Mit LOR-Geometrien verbinden
  # lor_mit_pumpen <- reactive({
  #   lor() %>%
  #     left_join(pumpen_pro_lor, by = "bzr_id") %>%
  #     mutate(pumpen_anzahl_lor = ifelse(is.na(pumpen_anzahl_lor), 0, pumpen_anzahl_lor))
  # })

  # # LOR-Zentren für Marker berechnen
  # lor_zentren <- reactive({
  #   st_centroid(lor()) %>%
  #     left_join(pumpen_pro_lor(), by = "bzr_id")
  # })

  
  output$karte_giessverhalten <- renderLeaflet({
    # Farbpaletten erstellen (beide blau für Konsistenz)
    pal_bezirk <- colorNumeric(palette = "Blues", domain = bezirke_karte()$gesamt_bewaesserung)
    pal_lor <- colorNumeric(palette = "Blues", domain = df_merged_mit_lor_sum()$gesamt_bewaesserung_lor)
    
    # Gruppennamen für die Layer-Control
    group_bezirke <- "Bezirke"
    group_lor <- "LOR-Bereiche"
    group_pumpen_bezirk <- "Pumpen (Bezirk)"
    group_pumpen_lor <- "Pumpen (LOR)"
    group_bezirksgrenzen <- "Bezirksgrenzen (LOR-Ebene)"
    
    # Pumpen pro LOR zählen und Zentren berechnen
    lor_zentren <-  reactive({
      st_centroid(lor()) %>%
      left_join(
        pumpen_mit_lor() %>% 
          st_drop_geometry() %>%
          group_by(bzr_id) %>%
          summarise(pumpen_anzahl = n()),
        by = "bzr_id"
      ) %>%
      mutate(pumpen_anzahl = ifelse(is.na(pumpen_anzahl), 0, pumpen_anzahl))
    })
    
    # LOR-Daten mit Bewässerung verknüpfen
    lor_mit_bewaesserung <- lor() %>%
      left_join(
        df_merged_mit_lor_sum() %>% 
          st_drop_geometry() %>%
          select(bzr_id, gesamt_bewaesserung_lor),
        by = "bzr_id"
      )
    
    # Karte erstellen
    leaflet() %>%
      addProviderTiles(providers$CartoDB.Positron) %>%
      
      # BEZIRKS-EBENE (bei niedrigem Zoom) -------------------------------
    # Bezirksflächen einfärben
    addPolygons(
      data = bezirke_karte(),
      group = group_bezirke,
      fillColor = ~pal_bezirk(gesamt_bewaesserung),
      fillOpacity = 0.7,
      weight = 2,
      color = "white",
      highlightOptions = highlightOptions(
        weight = 3,
        color = "#666",
        fillOpacity = 0.9
      ),
      label = ~paste0(Gemeinde_name, ": ", round(gesamt_bewaesserung), " Liter")
    ) %>%
      
      # Pumpen-Punkte auf Bezirksebene
      addCircleMarkers(
        data = bezirkszentren(),
        group = group_pumpen_bezirk,
        radius = ~sqrt(pumpenanzahl) * 2,
        color = "blue",
        fillColor = "white",
        fillOpacity = 0.9,
        stroke = TRUE,
        weight = 1,
        label = ~paste0(bezirk, ": ", pumpenanzahl, " Pumpen")
      ) %>%
      
      # BEZIRKSGRENZEN (für LOR-Ebene) - dickere Linien
      addPolylines(
        data = bezirksgrenzen(),
        group = group_bezirksgrenzen,
        color = "#333",
        weight = 3,
        opacity = 0.8,
        options = pathOptions(clickable = FALSE)
      ) %>%
      
      # LOR-EBENE (bei hohem Zoom) ---------------------------------------
    # LOR-Bereiche blau einfärben
    addPolygons(
      data = lor_mit_bewaesserung,
      group = group_lor,
      fillColor = ~pal_lor(gesamt_bewaesserung_lor),
      fillOpacity = 0.7,
      weight = 1,
      color = "#666",
      opacity = 0.7,
      highlightOptions = highlightOptions(
        weight = 2,
        color = "#000",
        bringToFront = FALSE
      ),
      label = ~paste0(bzr_name, ": ", round(gesamt_bewaesserung_lor), " Liter"),
      options = pathOptions(clickable = TRUE)
    ) %>%
      
      # Pumpen-Punkte auf LOR-Ebene
      addCircleMarkers(
        data = lor_zentren(),
        group = group_pumpen_lor,
        radius = ~sqrt(pumpen_anzahl) * 2,
        color = "red",
        fillColor = "white",
        fillOpacity = 0.9,
        stroke = TRUE,
        weight = 1,
        label = ~paste0(bzr_name, ": ", pumpen_anzahl, " Pumpen")
      ) %>%
      
      # Legenden
      addLegend(
        "bottomright",
        pal = pal_bezirk,
        values = bezirke_karte()$gesamt_bewaesserung,
        title = "Bewässerung (Liter)",
        opacity = 1,
        group = group_bezirke
      ) %>%
      
      # Layer-Control
      addLayersControl(
        overlayGroups = c(group_bezirke, group_pumpen_bezirk),
        options = layersControlOptions(collapsed = FALSE)
      ) %>%
      
      # ZOOM-LOGIK -------------------------------------------------------
    htmlwidgets::onRender("
      function(el, x) {
        var map = this;
        
        function updateLayers() {
          var zoom = map.getZoom();
          
          if (zoom >= 12) {
            // LOR-EBENE ANZEIGEN
            map.layerManager.getLayerGroup('Bezirke').remove();
            map.layerManager.getLayerGroup('Pumpen (Bezirk)').remove();
            map.layerManager.getLayerGroup('Bezirksgrenzen (LOR-Ebene)').addTo(map);
            map.layerManager.getLayerGroup('LOR-Bereiche').addTo(map);
            map.layerManager.getLayerGroup('Pumpen (LOR)').addTo(map);
          } else {
            // BEZIRKS-EBENE ANZEIGEN
            if (!map.hasLayer(map.layerManager.getLayerGroup('Bezirke'))) {
              map.layerManager.getLayerGroup('Bezirke').addTo(map);
              map.layerManager.getLayerGroup('Pumpen (Bezirk)').addTo(map);
            }
            map.layerManager.getLayerGroup('Bezirksgrenzen (LOR-Ebene)').remove();
            map.layerManager.getLayerGroup('LOR-Bereiche').remove();
            map.layerManager.getLayerGroup('Pumpen (LOR)').remove();
          }
        }
        
        // Initialen Zustand setzen
        updateLayers();
        
        // Bei Zoom-Änderungen aktualisieren
        map.on('zoomend', updateLayers);
      }
    ")
  })
  
  
  
  
  
  
  # Daten in long-format bringen:
  giess_long <- reactive({
    giess_pumpen_dichte_df_pump() %>%
      select(bezirk, anzahl_pumpen, gesamt_bewaesserung) %>%
      pivot_longer(cols = c(anzahl_pumpen, gesamt_bewaesserung),
                   names_to = "variable",
                   values_to = "wert")
  })
  
  
  
  # output$balken_plot <- renderPlotly({ 
  #   
  #   # Skalierungsfaktor berechnen
  #   max_bewaesserung <- max(giess_pumpen_dichte_df$gesamt_bewaesserung, na.rm = TRUE)
  #   max_pumpen <- max(giess_pumpen_dichte_df$anzahl_pumpen, na.rm = TRUE)
  #   scaling_factor <- max_bewaesserung / max_pumpen
  #   
  #   # ggplot erstellen
  #   p <- ggplot(giess_pumpen_dichte_df, aes(x = bezirk)) +
  #     geom_bar(
  #       aes(y = gesamt_bewaesserung, fill = "Bewässerung", 
  #           text = paste("Bezirk:", bezirk, "<br>Bewässerung:", gesamt_bewaesserung, "L")), 
  #       stat = "identity", position = position_nudge(x = -0.2), width = 0.4
  #     ) +
  #     geom_bar(
  #       aes(y = anzahl_pumpen * scaling_factor, fill = "Pumpenanzahl", 
  #           text = paste("Bezirk:", bezirk, "<br>Anzahl Pumpen:", anzahl_pumpen)), 
  #       stat = "identity", position = position_nudge(x = 0.2), width = 0.4
  #     ) +
  #     scale_fill_manual(
  #       name = "Kategorie", 
  #       values = c("Bewässerung" = "steelblue", "Pumpenanzahl" = "seagreen")
  #     ) +
  #     scale_y_continuous(
  #       name = "Gesamtbewässerung (Liter)",
  #       sec.axis = sec_axis(~ . / scaling_factor, name = "Anzahl Pumpen")
  #     ) +
  #     labs(
  #       title = "Pumpenanzahl und Bewässerung pro Bezirk",
  #       x = "Bezirk"
  #     ) +
  #     theme_minimal() +
  #     theme(
  #       axis.text.x = element_text(angle = 45, hjust = 1),
  #       legend.position = "top"
  #     )
  #   
  #   ggplotly(p, tooltip = "text")
  # })
  
  output$balken_plot <- renderPlotly({
    
    # Skalierungsfaktor berechnen
    max_bewaesserung <- max(giess_pumpen_dichte_df_pump()$gesamt_bewaesserung, na.rm = TRUE)
    max_pumpen <- max(giess_pumpen_dichte_df_pump()$anzahl_pumpen, na.rm = TRUE)
    scaling_factor <- max_bewaesserung / max_pumpen
    
    plot_ly() %>%
      add_bars(
        data = giess_pumpen_dichte_df_pump(),
        x = ~bezirk,
        y = ~gesamt_bewaesserung,
        name = "Bewässerung (L)",
        marker = list(color = '#0072B2'),
        hovertemplate = paste(
          "<b>Bezirk:</b> %{x}<br>",
          "<b>Bewässerung:</b> %{y} L"
        )
      ) %>%
      add_bars(
        data = giess_pumpen_dichte_df_pump(),
        x = ~bezirk,
        y = ~anzahl_pumpen * scaling_factor,
        name = "Pumpenanzahl",
        marker = list(color = '#E69F00'),
        customdata = giess_pumpen_dichte_df_pump()$anzahl_pumpen,
        hovertemplate = paste(
          "<b>Bezirk:</b> %{x}<br>",
          "<b>Anzahl Pumpen:</b> %{customdata}"
        )
      ) %>%
      add_trace(
        x = c(NA), y = c(NA),
        type = "bar",
        yaxis = "y2",
        showlegend = FALSE,
        hoverinfo = "none"
      ) %>%
      layout(
        title = "Pumpenanzahl und Bewässerung pro Bezirk",
        barmode = "group",
        xaxis = list(title = "Bezirk"),
        yaxis = list(
          title = "Gesamtbewässerung (Liter)",
          side = "left"
        ),
        yaxis2 = list(
          title = "Anzahl Pumpen",
          overlaying = "y",
          side = "right",
          tickvals = seq(0, max_bewaesserung, length.out = 6),
          ticktext = round(seq(0, max_pumpen, length.out = 6), 0),
          range = c(0, max_bewaesserung),
          showgrid = FALSE
        ),
        legend = list(orientation = "h", x = 0.3, y = 1.1)
      )
  })
  
  
  df_merged_sum_distanz_umkreis_pump_ok_lor_kat <- reactive({
      df_merged_sum_distanz_umkreis_pump_ok_lor() %>%
      mutate(pumpenkategorie = case_when(
        pumpen_im_umkreis_100m == 0 ~ "Keine Pumpe",
        pumpen_im_umkreis_100m == 1 ~ "Eine Pumpe",
        pumpen_im_umkreis_100m >= 2 ~ "Mehrere Pumpen"
      ))
  })
  
  # output$pumpenkategorien_plot_pump_ok <- renderPlot({
  #   
  #   df_kategorie_mittelwert <- df_merged_sum_distanz_umkreis_pump_ok_lor_kat %>%
  #     group_by(pumpenkategorie) %>%
  #     summarise(
  #       durchschnittliche_giessmenge = mean(gesamt_bewaesserung, na.rm = TRUE),
  #       anzahl_baeume = n(),
  #       .groups = "drop"
  #     ) %>%
  #     mutate(pumpenkategorie_label = paste0(pumpenkategorie, " (Anzahl Bäume = ", anzahl_baeume, ")"))
  #   
  #   ggplot(df_kategorie_mittelwert, aes(x = pumpenkategorie_label, y = durchschnittliche_giessmenge)) +
  #     geom_col(aes(fill = pumpenkategorie)) +
  #     scale_fill_viridis_d(option = "D", name = "Pumpenkategorie") +
  #     labs(
  #       title = "Durchschnittliche Gießmenge nach Pumpen-Kategorie im 100 m Umkreis für Straßen- und Anlagebäume",
  #       x = "Pumpenkategorie",
  #       y = "Durchschnittliche Gießmenge (Liter)"
  #     ) +
  #     theme_minimal() +
  #     theme(
  #       axis.text = element_text(size = 12),
  #       axis.title = element_text(size = 13),
  #       plot.title = element_text(size = 15, face = "bold"),
  #       panel.grid.major.y = element_line(color = "gray90")
  #     )
  # })
  # 
  output$pumpenkategorien_plot_pump_ok_nur_straße <- renderPlotly({
    
    df_strassenbaeume <- df_merged_sum_distanz_umkreis_pump_ok_lor_kat() %>%
      filter(str_starts(gml_id, "strassenbaeume."))
    
    df_kategorie_mittelwert <- df_strassenbaeume %>%
      group_by(pumpenkategorie) %>%
      summarise(
        durchschnittliche_giessmenge = mean(gesamt_bewaesserung, na.rm = TRUE),
        anzahl_baeume = n(),
        .groups = "drop"
      ) %>%
      mutate(
        pumpenkategorie_label = paste0(pumpenkategorie, " (Anzahl Bäume = ", anzahl_baeume, ")")
      )
    
    p <- ggplot(df_kategorie_mittelwert, 
                aes(x = pumpenkategorie_label, 
                    y = durchschnittliche_giessmenge, 
                    fill = pumpenkategorie,
                    text = paste0(
                      "<b>Pumpenkategorie:</b> ", pumpenkategorie, "<br>",
                      "<b>Durchschnittliche Gießmenge:</b> ", round(durchschnittliche_giessmenge, 1), " L<br>",
                      "<b>Anzahl Bäume:</b> ", anzahl_baeume
                    )
                )) +
      geom_col() +
      scale_fill_viridis_d(option = "D", name = "Pumpenkategorie") +
      labs(
        title = "Durchschnittliche Gießmenge nach Pumpen-Kategorie im 100 m Umkreis (nur Straßenbäume)",
        x = "Pumpenkategorie",
        y = "Durchschnittliche Gießmenge (Liter)"
      ) +
      theme_minimal() +
      theme(
        axis.text = element_text(size = 12),
        axis.title = element_text(size = 13),
        plot.title = element_text(size = 15, face = "bold"),
        panel.grid.major.y = element_line(color = "gray90")
      )
    
    ggplotly(p, tooltip = "text")
  })
  
  
  df_merged_sum_mit_distanzen_mit_umkreis_kat <- reactive({
    df_merged_sum_mit_distanzen_mit_umkreis() %>%
      mutate(pumpenkategorie = case_when(
        pumpen_im_umkreis_100m == 0 ~ "Keine Pumpe",
        pumpen_im_umkreis_100m == 1 ~ "Eine Pumpe",
        pumpen_im_umkreis_100m >= 2 ~ "Mehrere Pumpen"
      ))
  })

  # 
  # output$pumpenkategorien_plot <- renderPlot({
  #   
  #   df_kategorie_mittelwert <- df_merged_sum_mit_distanzen_mit_umkreis_kat() %>%
  #     group_by(pumpenkategorie) %>%
  #     summarise(
  #       durchschnittliche_giessmenge = mean(gesamt_bewaesserung, na.rm = TRUE),
  #       anzahl_baeume = n(),
  #       .groups = "drop"
  #     ) %>%
  #     mutate(pumpenkategorie_label = paste0(pumpenkategorie, " (Anzahl Bäume = ", anzahl_baeume, ")"))
  #   
  #   ggplot(df_kategorie_mittelwert, aes(x = pumpenkategorie_label, y = durchschnittliche_giessmenge)) +
  #     geom_col(fill = "seagreen") +
  #     labs(
  #       title = "Durchschnittliche Gießmenge nach Pumpen-Kategorie im 100 m Umkreis mit Pumpen außerbetrieb",
  #       x = "Pumpenkategorie",
  #       y = "Durchschnittliche Gießmenge (Liter)"
  #     ) +
  #     theme_minimal() +
  #     theme(
  #       axis.text = element_text(size = 12),
  #       axis.title = element_text(size = 13),
  #       plot.title = element_text(size = 15, face = "bold"),
  #       panel.grid.major.y = element_line(color = "gray90")
  #     )
  # })
  # 
  
  output$hist_bewaesserung_pro_lor <- renderPlotly({
    req(input$selected_bezirk)
    
    df <- sozialindex_mit_Gesamtbewasserung_agg() %>%
      filter(bez == input$selected_bezirk) %>%
      drop_na(gesamt_bewaesserung_lor, bzr_name, GESIx_2022)
    
    plot_ly() %>%
      add_bars(
        data = df,
        x = ~bzr_name,
        y = ~gesamt_bewaesserung_lor,
        name = "Gießmenge",
        marker = list(color = "steelblue"),
        hoverinfo = "text",
        text = ~paste0("LOR: ", bzr_name,
                       "<br>Gießmenge: ", round(gesamt_bewaesserung_lor)),
        textposition = "none"
      )%>%
      add_lines(
        data = df,
        x = ~bzr_name,
        y = ~GESIx_2022,
        yaxis = "y2",
        name = "Sozialindex",
        line = list(color = "firebrick", width = 3),
        hoverinfo = "text",
        text = ~paste0("LOR: ", bzr_name,
                       "<br>Sozialindex: ", round(GESIx_2022, 1))
      ) %>%
      layout(
        title = "Gießmenge und Sozialindex pro LOR",
        xaxis = list(title = "LOR", tickangle = -45),
        yaxis = list(title = "Gießmenge"),
        yaxis2 = list(
          title = "Sozialindex (GESIx_2022)",
          overlaying = "y",
          side = "right",
          showgrid = FALSE
        ),
        legend = list(x = 0.1, y = 1)
      )
  })
  
  output$correlation_plot <- renderPlotly({
    p <- ggplot(kpi(), aes(x = .data[[input$x_var]], y = .data[[input$y_var]])) +
      geom_point(color = "#E91E63", size = 3) +
      geom_smooth(method = "lm", se = FALSE, color = "#3F51B5") +
      labs(title = paste("Korrelation zwischen", input$x_var, "und", input$y_var),
           x = input$x_var, y = input$y_var) +
      theme_minimal()
    ggplotly(p)
  })
  
  # Korrelationskoeffizient anzeigen
  output$correlation_value <- renderPrint({
    cor_value <- cor(kpi()[[input$x_var]], kpi()[[input$y_var]], use = "complete.obs")
    paste("Korrelationskoeffizient (r):", round(cor_value, 2))
  })
  
  
  output$corPlot <- renderPlot({
    req(cor_data())
    df <- cor_data()
    
    if (input$plot_type == "scatter") {
      ggplot(df, aes(x = einwohner, y = giessmenge, label = bezirk)) +
        geom_point(color = "steelblue", size = 3) +
        geom_smooth(method = "lm", se = FALSE, color = "red") +
        ggrepel::geom_text_repel() +
        labs(x = "Einwohnerzahl", y = "Gießmenge (Liter)",
             title = "Scatter: Einwohner vs. Gießmenge pro Bezirk")
    } else {
      ggplot(df, aes(x = reorder(bezirk, giessmenge), y = giessmenge, fill = einwohner)) +
        geom_col() +
        coord_flip() +
        labs(x = "Bezirk", y = "Gießmenge (Liter)",
             title = "Balkendiagramm: Gießmenge vs. Einwohner")
    }
  })
  
  output$corValue <- renderPrint({
    df <- einwohnerGiessm() %>% drop_na(einwohner, giessmenge)
    if (nrow(df) == 0) {
      cat("Keine passenden Daten für Korrelation gefunden. Check die Bezirksnamen und Einwohnerzahlen.")
    } else {
      cor_val <- cor(df$einwohner, df$giessmenge, method = "pearson")
      cat("Korrelationskoeffizient (Pearson):", round(cor_val, 3))
    }
  })
  

  output$corPlot_Einw_Giessm <- renderPlotly({ 
    df <- einwohnerGiessm()
    
    # Skalierungsfaktor berechnen
    max_giess <- max(df$giessmenge, na.rm = TRUE)
    max_einwohner <- max(df$einwohner, na.rm = TRUE)
    scaling_factor <- max_giess / max_einwohner
    
    plot_ly() %>%
      add_bars(
        data = df,
        x = ~bezirk,
        y = ~giessmenge,
        name = "Bewässerung (L)",
        marker = list(color = '#0072B2'),
        hovertemplate = paste(
          "<b>Bezirk:</b> %{x}<br>",
          "<b>Bewässerung:</b> %{y} L"
        )
      ) %>%
      add_bars(
        data = df,
        x = ~bezirk,
        y = ~einwohner * scaling_factor,
        name = "Einwohner",
        marker = list(color = '#E69F00'),
        customdata = df$einwohner,
        hovertemplate = paste(
          "<b>Bezirk:</b> %{x}<br>",
          "<b>Einwohner:</b> %{customdata}"
        )
      ) %>%
      add_trace(
        x = c(NA), y = c(NA),
        type = "bar",
        yaxis = "y2",
        showlegend = FALSE,
        hoverinfo = "none"
      ) %>%
      layout(
        title = "Bewässerung und Einwohner pro Bezirk",
        barmode = "group",
        xaxis = list(title = "Bezirk"),
        yaxis = list(
          title = "Bewässerung (L)",
          side = "left"
        ),
        yaxis2 = list(
          title = "Einwohner",
          overlaying = "y",
          side = "right",
          tickvals = seq(0, max_giess, length.out = 6),
          ticktext = round(seq(0, max_einwohner, length.out = 6), 0),
          range = c(0, max_giess),
          showgrid = FALSE
        ),
        legend = list(orientation = "h", x = 0.3, y = 1.1)
      )
  })

  
  
}