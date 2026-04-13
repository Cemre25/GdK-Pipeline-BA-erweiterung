ui <- dashboardPage(
  dashboardHeader(title = "Gieß den Kiez"),
  
  dashboardSidebar(
    sidebarMenu(
      menuItem("Dashboard", tabName = "dashboard", icon = icon("dashboard")),
      menuItem("Karte", tabName = "map", icon = icon("map")),
      menuItem("Analyse", tabName = "analysis", icon = icon("chart-line")),
      menuItem("Engagement", tabName = "engagement", icon = icon("users"))
    )
  ),
  
  dashboardBody(
    tags$script(HTML("$(document).ready(function() {
      var map = $('#map').find('div.leaflet-container')[0];
      if (map) {
        var leafletMap = $(map).data('leaflet-map');
        leafletMap.on('zoomend', function() {
          Shiny.setInputValue('map_zoom', leafletMap.getZoom());
        });
      }
    });")),
    
    tabItems(
      
      # Tab 1: Dashboard
      tabItem(tabName = "dashboard",
              fluidRow(
                box(
                  title = tagList(
                    "Overview",
                    div(actionButton("info_boxes", label = "", icon = icon("info-circle")),
                        style = "position: absolute; right: 15px; top: 5px;")
                  ),
                  status = "primary", solidHeader = TRUE, width = 12,
                  fluidRow(
                    uiOutput("dynamic_tree_box"),
                    valueBoxOutput("total_water"),
                    valueBoxOutput("avg_water")
                  ),
                  fluidRow(
                    column(width = 6,
                           selectInput("start_year", "Jahr der Bewässerung auswählen:", 
                                       choices = c("2020-2024", "Baumbestand Stand 2025"), selected = "Baumbestand Stand 2025", multiple = TRUE)
                    ),
                    column(width = 6,
                           selectInput("bezirk", "Bezirk auswählen:", 
                                       choices = "Alle", selected = "Alle", multiple = TRUE)
                    )
                  )
                )
              ),
              fluidRow(
                box(status = "primary", solidHeader = TRUE, width = 12, 
                    title = tagList("Einfluss von Baum- und Bevölkerungsdichte auf das Gießverhalten", 
                                    div(actionButton("info_tree_distribution", label = "", icon = icon("info-circle")), style = "position: absolute; right: 15px; top: 5px;")),
                    selectInput("stats_baumvt_year", "Jahr auswählen:", 
                                choices = c("2020-2024", "Baumbestand Stand 2025"), selected = "Baumbestand Stand 2025", multiple = TRUE),
                   
                    radioButtons("water_mode", "Anzeige wählen:", 
                                 choices = c("Baumdichte" = "bd", "Bevölkerungsdichte" = "bev"),
                                 selected = "bd", inline = TRUE),
                    conditionalPanel(condition = "input.water_mode == 'bd'", plotlyOutput("tree_distribution")),
                    conditionalPanel(condition = "input.water_mode == 'bev'", plotlyOutput("population_distribution"))
                )
              ),
              fluidRow(
                box(title = tagList("Häufig gegossene Baumarten im Verhältnis zu ihrem Vorkommen", 
                                    div(actionButton("info_tree_pie_chart", label = "", icon = icon("info-circle")), style = "position: absolute; right: 15px; top: 5px;")),
                    status = "primary", solidHeader = TRUE, width = 12, height = "auto", 
                    selectInput("pie_bezirk", "Bezirk auswählen:", 
                                choices = "Alle", selected = "Alle", multiple = TRUE),
                    plotlyOutput("tree_pie_chart"),
                    fill = TRUE
                )
              )
      ),
      
      # Tab 2: Karte
      tabItem(tabName = "map",
              fluidRow(
                box(title = tagList("Filter", 
                                    div(actionButton("info_map", label = "", icon = icon("info-circle")), 
                                        style = "position: absolute; right: 15px; top: 5px;")),
                    status = "primary", solidHeader = TRUE, width = 12,
                    column(width = 6,
                           selectInput("map_bezirk", "Bezirk auswählen:", 
                                       choices = "Alle", selected = "Alle", multiple = TRUE)
                    ),
                    column(width = 6,
                           selectInput("map_lor", "Lebensweltlich orientierte Räume auswählen:", 
                                       choices = "Alle", selected = "Alle", multiple = TRUE)
                    ),
                    column(width = 6,
                           selectInput("map_year", "Jahr auswählen:", 
                                       choices = "2020-2024", selected = "2020-2024", multiple = TRUE)
                    ),
                    column(width = 6,
                           selectInput("map_saison", "Saison auswählen:", 
                                       choices = c("Alle", "Winter", "Frühling", "Sommer", "Herbst"), selected = "Alle", multiple = TRUE)
                    ),
                    column(width = 6,
                           selectInput("map_baumgattung", "Baumgattung auswählen:", 
                                       choices = "Alle", selected = "Alle", multiple = TRUE)
                    )
                )
              ),
              fluidRow(
                box(title = "Karte", status = "primary", solidHeader = TRUE, width = 12,
                    leafletOutput("map", height = "800px")
                )
              )
      ),
      
      # Tab 3: Analyse
      tabItem(tabName = "analysis",
              fluidRow(
                box(title = tagList("Bewässerung pro Bezirk (2020-2024)", 
                                    div(actionButton("info_hist_bewaesserung_pro_bezirk", label = "", icon = icon("info-circle")), 
                                        style = "position: absolute; right: 15px; top: 5px;")),
                    status = "primary", solidHeader = TRUE, width = 12,
                    radioButtons("water_mode", "Anzeige wählen:", 
                                 choices = c("Durchschnittliche Bewässerung" = "avg", "Verhältnis Bewässerung / Anzahl Bäume" = "ratio"),
                                 selected = "avg", inline = TRUE),
                    conditionalPanel(condition = "input.water_mode == 'avg'", plotlyOutput("hist_bewaesserung_pro_bezirk")),
                    conditionalPanel(condition = "input.water_mode == 'ratio'", plotlyOutput("hist_bewaesserung_verhaeltnis"))
                )
              ),
              fluidRow(
                box(title = "Ranking", status = "primary", solidHeader = TRUE, width = 12,
                    selectInput("bar_bezirk", "Bezirk auswählen:", choices = "Alle", selected = "Alle", multiple = TRUE),
                    box(title = tagList("Top 10 Straßen mit höchster Bewässerung (2020-2024)", 
                                        div(actionButton("info_hist_Top_10_best_verhaeltnis_baum", label = "", icon = icon("info-circle")), style = "position: absolute; right: 15px; top: 5px;")),
                        status = "primary", solidHeader = TRUE, width = 6,
                        plotlyOutput("hist_Top_10_best_verhaeltnis_baum")
                    ),
                    box(title = tagList("Bottom 10 Straßen mit geringster Bewässerung (2020-2024)", 
                                        div(actionButton("info_btn_bottom", label = "", icon = icon("info-circle")), style = "position: absolute; right: 15px; top: 5px;")),
                        status = "primary", solidHeader = TRUE, width = 6,
                        plotlyOutput("hist_Top_10_worst_verhaeltnis_baum")
                    )
                )
              ),
              fluidRow(
                box(title = tagList("Trend der Bewässerung je Pflanzjahr", 
                                    div(actionButton("info_trend_water", label = "", icon = icon("info-circle")), style = "position: absolute; right: 15px; top: 5px;")),
                    status = "primary", solidHeader = TRUE, width = 12,
                    sliderInput("trend_year", "Jahre filtern:", min = 1900, max = 2025, value = c(2000, 2024)),
                    selectInput("trend_bezirk_pj", "Bezirk auswählen:", choices = "Alle", selected = "Alle", multiple = TRUE),
                    plotlyOutput("trend_water")
                )
              ),
              fluidRow(
                box(title = tagList("Trend der Bewässerung", 
                                    div(actionButton("info_trend_water_ts", label = "", icon = icon("info-circle")), style = "position: absolute; right: 15px; top: 5px;")),
                    status = "primary", solidHeader = TRUE, width = 12,
                    radioButtons("trend_mode", "Anzeige wählen:", 
                                 choices = c("Monatsweise (pro Jahr)" = "month", "Jahresweise (2020-2024)" = "year"),
                                 selected = "month", inline = TRUE),
                    conditionalPanel(condition = "input.trend_mode == 'month'",
                                     selectInput("trend_year_ts", "Jahr auswählen:", choices = "2024", selected = "2024")
                    ),
                    selectInput("trend_bezirk", "Bezirk auswählen:", choices = "Alle", selected = "Alle", multiple = TRUE),
                    selectInput("trend_baumgattung", "Baumgattung auswählen:", choices = "Alle", selected = "Alle", multiple = TRUE),
                    plotlyOutput("trend_water_ts")
                )
              ),
              fluidRow(
                box(title = "", status = "primary", solidHeader = TRUE, width = 12,
                    selectInput("x_var", "X-Achse:", 
                                choices = c("Nutzer" = "uniqueusers", "Adoptierte Bäume" = "uniqueadoptedtrees", "Bewässerungen" = "treeswatered")),
                    selectInput("y_var", "Y-Achse:", 
                                choices = c("Wassermenge" = "totalwateramount", "Bewässerte Bäume" = "uniquetreeswatered")),
                    plotlyOutput("correlation_plot"),
                    verbatimTextOutput("correlation_value")
                )
              ), 
              fluidRow(
                box(title = "Korrelation: Einwohnerzahl vs. Gießmenge pro Bezirk", status = "primary", solidHeader = TRUE, width = 12,
                    selectInput("plot_type", "Visualisierung wählen:",
                                choices = c("Scatterplot" = "scatter", "Balkendiagramm" = "bar")),
                    
                    plotlyOutput("corPlot_Einw_Giessm"),
                    verbatimTextOutput("corValue")
                )
              )
      ),
      
      # Tab 4: Engagement
      tabItem(tabName = "engagement",
              fluidRow(
                box(title = tagList("Pumpenanzahl und Bewässerung pro Bezirk (2020-2024)", div(actionButton("info_balken_plot", label = "", icon = icon("info-circle")), style = "position: absolute; right: 15px; top: 5px;") ),
                   status = "primary", solidHeader = TRUE, width = 12,
                    plotlyOutput("balken_plot")
                )
              ),
              fluidRow(
                box(title = tagList("Gießverhalten nach Bezirk (2020-2024)", div(actionButton("info_karte_giessverhalten", label = "", icon = icon("info-circle")), style = "position: absolute; right: 15px; top: 5px;")), 
                status = "primary", solidHeader = TRUE, width = 12,
                    leafletOutput("karte_giessverhalten", height = "800px")
                )
              ),
              fluidRow(
                box(title = tagList("Durchschnittliche Gießmenge nach Pumpen-Kategorie (nur Straßenbäume)", div(actionButton("info_pumpenkategorien_plot_pump_ok_nur_straße", label = "", icon = icon("info-circle")), style = "position: absolute; right: 15px; top: 5px;")), 
                status = "primary", solidHeader = TRUE, width = 12,
                    plotlyOutput("pumpenkategorien_plot_pump_ok_nur_straße")
                )
              ),
              fluidRow(
                box(title = tagList("Gießverhalten im Verhältnis zu LOR", div(actionButton("info_hist_bewaesserung_pro_lor", label = "", icon = icon("info-circle")), style = "position: absolute; right: 15px; top: 5px;")), 
                status = "primary", solidHeader = TRUE, width = 12,
                    selectInput("selected_bezirk", "Bezirk auswählen:", choices = NULL),
                    plotlyOutput("hist_bewaesserung_pro_lor")
                )
              )
      )
    )
    #,
  #   bsModal("info_total_water", "Erklärung:", "info_total_water", size = "medium",
  #           HTML("
  #   <b>Was wird angezeigt?</b><br>
  #   Zeigt die <strong>Gesamtmenge an Wasser</strong>, die im gewählten Zeitraum und Bezirk zum Gießen verwendet wurde – automatisch umgerechnet in passende Einheiten (Liter, m³, ML).
  # ")
  #   ),
  #   
  #   bsModal("info_avg_water", "Erklärung:", "info_avg_water", size = "medium",
  #           HTML("
  #   <b>Was wird angezeigt?</b><br>
  #   Der <strong>Durchschnitt</strong> an Wassermenge pro gegossenen Baum – also: wie viel Wasser bekam ein Baum im Schnitt.
  # ")
  #   ),
  #   
  #   bsModal("info_tree_distribution", "Erklärung:", "info_tree_distribution", size = "medium",
  #           HTML("
  #   <b>Was wird angezeigt?</b><br>
  #   Dieses Diagramm zeigt, wie viele <strong>Bäume pro Hektar</strong> Fläche in den Berliner Bezirken stehen. Es hilft zu erkennen, wie dicht bepflanzt ein Bezirk ist – unabhängig von seiner Größe.<br><br>
  #   <b>Wie Filter wirken:</b>
  #   <ul>
  #     <li><strong>Jahr-Auswahl:</strong> Zeigt entweder alle gepflanzten Bäume („Baumbestand Stand 2025“) oder nur tatsächlich bewässerte Bäume („2020–2024“ oder ein bestimmtes Jahr).</li>
  #     <li><strong>Bezirk:</strong> Eingrenzung auf einzelne Bezirke möglich.</li>
  #   </ul>
  # ")
  #   ),
  #   
  #   bsModal("info_hist_bewaesserung_pro_bezirk", "Erklärung:", "info_hist_bewaesserung_pro_bezirk", size = "medium",
  #           HTML("
  #   <b>Was wird angezeigt?</b><br>
  #   Ein Balkendiagramm mit der <strong>Gesamtwassermenge</strong>, die pro Bezirk zum Gießen verwendet wurde. Die Werte sind automatisch in passende Einheiten (Liter, m³ oder ML) umgerechnet.
  # ")
  #   ),
  #   
  #   bsModal("info_hist_bewaesserung_verhaeltnis", "Erklärung:", "info_hist_bewaesserung_verhaeltnis", size = "medium",
  #           HTML("
  #   <b>Was wird angezeigt?</b><br>
  #   Wie viel Wasser im Durchschnitt <strong>pro Baum</strong> verwendet wurde – getrennt nach Bezirken. Das hilft, Bezirke mit intensiver oder sparsamer Bewässerung zu erkennen.
  # ")
  #   ),
  #   
  #   bsModal("info_trend_water", "Erklärung:", "info_trend_water", size = "medium",
  #           HTML("
  #   <b>Was wird angezeigt?</b><br>
  #   Dieses Diagramm zeigt, wie viel Wasser für Bäume <strong>verschiedener Pflanzjahre</strong> verwendet wurde. Es zeigt z. B., ob jüngere Bäume häufiger gegossen wurden.<br><br>
  #   <b>Wie Filter wirken:</b>
  #   <ul>
  #     <li><strong>Jahr-Spanne:</strong> Auswahl bestimmter Pflanzjahre.</li>
  #     <li><strong>Bezirk:</strong> Fokus auf einzelne Stadtteile möglich.</li>
  #   </ul>
  # ")
  #   ),
  #   
  #   bsModal("info_trend_water_ts", "Erklärung:", "info_trend_water_ts", size = "medium",
  #           HTML("
  #   <b>Was wird angezeigt:</b><br>
  #   Die Entwicklung von <strong>Bewässerungsmenge, Temperatur und Niederschlag</strong> im zeitlichen Verlauf – monatlich oder jährlich.<br><br>
  #   <b>Wie Filter wirken:</b>
  #   <ul>
  #     <li><strong>Jahr/Modus:</strong> Auswahl eines bestimmten Jahres oder Modus (monatlich/jährlich).</li>
  #     <li><strong>Bezirk & Baumart:</strong> Eingrenzung nach Region oder Art möglich.</li>
  #   </ul>
  # ")
  #   ),
  #   
  #   bsModal("info_tree_pie_chart", "Erklärung:", "info_tree_pie_chart", size = "medium",
  #           HTML("
  #   <b>Was wird angezeigt?</b><br>
  #   Ein Kreisdiagramm der <strong>Top 10 Baumarten</strong> mit ihrem Anteil an gegossenen Bäumen – also, welche Arten besonders häufig bewässert wurden.<br><br>
  #   <b>Wie Filter wirken:</b>
  #   <ul><li>Optional Eingrenzung auf einen Bezirk.</li></ul>
  # ")
  #   ),
  #   
  #   bsModal("info_map", "Erklärung:", "info_map", size = "medium",
  #           HTML("
  #   <b>Was wird angezeigt?</b><br>
  #   Eine interaktive <strong>Karte von Berlin</strong> mit allen Bäumen als Punkte und ihrer Bewässerungshistorie. Ab Zoomstufe 14 werden auch Pumpen eingeblendet.<br><br>
  #   <b>Wie Filter wirken:</b>
  #   <ul>
  #     <li><strong>Bezirk, Baumart, Jahr, Saison:</strong> bestimmen die Auswahl der Bäume.</li>
  #     <li><strong>Kartenbereich:</strong> Nur sichtbare Bäume werden geladen.</li>
  #     <li><strong>Zoom-Level:</strong> Pumpen werden erst ab Zoomstufe 14 angezeigt.</li>
  #   </ul>
  # ")
  #   ),
  #   
  #   bsModal("info_balken_plot", "Erklärung:", "info_balken_plot", size = "medium",
  #           HTML("
  #   <b>Was wird angezeigt?</b><br>
  #   Ein gruppiertes Balkendiagramm mit:
  #   <ul>
  #     <li><strong>Blau:</strong> Gesamtbewässerung pro Bezirk</li>
  #     <li><strong>Orange:</strong> Anzahl der funktionierenden Pumpen</li>
  #   </ul>
  #   <b>Hinweis:</b> Die beiden Größen sind aufeinander skaliert, damit sie gemeinsam dargestellt werden können.
  # ")
  #   ),
  #   
  #   bsModal("info_karte_giessverhalten", "Erklärung:", "info_karte_giessverhalten", size = "medium",
  #           HTML("
  #   <b>Was wird angezeigt?</b><br>
  #   Eine Karte mit Bezirken (grobe Ebene) oder LORs (feinere Gebietseinheiten), eingefärbt nach ihrer <strong>Gesamtbewässerung</strong>. Pumpenstandorte werden je nach Zoomstufe eingeblendet.
  # ")
  #   ),
  #   
  #   bsModal("info_hist_Top_10_best_verhaeltnis_baum", "Erklärung:", "info_hist_Top_10_best_verhaeltnis_baum", size = "medium",
  #           HTML("
  #   <b>Was wird angezeigt?</b><br>
  #   Die zehn Straßen, bei denen im Verhältnis pro Baum <strong>am meisten oder am wenigsten Wasser</strong> verwendet wurde.<br><br>
  #   <b>Wie Filter wirken:</b>
  #   <ul>
  #     <li><strong>Bezirk:</strong> Auswahl über <code>bar_bezirk</code>.</li>
  #     <li>Nur gegossene Bäume mit zugeordneter Straße werden berücksichtigt.</li>
  #   </ul>
  # ")
  #   ),
  #   
  #   bsModal("info_pumpenkategorien_plot_pump_ok_nur_straße", "Erklärung:", "info_pumpenkategorien_plot_pump_ok_nur_straße", size = "medium",
  #           HTML("
  #   <b>Was wird angezeigt?</b><br>
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
  #   </ul>
  # ")
  #   ),
  #   
  #   bsModal("hist_bewaesserung_pro_lor", "Erklärung:", "hist_bewaesserung_pro_lor", size = "medium",
  #           HTML("
  #   <b>Was wird angezeigt?</b><br>
  #   Eine <strong>feinräumige Darstellung</strong>: Zeigt die gesamte Wassermenge, die in jedem <strong>LOR-Gebiet</strong> (Lebensweltlich orientierter Raum) verwendet wurde.<br><br>
  #   <b>Wie Filter wirken:</b>
  #   <ul>
  #     <li>Berücksichtigt Bezirks- und Jahresauswahl.</li>
  #     <li>Basiert auf <code>df_merged_mit_lor_sum</code>.</li>
  #   </ul>
  # ")
  #   )
  )
)