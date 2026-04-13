sozialindex <- read.csv("data/gssa_2022_bezirksregionen.csv", sep = ",", stringsAsFactors = FALSE, fileEncoding = "UTF-8")

write.csv2(sozialindex, file = "data/sozialindex.csv", sep = ";")
