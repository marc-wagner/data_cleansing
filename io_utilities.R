#IO utilities


readRawData <- function(filename) {
    readxl::read_excel(filename
                      ,range = "R1C1:R12000C39"
                      ,sheet = 1
                      ,col_names = TRUE
                      ,progress = readxl_progress()
                      ,trim_ws = TRUE
                      ,guess_max = 10000
                      ,col_types = 'text')   %>% as.data.table()
}

readRawDataFolder <- function(path) {
  files_in_folder <- list.files(path = path
                                ,full.names = TRUE
                                ,pattern = ".xlsx$")
  for (file in files_in_folder) {
    try(databuffer <- readRawData(filename = file)) 
    data <- if(!is.null(nrow(data))  ) {
        rbindlist(list(copy(databuffer), data), fill = FALSE, idcol = NULL)
    } else databuffer
  }
  data
}

cleanRawData <- function(dt, check_dt){
  
  #check data quality
  browser()
  #check completeness
  
  #expected nr records for batch001: 45944 
  print(nrow(dt))
  if(nrow(dt[!is.na(id),]) != nrow(check_dt[!is.na(check_dt$id),])){
    check_dt[, id:=as.character(id)]
    diff_raw_cleansed <- base::merge(check_dt, dt, by='id', all.x = T)
    
  }
    
  
  
  table(table(rawData$page, useNA = 'ifany'))

  
  #1000 pages
  
  #nr of unique id's and guids'
  
  dt[!is.na(id) ,]
}