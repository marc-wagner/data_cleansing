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

cleanRawData <- function(dt, check_dt= NULL){
  
  #1) non altering checks for data quality
  browser()
  #check completeness
  
  #expected nr records for batch001: 45944 
  print(nrow(dt))
  if(!is.null(check_dt)){
    if(nrow(dt[!is.na(id),]) != nrow(check_dt[!is.na(check_dt$id),])){
      check_dt[, id:=as.character(id)]
      diff_raw_cleansed <- base::merge(check_dt, dt, by='id', all.x = T)
    }
  }
    
  print('records with no id')
  View(dt[is.na(id),])

  
  print('page count')
  print(table(table(dt$page, useNA = 'ifany')))
  print('country count')  
  print(table(dt[,country],  useNA = 'ifany'))
  print('reason count')
  print(table(dt[,reason], useNA = 'ifany'))
  
  #alter data to cleanse for future processing
  cleanData <- copy(dt) 
  #remove address 2 if it is redundant
  cleanData <- cleanData[(coll(address2) == coll(street_nb)) | (coll(address2) == coll(zip)) , address2:= NA]
  cleanData <- cleanData[country == 'BE', country:='Belgium']

  #1000 pages
  
  #nr of unique id's and guids'
  
  #remove duplicates and errors 
  cleanData <- cleanData[reason %in% c('BV', 'C') | is.na(reason),]
  
  #remove records with no id
  cleanData <- cleanData[!is.na(id),]
  
  print('number of unique ids that are not duplicates')
  length(rawData[!is.na(id) & !reason %in% c('D'),id])
  length(cleanData[,id])
  
  #cleanse country: mostly due to escel import, so Belgium as default looks great.
  
  #invert zip and locality if appropriate
  
  #eyeball data to setup some other rules
  cleanData
  
}