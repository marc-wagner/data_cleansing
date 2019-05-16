#IO utilities


readRawData <- function(filename) {
    readxl::read_excel(filename
                      ,range = "R1C1:R12000C39"
                      ,sheet = 1
                      ,col_names = TRUE
                      ,trim_ws = TRUE
                      ,guess_max = 10000
                      ,col_types = 'text'
                      ,)   %>% as.data.table()
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
  #temporarily leaving duplicates in there.
  #cleanData <- cleanData[reason %in% c('BV', 'C') | is.na(reason),]
  
  #remove records with no id
  cleanData <- cleanData[!is.na(id),]
  
  print('number of unique ids that are not duplicates in raw data:')
  length(rawData[!is.na(id) & !reason %in% c('D'),id])
  print('number of records in clean data:')
    length(cleanData[,id])
  
  
  #cleanse country: mostly due to excel import, so Belgium as default looks great.
  cleanData[is.na(country), country:='Belgium']
  
  #invert zip and locality if appropriate
  
  #eyeball data to setup some other rules
  cleanData
}

writeCsvIntoDirectory <- function(dt, filename, directory){
  file = paste('..',parameters$batch_name, sep = '/') %>% 
    paste( directory, sep = '/')  %>%
    paste(filename, sep = "/")  %>%
    paste("csv", sep = ".")
  
  write.csv2(dt , file, row.names = TRUE , quote = FALSE,  fileEncoding='UTF-8' )
  file.exists(file)
}

writeFstIntoDirectory <- function(dt, filename, directory){
  file = paste('..',parameters$batch_name, sep = '/') %>% 
    paste( directory, sep = '/')  %>%
    paste(filename, sep = "/")  %>%
    paste("fst", sep = ".")
  
  write.fst(dt , file, uniform_encoding = FALSE)
  file.exists(file)
}

readCsvFromDirectory <- function(filename, directory){
  read.csv(  file = paste('..' , parameters$batch_name, sep = '/')  %>%
                           paste(directory, sep = '/')  %>%
                           paste(filename, sep = '/') %>%
                           paste('csv', sep = '.')
                         ,header = TRUE
                         , sep = ";"
                         , quote = ""
                         , encoding="UTF-8"
                         , stringsAsFactors=FALSE)  %>%
          as.data.table()
}

readFstFromDirectory <- function(filename, directory){
  read.fst( paste('..' , parameters$batch_name, sep = '/')  %>%
               paste(directory, sep = '/')  %>%
               paste(filename, sep = '/') %>%
               paste('fst', sep = '.')
             ,
           )  %>%
    as.data.table()
}