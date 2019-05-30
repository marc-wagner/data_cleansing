#IO utilities

#DB connection
con <- dbConnect(odbc::odbc(), parameters$db_connection, timeout = 10)

#read an individual excel file
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

#traverse folder with manually curated excel files
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

  #remove records without an id
  data <- data[!is.na(id),]
  
  #assign proper types
  data[, id := as.integer(id)]
  data[,dob := as.Date('1900-01-01')+ as.integer(dob) - 2]
  data[,procuration_date := as.Date('1900-01-01')+ as.integer(procuration_date) - 2]
  colToInt <- c( 'newsletter',
                 'is_public',
                 'is_complete',
                 'is_paper',
                 'is_imported_from_excel',
                 'is_not_minor_anymore',
                 'has_warning',
                 'has_email_warning',
                 'has_address_warning',
                 'has_dob_warning',
                 'is_minor',
                 'has_profession_warning',
                 'has_newsletter_or_public_warning',
                 'has_duplicate_warning',
                 'has_language_warning'    )
  for(i in colToInt) {
    data[[i]] <- as.numeric(data[[i]])
  }
  
  data
}

#replace records by manual fixes if matched on ids
#set duplicate_id and reason to NA for new records
blendData <- function(raw_data, manual_fixes){

  manual_id_exist <- as.data.table(copy(manual_fixes[, id]))
  manual_id_exist[, id_exists := 1]
  manual_id_exist <- manual_id_exist[,.(id = V1, id_exists)]
  new_records <- base::merge(raw_data, manual_id_exist,   all.x = TRUE )
  new_records <- new_records[is.na(id_exists),]  
  new_records[, id_exists := NULL]
  
  table(rbindlist(list(new_records, manual_fixes), use.names =  TRUE, idcol = FALSE, fill=TRUE)$reason, useNA = 'ifany')
  rbindlist(list(new_records, manual_fixes), use.names =  TRUE, idcol = FALSE, fill=TRUE)
  #small diff : raw_data  = 62004 records, manual_fixes = 43335 records, new_records = 18671 records
  #checksum 43335 + 18671 = 62006 records. 2 records diff
}


cleanRawData <- function(dt, check_dt= NULL){
  
  #1) non altering checks for data quality
  
    #check completeness: obsolete, as we will continue with the manual fixes as they are, for better or worse.
    #expected nr records for batch001: 45944 , to be checked against manual fixes (have page number)
    print(nrow(dt))
    if(!is.null(check_dt)){
   
      if(nrow(dt[!is.na(id) & !is.na(page),]) != nrow(check_dt[!is.na(check_dt$id),])){
        print("WARNING: difference in record count: input to manual fixes:")
        print( nrow(check_dt[!is.na(check_dt$id),]) )
        print("output of manual fixes:")
        print(nrow(dt[!is.na(id) & !is.na(page),]))
        #diff_raw_cleansed <- base::merge(check_dt, dt, by='id', all.x = T)
      }
    }
      
    if(nrow(dt[is.na(id),]) != 0){
      print('records with no id: ')
      print(nrow(dt[is.na(id),]))
      View(dt[is.na(id),])
    }  
    
    print('page count')
    print(table(table(dt$page, useNA = 'ifany')))
    print('country count')  
    print(table(dt[,country],  useNA = 'ifany'))
    print('reason count')
    print(table(dt[,reason], useNA = 'ifany'))
  
    print('has_warning count')
    print(table(dt$has_warning , useNA = 'ifany' ))
    
    print('has_address_warning count')
    print(table(dt$has_address_warning , useNA = 'ifany' ))
    
    print('has_dob_warning count')
    print(table(dt$has_dob_warning , useNA = 'ifany' ))
    
    print('has_email_warning count') 
    print(table(dt$has_email_warning , useNA = 'ifany' ))
    
    print('has_duplicate_warning count') 
    print(table(dt$has_duplicate_warning , useNA = 'ifany' ))
    
    print('has_language_warning count') 
    print(table(dt$has_language_warning , useNA = 'ifany' ))
    
  #1) Modification to data to increase quality  
    
  #alter data to cleanse for future processing
  cleanData <- copy(dt) 
  
  #remove records with no id
  cleanData <- cleanData[!is.na(id),]
  
  #remove address 2 if it is redundant
  cleanData <- cleanData[(coll(address2) == coll(street_nb)) | (coll(address2) == coll(zip)) , address2:= NA]
  #leaving BE as this is what google geoloc expects
  #https://developers.google.com/maps/documentation/geocoding/intro#ComponentFiltering
  #country matches a country name or a two letter ISO 3166-1 country code. 
  #The API follows the ISO standard for defining countries, and the filtering works best when using the corresponding ISO code of the country.
  #cleanData <- cleanData[country == 'BE', country:='Belgium']

  #1000 pages
  
  #nr of unique id's and guids'
  
  #remove duplicates and errors 
  #temporarily leaving duplicates in there.
  #cleanData <- cleanData[reason %in% c('BV', 'C') | is.na(reason),]
  

  print('number of unique ids that are not duplicates in raw data:')
  print(length(dt[!is.na(id) & !reason %in% c('D'),id]))
  print('number of records in clean data:')
  print(length(cleanData[,id]))
  
  #invert zip and locality if appropriate 
  cleanData[(!grepl("[^A-Za-z]", zip ) | is.na(zip)) & (!grepl("[^0-9]", locality ) | is.na(locality)) , swapbuffer := zip]
  cleanData[!is.na(swapbuffer) & is.na(reason), reason := 'C']
  cleanData[!is.na(swapbuffer), zip := locality]
  cleanData[!is.na(swapbuffer), locality := swapbuffer]
  cleanData[, swapbuffer := NULL]
  
  
  #extract zip from locality
  cleanData[ as.integer(substr(locality,1,4)) >= 1000  & as.integer(substr(locality,1,4)) <= 9999, zipbuffer:= as.integer(substr(locality,1,4)) ]
  cleanData[!is.na(zipbuffer)  , reason:='C'] #flag as manual correction
  cleanData[!(as.integer(zip) >= 1000  & as.integer(zip) <= 9999), zip:= zipbuffer]
  cleanData[, zipbuffer := NULL]
  
  #cleanse country: if zip code is not 4 digits numeric, flag it for review because country needs fixing
  cleanData[!is.numeric(zip) & !is.na(zip) & (as.numeric(zip) < 1000 | as.numeric(zip) > 9999)  , reason:='E']
  
  #cleanse country: mostly due to excel import, 
  #so setting Belgium as default since we filtered on zip code above.
  #20190524: removed this workaround, letting google geoloc figure out the country.
  #cleanData[is.na(country), reason:='C']
  #cleanData[is.na(country), country:='Belgium']
  
  #flag all records that have a warning for manual review
  cleanData[has_warning ==1 & is.na(reason) , reason:='M']
  
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
                         , stringsAsFactors=FALSE
                         )  %>%
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