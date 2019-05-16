#main file for data cleansing of subscriber list
#built under GNU General Public License

source('dependencies.R')
source('io_utilities.R')
source('geocode.R')


#load source files
print('start')
rawData <- readRawDataFolder(path = paste( paste ( '..',  parameters$batch_name, sep='/') , parameters$path_input_data, sep='/'))

#identify duplicates
#train duplicates finding


#data contains lots of NA lines, corresponding to empty XLS lines.

#cleanse data
#had to preprocess data: remove quotes " and add a semicolon (extra field) at the end of column names
check_data <- readCsvFromDirectory(paste(parameters$batch_name, 'noQuotes', sep = '_'), 'Data')

cleanData <- cleanRawData(rawData, check_data)

#store collection
writeCsvIntoDirectory(cleanData, 'concatenated_raw_data', parameters$path_dataQualityCheck)

#classify errors


#validate addresses and store in a separate permanent store

mapIdtoAddress <- buildGoogleApiGeocodeJsonUrlEncode(cleanData)
writeCsvIntoDirectory(mapIdtoAddress, 'mapIdtoAddress', parameters$path_geocoded_address)

mapIdtoAddressValidated <- geocodeAddress(mapIdtoAddress , get_new = FALSE)

geocodedData <- base::merge(cleanData, mapIdtoAddressValidated)
writeCsvIntoDirectory(geocodedData, 'geocoded_data', parameters$path_forupload)

library(DBI)
con <- dbConnect(odbc::odbc(), parameters$db_connection, timeout = 10)

rodbcChannel <- RODBC::odbcConnect(parameters$db_connection )

sqlSave(rodbcChannel, geocodedData, tablename = 'plaintiffs_geocoded_batch001') #, append = FALSE,  rownames = FALSE, colnames = TRUE)


cat(query)

  
  dbGetQuery(con, query)


#store Address mapping and coordinates


View(matrix(unlist(geocodedAddress), nrow=length(geocodedAddress), byrow=T))
