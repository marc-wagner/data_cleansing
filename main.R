#main file for data cleansing of subscriber list
#built under GNU General Public License

#classification of processing types stored in column 'reason'
#
# D duplicate to be discarded. the corresponding record does not have that tag
# E error, should be discarded
# C correction, either manual or through automated cleansing. should overwrite what comes from source.
# M flagged for manual processing.

source('dependencies.R')
source('io_utilities.R')
source('geocode.R')
source('RecordLinkage.R')



#1) Extract latest records

result <- dbSendQuery(con,  "SELECT * FROM plaintiff_latest")
raw_data <- as.data.table(dbFetch(result))  #dbFetch(result, n = 100)
raw_data$procuration_date <- as.Date(raw_data$procuration_date)


#2) Transform: x-check and cleanse data

#load manually deduplicated and corrected source files
print('start')
manual_fixes <- readRawDataFolder(path = paste( paste ( '..',  parameters$batch_name, sep='/') , parameters$path_input_data, sep='/'))

#replace records by manual fixes
raw_data <- blendData(raw_data, manual_fixes)

#load intput files to manual cleanup : check that nothing got lost in the process
#had to preprocess data: remove quotes " and add a semicolon (extra field) at the end of column names
check_data <- readCsvFromDirectory(paste(parameters$batch_name, 'noQuotes', sep = '_'), 'Data')

cleanData <- cleanRawData(raw_data, check_data)

#store collection
writeCsvIntoDirectory(cleanData, 'concatenated_raw_data', parameters$path_dataQualityCheck)


#classify errors
#TO DO

#validate addresses and store in a separate permanent store
mapIdtoAddress <- buildGoogleApiGeocodeJsonUrlEncode(cleanData)
writeCsvIntoDirectory(mapIdtoAddress, 'mapIdtoAddress', parameters$path_geocoded_address)
mapIdtoAddressValidated <- geocodeAddress(mapIdtoAddress , get_new = FALSE)
geocodedData <- base::merge(cleanData, mapIdtoAddressValidated)
writeCsvIntoDirectory(geocodedData, 'geocoded_data', parameters$path_forupload)




#store Address mapping and coordinates


View(matrix(unlist(geocodedAddress), nrow=length(geocodedAddress), byrow=T))



#final checks: data complete, no NA's left on relevant fields?
finalCheck <- 0
base::stopifnot(finalCheck != 1) 
