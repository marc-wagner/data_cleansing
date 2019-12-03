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

#load manually deduplicated and corrected source files Batch001
print('start manual corrections batch001: paper checks')
manual_fixes1 <- readRawDataFolder(path = paste( paste ( '..',  'Batch001', sep='/') , parameters$path_input_data, sep='/'))

#replace records by manual fixes
fixed_data <- blendData(raw_data, manual_fixes1)

print('start manual corrections batch002: original coplaintiffs, first dedup')
manual_fixes2 <- readRawDataFolder(path = paste( paste ( '..',  'Batch002', sep='/') , parameters$path_input_data, sep='/'))

#replace records by manual fixes
fixed_data <- blendData(fixed_data, manual_fixes2)

print('start manual corrections batch003: geocoding and foreign addressed')
manual_fixes3 <- readRawDataFolder(path = paste( paste ( '..',  'Batch003', sep='/') , parameters$path_input_data, sep='/'))

#replace records by manual fixes
fixed_data <- blendData(fixed_data, manual_fixes3)

print('start manual corrections current batch')
manual_fixes4 <- readRawDataFolder(path = paste( paste ( '..',  parameters$batch_name, sep='/') , parameters$path_input_data, sep='/'))
#corrections on 20190612: false negatives from deduplication, last cleanups

#replace records by manual fixes
fixed_data <- blendData(fixed_data, manual_fixes4)



#load intput files to manual cleanup : check that nothing got lost in the process
#had to preprocess data: remove quotes " and add a semicolon (extra field) at the end of column names
#20190603 no longer applicable, as this was specific to Batch001
#check_data <- readCsvFromDirectory(paste(parameters$batch_name, 'noQuotes', sep = '_'), 'Data')
check_data <- NULL

cleanData <- cleanRawData(fixed_data, check_data)

#store collection
writeCsvIntoDirectory(cleanData, 'concatenated_raw_data', parameters$path_dataQualityCheck)


#classify errors
#TO DO

#validate addresses and store in a separate permanent store
mapIdtoAddress <- buildGoogleApiGeocodeJsonUrlEncode(cleanData)
writeCsvIntoDirectory(mapIdtoAddress, 'mapIdtoAddress', parameters$path_geocoded_address)
mapIdtoAddressValidated <- geocodeAddress(mapIdtoAddress , get_new = parameters$get_new_geocoding)

#no need to geolocate the id's above 100000, which are the plaintiffs already added to trial.
missing_geoloc <- base::merge(cleanData[id < 100000,], mapIdtoAddressValidated, all.x = TRUE)[is.na(address_validated) & reason != 'U'  & reason != 'D',]
writeCsvIntoDirectory(missing_geoloc, 'missing_geoloc', parameters$path_dataQualityCheck)

geocodedData <- base::merge(cleanData, mapIdtoAddressValidated, all.x = TRUE)
#build validated address from components for reason = 'U'se existing
geocodedData[reason == 'U' & is.na(address_validated) , address_validated := paste(address,street_nb, ifelse(is.na(address2), '', address2 ), ',' ,zip, locality, country)  ]
writeCsvIntoDirectory(geocodedData, 'geocoded_data', parameters$path_forupload)
#writeFstIntoDirectory(geocodedData, 'geocoded_data', parameters$path_forupload)

#free up memory before deduplication
rm(raw_data, mapIdtoAddress, mapIdtoAddressValidated, manual_fixes1,manual_fixes2,manual_fixes3, manual_fixes4, check_data, cleanData, fixed_data, missing_geoloc, result, con)

dedupData <- deduplicate(geocodedData)
dedupData[, duplicate_id.x:=NULL]
dedupData[, duplicate_id := duplicate_id.y]
dedupData[, duplicate_id.y:=NULL]
dedupData[!is.na(duplicate_id) , auto_reason := 'D']

#remove original plaintiffs : no longer needed now.
#dedupData <- dedupData[id < parameters$original_plaintiffs_offset ]

#writeFstIntoDirectory(dedupData[order(-previously_checked_dups, -Weight, cluster_id),], 'deduplicated_data', parameters$path_forupload)

# hack to get quotes around text that has semicolons, replaces
# call to writeCsvIntoDirectory(dedupData[order(-cluster_id),], 'deduplicated_data', parameters$path_forupload)
file = paste('..',parameters$batch_name, sep = '/') %>% 
  paste( parameters$path_forupload, sep = '/')  %>%
  paste('deduplicated_data', sep = "/")  %>%
  paste("csv", sep = ".")

write.csv2(dedupData[order(-cluster_id),.(cluster_id,duplicate_id,auto_reason, Weight, page,line,reason, id,firstname,lastname,language,profession,dob,address_validated, address,street_nb,address2,zip,locality,country,phone,email,guid,procuration_date,extra_info,newsletter,is_public,is_complete,sms_code,twilio_reference,is_paper,legacy_data,is_imported_from_excel,is_not_minor_anymore,has_warning,has_email_warning,has_address_warning, has_dob_warning,is_minor,has_profession_warning,has_newsletter_or_public_warning,has_duplicate_warning,has_language_warning,components,postal_code
)] , file, row.names = TRUE , quote = TRUE,  fileEncoding='UTF-8' )



#flag previously checked duplicates that match current classification, to avoid double work
new_dups <- unique(dedupData[(reason != 'D' | is.na(reason)) & auto_reason == 'D',  cluster_id])
residual_dusp <- dedupData[cluster_id %in% new_dups ,]

file = paste('..',parameters$batch_name, sep = '/') %>% 
  paste( parameters$path_forupload, sep = '/')  %>%
  paste('deduplicated_data_to_check', sep = "/")  %>%
  paste("csv", sep = ".")

write.csv2(residual_dusp[order(-cluster_id),.(cluster_id,duplicate_id,auto_reason, Weight, page,line,reason, id,firstname,lastname,language,profession,dob,address_validated, address,street_nb,address2,zip,locality,country,phone,email,guid,procuration_date,extra_info,newsletter,is_public,is_complete,sms_code,twilio_reference,is_paper,legacy_data,is_imported_from_excel,is_not_minor_anymore,has_warning,has_email_warning,has_address_warning, has_dob_warning,is_minor,has_profession_warning,has_newsletter_or_public_warning,has_duplicate_warning,has_language_warning,components,postal_code
)] , file, row.names = TRUE , quote = TRUE,  fileEncoding='UTF-8' )




#final checks: data complete, no NA's left on relevant fields?
finalCheck <- 0
base::stopifnot(finalCheck != 1) 
