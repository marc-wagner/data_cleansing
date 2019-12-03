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

#1) Extract latest records and manual fixes
fixed_data <- fullLoadCoPlaintiffs()

#check_data <- readCsvFromDirectory(paste(parameters$batch_name, 'noQuotes', sep = '_'), 'Data')
check_data <- NULL

cleanData <- cleanRawData(fixed_data, check_data)

#store collection
writeCsvIntoDirectory(cleanData, 'concatenated_raw_data', parameters$path_dataQualityCheck)


#classify errors
#TO DO
geocodedData <- cleanupAddresses(cleanData)

#free up memory before deduplication
rm(raw_data, mapIdtoAddress, mapIdtoAddressValidated, check_data, cleanData, fixed_data, missing_geoloc)
dedupData <- deduplicate(geocodedData)

storeDedupedCoPlaintiffs(dedupData)



#final checks: data complete, no NA's left on relevant fields?
finalCheck <- 0
base::stopifnot(finalCheck != 1) 
