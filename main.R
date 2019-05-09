#main file for data cleansing of subscriber list
#built under GNU General Public License

source('dependencies.R')
source('io_utilities.R')
source('web_requests.R')

#load source files
print('start')
rawData <- readRawDataFolder(path = paste( paste ( '..',  parameters$batch_name, sep='/') , parameters$path_input_data, sep='/'))
#data contains lots of NA lines, corresponding to empty XLS lines.


#cleanse data
#had to preprocess data: remove quotes " and add a semicolon (extra field) at the end of column names
check_data <- readCsvFromDirectory(paste(parameters$batch_name, 'noQuotes', sep = '_'), 'Data')

cleanData <- cleanRawData(rawData, check_data)

#store collection
writeCsvIntoDirectory(cleanData, 'concatenated_raw_data', parameters$path_dataQualityCheck)

#classify errors

#identify duplicates

#train duplicates finding



#validate addresses and store in a separate permanent store

#using Bpost: works in postman, fails here curl::curl_fetch_memory(url, handle = handle) :Failure when receiving data from the peer
# jsonBody <- buildBpostValidateJsonBody(cleanData[1001:1001,])
# postMultipleBpostValidationRest(jsonBody)

mapIdtoAddress <- buildGoogleApiGeocodeJsonUrlEncode(cleanData)
writeCsvIntoDirectory(mapIdtoAddress, 'mapIdtoAddress', parameters$path_geocoded_address)

uniqueAddress <- as.data.table(unique(mapIdtoAddress$Address))[, .(Address = V1)]
#TO DO: check uniqueAddress against data store of validated addresses

#initialize 
geocodedAddress <- geocodedAddress[0,]

#submit addresses that were not matched 

for(i in 79:floor(nrow(uniqueAddress)/ parameters$fetch_size)){
  geocodedAddressBuffer <- uniqueAddress[(((i-1)*parameters$fetch_size)+1):(i*parameters$fetch_size), .(Address, lapply(Address, getSingleGoogleGeocodingRest))]
#  print('debug API call finished ')
  geocodedAddressBuffer[, c("address_validated", "latitude", "longitude", "location_type", "status") := tstrsplit(V2, "|", fixed=TRUE)]
  geocodedAddressBuffer[, V2:= NULL]
  print('debug iteration ')
  print(i)  
  print('nr records ')
  print(dim(geocodedAddressBuffer))  
  
  write.csv2(geocodedAddressBuffer
            , file = paste(paste('..',parameters$batch_name, sep = '/'), parameters$path_geocoded_address, sep = '/')  %>%
              paste(i, sep = "/")  %>%
              paste("geocodedAddress.csv" ,sep='_') 
              
            , row.names = TRUE
            , quote = FALSE)
#  print('debug write csv finished ')
  geocodedAddress <- rbindlist(list(geocodedAddress, geocodedAddressBuffer), idcol = FALSE, fill =FALSE , use.names = TRUE)
  }
write.csv2(unique(geocodedAddress)
          , file = paste(paste('..',parameters$batch_name, sep = '/'), parameters$path_geocoded_address, sep = '/')  %>%
            paste("geocodedAddress.csv", sep = "/")  
          , row.names = TRUE
          , quote = FALSE)

#merge with id
validated_data <- merge(geocodedAddress, mapIdtoAddress) 
write.csv(validated_data
          , file = paste(paste('..',parameters$batch_name, sep = '/'), parameters$path_geocoded_address, sep = '/')  %>%
            paste("geocodedAddress.csv", sep = "/")  
          , row.names = TRUE
          , quote = FALSE)



geocodedAddress2 <- uniqueAddress[11:1000, .(Address, lapply(Address, getSingleGoogleGeocodingRest))]


#store Address mapping and coordinates


View(matrix(unlist(geocodedAddress), nrow=length(geocodedAddress), byrow=T))
