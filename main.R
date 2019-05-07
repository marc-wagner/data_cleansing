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
check_data <- read.csv(  file = paste('..' , parameters$batch_name, sep = '/')  %>%
                                paste('Data', sep = '/')  %>%
                                paste(parameters$batch_name, sep = '/') %>%
                                paste('_noQuotes.csv', sep = '')
                        ,header = TRUE
                        , sep = ";"
                        , quote = ""
                        , encoding="UTF-8"
                        , stringsAsFactors=FALSE)  %>%
              as.data.table()
cleanData <- cleanRawData(rawData, check_data)

#store collection
write.csv(cleanData
          , file = paste(parameters$batch_root, parameters$path_dataQualityCheck, sep = '/')  %>%
                   paste("concatenated_raw_data.csv", sep = "/")  
          , append = FALSE
          , row.names = TRUE
          , quote = FALSE)





#classify errors

#identify duplicates

#train duplicates finding



#validate addresses and store in a separate permanent store

#using Bpost: works in postman, fails here curl::curl_fetch_memory(url, handle = handle) :Failure when receiving data from the peer
# jsonBody <- buildBpostValidateJsonBody(cleanData[1001:1001,])
# postMultipleBpostValidationRest(jsonBody)

mapIdtoAddress <- buildGoogleApiGeocodeJsonUrlEncode(cleanData)
write.csv(mapIdtoAddress
          , file = paste(paste('..',parameters$batch_name, sep = '/'), parameters$path_geocoded_address, sep = '/')  %>%
            paste("mapIdtoAddress.csv", sep = "/")  
          , row.names = TRUE
          , quote = FALSE)


uniqueAddress <- as.data.table(unique(mapIdtoAddress$Address))[, .(Address = V1)]
#TO DO: check uniqueAddress against data store of validated addresses

#initialize 
geocodedAddress <- geocodedAddress[0,]

#submit addresses that were not matched 
browser()

for(i in 1:floor(nrow(uniqueAddress)/ parameters$fetch_size)){
  geocodedAddressBuffer <- uniqueAddress[(((i-1)*parameters$fetch_size)+1):(i*parameters$fetch_size), .(Address, lapply(Address, getSingleGoogleGeocodingRest))]
#  print('debug API call finished ')
  geocodedAddressBuffer[, c("address_validated", "latitude", "longitude", "location_type", "status") := tstrsplit(V2, "|", fixed=TRUE)]
  geocodedAddressBuffer[, V2:= NULL]
  print('debug iteration ')
  print(i)  
  print('nr records ')
  print(dim(geocodedAddressBuffer))  
  
  write.csv(geocodedAddress
            , file = paste(paste('..',parameters$batch_name, sep = '/'), parameters$path_geocoded_address, sep = '/')  %>%
              paste(i, sep = "/")  %>%
              paste("geocodedAddress.csv" ,sep='_') 
              
            , row.names = TRUE
            , quote = FALSE)
#  print('debug write csv finished ')
  geocodedAddress <- rbindlist(list(geocodedAddress, geocodedAddressBuffer), idcol = FALSE, fill =FALSE , use.names = TRUE)
  }
write.csv(geocodedAddress
          , file = paste(paste('..',parameters$batch_name, sep = '/'), parameters$path_geocoded_address, sep = '/')  %>%
            paste("geocodedAddress.csv", sep = "/")  
          , row.names = TRUE
          , quote = FALSE)

#merge with id
validated_data <- merge(geocodedAddress, mapIdtoAddress , by='Address', all.y=all) 
write.csv(validated_data
          , file = paste(paste('..',parameters$batch_name, sep = '/'), parameters$path_geocoded_address, sep = '/')  %>%
            paste("geocodedAddress.csv", sep = "/")  
          , row.names = TRUE
          , quote = FALSE)



geocodedAddress2 <- uniqueAddress[11:1000, .(Address, lapply(Address, getSingleGoogleGeocodingRest))]


#store Address mapping and coordinates


View(matrix(unlist(geocodedAddress), nrow=length(geocodedAddress), byrow=T))
