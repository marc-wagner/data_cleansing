#geocoding

# input: data.table of id s and addresses (mapIdtoAddress) 
# output:same structure id s and cleansed addresses     (mapIdtoAddress)
geocodeAddress <- function(mapIdtoAddress){

  #internal persistence
  #connect to DB table or file
  #to retrieve addresses
  existingGeocodedAddress <- readFstFromDirectory('geocodedAddress', parameters$path_geocoded_address)

  #check uniqueAddress against data store of validated addresses  
  #only submit missing addresses for geocoding 
  uniqueAddress <- as.data.table(unique(mapIdtoAddress$Address))[, .(Address = V1)]
  newUniqueAddress <- merge(uniqueAddress, existingGeocodedAddress[1:20000,], all.x = TRUE )

  
  #initialize 
  geocodedAddress <- geocodedAddress[0,]
  
  #submit addresses that were not matched 
  
  for(i in 79:floor(nrow(newUniqueAddress)/ parameters$fetch_size)){
    geocodedAddressBuffer <- newUniqueAddress[(((i-1)*parameters$fetch_size)+1):(i*parameters$fetch_size), .(Address, lapply(Address, getSingleGoogleGeocodingRest))]
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
  
  #internal persistence
  #combine new with existing
  writeFstIntoDirectory(appendedGeocodedAddress, 'geocodedAddress', parameters$path_geocoded_address)
  
  
   #return mapIdToAddress id, validatedAddress
  }  
