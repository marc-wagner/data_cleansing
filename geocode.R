#geocoding


getjsonHeader <-function() {  
  '{"ValidateAddressesRequest": { "AddressToValidateList": { "AddressToValidate": ['
  # '{ValidateAddressesRequest: { AddressToValidateList: { AddressToValidate: ['
}

getjsonFooter <-function() {  
  # add 2 extra } at beginning to close the last item in enumeration due to replacement  of 
  # AddressBlockLines":"' by '"AddressBlockLines":{"UnstructuredAddressLine": {"*body
  '}}]   }, "CallerIdentification": {  "CallerName": "klimaatzaak" } } }'
}



buildGoogleApiGeocodeJsonUrlEncode <- function(dt){
  jsonInput <- copy(dt)
  jsonInput[is.na(jsonInput)] <- ''  #replace NAs by blanks for building address
  jsonInput <- jsonInput[, paste(c(address,street_nb, address2, ',' ,zip, locality, country), collapse = '+') ,by = id] 
  jsonInput <- jsonInput[, .(id, Address = str_replace_all(V1, fixed(' '), '+'))]
}


getSingleGoogleGeocodingRest <- function(address, language = NULL){
  if(!is.null(language)) { language_tag <- paste('&language=',language, sep = '')}
  urlFromParts <- str_replace("https://maps.googleapis.com/maps/api/geocode/json?address=XXXXXXXXX&key=", fixed('XXXXXXXXX'),paste(address, language_tag, sep=''))
  urlFromParts <- paste(urlFromParts, parameters$google_api_key,sep='')
  result <- GET(urlFromParts)
  stop_for_status(result)
  dfBuffer <- content(result,"parsed")
  if(length(dfBuffer$results) > 0){
    paste(dfBuffer$results[[1]]$formatted_address
          ,dfBuffer$results[[1]]$geometry$location$lat 
          ,dfBuffer$results[[1]]$geometry$location$lng
          ,dfBuffer$results[[1]]$geometry$location_type
          ,dfBuffer$status
          ,sep = '|')
  }
  else {
    paste( NA
           ,NA 
           ,NA
           ,NA
           ,dfBuffer$status
           ,sep = '|')
  }
}

# input: data.table of id s and addresses (mapIdtoAddress) 
# output:same structure id s and cleansed addresses     (mapIdtoAddress)
geocodeAddress <- function(mapIdtoAddress, get_new = TRUE){
  
  #internal persistence
  #connect to DB table or file to retrieve addresses
  existingGeocodedAddress <- readCsvFromDirectory('geocodedAddress', parameters$path_geocoded_address)

  #check uniqueAddress against data store of validated addresses  
  #only submit missing addresses for geocoding 
  uniqueAddress <- as.data.table(unique(mapIdtoAddress$Address))[, .(Address = V1)]
  newUniqueAddress <- base::merge(uniqueAddress, existingGeocodedAddress ,all.x = TRUE )
  newUniqueAddress <- as.data.table(newUniqueAddress[is.na(address_validated), Address])[,Address:=V1]
  newUniqueAddress[, V1:=NULL]
  
  #initialize 
  geocodedAddress <- existingGeocodedAddress
  
  if(get_new == TRUE) {
      #submit addresses that were not matched 
      
      for(i in 1:floor(nrow(newUniqueAddress)/ parameters$fetch_size)){
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
        
        #internal persistence
        #store incrementally into fst as we loop, since process crashes frequently
        writeCsvIntoDirectory(unique(geocodedAddress), 'geocodedAddress', parameters$path_geocoded_address)
        
      }
  }
      
 
  #merge with id
  validated_data <- base::merge(mapIdtoAddress, geocodedAddress[,.(Address, address_validated)] ) 
  validated_data[, Address:=NULL]
  writeCsvIntoDirectory(validated_data, 'mapIdtoAddressValidated', parameters$path_geocoded_address)
  
  #return mapIdToAddress id, validatedAddress
  validated_data
  }  
