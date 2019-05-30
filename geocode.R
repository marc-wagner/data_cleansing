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
  jsonInput <- copy(dt[,.(id, address,street_nb, address2,zip, locality, country)])
  jsonInput[is.na(jsonInput)] <- ''  #replace NAs by blanks for building address
  jsonOutput <- jsonInput[, .(  Address    = paste(c(address,street_nb, address2, ',' ,zip, locality, country), collapse = '+') 
                               ,components = paste(c( paste(c('postal_code'  , zip     ), collapse = ':')
                                        ,paste(c('locality'     , locality), collapse = ':')
                                        ,paste(c('country'      , country ), collapse = ':')
                                        )
                                      , collapse = '|')
                               ,postal_code = zip
                               )
                         ,by = id] 
  jsonOutput[, Address := str_replace_all(Address, fixed(' '), '+')]
  jsonOutput[, components := str_replace_all(components, fixed(' '), '+')]
  jsonOutput[, postal_code := str_replace_all(postal_code, fixed(' '), '+')]
}


getSingleGoogleGeocodingRest <- function(address, components = NULL, postal_code = NULL){
  #if zip code in Brussels area, need FR. else set according to region.
  #https://en.wikipedia.org/wiki/List_of_postal_codes_in_Belgium
  
  if(postal_code < 1500)    
     { language_tag <- '&language=fr'} 
  else {
       if(postal_code < 4000)    
         { language_tag <- '&language=nl'} 
       else {
         if(postal_code < 8000)    
         { language_tag <- '&language=fr'} 
         else  
         { language_tag <- '&language=nl'}
       }
  }

  #build URL: add address, then optionally language, then components then key
  urlFromParts <- paste( "https://maps.googleapis.com/maps/api/geocode/json?address", paste(address     , language_tag             , sep='' ),sep='=')  
  urlFromParts <- paste(urlFromParts                                                , paste('components', components               , sep='='),sep='&')
  urlFromParts <- paste(urlFromParts                                                , paste('key'       , parameters$google_api_key, sep='='),sep='&')
  
  #print(paste('debug: ',urlFromParts, sep= ''))
  
  result <- GET(urlFromParts)
  stop_for_status(result)
  dfBuffer <- content(result,"parsed")

  if(length(dfBuffer$results) == 0) {
    #retry without components
    #to handle cases where country= BE is wrong because it s an address abroad
    urlFromParts <- paste( "https://maps.googleapis.com/maps/api/geocode/json?address", paste(address     , language_tag             , sep='' ),sep='=')  
    urlFromParts <- paste(urlFromParts                                                , paste('key'       , parameters$google_api_key, sep='='),sep='&')
    
    #print(paste('debug: ',urlFromParts, sep= ''))
    
        result <- GET(urlFromParts)
    stop_for_status(result)
    dfBuffer <- content(result,"parsed")
  }
  
  #still no luck
  if(length(dfBuffer$results) == 0) {
      paste( NA
             ,NA 
             ,NA
             ,NA
             ,dfBuffer$status
             ,sep = '|')
    }
  else {
    paste(dfBuffer$results[[1]]$formatted_address
          ,dfBuffer$results[[1]]$geometry$location$lat 
          ,dfBuffer$results[[1]]$geometry$location$lng
          ,dfBuffer$results[[1]]$geometry$location_type
          ,dfBuffer$status
          ,sep = '|')
  }
}

# input: data.table of id s and addresses (mapIdtoAddress) 
# output:same structure id s and cleansed addresses     (mapIdtoAddress)
geocodeAddress <- function(my_mapIdtoAddress, get_new = TRUE){
  
  #internal persistence
  #connect to DB table or file to retrieve addresses
  existingGeocodedAddress <- readCsvFromDirectory('geocodedAddress', parameters$path_geocoded_address)
  existingGeocodedAddress[, X:=NULL]

  #check uniqueAddress against data store of validated addresses  
  #only submit missing addresses for geocoding 
  uniqueAddress <- unique(my_mapIdtoAddress[, .(Address, components, postal_code)])
  newUniqueAddress <- base::merge(uniqueAddress, existingGeocodedAddress ,all.x = TRUE )
  newUniqueAddress <- newUniqueAddress[is.na(address_validated), .(Address, components, postal_code)]
  
  print('addresses to geolocate using webservice: ')
  print(nrow(newUniqueAddress))
  
  #initialize 
  geocodedAddress <- existingGeocodedAddress
  if(get_new == TRUE) {
      #submit addresses that were not matched 
      
      for(i in 1:floor(nrow(newUniqueAddress)/ parameters$fetch_size)){
        
        geocodedAddressBuffer = NULL
        
        for(j in 1:parameters$fetch_size){
          geocodedAddressBuffer <- rbind (geocodedAddressBuffer, newUniqueAddress[(((i-1)*parameters$fetch_size)+j)
                                                    , .(Address, lapply(Address
                                                                                    , getSingleGoogleGeocodingRest
                                                                                    , components = components
                                                                                    , postal_code = postal_code))]) 
        }
        #  print('debug API call finished ')
        geocodedAddressBuffer[, c("address_validated", "latitude", "longitude", "location_type", "status") := tstrsplit(V2, "|", fixed=TRUE)]
        geocodedAddressBuffer[, V2:= NULL]
          
        write.csv2(geocodedAddressBuffer
                   , file = paste(paste('..',parameters$batch_name, sep = '/'), parameters$path_geocoded_address, sep = '/')  %>%
                     paste(i, sep = "/")  %>%
                     paste("geocodedAddress.csv" ,sep='_') 
                   
                   , row.names = TRUE
                   , quote = FALSE)
        #  print('debug write csv finished ')
        
        print('debug iteration ')
        print(i)  
        print('valid records ')
        print(nrow(geocodedAddressBuffer[status=='OK' & location_type != 'APPROXIMATE']))
        
        geocodedAddress <- rbindlist(list(geocodedAddress, geocodedAddressBuffer[status=='OK' & location_type != 'APPROXIMATE',]), idcol = FALSE, fill =FALSE , use.names = TRUE)
        
        #internal persistence
        #store incrementally into fst as we loop, since process crashes frequently
        writeCsvIntoDirectory(unique(geocodedAddress[,.(Address,address_validated, latitude, longitude, location_type, status)]), 'geocodedAddress', parameters$path_geocoded_address)
        
      }
  }
      
 
  #merge with id
  validated_data <- base::merge(my_mapIdtoAddress, geocodedAddress[,.(Address, address_validated)] ) 
  validated_data[, Address:=NULL]
  writeCsvIntoDirectory(validated_data, 'mapIdtoAddressValidated', parameters$path_geocoded_address)
  
  #return mapIdToAddress id, validatedAddress
  validated_data
  }  


#ad hoc task start
#run from within "geocodeAddress" context
#filter geocoded addresses that are not in Brussels

#initial statistics
#            valid_zip
# geocoded FALSE  TRUE
# FALSE      956 14507
# TRUE       106 38782

#uniqueAddress[as.integer(postal_code) < parameters$zip_codes_brussels_region_lowerbound  
#   | as.integer(postal_code) > parameters$zip_codes_brussels_region_upperbound,]
#43462 unique address records not in brussels at the moment of cleanup


# geocodeAddressNoBrussels <- base::merge(existingGeocodedAddress, uniqueAddress[as.integer(postal_code) < parameters$zip_codes_brussels_region_lowerbound  
#                                                                                  |as.integer(postal_code) > parameters$zip_codes_brussels_region_upperbound
#                                                                                  ,.(Address, postal_code)]
#                                         , by=c('Address') 
#                                         ,all.x = TRUE )
# geocodeAddressNoBrussels <- geocodeAddressNoBrussels[!is.na(postal_code),]
# geocodeAddressNoBrussels[,postal_code:=NULL]  

#            valid_zip
# geocoded FALSE    TRUE
# FALSE        2      32
# TRUE      7370   31599

#ad hoc task end  
