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

#validate addresses
#using Bpost: works in postman, fails here curl::curl_fetch_memory(url, handle = handle) :Failure when receiving data from the peer
jsonBody <- buildBpostValidateJsonBody(cleanData[1001:1010,])
postMultipleBpostValidationRest(jsonBody)

mapIdtoAddress <- buildGoogleApiGeocodeJsonUrlEncode(cleanData)
uniqueAddress <- as.data.table(unique(mapIdtoAddress$Address))[, .(Address = V1)]
geocodedAddress <- uniqueAddress[1:10, .(Address, lapply(Address, getSingleGoogleGeocodingRest))]
geocodedAddress2 <- uniqueAddress[11:1000, .(Address, lapply(Address, getSingleGoogleGeocodingRest))]
