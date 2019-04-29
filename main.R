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
buildBpostValidateJson(rawData[1000:1100,])

postSingleBpostValidation(id= rawData[15,id]
                          ,StreetName = rawData[15, address]
                          ,StreetNumber = rawData[15,street_nb]
                          ,BoxNumber = rawData[15,address2]
                          ,PostalCode = rawData[15,zip]
                          ,MunicipalityName = rawData[15,locality])
