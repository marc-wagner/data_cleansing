#Profiling Donateurs
source('dependencies.R')
source('io_utilities.R')
source('geocode.R')
source('RecordLinkage.R')


print('start loading donateurs data files')
donateurs <- readRawDataFolder(path = paste( paste ( '..',  'Donateurs', sep='/') , parameters$path_input_data, sep='/'), type = 'donateur')

#start copy of main.R to load deduplicated codemandeurs
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
      rm( check_data, cleanData)
      dedupData <- deduplicate(geocodedData)
#end copy of main.R to load deduplicated codemandeurs
      
#remove duplicates from dedupData
coplaintiffs_clean <- dedupData[auto_reason!='D' | is.na(auto_reason),]      
coplaintiffs_clean [,name_grouped:= paste(lastname, firstname, sep = ' ')]       
      
#cleanup donateurs: split first name and last name
check_data <- NULL
donateurs[,is_complete:=1]
# remove Mme, Mr, M Mevr etc if there are more than 2 spaces
# remove BVBA etc... companies
# build equivalent of address_validated

cleandonateurs<-cleanRawData(donateurs, check_data)
cleandonateurs[ , address_validated := paste(ifelse(is.na(address), '', address )
                                             ,ifelse(is.na(street_nb), '', street_nb )
                                             ,ifelse(is.na(address2), '', address2 )
                                             ,',' 
                                             ,ifelse(is.na(zip), '', zip ) 
                                             ,ifelse(is.na(locality), '', locality ) 
                                             ,ifelse(is.na(country), '', country ) )]  

#      
#run recordlinkage on best info available between donateurs and codemandeurs





#end