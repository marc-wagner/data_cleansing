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
      
      
      
#cleanup donateurs: split first name and last name
check_data <- NULL
donateurs[,is_complete:=1]
cleandonateurs<-cleanRawData(donateurs, check_data)
      
      
      
#      
#run recordlinkage on best info available between donateurs and codemandeurs