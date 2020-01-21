#Profiling Donateurs
source('dependencies.R')
source('io_utilities.R')
source('geocode.R')
source('RecordLinkage.R')


prepareLinkage <- function(dt){
   dt_out <- copy(dt[, .(id, first_last_name, address_validated, duplicate_id, email, phone, reason)])
   dt_out[, duplicate_id := as.integer(duplicate_id)]
   dt_out[, reason:= NULL]  
   dt_out[is.na(duplicate_id), duplicate_id := id]
   dt_out[, first_last_name := as.factor(first_last_name)]
   dt_out[, email := as.factor(email)]
   dt_out[, phone := as.factor(phone)]
   
   dt_out <- as.data.frame(dt_out)
   dt_out
}

linkEpiWeights <-function(dt){
   
   browser()
   #dereference in a dumb way to avoid any trip up during assignment
   first_last_name_idx <- which(colnames(dt)=="first_last_name")
   address_validated_idx <- which(colnames(dt)=="address_validated")
   email_idx <- which(colnames(dt)=="email")
   
   id_idx <- which(colnames(dt)=="id")
   duplicate_id_idx <- which(colnames(dt)=="duplicate_id")
   
   pairs <- compare.dedup(dt
                          ,blockfld = list(first_last_name_idx) 
                          ,phonetic = c(first_last_name_idx, address_validated_idx, email_idx)  #firstname, lastname, address, locality, email
                          ,exclude = c(id_idx,duplicate_id_idx)  # id , duplicate_id
   )
   
   pairs <- epiWeights(pairs)
   
   calibrateDuplicates <- splitData(dataset=pairs, prop=0.5, keep.mprop=FALSE)
   
   threshold <- optimalThreshold(calibrateDuplicates$train)
   
   summary(epiClassify(calibrateDuplicates$valid,threshold))
   #paste results here
   
      # classify with single threshold
   result <- epiClassify(pairs, threshold.upper = threshold, threshold.lower = threshold)
   summary(result)
   #paste results here
   
   pairs
}


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

# build equivalent of address_validated
cleandonateurs<-cleanRawData(donateurs, check_data)
cleandonateurs[ , address_validated := paste(ifelse(is.na(address), '', address )
                                             ,ifelse(is.na(street_nb), '', street_nb )
                                             ,ifelse(is.na(address2), '', address2 )
                                             ,',' 
                                             ,ifelse(is.na(zip), '', zip ) 
                                             ,ifelse(is.na(locality), '', locality ) 
                                             ,ifelse(is.na(country), '', country ) )]  

# remove Mme, Mr, M Mevr etc if there are more than 2 spaces
# remove BVBA etc... companies
#run address validation

#group firstname and lastname in both datasets
cleandonateurs[ , first_last_name := paste(ifelse(is.na(firstname),'',firstname), ifelse(is.na(lastname),'',lastname), sep = ' ')]
coplaintiffs_clean[ , first_last_name := paste(ifelse(is.na(firstname),'',firstname), ifelse(is.na(lastname),'',lastname), sep = ' ')]

#group both datasets into 1
cleandonateurs         <- cleandonateurs[,.(id, first_last_name, address_validated, duplicate_id,  email, phone, reason)]
coplaintiffs_clean <- coplaintiffs_clean[,.(id, first_last_name, address_validated, duplicate_id,  email, phone, reason)]
donateurs_coplaintiffs_union <- rbindlist(list(cleandonateurs, coplaintiffs_clean), use.names = TRUE, idcol = FALSE, fill = TRUE)

rm(donateurs, fixed_data, geocodedData, dedupData)
#run recordlinkage on best info available between donateurs and codemandeurs

dedupData <- prepareLinkage(donateurs_coplaintiffs_union)
#not applicable identity.dedupData <- as.vector(dedupData$duplicate_id)

# create record pairs and calculate epilink weights
rpairs <- linkEpiWeights(dedupData)

#recordlinkage instead of dedup
link_donateurs <- prepareLinkage(cleandonateurs)
link_coplaintiffs <- prepareLinkage(coplaintiffs_clean)
#next step: modify linkEpiWeights to use  compare.linkage() instead of compare.dedup()

#end