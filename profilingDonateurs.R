#Profiling Donateurs
source('dependencies.R')
source('io_utilities.R')
source('geocode.R')
source('RecordLinkage.R')


prepareLinkage <- function(dt){
   dt_out <- copy(dt[, .(id, first_last_name, street_and_nr, zip, locality, duplicate_id, email, phone, reason, dob, language)])
   dt_out[, duplicate_id := as.integer(duplicate_id)]
   dt_out[, reason:= NULL]  
   dt_out[is.na(duplicate_id), duplicate_id := id]
   dt_out[, first_last_name := as.factor(first_last_name)]
   dt_out[, email := as.factor(email)]
   dt_out[, phone := as.factor(phone)]
   dt_out[, zip := as.factor(zip)]
   dt_out[, locality := as.factor(locality)]
   dt_out <- as.data.frame(dt_out)
   dt_out
}

linkEpiWeights <-function(dt1, dt2, blockfld){

   #dereference in a dumb way to avoid any trip up during assignment
   first_last_name_idx <- which(colnames(dt1)=="first_last_name")
   street_and_nr_idx <- which(colnames(dt1)=="street_and_nr")
   email_idx <- which(colnames(dt1)=="email")
   zip_idx <- which(colnames(dt1)=="zip")
   locality_idx <- which(colnames(dt1)=="locality")   
   id_idx <- which(colnames(dt1)=="id")
   duplicate_id_idx <- which(colnames(dt1)=="duplicate_id")
   language_idx <- which(colnames(dt1)=="language")
   dob_idx <- which(colnames(dt1)=="dob")
   
   pairs <- compare.linkage(dt1
                          ,dt2
                          ,blockfld = list( ifelse(blockfld =='email', email_idx, first_last_name_idx))
                          ,phonetic = c(first_last_name_idx, street_and_nr_idx, locality_idx, email_idx)  #firstname, lastname, address, locality, email
                          ,exclude = c(id_idx,duplicate_id_idx, language_idx, dob_idx)  # id , duplicate_id, language, dob
   )
   
   pairs <- epiWeights(pairs)
   
   # classify with single threshold
   result <- epiClassify(pairs, threshold.lower =  0.4, threshold.upper = 0.7)
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

# concatenate street and nr
cleandonateurs<-cleanRawData(donateurs, check_data)
cleandonateurs[     , street_and_nr := paste(ifelse(is.na(address), '', tolower(address) )
                                             ,ifelse(is.na(street_nb), '', tolower(street_nb) )
                                             ,ifelse(is.na(address2), '', tolower(address2) )
                                      )]  
coplaintiffs_clean[ , street_and_nr := paste(ifelse(is.na(address), '', tolower(address) )
                                      ,ifelse(is.na(street_nb), '', tolower(street_nb) )
                                      ,ifelse(is.na(address2), '', tolower(address2) )
)]  


# remove Mme, Mr, M Mevr etc if there are more than 2 spaces
# remove BVBA etc... companies
#run address validation

#group firstname and lastname in both datasets
cleandonateurs[     , first_last_name := paste(ifelse(is.na(firstname),'',tolower(firstname)), ifelse(is.na(lastname),'',tolower(lastname)), sep = ' ')]
coplaintiffs_clean[ , first_last_name := paste(ifelse(is.na(firstname),'',tolower(firstname)), ifelse(is.na(lastname),'',tolower(lastname)), sep = ' ')]

cleandonateurs     <- cleandonateurs[    ,.(id, first_last_name=tolower(first_last_name), street_and_nr=tolower(street_and_nr), zip, locality = tolower(locality), duplicate_id, email=tolower(email), phone, reason, dob, language)]
cleancoplaintiffs  <- coplaintiffs_clean[,.(id, first_last_name=tolower(first_last_name), street_and_nr=tolower(street_and_nr), zip, locality = tolower(locality), duplicate_id, email=tolower(email), phone, reason, dob, language)]

#run recordlinkage on best info available between donateurs and codemandeurs

#free up memory
#rm(donateurs, fixed_data, geocodedData, dedupData)

#try 1: use dedup functionality, group both datasets into 1
#donateurs_coplaintiffs_union <- rbindlist(list(cleandonateurs, cleancoplaintiffs), use.names = TRUE, idcol = FALSE, fill = TRUE)
#dedupData <- prepareLinkage(donateurs_coplaintiffs_union)
#not applicable identity.dedupData <- as.vector(dedupData$duplicate_id)

#try 2: use recordlinkage instead of dedup, submit 2 separate datasets
link_donateurs <- prepareLinkage(cleandonateurs)
link_coplaintiffs <- prepareLinkage(cleancoplaintiffs)
#next step: modify linkEpiWeights to use  compare.linkage() instead of compare.dedup()

# create record pairs and calculate epilink weights
rpairs_email <- linkEpiWeights(link_donateurs, link_coplaintiffs, 'email')

#check pairs
#View(rpairs$pairs)
#spot check 3801	15360
# and       3321	15360
link_donateurs[3801,]
link_donateurs[3321,]
link_coplaintiffs[15360,]
#ok
#spot check 12006	49338
link_donateurs[12006,]
link_coplaintiffs[49338,]
#ok

View(RecordLinkage::getPairs(rpairs_email, single.rows=TRUE))

#enrich donateurs with info from coplaintiff using linked records
enriched_donateurs <- donateurs[,.( id.1 = id, page, donation_amount, donation_date)]
enriched_donateurs <-  base::merge(  enriched_donateurs
                                   , data.table(RecordLinkage::getPairs(rpairs_email, single.rows=TRUE))[,.(id.1
                                                                                                , dob = dob.2
                                                                                                , language= language.2
                                                                                                , zip = zip.2)]
                                   ,   all.x = TRUE )
#end
