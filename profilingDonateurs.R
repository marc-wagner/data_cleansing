#Profiling Donateurs

#4 files: GocardLess, Triodos, PayPal and Mollie 
#Mollie and Paypal are far from optimal: can get better data through the API 

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
   
   if(blockfld == 'none')
      {
      
      pairs <- compare.linkage(dt1
                               ,dt2
                              # ,blockfld is left empty
                               ,phonetic = c(first_last_name_idx, street_and_nr_idx, locality_idx, email_idx)  #firstname, lastname, address, locality, email
                               ,exclude = c(id_idx,duplicate_id_idx, language_idx, dob_idx)  # id , duplicate_id, language, dob
                              )        
   }
   else
   {
      pairs <- compare.linkage(dt1
                          ,dt2
                          ,blockfld = list(  ifelse(blockfld =='email', email_idx, first_last_name_idx)) #either e-mail or first_last_name
                          ,phonetic = c(first_last_name_idx, street_and_nr_idx, locality_idx, email_idx)  #firstname, lastname, address, locality, email
                          ,exclude = c(id_idx,duplicate_id_idx, language_idx, dob_idx)  # id , duplicate_id, language, dob
                        )
   }
   pairs <- epiWeights(pairs)
   
   # classify with single threshold
   result <- epiClassify(pairs, threshold.lower =  0.2, threshold.upper = 0.5)
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

#use recordlinkage instead of dedup, submit 2 separate datasets
link_donateurs <- prepareLinkage(cleandonateurs)
link_coplaintiffs <- prepareLinkage(cleancoplaintiffs)

# create record pairs and calculate epilink weights
rpairs_email <- linkEpiWeights(link_donateurs, link_coplaintiffs, 'email')

#check pairs
#View(rpairs$pairs)
#spot check 3801	15360
# and       3321	15360
#link_donateurs[3801,]
#link_donateurs[3321,]
#link_coplaintiffs[15360,]
#ok
#spot check 12006	49338
#link_donateurs[12006,]
#link_coplaintiffs[49338,]
#ok

#enrich donateurs with info from coplaintiff using linked records
enriched_donateurs <- donateurs[,.( id.1 = id, page, zip1 = zip, donation_amount, donation_date)]
enriched_donateurs <-  base::merge(  enriched_donateurs
                                   , data.table(RecordLinkage::getPairs(rpairs_email, single.rows=TRUE))[,.(id.1
                                                                                                , dob_email = dob.2
                                                                                                , language_email= language.2
                                                                                                , zip_email = zip.2
                                                                                                , match_email = 1)]
                                   ,   all.x = TRUE )

table(enriched_donateurs[,.(page, match_email)],useNA = 'ifany')
# match_field
# page      e-mail <NA>
#    GCL       1896  954
# mollie     315  178
# paypal    4346 4626
# triodos      0   35

# create record pairs and calculate epilink weights with name as blocking factor
rpairs_name <- linkEpiWeights(link_donateurs, link_coplaintiffs, 'name')
#then merge this in too and blend to complete the data
enriched_donateurs <-  base::merge(  enriched_donateurs
                                     , data.table(RecordLinkage::getPairs(rpairs_name, single.rows=TRUE))[,.(id.1
                                                                                                              , dob_name = dob.2
                                                                                                              , language_name = language.2
                                                                                                              , zip_name = zip.2
                                                                                                              , match_name = 1)]
                                     ,   all.x = TRUE )
table(enriched_donateurs[,.(page,  match_name, match_email)],useNA = 'ifany')
# match_email = 1
# match_name 1     <NA>
#    GCL     1836  145
# mollie     0     315
# paypal     0    4346
# triodos    0       0
# match_email = NA
# match_name 1     <NA>
#    GCL      555  434  >> 555 GCL donateurs matched on name not on email
# mollie     0     178
# paypal     0    4626
# triodos    0      35

#match remaining using no blocking criterion
#as this is a very memory intensive task ( 2^n ), we need to narrow down link_donateurs to unmatched id's only
link_donateurs_noblock <- data.table(base::merge(link_donateurs
                                      , unique(data.table(RecordLinkage::getPairs(rpairs_email, single.rows=TRUE))[,.(id=id.1 , found_email = 1)])
                                      , all.x = TRUE ))
link_donateurs_noblock <- data.table(base::merge(link_donateurs_noblock
                                      , unique(data.table(RecordLinkage::getPairs(rpairs_name, single.rows=TRUE))[,.(id=id.1 , found_name = 1)])
                                      , all.x = TRUE ))
link_donateurs_noblock <- link_donateurs_noblock[is.na(link_donateurs_noblock$found_email) & is.na(link_donateurs_noblock$found_name),]
link_donateurs_noblock[, found_email:=NULL]
link_donateurs_noblock[, found_name :=NULL]

#free up memory
rm(coplaintiffs_clean, dedupData, fixed_data, geocodedData, cleancoplaintiffs)


#TO DO : increase matching by testing various strategies:
#                 increase weight of name and email
#                 break up name in namepart 1 and namepart 2, then store swapped nameparts in new columns for matching
#                 find a better phonetic algorithm
#                 adjust threshold for matching according to epiweights in current set

#loop over chunks of umatched donateurs and check against all coplaintiffs,
#then build a collection of best matches
matched_noblock_donateurs = NULL
for (chunk in seq(1:(nrow(link_donateurs_noblock)/400)))
{
   chunk_results   <- linkEpiWeights(link_donateurs_noblock[((chunk - 1) * 400 +1): ifelse((400 * chunk)>nrow(link_donateurs_noblock), nrow(link_donateurs_noblock), (400 * chunk)),]
                                     , link_coplaintiffs
                                     , 'none')
   matched_noblock_donateurs <- rbindlist(list(matched_noblock_donateurs, chunk_results), fill = FALSE, idcol = NULL)
}

#rpairs_noblock   <- linkEpiWeights(link_donateurs_noblock[1:1000,], link_coplaintiffs[1:15000,]    , 'none')



profiling_data <- enriched_donateurs[,.(  id             = id.1
                                             ,payment_method = page
                                             ,donation_amount
                                             ,donation_date   = as.Date('1900-01-01')+ as.integer(donation_date) - 2
                                             ,dob      = as.Date(ifelse(is.na(dob), dob.2, dob), origin = '1970-01-01')
                                             ,language        = ifelse(is.na(language), language.2, language)
                                             ,postal_code             = ifelse(is.na(zip), ifelse(is.na(zip.2), as.character(zip1), as.character(zip.2)) , as.character(zip)) 
                                             )]

#and proceed to profiling.R with donateur data

#give stats based on zip code, extrapolate language when not in Bxl, skip dob.

#end


#ad hoc test try giving more weight to name and streetname, nr
#by duplicating the fields: didnt give substantially better results

""