#Profiling Donateurs

#4 files: GocardLess, Triodos, PayPal and Mollie 
#Mollie and Paypal are far from optimal: can get better data through the API 

source('dependencies.R')
source('io_utilities.R')
source('geocode.R')
source('RecordLinkage.R')
source('profiling.R')

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

#test if precomputed deduplicated coplaintiffs file exists in memory or on disk, else reload
if(!is.data.table(try(coplaintiffs_clean)))
{
   if(!is.data.table(try(coplaintiffs_clean <- readFstFromDirectory("coplaintiffs_deduplicated","data")))) {
      
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
      
      #post process coplaintiffs : remove duplicates from dedupData and store
      coplaintiffs_clean <- dedupData[auto_reason!='D' | is.na(auto_reason),]      
      coplaintiffs_clean [,name_grouped:= paste(lastname, firstname, sep = ' ')]      
      coplaintiffs_clean[ , street_and_nr := paste(ifelse(is.na(address), '', tolower(address) )
                                                   ,ifelse(is.na(street_nb), '', tolower(street_nb) )
                                                   ,ifelse(is.na(address2), '', tolower(address2) )
      )]  
      writeCsvIntoDirectory(coplaintiffs_clean, 'coplaintiffs', "parameters$path_forupload")
      #TO DO fix encoding error to enable data save to fst. csv is not suitable for reuse.
      writeFstIntoDirectory(coplaintiffs_clean, "coplaintiffs_deduplicated", "data")      
   }
}

print('start loading donateurs data files')
donateurs <- readRawDataFolder(path = paste( paste ( '..',  'Donateurs', sep='/') , parameters$path_input_data, sep='/'), type = 'donateur')

#cleanup donateurs: split first name and last name
check_data <- NULL
donateurs[,is_complete:=1]

# concatenate street and nr
cleandonateurs<-cleanRawData(donateurs, check_data)
cleandonateurs[     , street_and_nr := paste(ifelse(is.na(address), '', tolower(address) )
                                             ,ifelse(is.na(street_nb), '', tolower(street_nb) )
                                             ,ifelse(is.na(address2), '', tolower(address2) )
                                      )]  


# TO DO set (first) donateur payment date to match against registration as coplaintiff
# TO DO remove Mme, Mr, M Mevr etc if there are more than 2 spaces
# TO DO flag company name, BVBA etc... as is_company, remove that data from names
# TO DO run address validation

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

# A1) create record pairs and calculate epilink weights based on e-mail as blocking factor
   rpairs_email <- linkEpiWeights(link_donateurs, link_coplaintiffs, 'email')
   
   #check pairs
   #View(rpairs_email$pairs)
   #spot check 3570	15360
   # and       2880	15360
   #link_donateurs[3570,]
   #link_donateurs[2880,]
   #link_coplaintiffs[25904,]
   #ok
   #spot check 12006	49338
   #link_donateurs[12006,]
   #link_coplaintiffs[49338,]
   #ok

# A2) enrich donateurs with info from coplaintiff using linked records
   enriched_donateurs <- donateurs[,.( id.1 = id, page, zip1 = zip, donation_amount, donation_date, d_firstname = firstname, d_lastname = lastname, d_email = email)]
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
   writeCsvIntoDirectory(RecordLinkage::getPairs(rpairs_email, single.rows=FALSE)[,-1], 'match_donateurs_coplaintiffs_email', parameters$path_forupload)

# A3) enrich coplaintiffs with info from donateurs matched on email
   enriched_coplaintiffs <- copy(coplaintiffs_clean[,])
   setnames(enriched_coplaintiffs, "id", "id.2")
   enriched_coplaintiffs <-  base::merge(  enriched_coplaintiffs
                                        , data.table(RecordLinkage::getPairs(rpairs_email, single.rows=TRUE))[,.(id.2
                                                                                                                 , id = id.1)]
                                        ,   all.x = TRUE )
   
   #debug table(enriched_coplaintiffs[,.(is.na(id))],useNA = 'ifany')

# B1) create record pairs and calculate epilink weights with name as blocking factor
   rpairs_name <- linkEpiWeights(link_donateurs, link_coplaintiffs, 'name')

# B2) then merge this in too and blend to complete the data
   enriched_donateurs <-  base::merge(  enriched_donateurs
                                        , data.table(RecordLinkage::getPairs(rpairs_name, single.rows=TRUE))[,.(id.1
                                                                                                                 , dob_name = dob.2
                                                                                                                 , language_name = language.2
                                                                                                                 , zip_name = zip.2
                                                                                                                 , match_name = 1)]
                                        ,   all.x = TRUE)
   enriched_donateurs[,match_any:=ifelse(is.na(match_email), match_name, match_email)]
   table(enriched_donateurs[,.(page,  match_any)],useNA = 'ifany')
   
   # match_any
   # page       1    <NA>
   #    GCL  2520     434
   # mollie   315     178
   # paypal  4338    4634
   # triodos    0      35
   
   #debug nr of unique records matched nrow(as.data.frame(unique(enriched_donateurs[match_any==1,id.1])))
   #6934
   
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
   
      
   writeCsvIntoDirectory(RecordLinkage::getPairs(rpairs_name, single.rows=FALSE)[,-1], 'match_donateurs_coplaintiffs_name', parameters$path_forupload)


# B3 enrich coplaintiffs with info from donateurs matched on name
   enriched_coplaintiffs <-  base::merge(  enriched_coplaintiffs
                                        , data.table(RecordLinkage::getPairs(rpairs_name, single.rows=TRUE))[,.(id.2
                                                                                                                 , id.name = id.1)]
                                        ,   all.x = TRUE )
   #debug table(enriched_coplaintiffs[,.(is.na(id), is.na(id.name))],useNA = 'ifany')
   enriched_coplaintiffs[,id:=ifelse(is.na(id), id.name, id)]  #merge both lookups into 1 id field
   enriched_coplaintiffs[,id.name:=NULL]  #merge both lookups into 1 id field
   
   enriched_coplaintiffs <-  unique(base::merge(  enriched_coplaintiffs
                                        ,   donateurs[,.(id
                                                         ,donation_platform = page
                                                         ,donation_amount
                                        )]
                                        ,   all.x = TRUE
                                        ,   by = "id"
                                        # ,   by.y = id
                                          ))
   #debug table(enriched_coplaintiffs[,.(is.na(donation_amount), donation_platform)],useNA = 'ifany')
   writeCsvIntoDirectory(enriched_coplaintiffs, 'coplaintiffs_for_model',"data")


# C) match remaining using no blocking criterion
   
         # # The below never yielded any useful matches in addition to the email and name linkage,
         # # but introduced a lot of false positives. so it has been abandoned
         #    
         # #as this is a very memory intensive task ( 2^n ), we need to narrow down link_donateurs to unmatched id's only
         # link_donateurs_noblock <- data.table(base::merge(link_donateurs
         #                                       , unique(data.table(RecordLinkage::getPairs(rpairs_email, single.rows=TRUE))[,.(id=id.1 , found_email = 1)])
         #                                       , all.x = TRUE ))
         # link_donateurs_noblock <- data.table(base::merge(link_donateurs_noblock
         #                                       , unique(data.table(RecordLinkage::getPairs(rpairs_name, single.rows=TRUE))[,.(id=id.1 , found_name = 1)])
         #                                       , all.x = TRUE ))
         # link_donateurs_noblock <- link_donateurs_noblock[is.na(link_donateurs_noblock$found_email) & is.na(link_donateurs_noblock$found_name),]
         # link_donateurs_noblock[, found_email:=NULL]
         # link_donateurs_noblock[, found_name :=NULL]
         # 
         # writeCsvIntoDirectory(link_donateurs_noblock, 'unmatched_donateurs', parameters$path_forupload)
         # 
         # 
         # #free up memory
         # rm(coplaintiffs_clean, dedupData, fixed_data, geocodedData, cleancoplaintiffs)
         # 
         # 
         # #TO DO : increase matching by testing various strategies:
         # #                 increase weight of name and email >> tried, no improvement.
         # #                 break up name in namepart 1 and namepart 2, then store swapped nameparts in new columns for matching
         # #                 find a better phonetic algorithm
         # #                 adjust threshold for matching according to epiweights in current set
         # 
         # #loop over chunks of umatched donateurs and check against all coplaintiffs,
         # #then build a collection of best matches
         # matched_noblock_donateurs = NULL
         # for (chunk in seq(1:(nrow(link_donateurs_noblock)/400)))
         # {
         #    chunk_rpairs_noblock   <- linkEpiWeights(link_donateurs_noblock[((chunk - 1) * 400 +1): ifelse((400 * chunk)>nrow(link_donateurs_noblock), nrow(link_donateurs_noblock), (400 * chunk)),]
         #                                      , link_coplaintiffs
         #                                      , 'none')
         #    matched_noblock_donateurs <- rbindlist(list(matched_noblock_donateurs, RecordLinkage::getPairs(chunk_rpairs_noblock, single.rows=TRUE, min.weight =0.4, max.weight=0.8, filter.match="match")), fill = FALSE, idcol = NULL)
         #    print('debug iteration ')
         #    print(chunk)  
         #    }

   
enriched_donateurs[, firstname :=  d_firstname] 
enriched_donateurs[, lastname := d_lastname] 
enriched_donateurs[, email := d_email]   
enriched_donateurs[, firstname :=  d_firstname] 
enriched_donateurs[, lastname := d_lastname] 
enriched_donateurs[, email := d_email]  
#TO DO: use this info, not currently used
profiling_data_amounts <- enriched_donateurs[,.(  id             = id.1
                                             ,payment_method = page
                                             ,donation_amount
                                             ,donation_date   = as.Date('1900-01-01')+ as.integer(donation_date) - 2
                                             ,dob      = as.Date(ifelse(is.na(dob_email), dob_name, dob_email), origin = '1970-01-01')
                                             ,language        = ifelse(is.na(language_email), language_name, language_email)
                                             ,postal_code             = ifelse(is.na(zip_email), ifelse(is.na(zip_name), as.character(zip1), as.character(zip_name)) , as.character(zip_email)) 
                                             )]

profiling_data  <- enriched_donateurs[,.(   firstname = d_firstname
                                           ,lastname = d_lastname
                                           ,email = d_email
                                           ,payment_method = page
                                           ,dob            = as.Date(ifelse(is.na(dob_email), dob_name, dob_email), origin = '1970-01-01')
                                           ,language       = ifelse(is.na(language_email), language_name, language_email)
                                           ,postal_code    = ifelse(is.na(zip_email), ifelse(is.na(zip_name), as.character(zip1), as.character(zip_name)) , as.character(zip_email)) 
)]

#and proceed to profiling.R with donateur data
profiling_data <- unique(profiling_data)


#give stats based on zip code, extrapolate language when not in Bxl, skip dob.

#end


#ad hoc test try giving more weight to name and streetname, nr
#by duplicating the fields: didnt give substantially better results

""

rm(fixed_data, geocodedData,link_coplaintiffs, link_donateurs,rpairs_email,rpairs_name,cleandonateurs, cleancoplaintiffs)


# TO DO
# 
# check why coplaintiffs file is not complete: schaerlaekens
# add original coplaintiffs
# add supporters
# rerun profiling with below unique combo: email, firstname, lastname
# rerun model with demographic features only (not newsletter, not valid_phone)
# 
# test_unique <- unique(cleandonateurs[,.(page, email)])
# > table(test_unique$page)
# 
# GCL  mollie  paypal triodos 
# 2734     441     882       1 
# > test_unique <- unique(cleandonateurs[,.(page, email, firstname, lastname)])
# > table(test_unique$page)
# 
# GCL  mollie  paypal triodos 
# 2755     446     890      35    =  4126
# 
# 
# table(unique(enriched_donateurs[,.(match_any, page, d_email, d_firstname, d_lastname)])[,.(match_any, page)], useNA = 'ifany')
# page
# match_any  GCL mollie paypal triodos
# 1    2325    291    414       0
# <NA>  430    155    476      35
# > table(unique(enriched_donateurs[,.(match_any, page, d_email, d_firstname, d_lastname)])[,.(match_any)], useNA = 'ifany')
# 
# 1 <NA> 
#    3030 1096  = 4126
# 
# 
# - wim schaerlaekens (48)  = reason PsvR, id 49959    in excel  
# - lionel fueyo (83)  =   id 32463  in excel  ,  2577 in dataset !!!
#    
#    in list of intervenin parties:  50166 records submitted 
