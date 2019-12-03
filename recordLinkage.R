# deduplication RecordLinkage 

#pre process data for deduplication
prepareDedup <- function(dt){
  dt_out <- copy(dt[, .(id, firstname, lastname, address, dob, duplicate_id, locality, email, phone, reason)])
  
  #increase the id of records flagged as 'K'eep by parameters$shift_records_to_keep_by 
  #so that they override all other records registered on the website (range 1 to 110000), 
  #but don't override the records from initial plaintiffs (range parameters$original_plaintiffs_offset)'
  ids_keep <- dt_out[reason == 'K', id]
  dt_out[, duplicate_id := as.integer(duplicate_id)]
  dt_out[duplicate_id %in% ids_keep, duplicate_id := duplicate_id + parameters$shift_records_to_keep_by]
  dt_out[id           %in% ids_keep, id           := id + parameters$shift_records_to_keep_by]
  dt_out[, reason:= NULL]  
  
  dt_out[is.na(duplicate_id), duplicate_id := id]
  dt_out[, dobYear := year(dob)] 
  dt_out[, dobMonth := month(dob)] 
  dt_out[, dobDay := mday(dob)]
  dt_out[, dob := NULL]
  dt_out[, firstname := as.factor(firstname)]
  dt_out[, lastname := as.factor(lastname)]
  dt_out[, email := as.factor(email)]
  dt_out[, phone := as.factor(phone)]
  
  dt_out <- as.data.frame(dt_out)
  dt_out
}

#generate pairs using pre-identified pairs for training
dedupEpiWeights <-function(dt, identity){
  
  #dereference in a dumb way to avoid any trip up during assignment
  firstname_idx <- which(colnames(dt)=="firstname")
  lastname_idx <- which(colnames(dt)=="lastname")
  dobYear_idx <- which(colnames(dt)=="dobYear")
  
  address_idx <- which(colnames(dt)=="address")
  locality_idx <- which(colnames(dt)=="locality")
  email_idx <- which(colnames(dt)=="email")
  
  id_idx <- which(colnames(dt)=="id")
  duplicate_id_idx <- which(colnames(dt)=="duplicate_id")
  
  pairs <- compare.dedup(dt
                          ,blockfld = list(firstname_idx ,lastname_idx, dobYear_idx)  #firstname, lastname, dobYear 
                          ,phonetic = c(firstname_idx ,lastname_idx, address_idx , locality_idx , email_idx)  #firstname, lastname, address, locality, email
                          ,exclude = c(id_idx,duplicate_id_idx)  # id , duplicate_id
                          ,identity = identity
                          )
  
  pairs <- epiWeights(pairs)
  
  calibrateDuplicates <- splitData(dataset=pairs, prop=0.5, keep.mprop=TRUE)
  
  threshold <- optimalThreshold(calibrateDuplicates$train)
  
  summary(epiClassify(calibrateDuplicates$valid,threshold))
  # threshold of  0.5355089
  # resulting in 4656 matches:  OK vs 2454 pre-identified
  # true status        N        P        L
  # FALSE       21166709        0     3112
  # TRUE             176        0     4480
  
  # classify with single threshold
  result <- epiClassify(pairs, threshold.upper = threshold, threshold.lower = threshold)
  summary(result)
  # classification
  # true status        N        P        L
  # FALSE       42333484        0     6159
  # TRUE             345        0     8967
  pairs
}

#print pairs to csv for manual check
exportPairsToCsv <- function(pairs){
  # export all record pairs with weights above threshold - 0.6 for visual check
  duplicatesRanked <- as.data.table(getPairs(pairs, min.weight=threshold , single.rows=FALSE))[,-1]
  writeCsvIntoDirectory(duplicatesRanked, 'duplicates_visualCheck_part1_certain', parameters$path_forupload)
  
  duplicatesRanked <-  as.data.table(getPairs(pairs, max.weight=threshold, min.weight = (threshold -0.3) , single.rows=FALSE))[,-1]
  writeCsvIntoDirectory(duplicatesRanked, 'duplicates_visualCheck_part2_possible', parameters$path_forupload)
  
  duplicatesRanked <-  as.data.table(getPairs(pairs, max.weight=(threshold -0.3), min.weight = (threshold -0.6) , single.rows=FALSE))[,-1]
  writeCsvIntoDirectory(duplicatesRanked, 'duplicates_visualCheck_part3_unlikely', parameters$path_forupload)
}

#ad hoc shift id s to correct range
adHoc_shift_id <- function(){
  
  #1) shift original from 100K to 200K range
  PairsAutoDedupShiftK[id.1 > 100000L, id.1 := id.1 + 100000L]
  PairsAutoDedupShiftK[id.2 > 100000L, id.2 := id.2  + 100000L]
  PairsAutoDedupShiftK[duplicate_id.1 > 100000L, duplicate_id.1 := duplicate_id.1 + 100000L]
  PairsAutoDedupShiftK[duplicate_id.2 > 100000L, duplicate_id.2 := duplicate_id.2 + 100000L]
  PairsAutoDedupShiftK[id.2 > 100000L,]
  
  #2) shift Keep from 0 to 120K range
  PairsAutoDedupShiftK[id.1 %in% listOfKs, id.1 := id.1 + parameters$shift_records_to_keep_by]
  PairsAutoDedupShiftK[id.2 %in% listOfKs, id.2 := id.2 + parameters$shift_records_to_keep_by]
  PairsAutoDedupShiftK[duplicate_id.1 %in% listOfKs, duplicate_id.1 := duplicate_id.1 + parameters$shift_records_to_keep_by]
  PairsAutoDedupShiftK[duplicate_id.2 %in% listOfKs, duplicate_id.2 := duplicate_id.2 + parameters$shift_records_to_keep_by]
  PairsAutoDedupShiftK[id.2 > 100000L & id.2 < 200000L,]
}



#for each pair candidate, keep record with greatest ID, flag other as D with id_dup
buildClusters <- function(pairs, threshold){  
  
  PairsAutoDedup <- as.data.table(getPairs(pairs, min.weight=threshold, single.rows=TRUE))
  PairsAutoDedup <- PairsAutoDedup[, .(id.1, id.2, duplicate_id.1 = as.integer(duplicate_id.1), duplicate_id.2 = as.integer(duplicate_id.2), Weight) ]
  
  
  clustersCollection = NULL
  
  #build array of currently identified pairs
  pairsPreviouslyMatched <- PairsAutoDedup[duplicate_id.1 == duplicate_id.2 & id.1 != id.2,]
  pairsToBeMatched       <- PairsAutoDedup[duplicate_id.1 != duplicate_id.2 & id.1 != id.2,]
  
  clustersCollection <- unique(rbind(
     pairsPreviouslyMatched[, .(duplicate_id = duplicate_id.1, member= id.1, initial_duplicate_id = duplicate_id.1, Weight)]
    ,pairsPreviouslyMatched[, .(duplicate_id = duplicate_id.1, member= id.2, initial_duplicate_id = duplicate_id.1, Weight)]
  ))[order(duplicate_id),]
  
  #build collection of ids (if A linked to B and A linked to C, need to have group of A, B, C before deciding which record is the best)
  for(i in 1:nrow(pairsToBeMatched)){  
    
    #start debug for id 6532
    if(pairsToBeMatched[i,id.1] == 6532) { browser()}
    #end debug for id 6532
    
    #for each id, check if in hashmap
    currentMatchesId <-   which(clustersCollection$member == pairsToBeMatched[i,id.1] 
                                | clustersCollection$member == pairsToBeMatched[i,id.2]
                                | clustersCollection$member == pairsToBeMatched[i,duplicate_id.1]
                                | clustersCollection$member == pairsToBeMatched[i,duplicate_id.2])
    
    currentMatches <- copy( clustersCollection[currentMatchesId,])
    
    #if match was found
    if (dim(currentMatches)[1] > 0) {
      
      #    print('match found')
      
      #    print('currentMatchMaxId')
      #    print(max(currentMatches$duplicate_id))
      
      #    print('pairsToBeMatched[i, max(duplicate_id.1, duplicate_id.2)]')
      #    print(pairsToBeMatched[i, max(duplicate_id.1, duplicate_id.2)])      
      
      currentMatchMaxId <- max( max(currentMatches$duplicate_id)
                                ,pairsToBeMatched[i, max(duplicate_id.1, duplicate_id.2)]
      )
      
      #      print(dim(currentMatches))
      #      print(pairsToBeMatched[i,])
      #      print(currentMatches[, duplicate_id])
      #      print('max id')
      #      print(currentMatchMaxId)
      
      #update existing records with new max id for cluster
      clustersCollection[currentMatchesId, duplicate_id := currentMatchMaxId]
      
    }
    else{
      currentMatchMaxId <- pairsToBeMatched[i, max(duplicate_id.1, duplicate_id.2)]
    }
    
    #in all cases, add the 2 new records to the cluster
    clustersCollection <- rbind(clustersCollection
                                ,pairsToBeMatched[i,.(duplicate_id = currentMatchMaxId ,member = id.1 ,initial_duplicate_id = NA, Weight )])
    clustersCollection <- rbind(clustersCollection
                                ,pairsToBeMatched[i,.(duplicate_id = currentMatchMaxId ,member = id.2 ,initial_duplicate_id = NA, Weight )])
    
    
  }
  unique(clustersCollection)
}

#main function for deduplication
deduplicate <- function(dt) {

  dedupData <- prepareDedup(dt)
  identity.dedupData <- as.vector(dedupData$duplicate_id)

  # create record pairs and calculate epilink weights
  rpairs <- dedupEpiWeights(dedupData, identity.dedupData)
  
  
  #after inspection, the threshold calculated by Batch004 is now too low. we will hardcode
  #calibrateDuplicates <- splitData(dataset=rpairs, prop=0.5, keep.mprop=TRUE)
  #threshold <- optimalThreshold(calibrateDuplicates$train)
  
  print('threshold hardcoded: ')
  threshold <- 0.5355089
  #0.5355089
  print(threshold)

  #validation set classification result
  
  #summary(epiClassify(calibrateDuplicates$valid,threshold))
  # true status        N        P        L
  # FALSE       15494975        0      197
  # TRUE              69        0     1874

  #full data set classification result
  summary(epiClassify(rpairs,threshold))
  # true status        N        P        L
  # FALSE       30989935        0      407
  # TRUE             154        0     3733
    

  clusters <- buildClusters(rpairs, threshold =  threshold)
  clusters[, id:=member]
  clusters[, member:=NULL]
  
  clusters[, cluster_id:=duplicate_id]
  clusters[duplicate_id == id, duplicate_id:= NA]

  
  #reset Keep ids (in parameters$shift_records_to_keep_by to parameters$original_plaintiffs_offset range) back to their original id
  clusters[id           >= parameters$shift_records_to_keep_by & id           < parameters$original_plaintiffs_offset, id          :=id           - parameters$shift_records_to_keep_by]
  clusters[duplicate_id >= parameters$shift_records_to_keep_by & duplicate_id < parameters$original_plaintiffs_offset, duplicate_id:=duplicate_id - parameters$shift_records_to_keep_by]
  clusters[cluster_id   >= parameters$shift_records_to_keep_by & cluster_id   < parameters$original_plaintiffs_offset, cluster_id  :=cluster_id   - parameters$shift_records_to_keep_by]
  clusters[initial_duplicate_id >= parameters$shift_records_to_keep_by & initial_duplicate_id   < parameters$original_plaintiffs_offset, initial_duplicate_id  :=initial_duplicate_id   - parameters$shift_records_to_keep_by]
  
  clusters[, duplicate_id := as.character(duplicate_id)]
  
  writeFstIntoDirectory(clusters, 'clusters_id_duplicateId', parameters$path_dataQualityCheck)
  writeCsvIntoDirectory(clusters, 'clusters_id_duplicateId', parameters$path_dataQualityCheck)
  
  clusters[, initial_duplicate_id := NULL]
  clusters <- unique(clusters[, .( duplicate_id, cluster_id, Weight = min(Weight)), by = id])  #deduplicate with minimum weight in chain
  #clusters <- readFstFromDirectory('clusters_id_duplicateId', parameters$path_dataQualityCheck)
  base::merge(dt, clusters, all.x = TRUE)
  
  #TO DO check for clustersCollection[duplicate_id == 61566, member]
  #then add auto_duplicate_id and D in auto_reason for matched members except if equal to 
  
  
  #check for id 6532 as duplicate_id for cluster with 44356 & 54042 
}  
  


