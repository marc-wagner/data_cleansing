# deduplication RecordLinkage 

#pre process data for deduplication
prepareDedup <- function(dt){
  dt_out <- copy(dt[, .(id, guid, firstname, lastname, address, dob, duplicate_id, locality, email, phone)])
  dt_out[is.na(duplicate_id), duplicate_id := id]
  dt_out[, dobYear := year(dob)] 
  dt_out[, dobMonth := month(dob)] 
  dt_out[, dobDay := mday(dob)] 
  dt_out[, firstname := as.factor(firstname)]
  dt_out[, lastname := as.factor(lastname)]
  dt_out[, email := as.factor(email)]
  dt_out[, phone := as.factor(phone)]
  dt_out <- as.data.frame(dt_out)
  dt_out
}

#generate pairs using pre-identified pairs for training
dedupEpiWeights <-function(dt, identity){
  pairs <- compare.dedup(dt
                          ,blockfld = list(3,4, 11)  #firstname, dobYear 
                          ,phonetic = c(3,4, 5, 8, 9)  #firstname, lastname, address, locality, email
                          ,exclude = c(1,2,6,7)  # id, guid, dob (replaced by components)
                          ,identity = identity
                          )
  
  pairs <- epiWeights(pairs)
  
  calibrateDuplicates <- splitData(dataset=pairs, prop=0.5, keep.mprop=TRUE)
  
  threshold <- optimalThreshold(calibrateDuplicates$train)
  
  summary(epiClassify(calibrateDuplicates$valid,threshold))
  # very high threshold of  0.9631064
  # resulting in too few matches:  2100 vs 2454 pre-identified
  # true status        N        P        L
  # FALSE       21122513        0     1637
  # TRUE             756        0      485
  
  # table(dt$reason)
  # BV    C    D    E    M 
  # 20  228 2454    5  344 
  
  # classify with single threshold
  result <- epiClassify(pairs, threshold.upper = threshold, threshold.lower = threshold)
  summary(result)
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

#for each pair candidate, keep record with greatest ID, flag other as D with id_dup
buildClusters <- function(pairs){  
  
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
  
  #after inspection, it appears that a more accurate cutoff is 0,5304313

  threshold <- 0.5304313

  #validation set classification result
  
  summary(epiClassify(calibrateDuplicates$valid,threshold))
    # true status        N        P        L
  # FALSE 21118729        0     5421
  # TRUE        39        0     1202

  #full data set classification result
  summary(epiClassify(rpairs,threshold))
  # classification
  # true status        N        P        L
  # FALSE       45110863        0    11831
  # TRUE               0        0        4
    

  clusters <- buildClusters(rpairs)
  clusters[, id:=member]
  clusters[, member:=NULL]
  clusters[, cluster_id:=duplicate_id]
  clusters[duplicate_id == id, duplicate_id:= NA]
  clusters[, duplicate_id = as.character(duplicate_id)]
  writeFstIntoDirectory(clusters, 'clusters_id_duplicateId', parameters$path_dataQualityCheck)
  writeCsvIntoDirectory(clusters, 'clusters_id_duplicateId', parameters$path_dataQualityCheck)
  
  #clusters <- readFstFromDirectory('clusters_id_duplicateId', parameters$path_dataQualityCheck)
  base::merge(dt, clusters, all.x = TRUE)
  
  #TO DO check for clustersCollection[duplicate_id == 61566, member]
  #then add auto_duplicate_id and D in auto_reason for matched members except if equal to 
  
  
  #check for id 6532 as duplicate_id for cluster with 44356 & 54042 
}  
  


