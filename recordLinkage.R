# deduplication RecordLinkage 

deduplicate <- function(dt) {

  dedupData <- copy(dt[, .(id, guid, firstname, lastname, address, dob, duplicate_id, locality, email, phone)])
  dedupData[is.na(duplicate_id), duplicate_id := id]
  identity.dedupData <- as.vector(dedupData$duplicate_id)
 # dedupData[,dob := as.Date('1900-01-01')+ as.integer(dob) - 2]  #convert Excel date to date
  dedupData[, dobYear := year(dob)] 
  dedupData[, dobMonth := month(dob)] 
  dedupData[, dobDay := mday(dob)] 
  dedupData[, firstname := as.factor(firstname)]
  dedupData[, lastname := as.factor(lastname)]
  dedupData[, email := as.factor(email)]
  dedupData[, phone := as.factor(phone)]
  dedupData <- as.data.frame(dedupData)
  
  # create record pairs and calculate epilink weights
  # rpairs <- RLBigDataDedup(dedupData
  #                          ,identity = identity.dedupData
  #                          ,blockfld=list(3,8)
  #                          ,strcmpfun = "jarowinkler")
  
  rpairs <- compare.dedup(dedupData
                          ,blockfld = list(3,4, 11)  #firstname, dobYear 
                          ,phonetic = c(3,4, 5, 8, 9)  #firstname, lastname, address, locality, email
                          ,exclude = c(1,2,6,7)  # id, guid, dob (replaced by components)
                          ,identity = identity.dedupData
                          )

  rpairs <- epiWeights(rpairs)
  
  calibrateDuplicates <- splitData(dataset=rpairs, prop=0.5, keep.mprop=TRUE)
  
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
  result <- epiClassify(rpairs, threshold.upper = threshold, threshold.lower = threshold)
  summary(result)
  
  # export all record pairs with weights above threshold - 0.6 for visual check
  duplicatesRanked <- as.data.table(getPairs(rpairs, min.weight=threshold , single.rows=FALSE))[,-1]
  writeCsvIntoDirectory(duplicatesRanked, 'duplicates_visualCheck_part1_certain', parameters$path_forupload)
  
  duplicatesRanked <-  as.data.table(getPairs(rpairs, max.weight=threshold, min.weight = (threshold -0.3) , single.rows=FALSE))[,-1]
  writeCsvIntoDirectory(duplicatesRanked, 'duplicates_visualCheck_part2_possible', parameters$path_forupload)

  duplicatesRanked <-  as.data.table(getPairs(rpairs, max.weight=(threshold -0.3), min.weight = (threshold -0.6) , single.rows=FALSE))[,-1]
  writeCsvIntoDirectory(duplicatesRanked, 'duplicates_visualCheck_part3_unlikely', parameters$path_forupload)
  
  #after inspection, it appears that a more accurate cutoff is 0,5304313

  threshold <- 0.5304313

  #validation set classification result
  summary(epiClassify(calibrateDuplicates$valid,threshold))
    # true status        N        P        L
  # FALSE 21118729        0     5421
  # TRUE        39        0     1202

  #full data set classification result
  summary(epiClassify(rpairs,threshold))
  # true status        N        P        L
  # FALSE       42237494        0    10806
  # TRUE              84        0     2397
    
  # see wrongly classified pairs
  #getFalsePos(result)
  #getFalseNeg(result)

  
  #for each pair candidate, keep record with greatest ID, flag other as D with id_dup
  
  PairsAutoDedup <- as.data.table(getPairs(rpairs, min.weight=threshold, single.rows=TRUE))
  PairsAutoDedup <- PairsAutoDedup[, .(id.1, id.2, duplicate_id.1, duplicate_id.2, Weight) ]
 # PairsAutoDedup <- PairsAutoDedup[duplicate_id.1 != duplicate_id.2, ]  #remove previously identified dups
  
  
  clustersCollection = NULL

  #build array of currently identified pairs
  pairsPreviouslyMatched <- PairsAutoDedup[duplicate_id.1 == duplicate_id.2,]
  pairsToBeMatched <- PairsAutoDedup[duplicate_id.1 != duplicate_id.2,]
  
  clustersCollection <- unique(rbind(
                              pairsPreviouslyMatched[, .(duplicate_id = duplicate_id.1, member= id.1, initial_duplicate_id = duplicate_id.1)]
                             ,pairsPreviouslyMatched[, .(duplicate_id = duplicate_id.1, member= id.2, initial_duplicate_id = duplicate_id.1)]
                              ))[order(duplicate_id),]
  #build collection of ids (if A linked to B and A linked to C, need to have group of A, B, C before deciding which record is the best)
  for(i in 1:nrow(pairsToBeMatched)){  
    
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
                                ,pairsToBeMatched[i,.(duplicate_id = currentMatchMaxId ,member = id.1 ,initial_duplicate_id = NA )])
    clustersCollection <- rbind(clustersCollection
                                ,pairsToBeMatched[i,.(duplicate_id = currentMatchMaxId ,member = id.2 ,initial_duplicate_id = NA )])
    
    
  }
  clustersCollection <- unique(clustersCollection)
  
  #TO DO check for clustersCollection[duplicate_id == 61566, member]
  #then add auto_duplicate_id and D in auto_reason for matched members except if equal to 

  
}

