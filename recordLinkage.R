# deduplication RecordLinkage 

deduplicate <- function(dt) {
  browser()
  dedupData <- copy(dt[, .(id, guid, firstname, lastname, address, dob, duplicate_id, locality)])
  dedupData[is.na(duplicate_id), duplicate_id := id]
  identity.dedupData <- as.vector(dedupData$duplicate_id)
  dedupData[,dob := as.Date('1900-01-01')+ as.integer(dob) - 2]  #convert Excel date to date
  dedupData[, dobYear := year(dob)] 
  dedupData[, dobMonth := month(dob)] 
  dedupData[, dobDay := mday(dob)] 
  dedupData[, firstname := as.factor(firstname)]
  dedupData[, lastname := as.factor(lastname)]
  dedupData <- as.data.frame(dedupData)
  
  # create record pairs and calculate epilink weights
  # rpairs <- RLBigDataDedup(dedupData
  #                          ,identity = identity.dedupData
  #                          ,blockfld=list(3,8)
  #                          ,strcmpfun = "jarowinkler")
  
  rpairs <- compare.dedup(dedupData
                          ,blockfld = list(3,9) 
                          ,phonetic = c(3,4)
                          ,exclude = c(1,2,5,6, 7, 8)
                          )

  rpairs <- epiWeights(rpairs)
  
  # classify with 2 thresholds
  result <- epiClassify(rpairs, threshold.upper = 0.75, threshold.lower = 0.4)
  summary(result)
  
  # show all record pairs with weights between 0.5 and 0.6
  duplicatesCertain <- as.data.table(getPairs(rpairs, min.weight=0.6, single.rows=TRUE))
  duplicatesCertain <- duplicatesCertain[, .(id.1, id.2, duplicate_id.1, duplicate_id.2, firstname.1, lastname.1, firstname.2, lastname.2, dob.1, dob.2, address.1, address.2, Weight) ]
  export_dups <- unique(duplicatesCertain[duplicate_id.1 != duplicate_id.2, ])
  writeCsvIntoDirectory(export_dups, 'duplicates_gt40pc', parameters$path_forupload)
  
  # show all links, do not show classification in the output
  getPairs(result)
  RecordLinkage::getPairs(result)
  
  # see wrongly classified pairs
  getFalsePos(result)
  getFalseNeg(result)
}

