# deduplication RecordLinkage 

#sample code

data(RLdata500)
str(RLdata500)

dedupData <- copy(cleanData[1:10000, .(id, guid, firstname, lastname, address, dob, duplicate_id)])
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
rpairs <- RLBigDataDedup(dedupData
                         ,identity = identity.dedupData
                         ,blockfld=list(3,4,6,7,8)
                         ,strcmpfun = "jarowinkler")
rpairs <- epiWeights(rpairs)

# show all record pairs with weights between 0.5 and 0.6
getPairs(rpairs, min.weight=0.5, max.weight=0.6)

# show only matches with weight <= 0.5
getPairs(rpairs, max.weight=0.5, filter.match="match")

# classify with one threshold
result <- epiClassify(rpairs, 0.5)

# show all links, do not show classification in the output
getPairs(result, filter.link="link", withClass = FALSE)

# see wrongly classified pairs
getFalsePos(result)
getFalseNeg(result)


