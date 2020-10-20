documentation

# duplicates matching

##1 Data Matching: Concepts and Techniques for Record Linkage, Entity Resolution ...
By Peter Christen
https://books.google.be/books?id=LZrT6eWf9NMC&pg=PR7&dq=machine+learning+identify+quasi+duplicates&hl=en&sa=X&ved=0ahUKEwjqvaSmlJ_iAhXBZ1AKHT8pDzsQ6AEIMzAC#v=onepage&q=machine%20learning%20identify%20quasi%20duplicates&f=false

identifying features: name, first name, year, month, day (separately)

odds matching

steps
    pre processing
       
    indexing
              phonetic algorithm like Soundex, NYSIIS, or metaphone
    
libraries 
      bigmatch
      d-dupe
      R recordlinkage
      
## 2   https://rpubs.com/ahmademad/RecordLinkage    
      
Preprocessing: developing link keys, extracting information from link keys, normalization of link keys
Reduction of search space: Blocking
Comparison: String metrics, year comparisons, numeric comparisons


Classification: Fellegi-Sunter Model
uses conditional independence assumption between fields, holds OK if just using firstname, lastname, dateofbirth

Final prediction: cut off scores, validation

Fellegi-Sunter Model



# R package:
http://infolab.stanford.edu/serf/
R-Swoosh 
R recordlinkage
