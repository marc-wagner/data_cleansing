#main file for data cleansing of subscriber list
#built under GNU General Public License

source('dependencies.R')
source('io_utilities.R')

#load source files
print('start')
rawData <- readRawDataFolder(path = '../Batch001/Cleansed_raw_files')
#data contains lots of NA lines, corresponding to empty XLS lines.

#cleanse data
cleanData <- cleanRawData(rawData)
table(rawData$page, useNA = 'ifany')
#store collection

#classify errors

#identify duplicates

#train duplicates finding

#