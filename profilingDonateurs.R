#Profiling Donateurs
source('dependencies.R')
source('io_utilities.R')
source('geocode.R')
source('RecordLinkage.R')


print('start loading donateurs data files')
manual_fixes1 <- readRawDataFolder(path = paste( paste ( '..',  'Donateurs', sep='/') , parameters$path_input_data, sep='/'))
