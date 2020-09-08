# Profiling  
# of co-plaintiffs
# 2019 10 01

# libraries
source('dependencies.R')
source('io_utilities.R')

# functions
profiling <- function(profiling_data){
  #load open data by NIS code
  open_data_postcode <-read_raw_feather(table_name = "open_data") 
  
  #gender
  #tricky, can't find a package with french or dutch first names for package gender.R
  
  #age
  table(profiling_data[, 10*floor((year(Sys.Date())- year(dob))/10)], useNA = 'ifany')
  profiling_data[ , age_group := 10*floor((year(Sys.Date())- year(dob))/10)] 
  profiling_data[ is.na(age_group), age_group :=999] 
  
  #language
  table(profiling_data[, casefold(language)], useNA = 'ifany')
  profiling_data[ , language :=casefold(language)] 
  profiling_data[ is.na(language) , language :='not specified'] 
  
  #region
  profiling_data[ postal_code < 1500 , region :='Brussels Capital Region']    
  profiling_data[ postal_code < 4000 & is.na(region) , region :='Flanders'] 
  profiling_data[ postal_code < 8000 & is.na(region) , region :='Wallonia'] 
  profiling_data[ postal_code>= 8000 & is.na(region) , region :='Flanders'] 
  table(profiling_data[, region], useNA = 'ifany')
  
  #province
  profiling_data[ postal_code < 1300 , province :='Brussels Capital Region']    
  profiling_data[ postal_code < 1500 & is.na(province) , province :='Walloon Brabant'] 
  profiling_data[ postal_code < 2000 & is.na(province) , province :='Flemish Brabant'] 
  profiling_data[ postal_code < 3000 & is.na(province) , province :='Antwerp'] 
  profiling_data[ postal_code < 3500 & is.na(province) , province :='Flemish Brabant'] 
  profiling_data[ postal_code < 4000 & is.na(province) , province :='Limburg'] 
  profiling_data[ postal_code < 5000 & is.na(province) , province :='Liège'] 
  profiling_data[ postal_code < 6000 & is.na(province) , province :='Namur'] 
  profiling_data[ postal_code < 6600 & is.na(province) , province :='Hainaut'] 
  profiling_data[ postal_code < 7000 & is.na(province) , province :='Luxembourg'] 
  profiling_data[ postal_code < 8000 & is.na(province) , province :='Hainaut'] 
  profiling_data[ postal_code < 9000 & is.na(province) , province :='West Flanders'] 
  profiling_data[ postal_code >=9000 & is.na(province) , province :='East Flanders'] 
  table(profiling_data[, province], useNA = 'ifany')
  profiling_data
}

#procedure starts here ->
main <- function() {
    #load latest data
    #run main.R until "dedupData[!is.na(duplicate_id) , auto_reason := 'D']"
    #keep original plaintiffs in there: 62281 records
    
    #remove duplicates
    profiling_data <- dedupData[(reason != 'D' | is.na(reason))  & (is.na(auto_reason) | auto_reason != 'D'),]
    
    profiling_data <- profiling(profiling_data)
    
    
    #combined table with 3 criteria: age, language, region:
    individual_stats <- table(profiling_data[, .(region, province, language, age_group)], useNA = 'ifany')
    write.csv2(individual_stats, 'export_profiling.csv')
    
    #link NIS code
    
    
    
    #urban vs rural
    # TO DO: develop this, based on density of population
          # library(RgoogleMaps)
          # library(googleVis)
          # 
          # 
          # lat = c(49.702147,51.30);
          # lon = c(3.650 , 5.30);
          # 
          # ratio =  (lon[2]-lon[1]) / (lat[2] - lat[1]) 
          # center = c(mean(lat), mean(lon));
          # zoom <- min(MaxZoom(range(lat), range(lon)));
          # newmap <- GetMap(center = center
          #                  , size = c(640, 640 )
          #                  , destfile = "belgium.png" 
          #                 # , maptype = "roadmap"
          #                 , API_console_key = parameters$google_api_key
          #                 , zoom = 8)
          # 
          # PlotOnStaticMap(newmap , lat = c(36.3, 35.8, 36.4), lon = c(-5.5, -5.6, -5.8), zoom = 10, 
          #                 cex = 4, pch = 19, col = "red", FUN = points, add = F)
          # 
          # Andrew <- data.frame(lat = c(36.3, 35.8, 36.4), lon = c(-5.5, -5.6, -5.8))
          # 
          # gvisMap(Andrew, "LatLong", "Tip", 
          #         options=list(showTip=TRUE, showLine=F, enableScrollWheel=TRUE, 
          #                      mapType='satellite', useMapTypeControl=TRUE, width=800,height=400))
}

# ad hoc profiling minors of age in original codemandeurs
main_adhoc_minors <- function(){
  
    #load all files in batch2
    original_coplaintiffs <- readRawDataFolder(path = paste( paste ( '..',  'Batch002', sep='/') , parameters$path_input_data, sep='/'))
    #keep only original coplaintiffs
    #original coplaintiffs were given an iD incremented by 200000 
    #to overrule all subsequent records in deduplication exercise
    original_coplaintiffs <- original_coplaintiffs[id>= 200000,]
    
    check_data <- NULL
    clean_original_coplaintiffs <- cleanRawData(original_coplaintiffs, check_data)
    
    
    #load original minor coplaintiffs that need to be removed
    file <- paste(paste( paste ( '..',  'Profiling', sep='/') , parameters$path_input_data, sep='/'),'nu_meerderjarig_te_verwijderen_processed.xlsx', sep='/' )
    original_coplaintiffs_to_remove <- readRawData(filename = file, nrCols = 14)
    original_coplaintiffs_to_remove <- original_coplaintiffs_to_remove[!is.na(id), id]
    View(clean_original_coplaintiffs[id %in% original_coplaintiffs_to_remove,])
    #visual check OK, 96 found
    
    #load id s of records to remove (source: Te verwijderen uit originele lijst.pdf)
    ids_to_remove <- c(1751,2083 ,2798 ,2849 ,4489 ,4652 ,7716 ,901)
    #give an iD incremented by 200000 to match IDs of original coplaintiffs in cleansed input file
    ids_to_remove_adjusted <- ids_to_remove + 200000
    
    View(clean_original_coplaintiffs[id %in% ids_to_remove_adjusted,])
    #visual check OK, 8 found
    
    #remove those records
    clean_original_coplaintiffs <- clean_original_coplaintiffs[!(id %in% ids_to_remove_adjusted),]
    clean_original_coplaintiffs <- clean_original_coplaintiffs[!(id %in% original_coplaintiffs_to_remove),]
    
    #8325 coplaintiffs left
    
    
    
    #age
    table(clean_original_coplaintiffs[, 10*floor((year(Sys.Date())- year(dob))/10)], useNA = 'ifany')
    clean_original_coplaintiffs[ , age_group := 10*floor((year(Sys.Date())- year(dob))/10)] 
    clean_original_coplaintiffs[ is.na(age_group), age_group :=999] 
}

