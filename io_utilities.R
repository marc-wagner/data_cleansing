#IO utilities


readRawData <- function(filename) {
    readxl::read_excel(filename
                      ,sheet = 1
                      ,col_names = TRUE
                      ,progress = readxl_progress()
                      ,trim_ws = TRUE
                      ,col_types = 'text')   %>% as.data.table()
}

readRawDataFolder <- function(path) {
  files_in_folder <- list.files(path = path
                                ,full.names = TRUE
                                ,pattern = ".xlsx$")
  for (file in files_in_folder) {
    try(databuffer <- readRawData(filename = file)) 
    data <- if(!is.null(nrow(data))  ) {
        rbindlist(list(copy(databuffer), data), fill = FALSE, idcol = NULL)
    } else databuffer
  }
  data
}


cleanRawData <- function(dt){
  dt[!is.na(id),]
}