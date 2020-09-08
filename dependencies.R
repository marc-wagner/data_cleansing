# general purpose
library("config")
library("magrittr")
library("data.table")
library("tibble")
library("stringr")
library("lubridate")

# io
library("readxl")
library("fst")
library("httr")
#library("RCurl")
library("XML")
library("jsonlite")
library("fst")
library("DBI")
library("odbc")
library("RODBC")
library("feather")

#classification
library("RecordLinkage")
library("mlr")
library("xgboost")
library("iml")

#visualization
library("ggplot2")
library("classifierplots")

parameters <- config::get(file = "../parameters.yml")


#general purpose
#library("glue")
#library("readr")
#library("lubridate")

#library("stringdist")
#library("purrr")
#library("plyr")
#library("tidyverse")
#library("RDS")
# library("Matrix")
# library("bit64")

#io
#library("RODBCext")

# Vizualisation
#library("ggplot2")
# data.table and database connections
#library("classifierplots")

# data preparation
# library("dummies")

# modelling
#library("mlr")
#library("iml")
# library("lift")
# library("devtools")
#library("textreuse")
#library("ade4")