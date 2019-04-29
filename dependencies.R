# general purpose
library("glue")
library("config")
library("magrittr")
library("data.table")
library("tibble")

# io
library("readxl")
library("fst")
library("httr")
library("RCurl")
library("XML")
library("jsonlite")

parameters <- config::get(file = "../parameters.yml")

# general purpose
#library("readr")
#library("lubridate")
#library("stringr")
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