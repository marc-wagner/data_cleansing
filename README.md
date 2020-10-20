# Klimaatzaak data_cleansing

source code here: https://github.com/marc-wagner/data_cleansing
data files located on klimaatzaak dropbox folder.

this tool has been used to process co-plaintiff data to the case Klimaatzaak vs Belgian governments
the original coplaintiffs had been submitted earlier. ~8000 plaintiffs
then a communication campaign added ~60000 plaintiffs to the trial

This tool has been developed primarily to prepare this batch of 60000 plaintiffs for addition to the trial.

## goal
data cleansing of subscriber lists: 
  fill out correct addresses, 
  filter out bad data
  filter out incomplete submissions
  
post-processing of subscriber data:
  address verification using google maps API
  translate address in french or dutch according to location

remove duplicates:
  keep latest record among duplicates within the 60000 new submissions, 
  remove new submission if it is a duplicate of an original submission.

add manual fixes:
  the process included several clerical reviews. 
  the output of those clerical reviews was fed back into the tool.

## technology
persistence: MariaDB ODBC
processing: R

## installation
build using 
R version 3.5.3 (2019-03-11) -- "Great Truth" 
Rstudio Version 1.2.5019

install a copy of the klimaatzaak database 
or connect to the live database (requires ssh key and valid mariadb user)
open R project 'cleansing_code.Rproj'
install packrat for package management
all necessary R modules are listed in dependencies.R

## modules
### main.R
load co-plaintiffs data and build a data set that is fit for submission to the court.
layer manual changes and fixes on top of data extracted from klimaatzaak website,
to achieve a re-runnable, traceable process.
the processing is dependent on the settings in ../parameters.yml

in particular, get_new_geocoding: TRUE
will call google Maps API for all addresses that have not been geolocated.
this should not be necessary, and geocoding all addresses from scratch
will result in a cost of ~400 EUR in google API calls.

### profilingDonateurs.R
load Excel files with list of donors and match them against coplaintiffs.
in order to extract the profile of donors. the output was some demographic statistics.

### main_donateurs_in_coplaintiffs.R
using above profilingDonateurs.R, build a predictive model to find 
co-plaintiffs who are likely to be donors.
the outcome of the model has not been satisfactory, with an AUC below 70% and 
most important features being individual ones, when demographic features were desired.

## parameters.yml 
below is a sanitized version of the parameters.yml file
which you need to place one directory up from this root directory
with the correct values for db_connection, google_api_key, database_pwd, db_server_version

default:
  batch_name : Batch003
  path_input_data : Cleansed_raw_files
  path_dataQualityCheck : Cleansed_dataQualityCheck
  path_geocoded_address : geocoded_addresses
  path_forupload : for_upload
  db_connection : xxxxxxxxxxxxxxxxxxxx
  debug: 1
  google_api_key : xxxxxxxxxxxxxxxxxxxx
  fetch_size : 500
  database_pwd: xxxxxxxxxxxxxxxxxxxx
  db_server_version: 10.0.38-MariaDB
  get_new_geocoding: FALSE
  shift_records_to_keep_by : 120000
  original_plaintiffs_offset : 200000
  train_test_ratio : 0.5

## folder structure

 parameters.yml
|
|- cleansing_code
|                |README.md
|                |main.R
|                |- packrat
|- Batch001
|          |-Cleansed_raw_files         : input files to cleansing process
|          |-Cleansed_dataQualityCheck  : intermediate files produces by cleaning process
|          |-for_upload                 : results for this batch
|          |-geocoded_addresses         : results of geolocation
|
|- Batch002
|          |-Cleansed_raw_files         : input files to cleansing process
...          ...

