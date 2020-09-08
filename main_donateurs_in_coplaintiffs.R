# Data preparation for predicting donateurs from coplaintiff DB.

# prerequisite: run profilingDonateurs.R to obtain data.table enriched_coplaintiffs.
# on 2020 09, there were 61757 records in this data table:
# 57666 deduplicated co-plaintiffs, matched with 6934 unique donateurs.

#import libaries
source('dependencies.R')
source('profiling.R')

#function declarations
main_train_classification <- function(dt_full){
  
  #set labels
  dt_full[, target := 9] #flag all as unknowns
  dt_full[Result %in% positive_filter, target := 1]
  dt_full[Result %in% negative_filter, target := 0]
  dt_full[target == 9, unknown:= TRUE] #flag all unknowns persistently to be able to reset them in loop
  
  dt_full[, Result := NULL] #remove features that bring signal leakage
  dt_full[, reason_for_refusal:=NULL]
  dt_full_dummy <- copy(dt_full)
  dt_full_dummy[, ind_existing_phone_number:=NULL]
  dt_full_dummy[, language_code:=NULL]
  
  
  #TO DO : get nace code to work for full set,
  # currently excluded as it didn't show up in the model anyhow.
  
  dt_full_dummy[, unknown:=NULL]
  dt_full_dummy[, excluded:=NULL]
  
  dt_full_dummy <- add_dummy_features(dt_full_dummy, add_nace = FALSE)  #add features to full set, to have a common ground  
  
  #loop on various training samples  , mixing in more unknowns as we go 
  for (negative_percentage in seq(1.0, 0.0, -0.1)) {
    print(paste("negative ratio: ", negative_percentage))
    
    set.seed(123) #reset seed
    dt_baseline <- sample_test_train_set(dt_full_dummy , negative_percentage )
    
    model <- train_model(dt_baseline) #hardcoded xgboost, with specific hyperparameters
    
    if(parameters$debug==1) debugClassificationModel(dt_baseline, model) 
    
    predictions <- predict_values(dt_baseline, model)
    
    print(assess_predictive_performance(model, predictions, dt_baseline))
    
  }
  
  #best performance is for using all negatives only:
  set.seed(123) #reset seed
  dt_baseline <- sample_test_train_set(dt_full_dummy , 1.0 )
  
  model <- train_model(dt_baseline) #hardcoded xgboost, with specific hyperparameters
  
  saveRDS(model, "model_B2B_prospect.rds")
  
  predictions <- predict_values(dt_baseline, model)
  
  classifierplots::lift_plot(pred.prob = predictions$data[,]$prob.1, test.y = predictions$data[,]$truth, granularity = 0.001)
  
  #now run model on all unknowns
  dt_pred <- filter_for_prediction(dt_full_dummy)   #filter all that have a result (target 0 or 1) or that were in initial population
  predictions <- predict_values(dt_pred, model)
  
  #create a list of top lift records  (should send 1000 and 3500, but sending more to let Dries cut through some more.)
  dt_full_pred <- cbind(dt_pred, as.data.table(predictions$data)[,prob.1])
  dt_full_pred <- merge(dt_full_pred, dt_full[,.(id,initial_pop, excluded)], by="id", all.x = TRUE)
  setnames(dt_full_pred , "V2", "Prob.positive")
  
  dt_full_pred <- dt_full_pred[excluded == FALSE,]
  pred.F <- head(dt_full_pred[order(-Prob.positive) & language_code_F == 1, id], 750) %>% as.data.table()
  pred.N <- head(dt_full_pred[order(-Prob.positive) & language_code_N == 1, id], 2625) %>% as.data.table()
  
  
  #debug sense check top ranked for Shapley 
  pred_predictor <- Predictor$new(model = model, data = as.data.frame(dt_pred), y= "target",  type="prob")
  pred_shapley_explain <- iml::Shapley$new(pred_predictor, x.interest = as.data.frame(dt_pred)[1,], sample.size = 100)
  View(as.data.table(pred_shapley_explain$results)[order(feature),])
  plot(pred_shapley_explain)
  
  #create a list of random records
  placebo.samples <- dt_full_pred[sample( nrow(dt_full_pred), 2000) , .(id, language_code_N, language_code_F)] #250
  placebo.F <- head(placebo.samples[language_code_F == 1, id], 250) %>% as.data.table()
  placebo.N <- head(placebo.samples[language_code_N == 1, id], 875) %>% as.data.table()
  
  #store all outputs for future reference
  pred.F %>%    as.data.frame() %>%write_fst(path = file.path("data", paste0("batch1_20190401.FR.predictions", ".fst")))
  pred.N %>%    as.data.frame() %>%write_fst(path = file.path("data", paste0("batch1_20190401.NL.predictions", ".fst")))
  placebo.F %>% as.data.frame() %>% write_fst(path = file.path("data", paste0("batch1_20190401.FR.placebo", ".fst")))
  placebo.N %>% as.data.frame() %>%write_fst(path = file.path("data", paste0("batch1_20190401.NL.placebo", ".fst")))
  
  
  #produce a mix of test set and predictions 
  rbindlist(list(placebo.F, pred.F), use.names = TRUE, fill = FALSE, idcol=NULL) %>% as.data.frame() %>% write_fst( path = file.path("data", paste0("batch1_20190401.FR.prospects", ".fst")))
  rbindlist(list(placebo.N, pred.N), use.names = TRUE, fill = FALSE, idcol=NULL) %>% as.data.frame() %>% write_fst( path = file.path("data", paste0("batch1_20190401.NL.prospects", ".fst")))
  rbindlist(list(placebo.F, pred.F), use.names = TRUE, fill = FALSE, idcol=NULL) %>% as.data.frame() %>% write_csv2(path = file.path("data", paste0("batch1_20190401.FR.prospects", ".csv")))
  rbindlist(list(placebo.N, pred.N), use.names = TRUE, fill = FALSE, idcol=NULL) %>% as.data.frame() %>% write_csv2(path = file.path("data", paste0("batch1_20190401.NL.prospects", ".csv")))
  
  
}  



# start of procedure ->
#test if precomputed enriched coplaintiffs file exists in memory or on disk, else error message
if(!is.data.table(enriched_coplaintiffs))
{
  if(!is.data.table(try(enriched_coplaintiffs <- readCsvFromDirectory("coplaintiffs_for_model","data"))))
  {
    print('file coplaintiffs_for_model.csv missing in current batch data folder. please run module profilingDonateurs.R to regenerate file')
  }
}


# A) build labels
  # start with a simple binary classification: TRUE if donateur, FALSE if not 
  # TO DO: extend to multi class model with different groups for recurrent payment vs one shot vs none 
  enriched_coplaintiffs[,target:=as.integer(is.na(id))]

# build features
  enriched_coplaintiffs<- profiling(enriched_coplaintiffs)
  #TO DO
  
  #clean all unnecessary variables for model
  dt_for_model <- enriched_coplaintiffs[,.(target
                                           ,profession
                                           ,language
                                           ,newsletter
                                           ,is.na(sms_code)
                                           ,age_group
                                           ,region
                                           ,province)]
# build model
  main_train_classification(dt_for_model)

# run predictions