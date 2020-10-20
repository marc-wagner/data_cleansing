# Data preparation for predicting donateurs from coplaintiff DB.

# prerequisite: run profilingDonateurs.R to obtain data.table enriched_coplaintiffs.
# on 2020 09, there were 61757 records in this data table:
# 57666 deduplicated co-plaintiffs, matched with 6934 unique donateurs.

#import libaries
source('dependencies.R')
source('profiling.R')
source('geocode.R')

#function declarations

sample_test_train_set <- function(dt, balance){
  ## PRE PROCESSING: create Train and Test set
  try(dt[,data_set := NULL])
  
  #positive train set
  size_positives_train <- round(nrow(dt[target == 1, ])*parameters$train_test_ratio,0)
  positive_rows <- which(dt$target == 1 )
  train_positive_rows <- sample(positive_rows, size_positives_train)
  dt[train_positive_rows, data_set:= "train"]
  
  #positive test set
  dt[is.na(data_set) & target == 1 , data_set:= 'test' ]
  
  #negative train set : either half the data or according to balance setting  (0.5 balance = even classes in train set)
  size_negatives_train <- round(min(nrow(dt[target == 0  , ]), balance * 4 * size_positives_train)* parameters$train_test_ratio ,0)
  negative_rows <- which(dt$target == 0 )
  train_negative_rows <- sample(negative_rows, size_negatives_train)
  dt[train_negative_rows, data_set := 'train' ]

  #negative train set
  dt[is.na(data_set) & target == 0 , data_set:= 'test' ]
  
  dt
}

convert_to_model_dt <- function(dt) {
  numeric_columns <- colnames(dt)[vapply(dt, is.numeric, TRUE)]
  
  dt %>% 
    .[, ..numeric_columns] %>% 
    .[, target := as.factor(target)] %>% 
    as.data.frame()
}

debugClassificationModel <-function(dt,model){
  #debug model start
  browser()
  final_model_raw <- mlr::getLearnerModel(model)
  xgboost::xgb.plot.multi.trees(model = final_model_raw)
  xgboost::xgb.plot.tree(model = final_model_raw, trees = 1)
  xgb_importances <- xgboost::xgb.importance(model = final_model_raw) 
  xgboost::xgb.plot.importance(xgb_importances ,top_n = 10)
  
  debug_predictions <- predict_values(dt, model)
  pred.perf <- assess_predictive_performance(model, debug_predictions, dt)
  print(pred.perf)
  
}

train_model <- function(dt){
  
  dt_train <- copy(dt[data_set == 'train',])
  
  train_task <- mlr::makeClassifTask(id = "classif_donateurs",
                                     data = convert_to_model_dt(dt_train),
                                     target = "target",
                                     positive = "1"  )
  
  par.vals = list(
    eta = 0.005,
    max_depth = 5,
    verbose = 0,
    colsample_bytree = 0.85,
    subsample = 0.7,
    alpha = 2,
    nrounds = 50,
    eval_metric = list("auc") # list("logloss","auc")
  )
  
  learner <- mlr::makeLearner("classif.xgboost", predict.type = "prob", par.vals = par.vals, verbose=0)
  
  cv <- mlr::makeResampleDesc("CV",iters=5)
  res <- mlr::resample(learner, train_task, cv, measures = list( auc), show.info = FALSE)
  mlr::train(learner = learner, task= train_task)
}

predict_values <- function(dt, model){
  dt_test <- dt[data_set == 'test',]
  pred_task <- mlr::makeClassifTask(id = "predict_donateurs",
                                    data = convert_to_model_dt(dt_test),
                                    target = "target" ,
                                    positive = "1"  )
  predict(model, task=pred_task )
}

assess_predictive_performance <- function(model, predictions, dt){
  
  #TO DO fix regression in classifierplots library due to R 3 upgrade.
  classifierplots::roc_plot(predictions$data$truth, predictions$data$prob.1)
  classifierplots::density_plot(predictions$data$truth, predictions$data$prob.1)
  #classifierplots(pred.prob = predictions$data[,]$prob.1, test.y = predictions$data[,]$truth)
  #classifierplots::lift_plot(pred.prob = predictions$data[,]$prob.1, test.y = predictions$data[,]$truth, granularity = 0.001)
  prd_perf <- rbindlist(list(
    data.table(measure = 'auc', value = performance(predictions, auc) )
    ,data.table(measure = 'logloss', value = performance(predictions, logloss) )
  ), fill = FALSE )
  
  train_task <- mlr::makeClassifTask(id = "classif_donateurs",
                                     data = convert_to_model_dt(dt),
                                     target = "target",
                                     positive = "1"  )
  
  par.vals = list(
    eta = 0.005,
    max_depth = 5,
    verbose = 0,
    colsample_bytree = 0.85,
    subsample = 0.7,
    alpha = 2,
    nrounds = 50,
    eval_metric = list("auc", "logl")
  )
  
  learner <- mlr::makeLearner("classif.xgboost", predict.type = "prob", par.vals = par.vals, verbose=0)
  
  cv <- mlr::makeResampleDesc("CV",iters=5)
  res <- mlr::resample(learner, train_task, cv, measures = list(logloss, auc), show.info = FALSE)
  
  data.table( train.auc = res$aggr["auc.test.mean"] , test.auc = prd_perf[measure =="auc", value], train.logloss = res$aggr["logloss.test.mean"], test.logloss = prd_perf[measure=="logloss", value] )
}


main_train_classification <- function(dt_full){
  
  browser()
    set.seed(123) #reset seed
    dt_baseline <- sample_test_train_set(dt_full, balance = 0.5)
    
    model <- train_model(dt_baseline) #hardcoded xgboost, with specific hyperparameters
    
    if(parameters$debug==1) debugClassificationModel(dt_baseline, model) 
    
    predictions <- predict_values(dt_baseline, model)
  
  #  TO DO fix 2 plots below. commented out below because of this error
  #  "Error in alpha * 255 : argument non numérique pour un opérateur binaire"
  #  print(assess_predictive_performance(model, predictions, dt_baseline))
  #  classifierplots::lift_plot(pred.prob = predictions$data[,]$prob.1, test.y = predictions$data[,]$truth, granularity = 0.001)
    
    saveRDS(model, "model_B2B_prospect.rds")
    
  #debug feature importance
  # need to rebuild training task first
  imp = generateFeatureImportanceData(train_task, "permutation.importance",
                                        learner, nmc = 10L, local = TRUE)
  print(imp)
    
  #debug sense check top ranked for Shapley 
  dt_pred <- copy(dt_baseline[data_set== 'test',])
  pred_predictor <- iml::Predictor$new(model = model, data = convert_to_model_dt(dt_pred), y= "target",  type="prob")
  pred_shapley_explain <- iml::Shapley$new(pred_predictor, x.interest = convert_to_model_dt(dt_pred)[1,], sample.size = 100)
  View(as.data.table(pred_shapley_explain$results)[order(feature),])
  plot(pred_shapley_explain)
  
}  



# start of procedure ->
#test if precomputed enriched coplaintiffs file exists in memory or on disk, else error message
if(!is.data.table(try(enriched_coplaintiffs)))
{
  if(!is.data.table(try(enriched_coplaintiffs <- readCsvFromDirectory("coplaintiffs_for_model","data"))))
  {
    print('file coplaintiffs_for_model.csv missing in current batch data folder. please run module profilingDonateurs.R to regenerate file')
  }
}


# A) build labels
  # start with a simple binary classification: TRUE if donateur, FALSE if not 
  # TO DO: extend to multi class model with different groups for recurrent payment vs one shot vs none 
  enriched_coplaintiffs[,target:=1-as.integer(is.na(id))]

# build features
  enriched_coplaintiffs<- profiling(enriched_coplaintiffs)
  #TO DO
  
  #clean all unnecessary variables for model
  dt_for_model <- enriched_coplaintiffs[,.(target
                                           ,profession
                                           ,language
                                           ,newsletter
                                           ,valid_phone
                                           ,age_group
                                           ,region
                                           ,province)]
# build model
  main_train_classification(dt_for_model)

# run predictions