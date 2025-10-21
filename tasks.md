🟩 To-Do
==========================

* Make product
* Why LF1?
* Output & input format (?)
* LF1-FO (?)
* Tune hyperparameters.
* Compare with declare
* Loggers
* Generation vs sen_temperature was good. How about other features?
* How about filter when generation is equal to declare?
* We have a and b of each plant
* try/ except
* generation = 250???
* time = 25???
* فرض: در جداول آن‌ها تغییر ساعت لحاظ نشده و همیشه +3:30 نوشته شده است
* فرض : ساعت ۱۵ یعنی از ساعت ۱۴ تا ۱۵
* We delete data that doesn't have "SCADAF" temp in cleaning
* We delete row that have even just one null value
* I have removed "season" column
* We have different set of (id, code) in different files some are "S" but شهدای پیروز G13 and G14 aren't in all files.
* visualize
* 

  🟨 Doing
  ==========================

* 

  🟥 Done
  ==========================

* Installing postgres
* Connecting postgres to my project
* Connect Database to Superset
* Edit superset_guide file
* Understanding the code in Logger
* Cleaning the codes in crawler
* Understanding and running the code in data
* Train a simple model on data
* Delete unnecessary files in the model folder
* Review/study the visualization files
* Divide into 3 separate module: 1. data selecting 2. feature selecting 3. train and fit the model
* Clean the codes in folders: models & visualization
* Connect to GitHub
* Edit function feature_over_time to plot more beautiful
* Choose the training data better. not just peak, but also M in summer and ...
* What is "require"?
* Add column color to plot better
* Check mse_computer
* Choose which features should be in the model training.
* Pay attention to affecting with delay. For example: temperature.
* How about generation with delay
* Add change point detection and choose the training data better
* Why null data in temperature??
* Edit "seasonality.yaml" file. It's very bad structured!
* Edit "tables_columns.yaml" file. Why each name is repeated twice?
* Accuracy of base model
* Pred or predict???
* Run Neural Network
* Why different loss between Neural Net error and our manual RMSE error
* From the beginning of Khordad, mid-load can be considered.
* The difference between our production changes and what was mentioned in the meeting.
* Give interval to the model and get interval.
* transfer new model from jupyter to python
* more than 24 rows of data in one day
* write predictions with mimo > 1
* check residual 'y' for new model
* Define two different accuracy.
* correct accuracy on flat y
* How to use model for prediction
* Presentation
* Is there any name, code which don't have any interval in is_good_peak = 3???? Yes there is but the problem is related to filter1. No problem just mention it.
* Clean the code
* 
