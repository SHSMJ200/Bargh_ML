import sys, os

current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = current_dir[:current_dir.find("src") - 1]
sys.path.insert(0, project_root)

import pandas as pd
from data_selector import Data_selector
from feature_modifier import Feature_selector, Feature_adder
from logs.logger import CustomLogger
from models import Random_Forest, Linear

logger = CustomLogger(name="model_main", log_file_name='model_main.log').get_logger()

if __name__ == "__main__":
    csv_path = os.path.join(project_root, "data", "processed", "integrated.csv")
    df = pd.read_csv(csv_path, encoding='utf-8')
    logger.info(f"Csv file has bean read successfully")

    feature_adder = Feature_adder(df)
    feature_adder.add_season()
    feature_adder.create_feature_with_delay("temperature", 1)
    feature_adder.create_feature_with_delay("temperature", 2)
    feature_adder.create_feature_with_delay("temperature", 3)
    logger.info(f"Some features have been added successfully")

    data_selector = Data_selector(feature_adder.df)
    df_modified = data_selector.select(m_in_summer=True)
    logger.info(f"Rows have been selected successfully")
    
    feature_selector = Feature_selector(df_modified, "generation")
    # feature_to_be_dropped = ['id', 'hour', 'date', 'status', 'declare']
    feature_to_be_dropped = ['id', 'date', 'declare', 'dew', 'apparent_temperature', 'rain', 'snow',
                             'evapotransporation', 'wind_direction', 'require']
    X, y = feature_selector.select(feature_to_be_dropped)
    logger.info(f"Features have been dropped successfully")

    n_est = 100
    depth = 45

    #model = Random_Forest()
    #model.scale_and_split_data(X, y)
    #model.fit(n_estimators=n_est, max_depth=depth)
    model = Linear()
    model.scale_and_split_data(X, y)
    model.fit()
    
    logger.info(f"Model has been trained successfully")

    mse_train_actual, mse_test_actual = model.compute_mse_error()
    print(f"Train Error: {mse_train_actual}%")
    print(f"Test Error: {mse_test_actual}%")
