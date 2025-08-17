import sys, os

current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = current_dir[:current_dir.find("src") - 1]
sys.path.insert(0, project_root)

import pandas as pd
from data_selector import Data_selector
from feature_selector import Feature_selector
from logs.logger import CustomLogger
from models import Random_Forest, Linear, Neural_network

logger = CustomLogger(name="model_main", log_file_name='model_main.log').get_logger()

if __name__ == "__main__":
    csv_path = os.path.join(project_root, "data", "processed", "with_prediction.csv")
    df = pd.read_csv(csv_path, encoding='utf-8')

    data_selector = Data_selector(df)
    df_modified = data_selector.select()

    feature_selector = Feature_selector(df_modified, "generation")
    feature_to_be_dropped = ['id', 'hour', 'date', 'status', 'declare']
    X, y = feature_selector.select(feature_to_be_dropped)

    n_est = 100
    depth = 37

    # model = Neural_network()
    # model = Random_Forest()
    model = Linear()

    model.scale_and_split_data(X, y)

    # model.fit(n_estimators=n_est, max_depth=depth)
    model.fit()

    mse_train_actual, mse_test_actual = model.compute_mse_error()
    print(f"Train Error: {mse_train_actual}%")
    print(f"Test Error: {mse_test_actual}%")
