import sys, os

current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = current_dir[:current_dir.find("src") - 1]
sys.path.insert(0, project_root)

import pandas as pd
import numpy as np
from data_selector import Data_selector
from feature_adder import Feature_adder
from logs.logger import CustomLogger
from models import Random_Forest, Linear, Polynomial, XGBoost, LinearL1, Neural_network

logger = CustomLogger(name="model_main", log_file_name='model_main.log').get_logger()

def add_features_and_filter(l_min, max_diff, c_thresh, read_from_integrated=False, write_on_csv=None):
    if write_on_csv == None: write_on_csv = read_from_integrated

    csv_read_path = os.path.join(project_root, "data", "processed", "integrated.csv")
    csv_semi_write_path = os.path.join(project_root, "data", "processed", "semi_processed.csv")

    if read_from_integrated:
        df = pd.read_csv(csv_read_path, encoding='utf-8')
        feature_adder = Feature_adder(df)
        feature_adder.create_feature_with_delay("temperature", 5)
        for hour in range(1, 4):
            feature_adder.create_feature_with_delay("generation", hour)
        feature_adder.create_feature_with_delay("generation", 24)
        feature_adder.filter1()
        feature_adder.filter2(l_min=l_min, max_diff=max_diff)
        feature_adder.filter3("temperature_with_5_delay", c_thresh=c_thresh, plot_pearsons_hist=True)
        df = feature_adder.df
        if write_on_csv:
            df.to_csv(csv_semi_write_path, index=False)
    else:
        df = pd.read_csv(csv_semi_write_path, encoding='utf-8')

    return df

if __name__ == "__main__":
    
    l_min = 4
    max_diff = 3
    c_thresh = 0.9

    add_features_and_filter(l_min, max_diff, c_thresh, read_from_integrated=False)