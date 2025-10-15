import os
import sys

current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = current_dir[:current_dir.find("src") - 1]
sys.path.insert(0, project_root)

import pandas as pd
from src.models.filter_data.feature_adder import Feature_adder
from logs.logger import CustomLogger

logger = CustomLogger(__name__).get_logger()


def add_features_and_filter(l_min, max_diff, c_thresh):
    csv_read_path = os.path.join(project_root, "data", "processed", "integrated.csv")
    csv_semi_write_path = os.path.join(project_root, "data", "processed", "semi_processed.csv")

    df = pd.read_csv(csv_read_path, encoding='utf-8')
    df.dropna(subset=["generation"],inplace=True)
    
    feature_adder = Feature_adder(df)
    feature_adder.filter1()
    feature_adder.filter2(l_min=l_min, max_diff=max_diff, initial_label=1)
    feature_adder.filter3("temperature", c_thresh=c_thresh, initial_label=2)
    feature_adder.add_interval_id(initial_label=3)
    feature_adder.filter4(initial_label=3)
    feature_adder.filter5(initial_label=4)

    feature_adder.df.to_csv(csv_semi_write_path, index=False)


if __name__ == "__main__":
    l_min = 4
    max_diff = 3
    c_thresh = 0.9

    add_features_and_filter(l_min, max_diff, c_thresh)
