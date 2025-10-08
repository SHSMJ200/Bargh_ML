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

    feature_adder = Feature_adder(df)
    for feature in ["temperature", "humidity", "dew", "surface_pressure"]:
        for hour in [1, 5]:
            feature_adder.create_feature_with_delay(feature, hour)

    feature_adder.create_feature_with_delay("generation", 24)
    # feature_adder.df.dropna(inplace=True)

    feature_adder.filter1()
    feature_adder.filter2(l_min=l_min, max_diff=max_diff)
    feature_adder.filter3("temperature", c_thresh=c_thresh)
    feature_adder.add_interval_id()
    feature_adder.filter5()

    feature_adder.df.to_csv(csv_semi_write_path, index=False)


if __name__ == "__main__":
    l_min = 4
    max_diff = 3
    c_thresh = 0.9

    add_features_and_filter(l_min, max_diff, c_thresh)
