import os
import sys

current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = current_dir[:current_dir.find("src") - 1]
sys.path.insert(0, project_root)

import pandas as pd
from src.models.filter_data.feature_adder import Feature_adder
from logs.logger import CustomLogger

logger = CustomLogger(__name__).get_logger()

split_dates_by_name_code = {
    ('سبلان', 'G11'): pd.Timestamp('2021-10-31'),
    ('سبلان', 'G14'): pd.Timestamp('2022-05-01'),

    ('سیکل ترکیبی ارومیه', 'G11'): pd.Timestamp('2022-01-31'),

    ('سیکل ترکیبی یزد', 'G11'): pd.Timestamp('2021-08-01'),
    ('سیکل ترکیبی یزد', 'G15'): pd.Timestamp('2022-03-01'),

    ('شهدای پاکدشت - دماوند', 'G13'): pd.Timestamp('2022-06-01'),
    ('شهدای پاکدشت - دماوند', 'G22'): pd.Timestamp('2022-07-01'),

    ('عسلویه', 'G11'): pd.Timestamp('2023-07-01'),
    ('عسلویه', 'G12'): pd.Timestamp('2022-03-01'),

    ('قم', 'G11'): pd.Timestamp('2021-06-01'),

    ('پرند', 'G11'): pd.Timestamp('2022-03-31'),
    ('پرند', 'G12'): pd.Timestamp('2022-05-01'),

    ('گیلان', 'G15'): pd.Timestamp('2022-05-01'),
}


def add_features_and_filter(df, l_min, max_diff, c_thresh, bin_length, temp_feature):
    feature_adder = Feature_adder(df, temp_feature)
    feature_adder.select_gas_plants()
    feature_adder.filter1()
    feature_adder.filter2(initial_label=1)
    # feature_adder.filter3(l_min=l_min, max_diff=max_diff, initial_label=2)
    # feature_adder.filter4(c_thresh=c_thresh, initial_label=3)
    # feature_adder.filter5(initial_label=4)
    feature_adder.new_filter_5(split_dates_by_name_code, initial_label=2)
    feature_adder.filter6(bin_length, initial_label=5)
    # feature_adder.new_filter_6(bin_length, initial_label=5)

    return feature_adder.df


if __name__ == "__main__":
    l_min = 4
    max_diff = 3
    c_thresh = 0.9
    bin_length = 100
    temp_feature = "temp_sens"

    csv_read_path = os.path.join(project_root, "data", "processed", "integrated.csv")
    df = pd.read_csv(csv_read_path, encoding='utf-8')

    necessary_features = ['name', 'code', "date", "hour", "generation", f"{temp_feature}", "load_level", "status"]
    df.dropna(subset=necessary_features, inplace=True)
    filtered_df = add_features_and_filter(df, l_min, max_diff, c_thresh, bin_length, temp_feature)

    csv_semi_write_path = os.path.join(project_root, "data", "processed", "semi_processed.csv")
    filtered_df.to_csv(csv_semi_write_path, index=False)
