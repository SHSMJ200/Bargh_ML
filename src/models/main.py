import sys, os

current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = current_dir[:current_dir.find("src") - 1]
sys.path.insert(0, project_root)

import pandas as pd
from data_selector import Data_selector
from feature_adder import Feature_adder
from feature_selector import Feature_selector
from logs.logger import CustomLogger
from models import Random_Forest, Linear, Polynomial, XGBoost, LinearL1

logger = CustomLogger(name="model_main", log_file_name='model_main.log').get_logger()


def add_features_and_filter(l_min, max_diff, c_thresh, read_from_integrated=False, write_on_csv=False):
    csv_read_path = os.path.join(project_root, "data", "processed", "integrated.csv")
    csv_semi_write_path = os.path.join(project_root, "data", "processed", "semi_processed.csv")

    if read_from_integrated:
        df = pd.read_csv(csv_read_path, encoding='utf-8')
        feature_adder = Feature_adder(df)
        feature_adder.create_feature_with_delay("temperature", 5)
        feature_adder.filter1()
        feature_adder.filter2(l_min=l_min, max_diff=max_diff)
        feature_adder.filter3("temperature_with_5_delay", c_thresh=c_thresh)
        df = feature_adder.df
        if write_on_csv:
            df.to_csv(csv_semi_write_path, index=False)
    else:
        df = pd.read_csv(csv_semi_write_path, encoding='utf-8')

    return df


def test_model(model):
    rmse_error_train, rmse_error_test = model.compute_rmse_error()
    print(f"Train Error: {rmse_error_train:0.2f}%, Test Error: {rmse_error_test:0.2f}%")


def write_result(df, model, X):
    csv_write_path = os.path.join(project_root, "data", "processed", "data_for_plot.csv")
    df.loc[X.index, "prediction"] = model.pred(X)
    df.to_csv(csv_write_path, index=False)


def select_features_and_get_X_and_y(df):
    feature_selector = Feature_selector(df, "generation")
    feature_to_be_select = ["name", "code", "temperature", "humidity", "surface_pressure", "value", "forecast",
                            "generation", "status"]
    feature_selector.select(feature_to_select=feature_to_be_select)
    X, y = feature_selector.get_X_and_y()
    return X, y


if __name__ == "__main__":
    write_predictions = False
    l_min = 4
    max_diff = 3
    c_thresh = 0.9
    df = add_features_and_filter(l_min, max_diff, c_thresh, read_from_integrated=False, write_on_csv=False)
    logger.info(f"Csv file has bean labeled successfully")

    ds = Data_selector(df)
    dfc = ds.filter_name_code(name="پرند", code="G11")
    ds = Data_selector(dfc)
    df_modified = ds.select_peaks(goodness=3)
    logger.info(f"Rows have been selected successfully")

    X, y = select_features_and_get_X_and_y(df_modified)
    logger.info(f"Some features have been dropped successfully")

    # model = Random_Forest(n_estimators=100, max_depth=30)
    model = Linear()
    # model = Polynomial()
    # model = XGBoost(n_estimators=1000, max_depth=3)
    model.scale_and_split_data(X, y)
    model.fit()
    logger.info(f"Model has been trained successfully")
    test_model(model)

    if write_predictions:
        write_result(df, model, X)
