import sys, os

current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = current_dir[:current_dir.find("src") - 1]
sys.path.insert(0, project_root)

import pandas as pd
from data_selector import Data_selector
from feature_adder import Feature_adder
from feature_selector import Feature_selector
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


def test_model(model, do_inverse_scale=True):
    rmse_error_train, rmse_error_test = model.rescale_and_compute_error(do_inverse_scale)
    logger.info(f"Train Error: {rmse_error_train:0.2f}%, Test Error: {rmse_error_test:0.2f}%")


def write_result(df, model, X):
    csv_write_path = os.path.join(project_root, "data", "processed", "data_for_plot.csv")
    df.loc[X.index, "prediction"] = model.pred(X)
    df.to_csv(csv_write_path, index=False)


def select_features_and_get_X_and_y(df, is_mimo=False, number_mimo=None):
    feature_selector = Feature_selector(df, target="generation")
    features_to_be_select = ["name", "code", "temperature", "humidity", "dew", "surface_pressure", "value", "forecast",
                             "status", "season"] + ["datetime"]
    # for hour in range(1, 4):
    #     features_to_be_select.append(f"generation_with_{hour}_delay")
    features_to_be_select.append(f"generation_with_{24}_delay")
    feature_selector.select(features_to_select=features_to_be_select)
    X, y = feature_selector.get_X_and_y(is_mimo=is_mimo, number_mimo=number_mimo)
    return X, y


def run_recursive_XGBoost_model(l_min, max_diff, c_thresh, n, n_mimo_final):
    df = add_features_and_filter(l_min, max_diff, c_thresh, read_from_integrated=False)
    logger.info(f"Csv file has bean labeled successfully")

    for i in range(1, n + 1):
        add_semi_pred(df, semi_iter=i)

    is_mimo = n_mimo_final > 1
    y_is_flat = not is_mimo

    ds = Data_selector(df)
    df_modified = ds.select_peaks(goodness=3)
    logger.info(f"Final model: Rows have been selected successfully")

    feature_selector = Feature_selector(df_modified, target="generation")
    features_to_be_select = ["name", "code", "temperature", "humidity", "dew", "surface_pressure", "value", "forecast",
                             "status", "season"] + ["datetime"]
    features_to_be_select.append(f"semi_prediction{n}")
    features_to_be_select.append(f"generation_with_{24}_delay")
    feature_selector.select(features_to_select=features_to_be_select)
    X, y = feature_selector.get_X_and_y(is_mimo=is_mimo, number_mimo=n_mimo_final)
    logger.info(f"Some features have been dropped successfully")

    model = XGBoost(n_estimators=2000, max_depth=7)
    model.scale_and_split_data(X, y, y_is_flat=y_is_flat, do_scale=False, random_state=42)
    model.fit()
    logger.info(f"Model has been trained successfully")
    test_model(model, do_inverse_scale=False)


def add_semi_pred(df, semi_iter):
    number_mimo = 1
    is_mimo = number_mimo > 1
    y_is_flat = not is_mimo

    feature_selector = Feature_selector(df, target="generation")
    features_to_be_select = ["name", "code", "temperature", "humidity", "dew", "surface_pressure", "value",
                             "forecast", "status", "season", "is_good_peak", "datetime"]
    if semi_iter > 1:
        features_to_be_select.append(f"semi_prediction{semi_iter - 1}")
    features_to_be_select.append(f"generation_with_{24}_delay")
    feature_selector.select(features_to_select=features_to_be_select)
    X, y = feature_selector.get_X_and_y(is_mimo=is_mimo, number_mimo=number_mimo)
    logger.info(f"Some features have been dropped successfully")
    ds = Data_selector(X)
    X_filtered = ds.select_peaks(goodness=3)
    y_filtered = y[X_filtered.index]
    model = XGBoost(n_estimators=2000, max_depth=7)
    model.scale_and_split_data(X_filtered, y_filtered, y_is_flat=y_is_flat, do_scale=False, random_state=42)
    model.fit()
    logger.info(f"Partial model {semi_iter} has been trained successfully")
    df[f"semi_prediction{semi_iter}"] = model.pred(X, do_scale=False)


if __name__ == "__main__":
    # TODO: for mimo > 1 doesn't work
    write_predictions = False

    l_min = 4
    max_diff = 3
    c_thresh = 0.9

    number_mimo = 1
    is_mimo = number_mimo > 1
    y_is_flat = not is_mimo

    df = add_features_and_filter(l_min, max_diff, c_thresh, read_from_integrated=False)
    logger.info(f"Csv file has bean labeled successfully")

    ds = Data_selector(df)
    df_modified = ds.select_peaks(goodness=3)
    logger.info(f"Rows have been selected successfully")

    X, y = select_features_and_get_X_and_y(df_modified, is_mimo=is_mimo, number_mimo=number_mimo)
    logger.info(f"Some features have been dropped successfully")

    # model = Random_Forest(n_estimators=100, max_depth=1000)
    # model = Linear()
    # model = Polynomial(degree=2)
    # model = XGBoost(n_estimators=1000, max_depth=5)
    # model = Neural_network(input_dim=X.shape[1], epochs=100, verbose=1)

    model = XGBoost(n_estimators=1000, max_depth=5)
    model.scale_and_split_data(X, y, y_is_flat=y_is_flat, do_scale=False)
    model.fit()
    logger.info(f"Model has been trained successfully")

    test_model(model, do_inverse_scale=False)

    if write_predictions:
        write_result(df, model, X)
    run_recursive_XGBoost_model(l_min, max_diff, c_thresh, n=4, n_mimo_final=4)
