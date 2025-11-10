import os
import sys

current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = current_dir[:current_dir.find("src") - 1]
sys.path.insert(0, project_root)

import xgboost as xgb
from sklearn.linear_model import LinearRegression
from joblib import dump

from src.models.data_selection.data_selector import Data_selector
from src.models.data_selection.feature_selector import Feature_selector
from logs.logger import CustomLogger
from src.models.utils import *

logger = CustomLogger(__name__).get_logger()


def select_data(goodness):
    csv_semi_processed_path = os.path.join(project_root, "data", "processed", "semi_processed.csv")
    df = pd.read_csv(csv_semi_processed_path, encoding='utf-8')

    df_r_selected = Data_selector(df).select_peaks(goodness)
    logger.info(f"Rows with goodness={goodness} have been selected")
    return df_r_selected


def select_features(df_r_selected, features, target="generation"):
    feature_selector = Feature_selector(df_r_selected, target=target)
    feature_selector.filter_features(features_to_select=features)
    df_f_selected = feature_selector.df
    logger.info(f"Features have been selected successfully")
    return df_f_selected


def train_and_test_model(X, y, folder_path, name, code):
    X_train, X_test, y_train, y_test = split_X_and_y(X, y, test_size=0.2, shuffle=False)

    # model = xgb.XGBRegressor(objective='reg:squarederror', n_estimators=100, max_depth=3, learning_rate=0.1)
    model = LinearRegression()
    model.fit(X_train, y_train)
    dump(model, f"{folder_path}/model_{name}_{code}.joblib")

    y_pred_train = model.predict(X_train)
    rmse_error_train = compute_relative_rmse(y_pred_train, y_train)

    y_pred_test = model.predict(X_test)
    rmse_error_test = compute_relative_rmse(y_pred_test, y_test)
    return rmse_error_train, rmse_error_test


def print_report(data_sizes, train_errors, test_errors):
    data_sizes = np.array(data_sizes)
    train_errors = np.array(train_errors)
    test_errors = np.array(test_errors)

    weighted_train_error = np.average(train_errors, weights=data_sizes)
    weighted_test_error = np.average(test_errors, weights=data_sizes)

    logger.info(f"Weighted train error: {weighted_train_error:0.3f}")
    logger.info(f"Weighted test error: {weighted_test_error:0.3f}")


if __name__ == "__main__":
    goodness_to_select = 6
    save_model_folder = os.path.join(project_root, "src", "models", "fitted_models")

    df_r_selected = select_data(goodness_to_select)

    temp_feature = "temp_sens"
    features = ["name", "code", f"{temp_feature}"]
    df_f_selected = select_features(df_r_selected, features)

    ds_n_c = Data_selector(df_f_selected)
    ds_n_c.df = ds_n_c.df.dropna()
    power_plants = ds_n_c.df[['name', 'code']].drop_duplicates()
    train_errors, test_errors, data_sizes = [], [], []
    for row in power_plants.itertuples():
        name, code = row.name, row.code
        logger.info(f"Train and test data related to {name}_{code}:")

        df_n_c = ds_n_c.filter_name_code(name, code)
        if len(df_n_c) < 100: continue  # TODO: Must be deleted

        fs_n_c = Feature_selector(df_n_c, "generation")
        fs_n_c.filter_features(features_to_drop=["name", "code"])
        X, y = fs_n_c.get_X_and_y()

        train_error, test_error = train_and_test_model(X, y, save_model_folder, name, code)
        logger.info(
            f"Train rmse error: {train_error:.3f}%, Test rmse error: {test_error:.3f}% , Size of data: {len(y)}")

        train_errors.append(train_error)
        test_errors.append(test_error)
        data_sizes.append(len(y))

    print_report(data_sizes, train_errors, test_errors)
