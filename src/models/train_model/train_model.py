import os
import sys

current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = current_dir[:current_dir.find("src") - 1]
sys.path.insert(0, project_root)

import pandas as pd
import xgboost as xgb
from sklearn.ensemble import RandomForestRegressor
from src.models.data_selection.data_selector import Data_selector
from src.models.data_selection.feature_selector import Feature_selector
from logs.logger import CustomLogger
from joblib import dump, load
from src.models.utils import *

logger = CustomLogger(name="train_model").get_logger()


def get_y_inverse_mimo(df, feature_selector, name_code_df, n_mimo, ys):
    if n_mimo == 1:
        return np.array(ys)

    dic = feature_selector.name_code_dictionary_index
    ys = np.array(ys)
    n = n_mimo
    ds = Data_selector(df.reset_index(drop=True))
    series_y = pd.Series([np.nan] * len(df))
    name_column = name_code_df.columns[0]
    code_column = name_code_df.columns[1]

    power_plants = df[['name', 'code']].drop_duplicates()
    for _, row in power_plants.iterrows():
        df_name_code = ds.filter_name_code(row["name"], row["code"])
        y_name_code = (ys[(name_code_df[name_column] == row["name"]) & (name_code_df[code_column] == row["code"])])
        indexes = dic[(row["name"], row["code"])]

        ll = []
        z = 0
        for (i1, i2) in indexes:
            z += 1
            arr = y_name_code[i1:i2]
            m = i2 - i1
            l = [0] * (m + n - 1)
            for i in range(m):
                for j in range(n):
                    l[i + j] += arr[i, j]

            mn = min(m, n)
            for k in range(m + n - 1):
                kk = min(k + 1, m + n - (k + 1))
                l[k] /= min(kk, mn)

            ll += l
        try:
            series_y[df_name_code.index] = ll
        except:
            logger.error(
                f"error len(ll):{len(ll)}, len(series_y):{len(series_y)}, len(df_name_code):{len(df_name_code)}")
    return series_y.to_numpy()


def add_is_test_column(df, test_fraction=0.2, random_state=42):
    df_selected = Data_selector(df).select_peaks(goodness=3)
    np.random.seed(random_state)
    group_ids = df_selected.groupby(["name", "code", "interval_id"], sort=False).ngroup()
    n_groups = max(group_ids) + 1
    rand_values = np.random.rand(n_groups)
    df["is_test"] = pd.Series(rand_values[group_ids] < test_fraction, index=df_selected.index)


def select_dataset_features(df, base_features, lag_features, time_features, target):
    feature_selector = Feature_selector(df, target)
    feature_selector.select(features_to_select=base_features + lag_features + time_features)
    df_selected = feature_selector.df.copy()
    logger.info(f"Some features have been dropped successfully")
    return df_selected


def train_model(df_train, model_X_cols, n_mimo, save_model=False, save_model_folder=None):
    feature_selector = Feature_selector(df_train, target="generation")
    Xs_train, ys_train, name_code_df = feature_selector.get_X_and_y(n_mimo=n_mimo)
    Xs_train = Xs_train.reindex(columns=model_X_cols)
    Xs_train = Xs_train.fillna(False).infer_objects()

    model = xgb.XGBRegressor(objective='reg:squarederror', n_estimators=2000, max_depth=7, learning_rate=0.1)
    # model = RandomForestRegressor(n_estimators=n_est, max_depth=m_depth, random_state=42)
    model.fit(Xs_train, ys_train)
    logger.info(f"Model has been trained successfully")

    ys_pred = model.predict(Xs_train)

    y_pred = get_y_inverse_mimo(df_train, feature_selector, name_code_df, n_mimo, ys_pred)
    y_train = get_y_inverse_mimo(df_train, feature_selector, name_code_df, n_mimo, ys_train)

    rmse_error_train = compute_relative_rmse(y_pred, y_train)
    thresh_error_train = compute_threshold_error(y_pred, y_train)
    rmae_error = compute_relative_mae(y_pred, y_train)
    r2_score = compute_r2_score(y_pred, y_train)
    logger.info(f"Train rmse error: {rmse_error_train:0.3f}%")
    logger.info(f"Train threshold error: {thresh_error_train:0.3f}%")
    logger.info(f"Train rmae error: {rmae_error:0.3f}%")
    logger.info(f"R2 score: {r2_score:0.3f}%")

    if save_model and save_model_folder is not None:
        for filename in os.listdir(save_model_folder):
            os.remove(os.path.join(save_model_folder, filename))

        dump(model, f"{save_model_folder}/model.joblib")
        with open(f"{save_model_folder}/model_cols.pkl", 'wb') as f:
            dump(model_X_cols, f)


def test_model(model, df_test, model_X_cols, n_mimo):
    feature_selector = Feature_selector(df_test, target="generation")
    Xs_test, ys_test, name_code_df = feature_selector.get_X_and_y(n_mimo=n_mimo)
    Xs_test = Xs_test.reindex(columns=model_X_cols)
    Xs_test = Xs_test.fillna(False).infer_objects()

    ys_pred = model.predict(Xs_test)
    y_pred = get_y_inverse_mimo(df_test, feature_selector, name_code_df, n_mimo, ys_pred)
    y_test = get_y_inverse_mimo(df_test, feature_selector, name_code_df, n_mimo, ys_test)

    rmse_error_test = compute_relative_rmse(y_pred, y_test)
    thresh_error_test = compute_threshold_error(y_pred, y_test)
    rmae_error = compute_relative_mae(y_pred, y_test)
    r2_score = compute_r2_score(y_pred, y_test)
    logger.info(f"Test rmse error: {rmse_error_test:0.3f}%")
    logger.info(f"Test threshold error: {thresh_error_test:0.3f}%")
    logger.info(f"Test rmae error: {rmae_error:0.3f}%")
    logger.info(f"R2 score: {r2_score:0.3f}%")


def load_model(path):
    model = load(f"{path}/model.joblib")
    return model


def find_after_mimo_cols(df, n_mimo):
    feature_selector = Feature_selector(df, target="generation")
    Xs, _, _ = feature_selector.get_X_and_y(n_mimo=n_mimo)
    return Xs.columns


if __name__ == "__main__":
    csv_semi_processed_path = os.path.join(project_root, "data", "processed", "semi_processed.csv")
    df = pd.read_csv(csv_semi_processed_path, encoding='utf-8')

    add_is_test_column(df, random_state=42)

    save_model = True
    save_model_folder = os.path.join(project_root, "src", "models", "fitted_models")
    n_mimo = 4

    df_r_selected = Data_selector(df).select_peaks(goodness=3)

    base_features = ["name", "code", "temperature", "humidity", "dew", "surface_pressure", "value",
                     "forecast", "status"]
    features_with_lag = ["temperature", "humidity", "dew", "surface_pressure"]
    hours_delay = [1, 5]
    lag_features = [f"{feature}_with_{hour}_delay" for feature in features_with_lag for hour in hours_delay]
    # lag_features.append("generation_with_24_delay")
    time_features = ["hour", "day_of_week", "month", "season", "datetime"]
    df_f_selected = select_dataset_features(df_r_selected, base_features, lag_features, time_features, "generation")

    model_X_cols = find_after_mimo_cols(df_f_selected, n_mimo)

    train_indices = (df_r_selected['is_test'] == False)
    train_df = df_f_selected[train_indices]
    train_model(train_df, model_X_cols, n_mimo, save_model=save_model, save_model_folder=save_model_folder)

    model = load_model(save_model_folder)

    test_indices = (df_r_selected['is_test'] == True)
    test_df = df_f_selected[test_indices]
    test_model(model, test_df, model_X_cols, n_mimo)
