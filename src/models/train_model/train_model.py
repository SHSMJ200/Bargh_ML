import sys, os

current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = current_dir[:current_dir.find("src") - 1]
sys.path.insert(0, project_root)

import pandas as pd
import numpy as np
import xgboost as xgb
from src.models.data_selector import Data_selector
from src.models.filter_data.feature_adder import Feature_adder
from src.models.feature_selector import Feature_selector
from logs.logger import CustomLogger
from joblib import dump, load
from src.models.utils import *

logger = CustomLogger(name="model_main").get_logger()

def get_y_inverse_mimo(df, number_mimo, name_code_df, y, dic):
    y = np.array(y)
    n = number_mimo
    ds = Data_selector(df.reset_index(drop=True))
    series_y = pd.Series([np.nan] * len(df))
    name_column = name_code_df.columns[0]
    code_column = name_code_df.columns[1]

    power_plants = df[['name', 'code']].drop_duplicates()
    for _, row in power_plants.iterrows():
        df_name_code = ds.filter_name_code(row["name"], row["code"])
        y_name_code = (y[(name_code_df[name_column] == row["name"]) & (name_code_df[code_column] == row["code"])])
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


def update_feature(Xs_train, feature, ys_pred, dictionary_columns):
    columns = [str(col) for col in dictionary_columns[feature]]
    Xs_train.loc[:, columns] = ys_pred


def train_model(df, n_recursive, n_mimo, save_model=False):
    df_train_modified = Data_selector(Data_selector(df).select_peaks(goodness=3)).select_train_test(is_test=True)
    base_features = ["name", "code", "temperature", "humidity", "dew", "surface_pressure", "value",
                     "forecast", "status", "season", "datetime", "generation_with_24_delay"]
    base_feature_selector = Feature_selector(df_train_modified, target="generation")
    base_feature_selector.select(features_to_select=base_features)
    df_selected = base_feature_selector.df.copy()
    df_selected.loc[:, "semi_prediction"] = np.float64(0)

    logger.info(f"Some features have been dropped successfully")

    models = []
    feature_selector = Feature_selector(df_selected, target="generation")
    Xs_train, ys_train, name_code_df, dictionary_columns = feature_selector.get_X_and_y(number_mimo=4)

    for i in range(1, n_recursive + 1):
        model = xgb.XGBRegressor(objective='reg:squarederror', n_estimators=2000, max_depth=7, learning_rate=0.1)
        model.fit(Xs_train, ys_train)
        models.append(model)
        logger.info(f"Partial model {i} has been trained successfully")

        ys_pred = model.predict(Xs_train)
        update_feature(Xs_train, "semi_prediction", ys_pred, dictionary_columns)

    if save_model:
        save_model_folder = os.path.join(project_root, "src", "models","fitted_models")
        for i in range(len(models)):
            model = models[i]
            dump(model, f"{save_model_folder}/model{i}.joblib")
    else:
        y_pred = make_y_flatten(df_train_modified, feature_selector, name_code_df, n_mimo, ys_pred)
        y_train = make_y_flatten(df_train_modified, feature_selector, name_code_df, n_mimo, ys_train)
        rmse_error_train = compute_relative_rmse(y_pred, y_train)
        logger.info(f"Train Error: {rmse_error_train:0.2f}%")


def make_y_flatten(df, feature_selector, name_code_df, n_mimo, ys):
    dic = feature_selector.name_code_dictionary_index
    y = get_y_inverse_mimo(df, n_mimo, name_code_df, ys, dic)
    return y


if __name__ == "__main__":
    csv_semi_processed_path = os.path.join(project_root, "data", "processed", "semi_processed.csv")
    df = pd.read_csv(csv_semi_processed_path, encoding='utf-8')

    add_is_test_column(df)

    save_model = True  # Todo : true
    write_predictions = False
    n_recursive = 5
    n_mimo_final = 4

    train_model(df, n_recursive, n_mimo_final, save_model)
    test_model()