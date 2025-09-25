import os
import sys

current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = current_dir[:current_dir.find("src") - 1]
sys.path.insert(0, project_root)

import pandas as pd
import xgboost as xgb
from src.models.data_selector import Data_selector
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


def update_feature_value(X_df, feature, new_values, dictionary_columns, n_mimo):
    if n_mimo > 1:
        columns = [str(col) for col in dictionary_columns[feature]]
        X_df.loc[:, columns] = new_values
    else:
        X_df.loc[:, feature] = new_values


def train_model(df_train, n_simple_rec, n_mimo_final, save_model=False, save_model_folder=None):
    base_features = ["name", "code", "temperature", "humidity", "dew", "surface_pressure", "value",
                     "forecast", "status", "season", "datetime", "generation_with_24_delay"]
    base_feature_selector = Feature_selector(df_train, target="generation")
    base_feature_selector.select(features_to_select=base_features)
    df_selected = base_feature_selector.df.copy()
    df_selected.loc[:, "semi_prediction"] = np.float64(0)

    logger.info(f"Train model: Some features have been dropped successfully")

    models = []
    feature_selector = Feature_selector(df_selected, target="generation")
    X_train, y_train, _, _ = feature_selector.get_X_and_y(n_mimo=1)

    for i in range(1, n_simple_rec + 1):
        model = xgb.XGBRegressor(objective='reg:squarederror', n_estimators=2000, max_depth=7, learning_rate=0.1)
        model.fit(X_train, y_train)
        models.append(model)
        logger.info(f"Partial model {i} has been trained successfully")

        y_pred = model.predict(X_train)
        update_feature_value(X_train, "semi_prediction", y_pred, _, n_mimo=1)

    new_df_selected = df_selected.copy()
    new_df_selected.loc[:, "semi_prediction"] = X_train["semi_prediction"]
    new_feature_selector = Feature_selector(new_df_selected, target="generation")
    Xs_train, ys_train, name_code_df, dic_col = new_feature_selector.get_X_and_y(n_mimo=n_mimo_final)

    model = xgb.XGBRegressor(objective='reg:squarederror', n_estimators=2000, max_depth=7, learning_rate=0.1)
    model.fit(Xs_train, ys_train)
    models.append(model)
    logger.info(f"Final model has been trained successfully")

    ys_pred = model.predict(Xs_train)

    if save_model and save_model_folder is not None:
        for i in range(len(models)):
            model = models[i]
            dump(model, f"{save_model_folder}/model{i}.joblib")
    else:
        y_pred = make_y_flatten(df_train, new_feature_selector, name_code_df, n_mimo_final, ys_pred)
        rmse_error_train = compute_relative_rmse(y_pred, y_train)
        logger.info(f"Train Error: {rmse_error_train:0.2f}%")


def test_model(models, df_test, n_mimo_final, Noh):
    base_features = ["name", "code", "temperature", "humidity", "dew", "surface_pressure", "value",
                     "forecast", "status", "season", "datetime", "generation_with_24_delay"]
    base_feature_selector = Feature_selector(df_test, target="generation")
    base_feature_selector.select(features_to_select=base_features)
    df_selected = base_feature_selector.df.copy()
    df_selected.loc[:, "semi_prediction"] = np.float64(0)

    logger.info(f"Test model: Some features have been dropped successfully")

    feature_selector = Feature_selector(df_selected, target="generation")
    X_test, y_test, _, _ = feature_selector.get_X_and_y(n_mimo=1)
    #X_test = Noh.normalize_df(X_test)

    n_simple_rec = len(models) - 1
    for i in range(n_simple_rec):
        model = models[i]
        y_pred = model.predict(X_test)
        update_feature_value(X_test, "semi_prediction", y_pred, _, n_mimo=1)

    new_df_selected = df_selected.copy()
    new_df_selected.loc[:, "semi_prediction"] = X_test["semi_prediction"]
    new_feature_selector = Feature_selector(new_df_selected, target="generation")
    Xs_test, ys_test, name_code_df, dic_col = new_feature_selector.get_X_and_y(n_mimo=n_mimo_final)

    model = models[-1]

    ys_pred = model.predict(Xs_test)
    y_pred = make_y_flatten(df_test, new_feature_selector, name_code_df, n_mimo_final, ys_pred)
    rmse_error_test = compute_relative_rmse(y_pred, y_test)
    logger.info(f"Test Error: {rmse_error_test:0.2f}%")


def make_y_flatten(df, feature_selector, name_code_df, n_mimo, ys):
    dic = feature_selector.name_code_dictionary_index
    y = get_y_inverse_mimo(df, n_mimo, name_code_df, ys, dic)
    return y


def load_models(path, n):
    models = []
    for i in range(n):
        models.append(load(f"{path}/model{i}.joblib"))
    return models

class Normalize_one_hot():
    def __init__(self,df):
        df_new = df.head(1).drop("generation")
        categorical_cols = df_new.select_dtypes(include=['object', 'category']).columns
        df_new = pd.get_dummies(df_new, columns=categorical_cols, drop_first=True)
        df_new.columns = df_new.columns.astype(str)
        self.list_columns_name = df_new.columns.to_list()

    def normalize_df(self,df):
        df_new = df.copy()
        columns_new = df_new.columns.to_list()
        for col in self.list_columns_name:
            if not col in columns_new:
                df_new[col] = 0
        return df_new


if __name__ == "__main__":
    csv_semi_processed_path = os.path.join(project_root, "data", "processed", "semi_processed.csv")
    df = pd.read_csv(csv_semi_processed_path, encoding='utf-8')

    add_is_test_column(df)

    save_model = True
    save_model_folder = os.path.join(project_root, "src", "models", "fitted_models")
    write_predictions = False
    n_recursive = 2
    n_mimo_final = 4

    train_test_ds = Data_selector(Data_selector(df).select_peaks(goodness=3))
    Noh = Normalize_one_hot(train_test_ds.df)
    train_df = train_test_ds.select_train_test(is_test=False)
    test_df = train_test_ds.select_train_test(is_test=True)

    train_model(train_df, n_recursive, n_mimo_final, save_model, save_model_folder)

    models = load_models(save_model_folder, n_recursive)

    test_model(models, test_df, n_mimo_final, Noh)
