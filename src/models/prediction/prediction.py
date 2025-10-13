import os
import sys

import yaml

from src.models.data_selection.data_selector import Data_selector
from src.models.filter_data.feature_adder import Feature_adder

current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = current_dir[:current_dir.find("src") - 1]
sys.path.insert(0, project_root)

from src.crawler.crawl import crawl_future
from src.models.data_selection.feature_selector import Feature_selector
from src.root import get_root

import pandas as pd
import re
from joblib import load

tables_config_path = get_root() + '/configs/tables_columns.yaml'
feature_dict = yaml.load(open(tables_config_path), Loader=yaml.SafeLoader)


def read_dfs():
    weather_forecast_path = os.path.join(project_root, "data", "interim", "weather_forecast.csv")
    xlsx_input_path = os.path.join(project_root, "src", "models", "prediction", "one_day_input.xlsx")
    weather_forecast_df = pd.read_csv(weather_forecast_path)
    input_df = pd.read_excel(xlsx_input_path)
    return input_df, weather_forecast_df


def preprocess_and_merge_dfs(input_df, weather_forecast_df):
    new_cols = ["id", "name", "date", "hour", "temperature", "humidity", "dew",
                "apparent_temperature", "precipitation", "rain", "snow",
                "surface_pressure", "evapotranspiration", "wind_speed", "wind_direction"]
    weather_forecast_df.columns = new_cols
    weather_forecast_df['date'] = pd.to_datetime(weather_forecast_df['date'])
    input_df['date'] = pd.to_datetime(input_df['date'])
    final_input_df = pd.merge(input_df, weather_forecast_df, on=['name', 'date', 'hour'], how='left')
    final_input_df = Feature_adder(final_input_df, add_label_column=False).df
    return final_input_df


def select_needed_features(final_input_df):
    final_input_df["generation"] = 0

    base_features = ["name", "code", "temperature", "humidity", "dew", "surface_pressure", "temp_sens"]
    time_features = ["hour", "day_of_week", "month"]
    feature_selector = Feature_selector(final_input_df, "generation")
    feature_selector.filter_features(features_to_select=base_features + time_features)
    df_f_selected = feature_selector.df
    return df_f_selected


def extract_name_code_from_filename(filename):
    # We assume that filename is : model_{name}_{code}.joblib
    pattern = r"model_(.+)_(.+)\.joblib"

    match = re.match(pattern, filename)
    if match:
        name = match.group(1)
        code = match.group(2)
    else:
        name, code = None, None

    return name, code


def load_models(folder_path):
    models_dict = {}
    for filename in os.listdir(folder_path):
        name, code = extract_name_code_from_filename(filename)
        if name and code:
            model = load(f"{folder_path}/{filename}")
            models_dict[name, code] = model

    return models_dict


if __name__ == "__main__":
    crawl_future()

    save_model_folder = os.path.join(project_root, "src", "models", "fitted_models")
    models_dict = load_models(save_model_folder)

    input_df, weather_forecast_df = read_dfs()

    final_input_df = preprocess_and_merge_dfs(input_df, weather_forecast_df)

    df_selected = select_needed_features(final_input_df)

    ds_n_c = Data_selector(df_selected)
    ds_n_c.df = ds_n_c.df.dropna()
    power_plants = ds_n_c.df[['name', 'code']].drop_duplicates()
    for row in power_plants.itertuples():
        name, code = row.name, row.code

        df_n_c = ds_n_c.filter_name_code(name, code)

        fs_n_c = Feature_selector(df_n_c, "generation")
        fs_n_c.filter_features(features_to_drop=["name", "code"])
        X, _ = fs_n_c.get_X_and_y()
        # X = make_onehot(X)

        model = models_dict[name, code]
        y_pred = model.predict(X)
        input_df.loc[X.index, "prediction"] = y_pred

    xlsx_output_path = os.path.join(project_root, "src", "models", "prediction", "one_day_output.xlsx")
    input_df.to_excel(xlsx_output_path)
