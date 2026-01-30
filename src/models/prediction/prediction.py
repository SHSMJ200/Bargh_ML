import os
import sys

import yaml

from src.crawler.crawl import crawl_future
from src.data.data_cleaner import jalali_to_gregorian
from src.logs.logger import CustomLogger
from src.models.data_selection.data_selector import Data_selector
from src.models.filter_data.feature_adder import Feature_adder

current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = current_dir[:current_dir.find("src") - 1]
sys.path.insert(0, project_root)

from src.models.data_selection.feature_selector import Feature_selector
from src.root import get_root

import pandas as pd
import re
from joblib import load

logger = CustomLogger(__name__).get_logger()

tables_config_path = get_root() + '/configs/tables_columns.yaml'
feature_dict = yaml.load(open(tables_config_path), Loader=yaml.SafeLoader)

prediction_config_path = get_root() + '/configs/raw_data.yaml'
prediction_config = yaml.safe_load(open(prediction_config_path, 'r', encoding='utf-8'))


def preprocess_and_merge_dfs(input_df, weather_forecast_df):
    new_cols = ["id", "name", "date", "hour", "temperature", "humidity", "dew",
                "apparent_temperature", "precipitation", "rain", "snow",
                "surface_pressure", "evapotranspiration", "wind_speed", "wind_direction"]
    weather_forecast_df.columns = new_cols
    weather_forecast_df['date'] = pd.to_datetime(weather_forecast_df['date'])
    if prediction_config["is_jalali"]:
        weather_forecast_df['Date'] = weather_forecast_df['Date'].apply(jalali_to_gregorian)

    input_df['date'] = pd.to_datetime(input_df['date'])
    final_input_df = pd.merge(input_df, weather_forecast_df, on=['name', 'date', 'hour'], how='left')
    final_input_df = Feature_adder(final_input_df, add_label_column=False).df
    return final_input_df


def select_needed_features(final_input_df):
    if "generation" not in list(final_input_df.columns):
        final_input_df["generation"] = 0

    features = ["name", "code", "temperature"]
    feature_selector = Feature_selector(final_input_df, "generation")
    feature_selector.filter_features(features_to_select=features)
    df_f_selected = feature_selector.df
    return df_f_selected


def extract_name_code_from_filename(filename):
    # We assume that filename is : {name}_{code}.joblib
    pattern = r"(.+)_(.+)\.joblib"

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


def predict_generation(xlsx_input_path, xlsx_output_path):
    normal_models_folder = os.path.join(project_root, "src", "models", "fitted_models", "normal")
    turbo_models_folder = os.path.join(project_root, "src", "models", "fitted_models", "turbo")
    normal_models = load_models(normal_models_folder)
    turbo_models = load_models(turbo_models_folder)

    input_df = pd.read_excel(xlsx_input_path)

    if prediction_config_path["has_temperature_column"]:
        crawl_future()
        weather_forecast_path = os.path.join(project_root, "data", "interim", "weather_forecast.csv")
        weather_forecast_df = pd.read_csv(weather_forecast_path)
        final_input_df = preprocess_and_merge_dfs(input_df, weather_forecast_df)
    else:
        final_input_df = input_df

    df_selected = select_needed_features(final_input_df)

    ds_n_c = Data_selector(df_selected)
    power_plants = ds_n_c.df[['name', 'code']].drop_duplicates()
    for row in power_plants.itertuples():
        name, code = row.name, row.code

        df_n_c = ds_n_c.filter_name_code(name, code)

        fs_n_c = Feature_selector(df_n_c, "generation")
        fs_n_c.filter_features(features_to_drop=["name", "code"])
        X, _ = fs_n_c.get_X_and_y()

        normal_model = normal_models[name, code]
        y_pred = normal_model.predict(X)
        input_df.loc[X.index, "prediction"] = y_pred

        turbo_model = turbo_models.get((name, code))
        if turbo_model:
            y_pred = turbo_model.predict(X)
            input_df.loc[X.index, "prediction_turbo"] = y_pred

    input_df.to_excel(xlsx_output_path)

    logger.info(f"The prediction of generation is written in the file below:\n{xlsx_output_path}")


if __name__ == "__main__":
    xlsx_input_path = os.path.join(project_root, "src", "models", "prediction", "one_day_input.xlsx")
    xlsx_output_path = os.path.join(project_root, "src", "models", "prediction", "one_day_output.xlsx")

    predict_generation(xlsx_input_path, xlsx_output_path)
