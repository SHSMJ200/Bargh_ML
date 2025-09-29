import os
import sys

from src.crawler.crawl import ForecastCrawler
from src.data.data_cleaning import RawData
from src.models.data_selection.feature_selector import Feature_selector
from src.models.filter_data.feature_adder import Feature_adder
from src.models.train_model.train_model import select_dataset_features, get_y_inverse_mimo

current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = current_dir[:current_dir.find("src") - 1]
sys.path.insert(0, project_root)

import pandas as pd
from joblib import load


def load_model():
    save_model_folder = os.path.join(project_root, "src", "models", "fitted_models")
    with open(f"{save_model_folder}/model_cols.pkl", 'rb') as f:
        model_X_cols = load(f)
    model = load(f"{save_model_folder}/model.joblib")
    return model, model_X_cols


def convert_df_to_Xs(final_input_df, model_X_cols, n_mimo):
    feature_adder = Feature_adder(final_input_df, add_label_column=False)
    features_with_lag = ["temperature", "humidity", "dew", "surface_pressure"]
    hours_delay = [1, 5]
    for feature in features_with_lag:
        for hour in hours_delay:
            feature_adder.create_feature_with_delay(feature, hour, drop_null=False)
    final_input_df.dropna(inplace=True)
    base_features = ["name", "code", "temperature", "humidity", "dew", "surface_pressure", "value",
                     "forecast", "status"]
    lag_features = [f"{feature}_with_{hour}_delay" for feature in features_with_lag for hour in hours_delay]
    time_features = ["hour", "day_of_week", "month", "season", "datetime"]
    final_input_df["generation"] = 0
    df_f_selected = select_dataset_features(final_input_df, base_features, lag_features, time_features, "generation")
    feature_selector = Feature_selector(df_f_selected, target="generation")
    Xs, ys, name_code_df = feature_selector.get_X_and_y(n_mimo=n_mimo)
    Xs = Xs.reindex(columns=model_X_cols)
    Xs = Xs.fillna(False).infer_objects(copy=False)
    meta_data = {}
    meta_data["df_f_selected"] = df_f_selected
    meta_data["feature_selector"] = feature_selector
    meta_data["name_code_df"] = name_code_df
    return Xs, meta_data


def preprocess_and_merge_dfs(commitment_df, input_df, weather_forecast_df):
    name_code_id_df = commitment_df[["id", "name", "code"]].drop_duplicates()
    weather_forecast_df = weather_forecast_df.rename(
        columns={'temperature_2m': 'temperature', 'relative_humidity_2m': 'humidity', 'dew_point_2m': 'dew',
                 "time": 'hour', 'unitid': 'id'})
    weather_forecast_df['date'] = pd.to_datetime(weather_forecast_df['date'])
    input_df['date'] = pd.to_datetime(input_df['date'])
    input_df = input_df.sort_values(by=["date", "hour"])
    weather_forecast_df = pd.merge(weather_forecast_df, name_code_id_df, on='id', how='left')
    final_input_df = pd.merge(input_df, weather_forecast_df, on=['name', 'code', 'date', 'hour'], how='outer')
    return final_input_df


def read_dfs():
    commitment_path = os.path.join(project_root, "data", "interim", "commitment.csv")
    weather_forecast_path = os.path.join(project_root, "data", "interim", "weather_forecast.csv")
    xlsx_input_path = os.path.join(project_root, "src", "models", "prediction", "one_day_input.xlsx")
    commitment_df = pd.read_csv(commitment_path)
    weather_forecast_df = pd.read_csv(weather_forecast_path)
    input_df = pd.read_excel(xlsx_input_path)
    return commitment_df, input_df, weather_forecast_df


def predict(Xs, meta_data):
    df_f_selected = meta_data["df_f_selected"]
    feature_selector = meta_data["feature_selector"]
    name_code_df = meta_data["name_code_df"]
    ys_pred = model.predict(Xs)
    y_pred = get_y_inverse_mimo(df_f_selected, feature_selector, name_code_df, n_mimo, ys_pred)
    return y_pred


if __name__ == "__main__":
    ForecastCrawler(file=RawData.PLANT.value).crawl()

    n_mimo = 4
    model, model_X_cols = load_model()

    commitment_df, input_df, weather_forecast_df = read_dfs()

    final_input_df = preprocess_and_merge_dfs(commitment_df, input_df, weather_forecast_df)

    Xs, meta_data = convert_df_to_Xs(final_input_df, model_X_cols, n_mimo)

    y_pred = predict(Xs, meta_data)

    input_df["prediction"] = y_pred
    xlsx_output_path = os.path.join(project_root, "src", "models", "prediction", "one_day_output.xlsx")

    input_df.to_excel(xlsx_output_path)
