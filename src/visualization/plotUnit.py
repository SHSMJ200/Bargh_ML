import os
from src.models.data_selection.data_selector import Data_selector
from src.root import get_root

import pandas as pd
import plotly.graph_objects as go
import numpy as np
from joblib import load

import plotly.express as px


color_map = {
    0: "purple",
    1: "orange",
    2: "pink",
    3: "blue",
    4: "black",
    5: "red",
    6: "green"
}

def draw_gen_temp_plot(df, coefs, name, code):
    
    fig = go.Figure()
    
    for peak_value, color in color_map.items():
        df_subset = Data_selector(df).select_peaks(goodness=peak_value)
        fig.add_trace(go.Scatter(
            x=df_subset["temperature"],
            y=df_subset["generation"],
            mode="markers",
            marker=dict(size=4, color=color),
            name=f"is_good_peak = {peak_value}",
            hovertext=df_subset["datetime"]
        ))
        
    fig.update_traces(marker=dict(size=4, sizemode="diameter", sizeref=1, opacity=0.7))


    a, b = coefs.get((name, code))
    x_line = np.linspace(df["temperature"].min(), df["temperature"].max(), 100)
    y_line = a * x_line + b
    fig.add_trace(go.Scatter(
        x=x_line,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               
        y=y_line,
        mode="lines",
        name=f"y = {a:.3f}x + {b:.3f}",
        line=dict(dash="dash", width=2)
    ))

    add_model_prediction(fig, x_line, name, code, "normal", "predict")
    add_model_prediction(fig, x_line, name, code, "turbo", "turbo predict")
    #add_model_prediction(fig, x_line, name, code, "LinearRegression", "linear predict")
    #add_model_prediction(fig, x_line, name, code, "QuantileRegressor", "quantile predict")
    #add_model_prediction(fig, x_line, name, code, "TwoSegmentQuantileRegressor", "two segment quantile predict")
    
    project_root = get_root()
    save_path = f"{project_root}/src/visualization/unit_figs/test_models/{name}-{code}_temp.html"
    fig.write_html(save_path)


def draw_gen_date_plot(df, name, code):

    fig = px.scatter(
        df,
        x="datetime",
        y='generation',
        color='is_good_peak',
        title='Generation over Time',
        labels={'generation': 'Generation', 'datetime': 'Time'},
        hover_data=['datetime', 'generation', "temperature"]
    )

    project_root = get_root()
    path = f"{project_root}/src/visualization/unit_figs/test_models/{name}-{code}_date.html"
    os.makedirs(os.path.dirname(path), exist_ok=True)
    fig.write_html(path)



def add_model_prediction(fig, x_line, name, code, model_subdir, trace_name, dash="solid", width=2):
    try:
        project_root = get_root()
        folder_path = os.path.join(project_root, "src", "models", "fitted_models")
        model_path = f"{folder_path}/{model_subdir}/{name}_{code}.joblib"
        model = load(model_path)
        df_line = pd.DataFrame(x_line, columns=["temperature"])
        y_line = model.predict(df_line)
        fig.add_trace(
            go.Scatter(x=df_line["temperature"], y=y_line, mode="lines", name=trace_name, line=dict(dash=dash, width=width),
                       visible="legendonly"))
    except Exception as e:
        pass

