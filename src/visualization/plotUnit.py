from src.root import get_root
import os

import matplotlib
import pandas as pd
import plotly.graph_objects as go

matplotlib.use('TkAgg')


def convert_to_tuple(x: str):
    elements = x.strip('()').split(', ')

    converted_elements = []
    for element in elements:
        try:
            converted_elements.append(int(element))
        except ValueError:
            try:
                converted_elements.append(float(element))
            except ValueError:
                converted_elements.append(element.strip("'"))

    return tuple(converted_elements)


def hsv_to_rgb(h, s, v):
    if s == 0.0:
        return v, v, v

    i = int(h * 6)
    f = h * 6 - i
    p = v * (1 - s)
    q = v * (1 - f * s)
    t = v * (1 - (1 - f) * s)
    i %= 6
    return {
        0: (v, t, p),
        1: (q, v, p),
        2: (p, v, t),
        3: (p, q, v),
        4: (t, p, v),
        5: (v, p, q),
    }[i]


def golden_ration(n):
    colors = []
    phi = (1 + 5 ** 0.5) / 2
    for i in range(n):
        hue = (i / phi) % 1
        color = hsv_to_rgb(hue, 1, 1)
        colors.append(color)
    return colors


def assign_color(df: pd.DataFrame):
    statuses = df['status'].unique()
    status_count = len(statuses)
    colors = golden_ration(status_count)
    color_col = {
        stat: c for stat, c in zip(statuses, colors)
    }
    colorized_df = df.copy()
    colorized_df['color'] = colorized_df['status'].map(color_col)
    return colorized_df


class UnitPlotter:

    def __init__(self, df):
        self.df = df

    def generation_over_time(self, name, code):
        self.features_over_time(name, code, ["generation"], ["red"])

    def prediction_and_generation_over_time(self, name, code):
        self.features_over_time(name, code, ["prediction", "generation"], ["blue", "red"])

    def temperature_and_generation_over_time(self, name, code):
        self.features_over_time(name, code, ["temperature", "generation"], ["blue", "red"])

    def temperature_and_generation_by_dot_over_time(self, name, code):
        self.features_over_time(name, code, ["temperature", "generation"], ["blue", "red"], flag_marker=True)
    
    def temperature_change_and_generation_change_flag_marker_over_time(self, name, code):
        self.features_over_time(name, code, ["temperature_change", "generation_change"], ["blue", "red"], flag_marker=True)
    
    def features_over_time(self, name, code, features, colors,flag_marker=False):
        
        sample = self.df.loc[(self.df['name'] == name) & (self.df['code'] == code)]
        sample = sample.sort_values(by='datetime')
        features_string = "_and_".join(features)

        fig = go.Figure()

        for color, feature in zip(colors, features):

            color_marker = None
            mode = 'lines'
            if feature in ["generation","generation_change"] and flag_marker:
                color_pick = {0:"red",1:"black",2:"blue"}      
                color_marker=dict(color=[color_pick[value] for value in sample["is_good_peak"]], size=5)     
                mode = 'lines+markers'

            fig.add_trace(go.Scatter(
                x=sample['datetime'],
                y=sample[feature],
                mode=mode,
                name=f"{feature}-{name}-{code}",
                marker=color_marker,
                line=dict(color=color, dash="solid"),
                legendgroup=str(name),
                hovertemplate=f"Label: {name}<br> feature : %{{y}}<br>Time: %{{x}}<extra></extra>"
            ))

        fig.update_layout(
            title=f'{features_string} over time',
            xaxis_title='Time',
            yaxis_title=f'{features_string}',
            hovermode='x unified'
        )

        project_root = get_root()
        folder = f"{features_string}_flag_marker_over_time" if flag_marker else f"{features_string}_over_time"
        fig.write_html(f"{project_root}/src/visualization/unit_figs/{folder}/{name}-{code}.html")
