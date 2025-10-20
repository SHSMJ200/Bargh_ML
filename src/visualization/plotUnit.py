import pandas as pd
import plotly.graph_objects as go

from src.root import get_root


class UnitPlotter:

    def __init__(self, df):
        self.df = df
        self.df['date'] = pd.to_datetime(self.df['date'])
        self.df['datetime'] = self.df['date'] + pd.to_timedelta(self.df['hour'], unit='h')

    def generation_over_time(self, name, code):
        self.features_over_time(name, code, ["generation"], ["red"])

    def prediction_and_generation_over_time(self, name, code):
        self.features_over_time(name, code, ["prediction", "generation"], ["blue", "red"])

    def prediction_and_generation_flag_marker_over_time(self, name, code):
        self.features_over_time(name, code, ["prediction", "generation"], ["blue", "red"], flag_marker=True)

    def temperature_and_prediction_and_generation_flag_marker_over_time(self, name, code):
        self.features_over_time(name, code, ["temperature", "prediction", "generation"], ["yellow", "blue", "red"],
                                flag_marker=True)

    def temperature_and_generation_over_time(self, name, code):
        self.features_over_time(name, code, ["temperature", "generation"], ["blue", "red"])

    def temperature_and_generation_flag_marker_over_time(self, name, code):
        self.features_over_time(name, code, ["temperature", "generation"], ["blue", "red"], flag_marker=True)

    def temperature_change_and_generation_change_flag_marker_over_time(self, name, code):
        self.features_over_time(name, code, ["temperature_change", "generation_change"], ["blue", "red"],
                                flag_marker=True)

    def generation_and_generation_with_24_delay_flag_marker_over_time(self, name, code):
        self.features_over_time(name, code, ["generation", "generation_with_24_delay"], ["red", "blue"],
                                flag_marker=True)

    def generation_and_mean_generation_and_generation_with_24_delay_flag_marker_over_time(self, name, code):
        self.features_over_time(name, code, ["generation", "mean_generation", "generation_with_24_delay"],
                                ["red", "green", "blue"],
                                flag_marker=True)

    def prediction_and_declare_and_generation_flag_marker_over_time(self, name, code):
        self.features_over_time(name, code, ["prediction", "declare", "generation"], ["blue", "red", "purple"],
                                flag_marker=True)

    def features_over_time(self, name, code, features, colors, flag_marker=False):
        sample = self.df.loc[(self.df['name'] == name) & (self.df['code'] == code)]
        sample = sample.sort_values(by='datetime')
        features_string = "_and_".join(features)

        fig = go.Figure()

        for color, feature in zip(colors, features):

            color_marker = None
            mode = 'lines'
            if feature in ["generation", "generation_change"] and flag_marker:
                color_pick = {0: "red", 1: "blue", 2: "black", 3: "green", 4: "yellow", 5: "orange"}
                color_marker = dict(color=[color_pick[value] for value in sample["is_good_peak"]], size=5)
                mode = 'lines+markers'

            fig.add_trace(go.Scatter(
                x=sample['datetime'],
                y=sample[feature],
                mode=mode,
                name=f"{feature}",
                marker=color_marker,
                line=dict(color=color, dash="solid"),
                hovertemplate=f"{feature} : %{{y}}<br>Time: %{{x}}<extra></extra>"
            ))

        fig.update_layout(
            title=f'{features_string} over time',
            xaxis_title='Time',
            yaxis_title=f'{features_string}',
            hovermode='x unified'
        )

        project_root = get_root()
        folder_path = f"{features_string}_flag_marker_over_time" if flag_marker else f"{features_string}_over_time"
        fig.write_html(f"{project_root}/src/visualization/unit_figs/{folder_path}/{name}-{code}.html")
