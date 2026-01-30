import pandas as pd

from src.logs.logger import CustomLogger

logger = CustomLogger(__name__).get_logger()


class Data_selector:
    def __init__(self, df: pd.DataFrame):
        self.df = df

    def select_peaks(self, goodness, is_tight=True):
        if is_tight:
            peak_condition = self.df['is_good_peak'] == goodness
        else:
            peak_condition = self.df['is_good_peak'] >= goodness

        selected_df = self.df[peak_condition]
        return selected_df

    def filter_name_code(self, name, code):
        df = self.df
        mask = (df["name"] == name) & (df["code"] == code)
        return df[mask]
