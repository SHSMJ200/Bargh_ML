import pandas as pd
from logs.logger import CustomLogger

logger = CustomLogger(name="data_selector").get_logger()


class Data_selector:
    def __init__(self, df: pd.DataFrame):
        self.df = df

    def select_good_peaks(self, goodness):
        return self.df[self.df["is_good_peak"] == goodness]

    def select_peaks(self, goodness):
        peak_condition = (self.df['is_good_peak'] >= goodness)
        logger.debug(f"Rows of data has been selected successfully!")
        return self.df[peak_condition]

    def filter_name_code(self, name, code, get_mask=False):
        df = self.df
        mask = (df["name"] == name) & (df["code"] == code)
        if get_mask:
            return mask
        return df[mask]

    def filter_time(self, date1, date2, get_mask=False):
        df = self.df
        mask = (df['datetime'] >= date1) & (df['datetime'] <= date2)
        if get_mask:
            return mask
        return df[mask]
