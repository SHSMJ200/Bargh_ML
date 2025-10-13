import pandas as pd
from logs.logger import CustomLogger

logger = CustomLogger(__name__).get_logger()


class Data_selector:
    def __init__(self, df: pd.DataFrame):
        self.df = df

    def select_peaks(self, goodness):
        peak_condition = (self.df['is_good_peak'] >= goodness)
        selected_df = self.df[peak_condition]
        logger.debug(f"Rows of data has been selected successfully!")
        return selected_df

    def filter_name_code(self, name, code, get_mask=False):
        df = self.df
        mask = (df["name"] == name) & (df["code"] == code)
        if get_mask:
            return mask
        return df[mask]

    def filter_time(self, date1, date2, get_mask=False, include_end=True):
        df = self.df

        if include_end:
            mask = (df['datetime'] >= date1) & (df['datetime'] <= date2)
        else:
            mask = (df['datetime'] >= date1) & (df['datetime'] < date2)

        if get_mask:
            return mask
        return df[mask]
