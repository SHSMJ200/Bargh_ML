import pandas as pd
from logs.logger import CustomLogger

logger = CustomLogger(name="data_selector", log_file_name='data_selector.log').get_logger()


class Data_selector:
    def __init__(self, df: pd.DataFrame):
        self.df = df

    def select_peaks(self, m_in_summer=True):
        self.df = self.df[((self.df['status'] == 'SO') | (self.df['status'] == 'LF1'))]

        if m_in_summer:
            peak_condition = (self.df['value'] == 'P') | (self.df['value'] == 'M') & (self.df['season'] == 'summer')
            self.df = self.df[peak_condition]
        else:
            self.df = self.df[self.df['value'] == 'P']

        logger.debug(f"Rows of data has been selected successfully!")

    def select_name_and_code(self, name, code):
        self.df = self.df[((self.df['name'] == name) & (self.df['code'] == code))]
        logger.debug(f"Data related to {name}_{code}  has been selected successfully!")

    def filter_name_code(self, name, code, get_bool=False):
        df = self.df
        mask = (df["name"] == name) & (df["code"] == code)
        if get_bool:
            return mask
        return df[mask]

    def filter_time(self, date1, date2, get_bool=False):
        df = self.df
        mask = (df['datetime'] >= date1) & (df['datetime'] <= date2)
        if get_bool:
            return mask
        return df[mask]
