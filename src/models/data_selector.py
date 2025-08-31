import pandas as pd
from logs.logger import CustomLogger

logger = CustomLogger(name="data_selector", log_file_name='data_selector.log').get_logger()


class Data_selector:
    def __init__(self, df: pd.DataFrame):
        self.df = df

    def select_good_peaks(self):
        return self.df[self.df["is_good_peak"] == 2]

    def select_peaks(self, m_in_summer=True, inplace = False):
        
        peak_condition = (self.df['value'] == 'P')
        if m_in_summer:
            peak_condition = peak_condition | (self.df['value'] == 'M') & (self.df['season'] == 'summer')    
        peak_condition = peak_condition & ((self.df['status'] == 'SO') | (self.df['status'] == 'LF1'))
            
        if inplace:
            self.df = self.df[peak_condition]
            logger.debug(f"Rows of data has been selected successfully!")
            return
        else:
            logger.debug(f"Rows of data has been selected successfully!")
            return self.df[peak_condition]

    def filter_name_code(self, name, code, get_bool=False):
        df = self.df
        mask = (df["name"] == name) & (df["code"] == code)
        if get_bool:
            return mask
        logger.debug(f"Data related to {name}_{code}  has been selected successfully!")
        return df[mask]

    def filter_time(self, date1, date2, get_bool=False):
        df = self.df
        mask = (df['datetime'] >= date1) & (df['datetime'] <= date2)
        if get_bool:
            return mask
        return df[mask]
