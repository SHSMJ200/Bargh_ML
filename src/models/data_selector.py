import pandas as pd
from logs.logger import CustomLogger

logger = CustomLogger(name="data_selector", log_file_name='data_selector.log').get_logger()


class Data_selector:
    def __init__(self, df: pd.DataFrame):
        self.df = df

    def select(self):
        df = self.df
        df = df[((df['status'] == 'SO') | (df['status'] == 'LF1'))]
        df = df[(df['value'] == 'P')]
        
        

        logger.debug(f"Rows of data selected successfully!")

        return df
