import pandas as pd
from src.logs.logger import CustomLogger

logger = CustomLogger(__name__).get_logger()


class Feature_selector:
    def __init__(self, df: pd.DataFrame, target):
        self.df = df
        self.target = target

    def filter_features(self, features_to_select=None, features_to_drop=None):
        if features_to_drop:
            self.df = self.df.drop(columns=features_to_drop)
        if features_to_select:
            self.df = self.df[features_to_select + [self.target]]
        logger.debug(f"Selected features : \n{self.df.columns}")

    def get_X_and_y(self):
        df = self.df
        X = df.drop(columns=[self.target])
        y = df[self.target].values
        return X, y