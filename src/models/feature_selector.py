import pandas as pd
from logs.logger import CustomLogger

logger = CustomLogger(name="feature_selector", log_file_name='feature_selector.log').get_logger()


class Feature_selector:
    def __init__(self, df: pd.DataFrame, target):
        self.df = df
        self.target = target

    def select(self, features_to_drop=None):
        df = self.df
        if features_to_drop is not None:
            df.drop(columns=features_to_drop, axis=1, inplace=True)

        categorical_cols = df.select_dtypes(include=['object', 'category']).columns
        df = pd.get_dummies(df, columns=categorical_cols, drop_first=True)

        df = df.dropna()

        logger.debug(f"Features training is applied on: {df.columns}")

        X = df.drop(columns=[self.target])
        y = df[self.target]

        return X, y
