import pandas as pd

from logs.logger import CustomLogger

logger = CustomLogger(name="feature_selector", log_file_name='feature_selector.log').get_logger()


class Feature_selector:
    def __init__(self, df: pd.DataFrame, target):
        self.df = df
        self.target = target

    def select(self, features_to_select=None, features_to_drop=None):

        if features_to_drop is not None:
            self.df = self.df.drop(columns=features_to_drop, axis=1)
        if features_to_select is not None:
            self.df = self.df[features_to_select + ["generation"]]

        logger.debug(f"Selected features : {self.df.columns}")

    def get_X_and_y(self, do_onehot=True):
        df = self.df.copy(deep=True)

        if do_onehot:
            categorical_cols = df.select_dtypes(include=['object', 'category']).columns
            df = pd.get_dummies(df, columns=categorical_cols, drop_first=True)

        df = df.dropna()

        X = df.drop(columns=[self.target])
        y = df[self.target]

        return X, y
