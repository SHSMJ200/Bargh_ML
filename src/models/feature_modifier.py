from src.root import get_root
import pandas as pd
from logs.logger import CustomLogger

logger = CustomLogger(name="feature_modifier", log_file_name='feature_modifier.log').get_logger()


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


class Feature_adder:
    def __init__(self, df: pd.DataFrame):
        self.df = df

    def create_feature_with_delay(self, feature, n_delay):
        new_feature = f"{feature}_with_{n_delay}_delay"
        self.df = self.df.sort_values(by=['code', 'name', 'date', 'hour'])
        self.df[new_feature] = self.df.groupby(['code', 'name'])[feature].shift(n_delay)

        logger.debug(f"A new column created: {feature} with {n_delay} hours delay")

    def add_season(self):
        self.df['date'] = pd.to_datetime(self.df['date'])
        self.df['season'] = self.df['date'].apply(get_season)

        logger.debug(f"Season column was created")


def get_season(date):
    month = date.month
    if month in [12, 1, 2]:
        return 'winter'
    elif month in [3, 4, 5]:
        return 'spring'
    elif month in [6, 7, 8]:
        return 'summer'
    else:
        return 'fall'
