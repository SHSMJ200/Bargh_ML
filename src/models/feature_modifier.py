import pandas as pd
from scipy.stats import pearsonr

from data_selector import Data_selector
from logs.logger import CustomLogger
import matplotlib.pyplot as plt
import numpy as np

logger = CustomLogger(name="feature_modifier", log_file_name='feature_modifier.log').get_logger()


class Feature_selector:
    def __init__(self, df: pd.DataFrame, target):
        self.df = df
        self.target = target

    def select(self, features_to_drop=None):
        if features_to_drop is not None:
            self.df.drop(columns=features_to_drop, axis=1, inplace=True)

        categorical_cols = self.df.select_dtypes(include=['object', 'category']).columns
        self.df = pd.get_dummies(self.df, columns=categorical_cols, drop_first=True)

        self.df = self.df.dropna()

        logger.debug(f"Features training is applied on: {self.df.columns}")

        X = self.df.drop(columns=[self.target])
        y = self.df[self.target]

        return X, y


def has_same_trend_temperature_generation(df, time_ranges, consistency_threshold):
    consistent_time_ranges = []
    corr_pearsons = []
    data_selector = Data_selector(df)
    for start, end in time_ranges:
        corr_pearson = find_consistency_in_an_interval(consistent_time_ranges, data_selector, start, end, consistency_threshold)
        corr_pearsons.append(corr_pearson)

    return corr_pearsons, consistent_time_ranges


def find_consistency_in_an_interval(consistent_time_ranges, data_selector, start, end, consistency_threshold):
    df_sliced = data_selector.filter_time(start, end)
    temperatures = df_sliced["temperature_with_5_delay"].values
    generations = df_sliced["generation"].values
    corr_pearson, are_similar = are_trends_similar(temperatures, generations, consistency_threshold, opposite_trend=True)
    if are_similar:
        consistent_time_ranges.append((start, end))

    return corr_pearson


def are_trends_similar(arr1, arr2, threshold, opposite_trend=False):
    if np.std(arr1) == 0 or np.std(arr2) == 0:
        return 0, True

    corr_pearson, _ = pearsonr(arr1, arr2)

    if not opposite_trend:
        return corr_pearson, (corr_pearson > threshold)
    return corr_pearson, (corr_pearson < -threshold)


class Feature_adder:
    def __init__(self, df: pd.DataFrame):
        self.df = df

    def create_feature_with_delay(self, feature, n_delay, drop_null=True):
        new_feature = f"{feature}_with_{n_delay}_delay"
        temp = self.df.sort_values(by=['code', 'name', 'date', 'hour'])
        self.df[new_feature] = temp.groupby(['code', 'name'])[feature].shift(n_delay)
        if drop_null:
            self.df.dropna(inplace=True)

        logger.debug(f"A new column created: {feature} with {n_delay} hours delay")

    def add_season(self):
        self.df['date'] = pd.to_datetime(self.df['date'])
        self.df['season'] = self.df['date'].apply(get_season)

        logger.debug(f"Season column was created")

    def add_date_time(self):
        self.df['date'] = pd.to_datetime(self.df['date'])
        self.df['datetime'] = self.df['date'] + pd.to_timedelta(self.df['hour'], unit='h')

    def add_is_good_peak(self, l_min, max_diff, consistency_threshold=0.9, add_col=True):
        df_modified = self.df[
            ["name", "code", "datetime", "hour", "status", 'temperature_with_5_delay', 'value', "generation", 'season']].copy(
            deep=True)
        df_modified = Data_selector(df_modified).select_peaks(m_in_summer=True)
        if add_col: self.df["is_good_peak"] = 0

        time_ranges_by_name_code = {}
        power_plants = df_modified[['name', 'code']].drop_duplicates()
        n_time_ranges = 0
        n_consistent_time_ranges = 0
        all_pearsons = []

        for _, row in power_plants.iterrows():
            df_name_code_smooth = Data_selector(df_modified).filter_name_code(row["name"], row["code"])
            time_ranges = get_interval(df_name_code_smooth, l_min, max_diff)

            corr_pearsons , consistent_time_ranges = has_same_trend_temperature_generation(df_name_code_smooth, time_ranges,
                                                                           consistency_threshold)
            all_pearsons += corr_pearsons
            n_time_ranges += len(time_ranges)
            n_consistent_time_ranges += len(consistent_time_ranges)

            time_ranges_by_name_code[(row["name"], row["code"])] = consistent_time_ranges

            if add_col: self.labeling_point(df_name_code_smooth, consistent_time_ranges, label=2)

        consistency_percentage = n_consistent_time_ranges / n_time_ranges * 100
        logger.info(
            f"In {consistency_percentage: 0.2f}% of intervals, there is consistency between temperature & generation")

        # plt.hist(all_pearsons)
        # plt.show()

        return time_ranges_by_name_code

    def labeling_point(self, df_n_c, date, label):
        for date1, date2 in date:
            flag_array = Data_selector(df_n_c).filter_time(date1, date2, get_bool=True)
            self.df.loc[flag_array.index[flag_array], "is_good_peak"] = label

    def add_difference_column(self, feature, order=1):
        df = self.df
        self.add_date_time()

        new_feature = f"{feature}_difference_{order}_order"
        df[new_feature] = None
        power_plants = df[['name', 'code']].drop_duplicates()
        for _, row in power_plants.iterrows():
            df_name_code_smooth = Data_selector(df).filter_name_code(row["name"], row["code"])
            df.loc[df_name_code_smooth.index, new_feature] = df_name_code_smooth[feature].diff()

        df[new_feature] = df[new_feature].astype(float)
        self.df.reset_index(drop=True, inplace=True)


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


def get_interval(df, l_min, max_diff):
    df_s = df.reset_index(drop=True)
    gap_mask_time = df_s['datetime'].diff() != pd.Timedelta(
        hours=1)  # هر جایی اختلاف دقیقاً 1 ساعت نیست، مرز بازه جدید است
    gap_mask_generatiion = df_s['generation'].diff().abs() > max_diff
    gap_mask = gap_mask_time | gap_mask_generatiion
    # ایندکس شروع بازه‌ها
    start_indices = df_s.index[gap_mask].tolist()
    # چون اولین ایندکس هم ابتدای یک بازه است، اگر نیست اضافه می‌کنیم
    if 0 not in start_indices: start_indices = [0] + start_indices
    # ایندکس پایان بازه‌ها یکی قبل از شروع بازه بعدی است
    end_indices = [i - 1 for i in start_indices[1:]] + [df_s.index[-1]]

    # ساخت لیست بازه‌های (i1, i2)
    index_ranges = [(start_indices[i], end_indices[i]) for i in range(len(start_indices)) if
                    end_indices[i] - start_indices[i] >= l_min - 1]
    # ساخت لیست بازه‌های زمانی (t1, t2)
    time_ranges = [(df_s.loc[i1, 'datetime'], df_s.loc[i2, 'datetime']) for i1, i2 in index_ranges]

    return time_ranges
