import pandas as pd
from scipy.stats import pearsonr

from data_selector import Data_selector
from logs.logger import CustomLogger
import matplotlib.pyplot as plt
import numpy as np

logger = CustomLogger(name="feature_adder", log_file_name='feature_adder.log').get_logger()


class Feature_adder:
    def __init__(self, df: pd.DataFrame,add_label_column=True):
        self.df = df
        self.add_season()
        self.add_date_time()
        if add_label_column : self.df["is_good_peak"] = 0
        self.time_ranges_by_name_code = {}

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

    def filter1(self):
        peak_condition = (self.df['value'] == 'P') | (self.df['value'] == 'M') & (self.df['season'] == 'summer')
        peak_condition = peak_condition & ((self.df['status'] == 'SO') | (self.df['status'] == 'LF1'))
        self.df.loc[peak_condition, "is_good_peak"] = 1

        self.log_filter_ratio(label=1)

    def filter2(self, l_min, max_diff):
        df_modified = self.df[["name", "code", "datetime", "generation", "is_good_peak"]].copy(deep=True)
        df_modified = df_modified[df_modified["is_good_peak"] >= 1]
        power_plants = df_modified[['name', 'code']].drop_duplicates()
        ds = Data_selector(df_modified)
        for _, row in power_plants.iterrows():
            df_name_code = ds.filter_name_code(row["name"], row["code"])
            time_ranges = get_interval(df_name_code, l_min, max_diff)
            self.time_ranges_by_name_code[(row["name"], row["code"])] = time_ranges
            self.label_points(df_name_code, time_ranges, label=2)

        self.log_filter_ratio(label=2)

    def filter3(self, feature1, c_thresh=0.9, plot_pearsons_hist=False):
        features = ["name", "code", "datetime", "generation", "is_good_peak"] + [feature1]
        df_modified = self.df[features].copy(deep=True)
        df_modified = df_modified[df_modified["is_good_peak"] >= 2]

        corr_pearsons = []
        power_plants = df_modified[['name', 'code']].drop_duplicates()
        for _, row in power_plants.iterrows():
            df_name_code = Data_selector(df_modified).filter_name_code(row["name"], row["code"])
            time_ranges = self.time_ranges_by_name_code[(row["name"], row["code"])]
            consistent_time_ranges = find_consistency(df_name_code, feature1, time_ranges, c_thresh, corr_pearsons)
            self.label_points(df_name_code, consistent_time_ranges, label=3)

        self.log_filter_ratio(label=3)

        if plot_pearsons_hist:
            plt.hist(corr_pearsons)
            plt.show()

    def log_filter_ratio(self, label):
        count_new_label = len(self.df[self.df["is_good_peak"] == label])
        count_old_label = len(self.df[self.df["is_good_peak"] >= label - 1])
        consistency_percentage = count_new_label / count_old_label * 100
        logger.info(f"{consistency_percentage:0.2f}% of rows have been chosen by filter{label}")

    def label_points(self, df_n_c, dates, label):
        for date1, date2 in dates:
            flag_array = Data_selector(df_n_c).filter_time(date1, date2, get_mask=True)
            self.df.loc[flag_array.index[flag_array], "is_good_peak"] = label

    def add_difference_column(self, feature, order=1):
        df = self.df

        new_feature = f"{feature}_difference_{order}_order"
        df[new_feature] = None
        power_plants = df[['name', 'code']].drop_duplicates()
        for _, row in power_plants.iterrows():
            df_name_code = Data_selector(df).filter_name_code(row["name"], row["code"])
            df.loc[df_name_code.index, new_feature] = df_name_code[feature].diff()

        df[new_feature] = df[new_feature].astype(float)


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
    gap_mask_time = df_s['datetime'].diff() != pd.Timedelta(hours=1)
    gap_mask_generation_up      = (df_s['generation'].diff() > max_diff) & (~gap_mask_time)
    gap_mask_generation_down    = (df_s['generation'].diff() < -max_diff) & (~gap_mask_time)
    gap_mask = gap_mask_time | gap_mask_generation_up | gap_mask_generation_down

    start_indices = df_s.index[gap_mask].tolist()
    if 0 not in start_indices: start_indices = [0] + start_indices
    end_indices = [i for i in start_indices[1:]] + [df_s.index[-1]]

    index_ranges = []
    for i in range(len(start_indices)):
        if end_indices[i] - start_indices[i] >= l_min:
            if not gap_mask_generation_down[start_indices[i]] and not gap_mask_generation_up[end_indices[i]]:
                index_ranges.append((start_indices[i], end_indices[i] - 1))
                    
                    
    time_ranges = [(df_s.loc[i1, 'datetime'], df_s.loc[i2, 'datetime']) for i1, i2 in index_ranges]

    return time_ranges


def find_consistency(df, feature1, time_ranges, c_thresh, corr_pearsons):
    consistent_time_ranges = []
    data_selector = Data_selector(df)
    for start, end in time_ranges:
        find_interval_consistency(consistent_time_ranges, corr_pearsons, data_selector, feature1, start, end, c_thresh)

    return consistent_time_ranges


def find_interval_consistency(consistent_time_ranges, corr_pearsons, data_selector, feature1, start, end, c_thresh):
    df_sliced = data_selector.filter_time(start, end)

    temperatures = df_sliced[feature1].values
    generations = df_sliced["generation"].values

    corr_pearson, are_similar = are_trends_similar(temperatures, generations, c_thresh, opposite_trend=True)
    corr_pearsons.append(corr_pearson)
    if are_similar:
        consistent_time_ranges.append((start, end))


def are_trends_similar(arr1, arr2, threshold, opposite_trend=False):
    if np.std(arr1) == 0 or np.std(arr2) == 0:
        return 0, np.std(arr1) == np.std(arr2)

    corr_pearson, _ = pearsonr(arr1, arr2)

    if not opposite_trend:
        return corr_pearson, (corr_pearson > threshold)
    return corr_pearson, (corr_pearson < -threshold)
