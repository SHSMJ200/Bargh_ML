import numpy as np
import pandas as pd
from scipy.signal import find_peaks
from scipy.stats import gaussian_kde

from logs.logger import CustomLogger
from src.models.data_selection.data_selector import Data_selector

logger = CustomLogger(__name__).get_logger()


class Feature_adder:
    def __init__(self, df: pd.DataFrame, temp_feature, add_label_column=True):
        self.df = df
        self.temp_feature = temp_feature
        self.time_ranges_by_name_code = {}
        self.c_time_ranges_by_name_code = {}

        self.add_time_features()
        self.df.sort_values(by=['name', 'code', 'datetime'], inplace=True)

        if add_label_column:
            self.df["is_good_peak"] = 0

    def create_feature_with_delay(self, feature, n_delay):
        new_feature = f"{feature}_with_{n_delay}_delay"
        sorted_df = self.df.sort_values(by=['code', 'name', 'datetime'])
        self.df[new_feature] = sorted_df.groupby(['code', 'name'])[feature].shift(n_delay)

        logger.debug(f"A new column created: {feature} with {n_delay} hours delay")

    def add_time_features(self):
        self.df['date'] = pd.to_datetime(self.df['date'])
        self.df['datetime'] = self.df['date'] + pd.to_timedelta(self.df['hour'], unit='h')
        self.df['season'] = self.df['date'].apply(get_season)
        self.df['day_of_week'] = self.df['datetime'].dt.dayofweek
        self.df['month'] = self.df['datetime'].dt.month

        logger.debug(f"Temporal columns were created")

    def select_gas_plants(self):
        self.df = self.df[self.df['code'].str.startswith("G")]

    def select_active_hours(self, init_label, final_label):
        df_modified = Data_selector(self.df).select_peaks(init_label)
        peak_condition = (df_modified['status'] == 'SO') | (df_modified['status'] == 'LF1')
        self.df.loc[peak_condition, "is_good_peak"] = final_label

        self.log_filter_ratio(label=final_label, old_label=init_label)

    def select_peak_level_hours(self, init_label, final_label):
        df_modified = Data_selector(self.df).select_peaks(init_label)

        # We assume that start_md < end_md
        start_md = "05-22"
        end_md = "09-22"
        statusM_mask = (df_modified['date'].dt.strftime('%m-%d') >= start_md) & (
                df_modified['date'].dt.strftime('%m-%d') <= end_md)
        peak_condition = (df_modified['load_level'] == 'P') | (df_modified['load_level'] == 'M') & statusM_mask

        self.df.loc[peak_condition[peak_condition].index, "is_good_peak"] = final_label

        self.log_filter_ratio(label=final_label, old_label=init_label)

    def select_normal_change_hours(self, l_min, max_diff, init_label, final_label):
        features = ["name", "code", "datetime", "generation", "is_good_peak"]
        df_modified = self.df[features].copy(deep=True)
        df_modified = Data_selector(df_modified).select_peaks(init_label)

        power_plants = df_modified[['name', 'code']].drop_duplicates()
        ds = Data_selector(df_modified)

        for _, row in power_plants.iterrows():
            name, code = row['name'], row['code']
            df_name_code = ds.filter_name_code(name, code)
            time_ranges = get_interval(df_name_code, l_min, max_diff)
            self.time_ranges_by_name_code[(name, code)] = time_ranges

        self.label_points(ds, power_plants, self.time_ranges_by_name_code, label=final_label)

        self.log_filter_ratio(label=final_label, old_label=init_label)

    def select_last_updated_plants_generation_function(self, split_dates_by_name_code, init_label, final_label):
        features = ['name', 'code', "datetime", "is_good_peak"]
        df_modified = self.df[features].copy(deep=True)
        df_modified = Data_selector(df_modified).select_peaks(init_label)

        power_plants = df_modified[['name', 'code']].drop_duplicates()
        ds = Data_selector(df_modified)
        all_chosen_indices = []
        for row in power_plants.itertuples():
            name, code = row.name, row.code
            df_name_code = ds.filter_name_code(name, code)
            split_date = split_dates_by_name_code.get((name, code))
            if split_date is None:
                chosen_indices = df_name_code.index
            else:
                chosen_indices = df_name_code.loc[df_name_code['datetime'] > split_date].index
            all_chosen_indices.extend(chosen_indices)

        self.df.loc[all_chosen_indices, "is_good_peak"] = final_label

        self.log_filter_ratio(label=final_label, old_label=init_label)

    def select_turbo_hours(self, df_factors, turbo_dict, p_min, p_max, delta, interval, init_label, final_label):

        features = ["name", "code", "generation", f"{self.temp_feature}", "is_good_peak"]
        df_modified = self.df[features].copy(deep=True)
        df_modified = Data_selector(df_modified).select_peaks(init_label)

        ds = Data_selector(df_modified)

        coefs = self.get_coefs(df_factors)

        power_plants = df_modified[['name', 'code']].drop_duplicates()

        all_indices = []
        for row in power_plants.itertuples():
            name, code = row.name, row.code
            one_unit_df = ds.filter_name_code(name, code)

            t = one_unit_df[self.temp_feature]
            g = one_unit_df['generation']

            coef = coefs.get((name, code))
            if coef is None:
                continue
            a, b = coef
            if not turbo_dict.get(name, {}).get(code):
                continue
            try:
                a, b = find_best_gap_line_given_a(t, g, a, p_min, p_max, delta, interval)
            except Exception as e:
                print(name, code)
                print(len(one_unit_df))
            g_ceil = a * t + b

            upper_line_indices = (g[g > g_ceil + delta]).index

            all_indices.extend(upper_line_indices)

        self.df.loc[all_indices, "is_good_peak"] = final_label

        self.log_filter_ratio(label=final_label, old_label=init_label)

    def select_envelope(self, init_label, final_label, p=0.98, q=0.01, dt=1, min_temp=None):
        features = ["name", "code", "generation", f"{self.temp_feature}", "is_good_peak"]
        df_modified = self.df[features].copy(deep=True)
        df_modified = Data_selector(df_modified).select_peaks(init_label, is_tight=True)

        ds = Data_selector(df_modified)

        all_indices = []
        power_plants = df_modified[['name', 'code']].drop_duplicates()
        for row in power_plants.itertuples():
            name, code = row.name, row.code
            one_unit_df = ds.filter_name_code(name, code)

            sens_temps = one_unit_df[self.temp_feature]
            gens = one_unit_df['generation']

            tmin = int(np.floor(min(sens_temps)))
            if min_temp:
                tmin = max(min_temp, tmin)

            tmax = int(np.ceil(max(sens_temps)))
            above_curve_indices = []

            temp = tmin
            temp2 = tmin + dt
            while temp < tmax:
                mask = (sens_temps >= temp) & (sens_temps <= temp2)
                y = gens[mask]
                if len(y) <= 10:
                    print(name, code, temp, "-", temp2)
                    #if temp2 > tmax:
                    #    temp = temp2 #break
                    #temp2 += dt

                    temp = temp2
                    temp2 += dt
                    continue

                if p >= 1:
                    index_sorted = np.argsort(y.values)
                    index = index_sorted[-p]
                    y_th = y.values[index]
                else:
                    y_th = np.quantile(y, p=p)
                mask_y = (y >= y_th * (1 - q)) & (y <= y_th * (1 + q))

                above_curve_indices.extend(mask_y[mask_y].index)

                temp = temp2
                temp2 += dt

            all_indices.extend(above_curve_indices)

        self.df.loc[all_indices, "is_good_peak"] = final_label

        self.log_filter_ratio(label=final_label, old_label=init_label)

    def label_points(self, ds, power_plants, dates_by_name_code, label):
        all_indices = []
        for _, row in power_plants.iterrows():
            name, code = row['name'], row['code']
            df_name_code = ds.filter_name_code(name, code)
            dates = dates_by_name_code[(name, code)]

            interval_ds = Data_selector(df_name_code)
            for date1, date2 in dates:
                flag_array = interval_ds.filter_time(date1, date2, get_mask=True)
                indices = flag_array.index[flag_array]
                all_indices.extend(indices)
        self.df.loc[all_indices, "is_good_peak"] = label

    def get_coefs(self, df_factors):
        df_factors["Date"] = pd.to_datetime(df_factors["Date"])
        coefs = {}
        grouped = df_factors.groupby(['PowerPlantCode', 'PowerPlantName', "UnitCode"])
        for (pp_code, pp_name, unit_code), g in grouped:
            latest_row = g.sort_values("Date", ascending=False).iloc[0]
            coefs[(pp_name, unit_code)] = (latest_row["a1IndexGas"], latest_row["b1IndexGas"])
        return coefs

    def log_filter_ratio(self, label, old_label=None):
        if old_label is None: old_label = label - 1

        count_new_label = len(self.df[self.df["is_good_peak"] == label])
        count_old_label = len(self.df[self.df["is_good_peak"] >= old_label])
        consistency_percentage = count_new_label / count_old_label * 100
        logger.info(f"{consistency_percentage:0.2f}% of rows have been chosen by filter{label}")


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
    gap_mask_generation_up = (df_s['generation'].diff() > max_diff) & (~gap_mask_time)
    gap_mask_generation_down = (df_s['generation'].diff() < -max_diff) & (~gap_mask_time)
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


def find_best_gap_line_given_a(x, y, a, p_min, p_max, delta, interval):
    t = (y - a * x) / np.sqrt(a * a + 1)
    temp_min = get_b_c(x, y, a, p_min, interval) / np.sqrt(a * a + 1)
    temp_max = (get_b_c(x, y, a, p_max, interval) + delta) / np.sqrt(a * a + 1)
    t0, density_min = find_valley_on_projection(t, temp_min, temp_max)
    if t0 is None:
        return None

    b = t0 * np.sqrt(a * a + 1)
    print(b, ":", get_b_c(x, y, a, p_min, interval), (get_b_c(x, y, a, p_max, interval) + delta))
    best_params = (a, b)

    return best_params


def get_b_c(x, y, a, p, interval=(0, 25)):
    mask = (interval[0] <= x) & (x <= interval[1])
    x = x[mask]
    y = y[mask]

    r = y - a * x
    b_c = np.quantile(r, p)
    return b_c


def find_valley_on_projection(t, temp_min=-1000, temp_max=1000):
    kde = gaussian_kde(t)
    t_grid = np.linspace(t.min(), t.max(), 500)
    left_1 = np.searchsorted(t_grid, temp_min, side='left')
    right_1 = np.searchsorted(t_grid, temp_max, side='left')

    density = kde(t_grid)
    peaks, _ = find_peaks(density)
    if len(peaks) < 2:
        return None, None

    peaks = peaks[np.argsort(density[peaks])[-2:]]
    left_0, right_0 = np.sort(peaks)
    print(left_0, left_1, "---", right_0, right_1)
    if right_1 <= left_0:
        valley_idx = right_1
    elif left_1 >= right_0:
        valley_idx = left_1
    else:
        left = max(left_0, left_1)
        right = min(right_0, right_1)
        valley_idx = left + np.argmin(density[left:right])

    # plot_callback(t_grid, density, (left_0, left_1, right_0, right_1, valley_idx))
    return t_grid[valley_idx], density[valley_idx]
