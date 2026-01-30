import numpy as np
import pandas as pd
from scipy.signal import find_peaks
from scipy.stats import gaussian_kde

from src.logs.logger import CustomLogger
from src.models.data_selection.data_selector import Data_selector

logger = CustomLogger(__name__).get_logger()


class Feature_adder:
    def __init__(self, df: pd.DataFrame, add_label_column=True):
        self.df = df
        self.add_time_features()
        self.df.sort_values(by=['name', 'code', 'datetime'], inplace=True)
        if add_label_column:
            self.df["is_good_peak"] = 0

    def add_time_features(self):
        self.df['date'] = pd.to_datetime(self.df['date'])
        self.df['datetime'] = self.df['date'] + pd.to_timedelta(self.df['hour'], unit='h')

        logger.debug(f"Temporal columns were created")

    def select_gas_plants(self):
        self.df = self.df[self.df['code'].str.startswith("G")]

    def select_active_hours(self, init_label, final_label):
        df_modified = Data_selector(self.df).select_peaks(init_label)
        peak_condition = (df_modified['status'] == 'SO') | (df_modified['status'] == 'LF1')

        self.df.loc[peak_condition, "is_good_peak"] = final_label
        self.log_filter_ratio(new_label=final_label, old_label=init_label)

    def select_peak_level_hours(self, init_label, final_label):
        df_modified = Data_selector(self.df).select_peaks(init_label)

        start_md = "05-22"
        end_md = "09-22"
        statusM_mask = (df_modified['date'].dt.strftime('%m-%d') >= start_md) & (
                df_modified['date'].dt.strftime('%m-%d') <= end_md)
        peak_condition = (df_modified['load_level'] == 'P') | (df_modified['load_level'] == 'M') & statusM_mask

        self.df.loc[peak_condition[peak_condition].index, "is_good_peak"] = final_label

        self.log_filter_ratio(new_label=final_label, old_label=init_label)

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
            split_date = split_dates_by_name_code.get(name, {}).get(code)
            if split_date is None:
                chosen_indices = df_name_code.index
            else:
                chosen_indices = df_name_code.loc[df_name_code['datetime'] > split_date].index
            all_chosen_indices.extend(chosen_indices)

        self.df.loc[all_chosen_indices, "is_good_peak"] = final_label
        self.log_filter_ratio(new_label=final_label, old_label=init_label)

    def select_turbo_hours(self, coefs, turbo_dict, p_min, p_max, delta, interval, init_label, final_label):
        features = ["name", "code", "generation", "temperature", "is_good_peak"]
        df_modified = self.df[features].copy(deep=True)
        df_modified = Data_selector(df_modified).select_peaks(init_label)

        df_modified = remove_units_with_insufficient_data(df_modified, thresh=10)

        ds = Data_selector(df_modified)
        power_plants = df_modified[['name', 'code']].drop_duplicates()

        all_indices = []
        for row in power_plants.itertuples():
            name, code = row.name, row.code
            coef = coefs.get((name, code))
            if turbo_dict.get(name, {}).get(code) and coef:
                one_unit_df = ds.filter_name_code(name, code)
                t = one_unit_df["temperature"]
                g = one_unit_df['generation']
                a, _ = coef
                a, b = find_best_gap_line_given_slope(t, g, a, p_min, p_max, delta, interval)
                g_ceil = a * t + b
                upper_line_indices = (g[g > g_ceil + delta]).index

                all_indices.extend(upper_line_indices)

        self.df.loc[all_indices, "is_good_peak"] = final_label
        self.log_filter_ratio(new_label=final_label, old_label=init_label)

    def select_envelope(self, init_label, final_label, p, q, dt, min_temp=None):
        features = ["name", "code", "generation", "temperature", "is_good_peak"]
        df_filtered = self.df[features].copy(deep=True)
        df_filtered = Data_selector(df_filtered).select_peaks(init_label)
        data_selector = Data_selector(df_filtered)
        plant_units = df_filtered[['name', 'code']].drop_duplicates()

        all_selected_indices = []
        for unit in plant_units.itertuples():
            plant_name, plant_code = unit.name, unit.code
            unit_df = data_selector.filter_name_code(plant_name, plant_code)

            temperatures = unit_df["temperature"]
            generation = unit_df["generation"]

            temp_min = int(np.floor(temperatures.min()))
            if min_temp:
                temp_min = max(min_temp, temp_min)
            temp_max = int(np.ceil(temperatures.max()))

            unit_selected_indices = []

            for temp in range(temp_min, temp_max, dt):
                temp_mask = (temperatures >= temp) & (temperatures <= temp + dt)
                y_values = generation[temp_mask]

                if len(y_values) <= 10:
                    continue

                if p >= 1:
                    sorted_indices = np.argsort(y_values.values)
                    threshold_index = sorted_indices[-p]
                    y_threshold = y_values.values[threshold_index]
                else:
                    y_threshold = np.quantile(y_values, p=p)

                envelope_mask = (y_values >= y_threshold * (1 - q)) & (y_values <= y_threshold * (1 + q))
                unit_selected_indices.extend(envelope_mask[envelope_mask].index)

            all_selected_indices.extend(unit_selected_indices)

        self.df.loc[all_selected_indices, "is_good_peak"] = final_label
        self.log_filter_ratio(new_label=final_label, old_label=init_label)

    def log_filter_ratio(self, new_label, old_label):
        count_new_label = len(Data_selector(self.df).select_peaks(new_label))
        count_old_label = len(Data_selector(self.df).select_peaks(old_label))
        consistency_percentage = count_new_label / (count_new_label + count_old_label) * 100
        logger.info(f"{consistency_percentage:0.2f}% of rows have been chosen by filter{new_label}")


def remove_units_with_insufficient_data(df, thresh):
    selected_units = df.groupby(['name', 'code']).filter(lambda x: len(x) >= thresh)

    return selected_units


def find_best_gap_line_given_slope(x, y, a, p_min, p_max, delta, interval):
    value_min = get_intercept_for_quantile(x, y, a, p_min, interval)
    if value_min is None:
        value_min = -200
    temp_min = value_min / np.sqrt(a ** 2 + 1)

    value_max = get_intercept_for_quantile(x, y, a, p_max, interval)
    if value_max is None:
        value_max = 200
    temp_max = (value_max + delta) / np.sqrt(a ** 2 + 1)

    normalized_residuals = (y - a * x) / np.sqrt(a ** 2 + 1)
    valley_position = find_valley_on_projection(normalized_residuals, temp_min, temp_max)

    if valley_position is None:
        return a, np.inf

    b = valley_position * np.sqrt(a ** 2 + 1)
    return a, b


def get_intercept_for_quantile(x, y, slope, quantile, interval):
    mask = (interval[0] <= x) & (x <= interval[1])
    x_filtered = x[mask]
    y_filtered = y[mask]
    if len(x_filtered) == 0:
        return None
    residuals = y_filtered - slope * x_filtered
    b = np.quantile(residuals, quantile)
    return b


def find_valley_on_projection(values, temp_min, temp_max):
    kde = gaussian_kde(values)
    grid = np.linspace(values.min(), values.max(), 500)

    left_idx = np.searchsorted(grid, temp_min, side='left')
    right_idx = np.searchsorted(grid, temp_max, side='left')

    density = kde(grid)
    peaks, _ = find_peaks(density)
    if len(peaks) < 2:
        return None

    top_two_peaks = peaks[np.argsort(density[peaks])[-2:]]
    left_peak, right_peak = np.sort(top_two_peaks)

    if right_idx <= left_peak:
        valley_idx = right_idx
    elif left_idx >= right_peak:
        valley_idx = left_idx
    else:
        left_bound = max(left_peak, left_idx)
        right_bound = min(right_peak, right_idx)
        valley_idx = left_bound + np.argmin(density[left_bound:right_bound])

    return grid[valley_idx]
