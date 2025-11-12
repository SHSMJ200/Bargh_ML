import numpy as np
import pandas as pd
from scipy.stats import pearsonr
from sklearn.linear_model import RANSACRegressor, LinearRegression
from sklearn.metrics import classification_report
from sklearn.svm import SVC
from scipy.interpolate import make_interp_spline

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

    def filter1(self):
        peak_condition = (self.df['status'] == 'SO') | (self.df['status'] == 'LF1')
        self.df.loc[peak_condition, "is_good_peak"] = 1

        self.log_filter_ratio(label=1)

    def filter2(self, initial_label):
        df_modified = self.df[self.df["is_good_peak"] >= initial_label]

        # We assume that start_md < end_md
        start_md = "05-22"
        end_md = "09-22"
        statusM_mask = (df_modified['date'].dt.strftime('%m-%d') >= start_md) & (
                df_modified['date'].dt.strftime('%m-%d') <= end_md)
        peak_condition = (df_modified['load_level'] == 'P') | (df_modified['load_level'] == 'M') & statusM_mask

        self.df.loc[peak_condition[peak_condition].index, "is_good_peak"] = 2

        self.log_filter_ratio(label=2)

    def filter3(self, l_min, max_diff, initial_label):
        features = ["name", "code", "datetime", "generation", "is_good_peak"]
        df_modified = self.df[features].copy(deep=True)
        df_modified = df_modified[df_modified["is_good_peak"] >= initial_label]

        power_plants = df_modified[['name', 'code']].drop_duplicates()
        ds = Data_selector(df_modified)

        for _, row in power_plants.iterrows():
            name, code = row['name'], row['code']
            df_name_code = ds.filter_name_code(name, code)
            time_ranges = get_interval(df_name_code, l_min, max_diff)
            self.time_ranges_by_name_code[(name, code)] = time_ranges

        self.label_points(ds, power_plants, self.time_ranges_by_name_code, label=3)

        self.log_filter_ratio(label=3, old_label=initial_label)

    def filter4(self, initial_label, c_thresh=0.9):
        features = ["name", "code", "datetime", f"{self.temp_feature}", "generation", "is_good_peak"]
        df_modified = self.df[features].copy(deep=True)
        df_modified = df_modified[df_modified["is_good_peak"] >= initial_label]

        corr_pearsons = []
        power_plants = df_modified[['name', 'code']].drop_duplicates()
        ds = Data_selector(df_modified)

        for _, row in power_plants.iterrows():
            name, code = row['name'], row['code']
            df_name_code = ds.filter_name_code(name, code)
            time_ranges = self.time_ranges_by_name_code[(name, code)]
            consistent_time_ranges = find_consistency(df_name_code, self.temp_feature, time_ranges, c_thresh,
                                                      corr_pearsons)
            self.c_time_ranges_by_name_code[(name, code)] = consistent_time_ranges

        self.label_points(ds, power_plants, self.c_time_ranges_by_name_code, label=4)

        self.log_filter_ratio(label=4, old_label=initial_label)

    def filter5(self, initial_label, thresh=0.9, k_filter=6, n_filter=4, l_min=3):
        # self.add_interval_id(initial_label)

        features = ['name', 'code', "datetime", "generation", f"{self.temp_feature}", "is_good_peak"]
        df_modified_orginal = self.df[features].copy(deep=True)
        df_modified = df_modified_orginal[df_modified_orginal['is_good_peak'] >= initial_label]

        power_plants = df_modified[['name', 'code']].drop_duplicates()
        ds = Data_selector(df_modified)
        dates_by_name_code = {}
        for row in power_plants.itertuples():
            name, code = row.name, row.code
            df_name_code = ds.filter_name_code(name, code)

            df_year_month, min_date, max_date = get_df_year_month(df_name_code)

            weights, accuracies, dates = compare_months(df_name_code, df_year_month, self.temp_feature)
            if len(dates) == 0:
                print(name, code)
                print(len(df_year_month), min_date, max_date)
            dates_by_name_code[(name, code)] = get_date_interval(accuracies, dates, weights, min_date, max_date, thresh,
                                                                 k_filter, n_filter, l_min)
        print(dates_by_name_code)
        ds_label_1 = Data_selector(df_modified_orginal[df_modified_orginal['is_good_peak'] >= 1])
        self.label_points(ds, power_plants, dates_by_name_code, label=5)

        self.log_filter_ratio(label=5, old_label=initial_label)

    def filter6(self, bin_length, initial_label):
        features = ["name", "code", "generation", f"{self.temp_feature}", "is_good_peak"]
        df_modified = self.df[features].copy(deep=True)
        df_modified = df_modified[df_modified["is_good_peak"] >= initial_label]
        ds = Data_selector(df_modified)

        all_indices = []
        power_plants = df_modified[['name', 'code']].drop_duplicates()
        for row in power_plants.itertuples():
            name, code = row.name, row.code
            one_unit_df = ds.filter_name_code(name, code)

            sens_temps = one_unit_df[self.temp_feature].values
            gens = one_unit_df['generation'].values

            X, y = find_points_on_envelope(gens, sens_temps, bin_length=bin_length, p=0.98)

            near_envelope_indices = select_envelope_neighbors_indices(X, y, one_unit_df, self.temp_feature, alpha=2,
                                                                      beta=5)
            all_indices.extend(near_envelope_indices)

        self.df.loc[all_indices, "is_good_peak"] = 6

        self.log_filter_ratio(label=6, old_label=initial_label)

    def new_filter_5(self, split_dates_by_name_code, initial_label):
        features = ['name', 'code', "datetime", "is_good_peak"]
        df_modified = self.df[features].copy(deep=True)
        df_modified = df_modified[df_modified['is_good_peak'] >= initial_label]

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

        self.df.loc[all_chosen_indices, "is_good_peak"] = 5

        self.log_filter_ratio(label=5, old_label=initial_label)

    def new_filter_6(self, bin_length, initial_label):
        features = ["name", "code", "generation", f"{self.temp_feature}", "is_good_peak"]
        df_modified = self.df[features].copy(deep=True)
        df_modified = df_modified[df_modified["is_good_peak"] >= initial_label]
        ds = Data_selector(df_modified)

        all_indices = []
        power_plants = df_modified[['name', 'code']].drop_duplicates()
        for row in power_plants.itertuples():
            name, code = row.name, row.code
            one_unit_df = ds.filter_name_code(name, code)

            try:
                sens_temps = one_unit_df[self.temp_feature].values
                gens = one_unit_df['generation'].values

                X_lower, y_lower = find_points_on_envelope(gens, sens_temps, p=0.8, bin_length=bin_length,
                                                           reshape_X=False)

                X_lower, y_lower = keep_max_y(X_lower, y_lower)

                curve_lower = make_interp_spline(X_lower, y_lower, k=2)

                above_curve_indices = select_points_above_curve(curve_lower, one_unit_df, X=sens_temps, y=gens,
                                                                beta=0.98)

            except:
                print(name, code)
                above_curve_indices = one_unit_df.index

            all_indices.extend(above_curve_indices)

        self.df.loc[all_indices, "is_good_peak"] = 6

        self.log_filter_ratio(label=6, old_label=initial_label)

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

    def add_interval_id(self, initial_label):
        df_modified = self.df[self.df["is_good_peak"] >= initial_label]

        power_plants = df_modified[['name', 'code']].drop_duplicates()
        ds = Data_selector(self.df)

        mapping = {}

        if initial_label == 2:
            dates_by_name_code = self.time_ranges_by_name_code
        elif initial_label == 3:
            dates_by_name_code = self.c_time_ranges_by_name_code
        else:
            dates_by_name_code = {}

        for _, row in power_plants.iterrows():
            name, code = row['name'], row['code']
            df_name_code = ds.filter_name_code(name, code)
            time_ranges = dates_by_name_code[(name, code)]
            interval_ds = Data_selector(df_name_code)
            for interval_id, (date1, date2) in enumerate(time_ranges):
                indices = interval_ds.filter_time(date1, date2).index
                mapping.update({idx: interval_id for idx in indices})

        interval_series = pd.Series(mapping, name="interval_id")
        self.df = self.df.join(interval_series, how="left")

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


def get_percentile(array, p):
    array = np.array(array)
    sorted_array = np.sort(array)
    idx = int(len(array) * p) - 1
    return sorted_array[idx]


def get_df_year_month(df):
    start = df['datetime'].min().replace(day=1)
    end = df['datetime'].max().replace(day=1)

    full_range = pd.date_range(start=start, end=end, freq='MS')
    df_year_month = pd.DataFrame({'year': full_range.year, 'month': full_range.month})

    return df_year_month, start, end


def get_date_interval(accuracies, dates, weights, min_date, max_date, thresh, k_filter=6, n_filter=1, l_min=3):
    normalized_accuracies = iterative_normal_filter(accuracies, k=k_filter, n=n_filter, weights=weights)

    binary_accuracies = [None if x is None else int(x > thresh) for x in normalized_accuracies]

    intervals = find_1_intervals(binary_accuracies, k=l_min)
    dates_cut_offs = find_time_cut_offs(intervals, dates)
    dates_cut_offs = [min_date] + dates_cut_offs
    if max_date + pd.DateOffset(months=1) not in dates_cut_offs:
        dates_cut_offs.append(max_date + pd.DateOffset(months=1))

    date_intervals = [(dates_cut_offs[i], dates_cut_offs[i + 1]) for i in range(len(dates_cut_offs) - 1)]
    return [date_intervals[-1]]


def iterative_normal_filter(result, k, n, weights):
    for _ in range(n):
        result = [normal_filter(result, i, n_neighbors=k, weights=weights) for i in range(len(result))]
    return np.array(result)


def normal_filter(array, index, n_neighbors, weights):
    n_neighbors = min(n_neighbors, index + 1, len(array) - index)
    sum = 0
    num = 0
    for j in range(index - n_neighbors, index + n_neighbors + 1):
        d_sum, d_num = get_element(array, j, weights)
        sum += d_sum
        num += d_num

    return sum / num if num != 0 else None


def get_element(array, i, weights):
    if 0 <= i < len(array):
        if array[i] is not None:
            return array[i] * weights[i], weights[i]
    return 0, 0


def find_1_intervals(b_accuracies, k):
    start = None
    count = 0
    intervals = []
    for i, num in enumerate(b_accuracies):
        if num == 1:
            if start is None:
                start = i
            count += 1
        elif num == 0:
            if count > k:
                intervals.append((start, i))
            start = None
            count = 0

    if count >= k or count == len(b_accuracies):
        intervals.append((start, len(b_accuracies)))

    return intervals


def find_time_cut_offs(intervals, dates):
    end = len(dates)
    if end == 0:
        return []

    cut_offs = []
    one_year = np.timedelta64(1, 'Y').astype('timedelta64[ns]')
    one_month = np.timedelta64(1, 'M').astype('timedelta64[ns]')
    for i1, i2 in intervals:
        if i2 != end:
            cut_offs.append(dates[i2 - 1] + one_month)
        else:
            cut_offs.append(dates[i1] + one_year)
    return cut_offs


def compare_months(df_name_code, df_year_month, temp_feature):
    weights = []
    accuracies = []
    dates = []

    df_year_month = df_year_month.iloc[0:-12]
    for _, row in df_year_month.iterrows():
        month, year = row['month'], row['year']

        df_two_months, n_df1, n_df2 = select_df_two_months(df_name_code, month, month, year, year + 1)
        weights.append(n_df1 + n_df2)

        dates_array = pd.to_datetime({'year': [year], 'month': [month], 'day': [1]})
        if n_df1 != 0 and n_df2 != 0:
            X = df_two_months[["generation", f"{temp_feature}"]]
            y = df_two_months["label_month"]
            sep_acc = linear_separability_check(X, y)
            accuracies.append(sep_acc)
        else:
            accuracies.append(None)
        dates.append(dates_array[0])

    accuracies = np.array(accuracies)
    dates = np.array(dates)
    weights = np.array(weights) / sum(weights)

    return weights, accuracies, dates


def select_df_two_months(df, m1, m2, y1, y2):
    years = df["datetime"].dt.year
    months = df["datetime"].dt.month

    mask1 = (years == y1) & (months == m1)
    df1 = df[mask1].copy()
    df1["label_month"] = 1

    mask2 = (years == y2) & (months == m2)
    df2 = df[mask2].copy()
    df2["label_month"] = 2

    df_two_months = pd.concat([df1, df2])

    return df_two_months, len(df1), len(df2)


def linear_separability_check(X, y):
    model = SVC(kernel='linear')
    model.fit(X, y)
    y_pred = model.predict(X)

    report = classification_report(y, y_pred, output_dict=True, zero_division=0)

    return report['accuracy']


def find_points_on_envelope(gens, sens_temps, p, bin_length=30, reshape_X=True):
    sorted_idx = np.argsort(sens_temps)
    sens_temps_sorted, gens_sorted = sens_temps[sorted_idx], gens[sorted_idx]

    n = len(sens_temps_sorted)
    points = []
    for start in range(0, n, bin_length):
        end = start + bin_length
        if end + bin_length > n:
            end = n
        x = sens_temps_sorted[start: end].mean()
        y = get_percentile(gens_sorted[start: end], p=p)
        points.append((x, y))

        if end == n:
            break

    X = np.array([a for a, b in points])
    y = np.array([b for a, b in points])

    if reshape_X:
        X = X.reshape(-1, 1)
    return X, y


import numpy as np
import matplotlib.pyplot as plt
from sklearn.preprocessing import PolynomialFeatures
from sklearn.linear_model import LinearRegression


class PolynomialModel:
    def __init__(self, degree=2):
        """
        کلاس مدل چندجمله‌ای

        پارامترها:
        degree : int
            درجه چندجمله‌ای
        """
        self.degree = degree
        self.poly_features = PolynomialFeatures(degree=self.degree, include_bias=False)
        self.model = LinearRegression()
        self.is_fitted = False

    def fit(self, X, y):
        """
        فیت کردن مدل

        X : array-like, shape (n_samples,) or (n_samples, 1)
        y : array-like, shape (n_samples,)
        """
        X = np.array(X).reshape(-1, 1)
        y = np.array(y) ** 2
        X_poly = self.poly_features.fit_transform(X)
        self.model.fit(X_poly, y)
        self.is_fitted = True

    def predict(self, X):
        """
        پیش‌بینی y برای X داده شده

        X : array-like
        """
        if not self.is_fitted:
            raise ValueError("Model is not fitted yet.")
        X = np.array(X).reshape(-1, 1)
        X_poly = self.poly_features.transform(X)
        return np.maximum(self.model.predict(X_poly), 0) ** 0.5

    def plot(self, X, y, num_points=100):
        """
        رسم داده‌ها و منحنی فیت شده
        """
        if not self.is_fitted:
            raise ValueError("Model is not fitted yet.")

        X = np.array(X).reshape(-1, 1)
        y = np.array(y)
        X_fit = np.linspace(X.min(), X.max(), num_points).reshape(-1, 1)
        y_fit = self.predict(X_fit)

        plt.scatter(X, y, color='red', label='Data points')
        plt.plot(X_fit, y_fit, color='blue', label=f'Polynomial degree {self.degree}')
        plt.legend()
        plt.show()


def select_envelope_neighbors_indices(X, y, one_unit_df, temp_feature, alpha, beta):
    # model = LinearRegression()#PolynomialModel(degree=2)
    # model.fit(X, y)

    model = RANSACRegressor(estimator=LinearRegression(), min_samples=0.9)

    sens_temps = one_unit_df[temp_feature].values
    gens = one_unit_df['generation'].values

    try:
        model.fit(X, y)
        X_all = sens_temps.reshape(-1, 1)
        y_pred_all = model.predict(X_all)
        is_in_area = (y_pred_all + alpha >= gens) & (gens >= y_pred_all - beta)
    except:
        print(one_unit_df["name"].iloc[0], one_unit_df["code"].iloc[0])
        is_in_area = (gens == gens)
    return one_unit_df[is_in_area].index


def select_points_above_curve(curve, one_unit_df, X, y, beta):
    y_lower_bounds = curve(X)

    return one_unit_df[y >= y_lower_bounds * beta].index


def keep_max_y(X, y):
    X_modified = []
    y_modified = []

    prev_i, prev_j = None, None
    for i, j in zip(X, y):
        if prev_i is None:
            prev_i, prev_j = i, j
        elif i != prev_i:
            X_modified.append(prev_i)
            y_modified.append(prev_j)
            prev_i, prev_j = i, j

        else:
            prev_j = max(prev_j, j)
    X_modified.append(prev_i)
    y_modified.append(prev_j)

    return X_modified, y_modified
