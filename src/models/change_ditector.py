import sys
import os
import pandas as pd
import numpy as np
from scipy.signal import savgol_filter
import matplotlib.pyplot as plt

from data_selector import Data_selector
from feature_modifier import Feature_selector, Feature_adder
from logs.logger import CustomLogger
from models import Random_Forest, Linear
from src.root import get_root

# اضافه‌کردن مسیر ریشه پروژه به sys.path
current_dir = os.getcwd()
project_root = current_dir[:current_dir.find("src") - 1]
sys.path.insert(0, project_root)

logger = CustomLogger(name="model_main", log_file_name='model_main.log').get_logger()


def filter_time(df, start_date, end_date, get_bool=False):
    """
    فیلتر کردن داده‌ها بر اساس بازه زمانی
    """
    mask = (df['datetime'] >= start_date) & (df['datetime'] <= end_date)
    if get_bool:
        return mask
    return df[mask]


def filter_name_code(df, name, code, get_bool=False):
    """
    فیلتر کردن داده‌ها بر اساس نام و کد
    """
    mask = (df["name"] == name) & (df["code"] == code)
    if get_bool:
        return mask
    return df[mask]


def get_interval(df, min_length):
    """
    بازگرداندن فهرست بازه‌های زمانی داده با حداقل طول
    """
    df_sorted = df.reset_index(drop=True)
    time_diff = df_sorted['datetime'].diff()
    gap_mask = time_diff != pd.Timedelta(hours=1)
    
    start_indices = df_sorted.index[gap_mask].tolist()
    if 0 not in start_indices:
        start_indices = [0] + start_indices
    end_indices = [i - 1 for i in start_indices[1:]] + [df_sorted.index[-1]]

    # حذف بازه‌های کوتاه‌تر از min_length
    for i in range(len(start_indices) - 1, -1, -1):
        if end_indices[i] - start_indices[i] < min_length - 1:
            end_indices.pop(i)
            start_indices.pop(i)

    index_ranges = list(zip(start_indices, end_indices))
    time_ranges = [
        (df_sorted.loc[s, 'datetime'], df_sorted.loc[e, 'datetime'])
        for s, e in index_ranges
    ]
    return index_ranges, time_ranges


def is_smooth_array(data, threshold, show_plot=False):
    """
    بررسی میزان یکنواختی داده‌ها
    """
    x = data["datetime"].to_numpy()
    y = data["generation"].to_numpy()
    window_length = len(y) - 1 + len(y) % 2
    polyorder = 2
    y_smooth = savgol_filter(y, window_length=window_length, polyorder=polyorder)

    residuals = y - y_smooth
    noise_std = np.std(residuals)
    value = np.var(np.diff(y, n=1))

    if show_plot:
        plt.plot(x, y, label='Raw data')
        plt.plot(x, y_smooth, label='Smoothed curve')
        plt.legend()
        plt.show()

    is_smooth = noise_std < threshold
    return noise_std, is_smooth, value


def is_smooth(df, start_date, end_date, data_var, data_date, threshold, flag=False):
    """
    بررسی یکنواختی داده در یک بازه زمانی مشخص
    """
    sliced = filter_time(df, start_date, end_date)[["hour", "generation", "datetime"]]
    if len(sliced) >= 8:
        noise, smooth, _ = is_smooth_array(sliced, threshold, flag)
        if data_var is not None:
            data_var[smooth].append(noise)
        if data_date is not None:
            data_date[smooth].append((start_date, end_date))
        return 1, int(smooth)
    return 0, 0


def get_smooth_good_slice(df, time_ranges, threshold):
    """
    استخراج درصد بخش‌های یکنواخت (smooth) در بازه‌های داده شده
    """
    data_var = {True: [], False: []}
    data_date = {True: [], False: []}
    total, count = 0, 0
    for start, end in time_ranges:
        n, t = is_smooth(df, start, end, data_var, data_date, threshold)
        count += n
        total += t
    percent = (total / count * 100) if count != 0 else None
    return percent, data_var, data_date


def labeling_point(df, df_nc, date_ranges, label):
    """
    برچسب‌گذاری داده‌ها بر اساس بازه زمانی مشخص
    """
    for start, end in date_ranges:
        mask = filter_time(df_nc, start, end, get_bool=True)
        df.loc[mask.index[mask], "is_good_pick"] = label


def main():
    df_raw = pd.read_csv(get_root() + "/data/processed/integrated.csv", encoding='utf-8')
    df_raw["is_good_pick"] = 0
    df_modified = df_raw[["name", "code", "date", "hour", "status", "value", "generation"]].copy(deep=True)

    feature_adder = Feature_adder(df_modified)
    feature_adder.add_season()
    df_modified = Data_selector(df_modified).select(m_in_summer=True)
    
    df_modified['datetime'] = df_modified['date'] + pd.to_timedelta(df_raw['hour'], unit='h')
    
    threshold = 2
    
    power_plants = df_modified[['name', 'code']].drop_duplicates()
    n = len(power_plants)
    for k, (_, row) in enumerate(power_plants.iterrows()):
        print(f"{k / n * 100:.0f}%")
        
        df_name_code = filter_name_code(df_modified, row["name"], row["code"])
        _, time_ranges = get_interval(df_name_code, min_length=4)
        percent, data_var, data_date = get_smooth_good_slice(df_name_code, time_ranges, threshold)
        if percent not in (0, None):
            print(row["name"], row["code"], percent)

        # برای فعال‌سازی برچسب‌زنی، این خطوط را از حالت کامنت خارج کنید:
        # labeling_point(df_raw, df_name_code, data_date[False], label=1)
        # labeling_point(df_raw, df_name_code, data_date[True], label=2)

    # برای ذخیره نتایج، در صورت نیاز این خط را فعال کنید:
    # df_raw.to_csv(get_root() + '/data/processed/prediction_only.csv', index=False, sep=',', header=True, na_rep='NULL')


if __name__ == "__main__":
    main()
