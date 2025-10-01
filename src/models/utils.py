from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
import numpy as np
import jdatetime


def compute_relative_rmse(y_pred, y):
    rmse_error = (mean_squared_error(y, y_pred) ** 0.5 / np.mean(y)) * 100
    return rmse_error


def compute_threshold_error(y_pred, y, threshold=0.01):
    y_diff_abs = abs(y_pred - y)
    bad_pred = y_diff_abs > y * threshold
    return (np.sum(bad_pred) / len(bad_pred)) * 100


def compute_relative_mae(y_pred, y):
    mae_error = (mean_absolute_error(y, y_pred) / np.mean(y)) * 100
    return mae_error


def compute_r2_score(y_pred, y):
    return r2_score(y, y_pred)


def jalali_to_gregorian_fast(date_str):
    jy, jm, jd = map(int, date_str.split('/'))
    jdate = jdatetime.date(jy, jm, jd)
    gdate = jdate.togregorian()
    return f"{gdate.year}-{gdate.month:02d}-{gdate.day:02d}"
