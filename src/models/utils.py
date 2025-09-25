from sklearn.metrics import mean_squared_error
import numpy as np


def compute_relative_rmse(y_pred, y):
    rmse_train_actual = (mean_squared_error(y, y_pred) ** 0.5 / np.mean(y)) * 100
    return rmse_train_actual


def compute_threshold_error(y_pred, y, threshold=0.01):
    y_diff_abs = abs(y_pred - y)
    bad_pred = y_diff_abs > y * threshold
    return np.sum(bad_pred) / len(bad_pred)
