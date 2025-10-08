import numpy as np
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score


def compute_relative_rmse(y_pred, y):
    rmse_error = (mean_squared_error(y, y_pred) ** 0.5 / np.mean(y)) * 100
    return rmse_error


def compute_relative_mae(y_pred, y):
    mae_error = (mean_absolute_error(y, y_pred) / np.mean(y)) * 100
    return mae_error


def compute_r2_score(y_pred, y):
    return r2_score(y, y_pred)
