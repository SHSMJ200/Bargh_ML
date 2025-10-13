import numpy as np
import pandas as pd
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
from sklearn.model_selection import train_test_split


def compute_relative_rmse(y_pred, y):
    rmse_error = (mean_squared_error(y, y_pred) ** 0.5 / np.mean(y)) * 100
    return rmse_error


def compute_relative_mae(y_pred, y):
    mae_error = (mean_absolute_error(y, y_pred) / np.mean(y)) * 100
    return mae_error


def compute_r2_score(y_pred, y):
    return r2_score(y, y_pred)


def split_X_and_y(X, y, test_size, shuffle):
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=test_size, shuffle=shuffle)
    return X_train, X_test, y_train, y_test


def make_onehot(X):
    categorical_cols = X.select_dtypes(include=['object', 'category']).columns
    X = pd.get_dummies(X, columns=categorical_cols, drop_first=True)
    X.columns = X.columns.astype(str)
    return X
