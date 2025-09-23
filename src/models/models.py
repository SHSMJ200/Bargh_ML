import os

import numpy as np
import xgboost as xgb
from sklearn.ensemble import RandomForestRegressor
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error
from sklearn.model_selection import train_test_split
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import PolynomialFeatures
from sklearn.preprocessing import StandardScaler

os.environ["TF_ENABLE_ONEDNN_OPTS"] = "0"
from tensorflow.keras.layers import Dense, Input
from tensorflow.keras.models import Sequential

from logs.logger import CustomLogger
from src.models.customized_ML_models.DelayModel import DelayModel
from src.models.customized_ML_models.LinearRegressionNorm1 import CustomLinearRegression
from src.models.customized_ML_models.SampleMeanModel import SampleMeanModel

logger = CustomLogger(name="models", log_file_name='models.log').get_logger()


def compute_relative_rmse(y_pred, y):
    rmse_train_actual = (mean_squared_error(y, y_pred) ** 0.5 / np.mean(y)) * 100
    return rmse_train_actual


class Model:
    def __init__(self):
        self.model = None
        self.model_info = None
        self.scaler_x = None
        self.scaler_y = None
        self.y_test = None
        self.X_test = None
        self.y_train = None
        self.X_train = None
        self.y_is_flat = None

    def rescale_and_compute_error(self, do_inverse_scale=True):
        y_pred_test = self.model.predict(self.X_test)
        y_pred_train = self.model.predict(self.X_train)

        y_pred_test_actual  = self.inverse_scale_array(self.scaler_y, y_pred_test)  if do_inverse_scale else y_pred_test
        y_pred_train_actual = self.inverse_scale_array(self.scaler_y, y_pred_train) if do_inverse_scale else y_pred_train
        y_test_actual       = self.inverse_scale_array(self.scaler_y, self.y_test)  if do_inverse_scale else self.y_test
        y_train_actual      = self.inverse_scale_array(self.scaler_y, self.y_train) if do_inverse_scale else self.y_train

        rmse_test_actual = compute_relative_rmse(y_pred_test_actual, y_test_actual)
        rmse_train_actual = compute_relative_rmse(y_pred_train_actual, y_train_actual)

        return rmse_train_actual, rmse_test_actual

    def inverse_scale_array(self,scaler, scaled_arr):
        if self.y_is_flat:
            arr_actual = scaler.inverse_transform(scaled_arr.reshape(-1, 1)).ravel()
        else:
            arr_actual = scaler.inverse_transform(scaled_arr)
        return arr_actual

    def scale_and_split_data(self, X, y, test_size=0.2, random_state=42, do_scale=True, y_is_flat=True):
        self.y_is_flat = y_is_flat
        if do_scale:
            x_scaled, y_scaled = self.scale_data(X, y)
            self.split_data(x_scaled, y_scaled, random_state, test_size)

        else:
            self.split_data(X, y, random_state, test_size)

    def split_data(self, X, y, random_state=42, test_size=0.2):
        if len(X) < 5:
            (X_train, y_train) = X, y
            X_test, y_test = X, y
        else:
            X_train, X_test, y_train, y_test = (
                train_test_split(X, y, test_size=test_size, random_state=random_state))

        self.X_train = X_train
        self.y_train = y_train
        self.X_test = X_test
        self.y_test = y_test

    def scale_data(self, X, y):
        scaler_x = StandardScaler()
        x_scaled = scaler_x.fit_transform(X)
        scaler_y = StandardScaler()
        if self.y_is_flat:
            y_scaled = scaler_y.fit_transform(y.values.reshape(-1, 1))
            y_scaled = y_scaled.flatten()

        else:
            y_scaled = scaler_y.fit_transform(y)

        self.scaler_x = scaler_x
        self.scaler_y = scaler_y
        return x_scaled, y_scaled

    def pred(self, X, do_scale=True):
        if do_scale:
            x_scaled = self.scaler_x.transform(X)
            y_pred_scaled = (self.model.predict(x_scaled)).reshape(-1, 1)
            y_pred = self.scaler_y.inverse_transform(y_pred_scaled)

        else:
            y_pred = self.model.predict(X)
        return y_pred
    



class Linear(Model):
    def __init__(self):
        super().__init__()

    def fit(self):
        try:
            model = LinearRegression()
            model.fit(self.X_train, self.y_train)
            self.model_info = dict(model.get_params().items())
            self.model = model
            logger.debug(msg=f"Model trained successfully.")

        except Exception as e:
            logger.error(f"Couldn't train Linear model. Exception below occurred.\n{e}\n")


class Polynomial(Model):
    def __init__(self, degree=2):
        super().__init__()
        self.degree = degree

    def fit(self):
        degree = self.degree
        try:
            model = make_pipeline(PolynomialFeatures(degree), LinearRegression())
            model.fit(self.X_train, self.y_train)
            self.model_info = dict(model.get_params().items())
            self.model = model
            logger.debug(msg=f"Model trained successfully.")

        except Exception as e:
            logger.error(f"Couldn't train Polynomial(d={degree}) model. Exception below occurred.\n{e}\n")


class Random_Forest(Model):
    def __init__(self, n_estimators=10, max_depth=5):
        super().__init__()
        self.n_estimators = n_estimators
        self.max_depth = max_depth

    def fit(self):
        n_estimators = self.n_estimators
        max_depth = self.max_depth
        try:
            rf_model = RandomForestRegressor(n_estimators=n_estimators, max_depth=max_depth, random_state=42)

            rf_model.fit(self.X_train, self.y_train)

            self.model_info = {
                "n_estimator": n_estimators,
                "depth": max_depth,
            }
            self.model = rf_model

            logger.debug(msg=f"Model trained successfully.")

        except Exception as e:
            logger.error(f"Couldn't train Random Forest model. Exception below occurred.\n{e}\n")


class XGBoost(Model):
    def __init__(self, n_estimators=100, max_depth=3, lr=0.1):
        super().__init__()
        self.n_estimators = n_estimators
        self.max_depth = max_depth
        self.lr = lr

    def fit(self):
        n_estimators = self.n_estimators
        max_depth = self.max_depth
        lr = self.lr
        try:
            model = (xgb.XGBRegressor
                     (objective='reg:squarederror', n_estimators=n_estimators, learning_rate=lr, max_depth=max_depth))

            model.fit(self.X_train, self.y_train)

            self.model_info = {
                "n_estimator": n_estimators,
                "depth": max_depth,
                "learning_rate": lr
            }
            self.model = model

            logger.debug(msg=f"Model trained successfully.")

        except Exception as e:
            logger.error(f"Couldn't train XGBoost model. Exception below occurred.\n{e}\n")


class LinearL1(Model):
    def __init__(self, learning_rate=0.01, epochs=1000):
        super().__init__()
        self.learning_rate = learning_rate
        self.epochs = epochs

    def fit(self):
        try:
            model = CustomLinearRegression(learning_rate=self.learning_rate, epochs=self.epochs)
            model.fit(self.X_train, self.y_train.flatten())
            self.model = model
            self.model_info = {
                "learning_rate": self.learning_rate,
                "epochs": self.epochs,
                "weights": model.w,
                "bias": model.b
            }
            logger.debug("Model trained successfully.")

        except Exception as e:
            logger.error(f"Couldn't train LinearL1 model. Exception below occurred.\n{e}")


class Neural_network(Model):
    def __init__(self, input_dim, epochs=500, verbose=0):
        super().__init__()
        self.input_dim = input_dim
        self.epochs = epochs
        self.verbose = verbose
        '''
        model = Sequential()
        model.add(Input(shape=(self.input_dim,)))
        model.add(Dense(64, activation='relu'))
        model.add(Dense(32, activation='relu'))
        model.add(Dense(16, activation='relu'))
        model.add(Dense(8, activation='relu'))
        model.add(Dense(1, activation='linear'))
        model.compile(loss='mean_squared_error', optimizer='adam')
        '''
        self.model = None

    def fit(self):
        try:
            self.model.fit(self.X_train, self.y_train, epochs=self.epochs, verbose=self.verbose)
            self.model_info = {
                "epochs": self.epochs,
            }
            logger.debug("Model trained successfully.")

        except Exception as e:
            logger.error(f"Couldn't train Neural Network model. Exception below occurred.\n{e}\n")


class SampleMean(Model):
    def __init__(self, clustering_features=None):
        super().__init__()
        if clustering_features is None:
            clustering_features = ['name', 'code']
        self.clustering_features = clustering_features

    def fit(self):
        try:
            model = SampleMeanModel(clustering_features=self.clustering_features)
            model.fit(self.X_train, self.y_train)
            self.model = model
            self.model_info = {
                "clustering features": self.clustering_features
            }
            logger.debug("Model trained successfully.")

        except Exception as e:
            logger.error(f"Couldn't train Sample Mean model. Exception below occurred.\n{e}")


class Delay(Model):
    def __init__(self, feature="generation", delay=24):
        super().__init__()
        self.feature = feature
        self.delay = delay

    def fit(self):
        try:
            model = DelayModel(self.feature, self.delay)
            model.fit(self.X_train, self.y_train)
            self.model = model
            self.model_info = {
                "feature": self.feature,
                "delay": self.delay
            }
            logger.debug("Model trained successfully.")

        except Exception as e:
            logger.error(f"Couldn't train Sample Mean model. Exception below occurred.\n{e}")
