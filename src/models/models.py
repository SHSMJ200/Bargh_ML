import numpy as np
from sklearn.ensemble import RandomForestRegressor
from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import PolynomialFeatures
from sklearn.pipeline import make_pipeline
from sklearn.metrics import mean_absolute_error,mean_squared_error
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
import xgboost as xgb

'''
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import Dense
'''
from logs.logger import CustomLogger

logger = CustomLogger(name="models", log_file_name='models.log').get_logger()


def scale(x, do_flat=False):
    scaler = StandardScaler()
    scaled_x = scaler.fit_transform(x)

    if not do_flat:
        logger.debug(f"X scaled successfully.")
        return scaled_x, scaler
    else:
        logger.debug(f"y scaled successfully")
        logger.debug(f"y flattened.")
        return scaled_x.flatten(), scaler


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

    def compute_rmse_error(self):
        y_pred_test = self.model.predict(self.X_test)
        y_pred_train = self.model.predict(self.X_train)
        y_pred_test_actual = self.scaler_y.inverse_transform(y_pred_test.reshape(-1, 1)).ravel()
        y_pred_train_actual = self.scaler_y.inverse_transform(y_pred_train.reshape(-1, 1)).ravel()
        y_test_actual = self.scaler_y.inverse_transform(self.y_test.reshape(-1, 1)).ravel()
        y_train_actual = self.scaler_y.inverse_transform(self.y_train.reshape(-1, 1)).ravel()

        rmse_test_actual = (mean_squared_error(y_test_actual, y_pred_test_actual) ** 0.5 / np.mean(y_test_actual)) * 100
        rmse_train_actual = (mean_squared_error(y_train_actual, y_pred_train_actual) ** 0.5 / np.mean(y_train_actual)) * 100

    def compute_mse_error_simple(self):
        y_pred_test = self.model.predict(self.X_test)
        y_pred_train = self.model.predict(self.X_train)
        y_pred_test_actual = self.scaler_y.inverse_transform(y_pred_test.reshape(-1, 1)).ravel()
        y_pred_train_actual = self.scaler_y.inverse_transform(y_pred_train.reshape(-1, 1)).ravel()
        y_test_actual = self.scaler_y.inverse_transform(self.y_test.reshape(-1, 1)).ravel()
        y_train_actual = self.scaler_y.inverse_transform(self.y_train.reshape(-1, 1)).ravel()
        
        mse_test_actual  = (mean_squared_error(y_test_actual, y_pred_test_actual))
        mse_train_actual = (mean_squared_error(y_train_actual, y_pred_train_actual))
        return mse_train_actual, mse_test_actual
    
    def scale_and_split_data(self, X, y, test_size=0.2, random_state=42, y_is_flat=True):
        x_scaled, scaler_x = scale(X)
        if y_is_flat:
            y_scaled, scaler_y = scale(y.values.reshape(-1, 1), do_flat=True)
        else:
            y_scaled, scaler_y = scale(y)

        self.scaler_x = scaler_x
        self.scaler_y = scaler_y

        if len(X) < 5:
            (X_train, y_train) = x_scaled, y_scaled
            X_test, y_test = x_scaled, y_scaled
        else:
            X_train, X_test, y_train, y_test = (
                train_test_split(x_scaled, y_scaled, test_size=test_size, random_state=random_state))

        self.X_train = X_train
        self.y_train = y_train
        self.X_test = X_test
        self.y_test = y_test

    def pred(self, X):
        x_scaled = self.scaler_x.transform(X)
        y_pred_scaled = (self.model.predict(x_scaled)).reshape(-1, 1)
        y_pred = self.scaler_y.inverse_transform(y_pred_scaled)
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
    def __init__(self):
        super().__init__()

    def fit(self, degree=2):
        try:
            model = make_pipeline(PolynomialFeatures(degree), LinearRegression())
            model.fit(self.X_train, self.y_train)
            self.model_info = dict(model.get_params().items())
            self.model = model
            logger.debug(msg=f"Model trained successfully.")

        except Exception as e:
            logger.error(f"Couldn't train Polynomial(d={degree}) model. Exception below occurred.\n{e}\n")


class Random_Forest(Model):
    def __init__(self):
        super().__init__()

    def fit(self, n_estimators=10, max_depth=5):
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
    def __init__(self):
        super().__init__()

    def fit(self, n_estimators=100, max_depth=3, lr=0.1):
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

'''
class Neural_network(Model):
    def __init__(self):
        super().__init__()

    def fit(self, epochs=500, verbose=0):
        try:
            model = Sequential()
            # TODO: The layer should be set correctly!
            model.add(Dense(4, input_dim=44, activation='relu'))
            model.add(Dense(1, activation='linear'))

            model.compile(loss='mean_squared_error', optimizer='adam')

            model.fit(self.X_train, self.y_train, epochs=epochs, verbose=verbose)

            self.model_info = {
                "epochs": epochs,
                "verbose": verbose,
            }
            self.model = model

        except Exception as e:
            logger.error(f"Couldn't train Neural Network model. Exception below occurred.\n{e}\n")
'''
