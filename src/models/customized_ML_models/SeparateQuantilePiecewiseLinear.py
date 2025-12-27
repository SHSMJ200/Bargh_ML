import numpy as np
import pwlf
from sklearn.base import BaseEstimator, RegressorMixin
from sklearn.linear_model import QuantileRegressor


class SeparateQuantilePiecewiseLinear(BaseEstimator, RegressorMixin):
    def __init__(self, quantiles=None, break_bound_temp=None):
        if quantiles is None:
            quantiles = [0.9, 0.9]
        self.n_segments = 2
        self.quantiles = quantiles
        self.breakpoints = None
        self.model = None
        self.fitted = False
        self.break_bound_temp = break_bound_temp
        self.info = {
            "slopes": None,
            "inter_point": None,
        }

    def fit(self, X, y):
        X = np.asarray(X).flatten()
        y = np.asarray(y).flatten()

        # مرحله 1: پیدا کردن breakpoints با pwlf
        mask = X > self.break_bound_temp
        pwlf_model = pwlf.PiecewiseLinFit(X[mask], y[mask])
        pwlf_model.fit(self.n_segments)
        self.breakpoints = pwlf_model.fit_breaks
        self.breakpoints[0] = np.min(X)

        breakpoint = self.breakpoints[1]

        model0 = QuantileRegressor(quantile=self.quantiles[0], alpha=0.0)
        mask = X < breakpoint
        model0.fit(X[mask].reshape(-1, 1), y[mask])

        model1 = QuantileRegressor(quantile=self.quantiles[1], alpha=0.0)
        mask = X >= breakpoint
        model1.fit(X[mask].reshape(-1, 1), y[mask])

        x_temp = np.linspace(self.breakpoints[0], self.breakpoints[2], 100)

        y0_temp = model0.predict(x_temp.reshape(-1, 1)) - 1
        y1_temp = model1.predict(x_temp.reshape(-1, 1))

        slope0 = model0.coef_[0]
        slope1 = model1.coef_[0]

        if slope0 > slope1:
            y_temp = np.minimum(y0_temp, y1_temp)
        else:
            y_temp = np.maximum(y0_temp, y1_temp)

        pwlf_model = pwlf.PiecewiseLinFit(x_temp, y_temp)
        pwlf_model.fit(self.n_segments)

        self.model = pwlf_model
        self.fitted = True

        self.info["inter_point"] = self.get_intersection_point(model0, model1)
        self.info["slopes"] = [slope0, slope1]
        return self

    def get_intersection_point(self, model0, model1):
        a0 = model0.coef_[0]
        b0 = model0.intercept_

        a1 = model1.coef_[0]
        b1 = model1.intercept_

        # بررسی موازی نبودن
        if np.isclose(a0, a1):
            x_int = None
            y_int = None
        else:
            x_int = (b1 - b0) / (a0 - a1)
            y_int = a0 * x_int + b0

        return x_int, y_int

    def predict(self, X):
        if not self.fitted:
            raise ValueError("Model not fitted yet")
        X = np.asarray(X).flatten()
        return self.model.predict(X)
