import numpy as np
import pwlf
from sklearn.base import BaseEstimator, RegressorMixin
from sklearn.linear_model import QuantileRegressor


class QuantilePiecewiseLinear(BaseEstimator, RegressorMixin):
    def __init__(self, n_segments=2, quantile=0.9, break_bound_temp=None):
        self.n_segments = n_segments
        self.quantile = quantile
        self.breakpoints = None
        self.model = None
        self.fitted = False
        self.break_bound_temp = break_bound_temp

    def _hinge_features(self, X):
        X = np.asarray(X).flatten()
        features = [X]  # اولین feature = x اصلی
        for bp in self.breakpoints[1:-1]:  # exclude min & max
            features.append(np.maximum(0, X - bp))
        return np.column_stack(features)

    def fit(self, X, y):
        X = np.asarray(X).flatten()
        y = np.asarray(y).flatten()

        # مرحله 1: پیدا کردن breakpoints با pwlf
        if self.break_bound_temp:
            mask = X > self.break_bound_temp
            pwlf_model = pwlf.PiecewiseLinFit(X[mask], y[mask])
            pwlf_model.fit(self.n_segments)
            self.breakpoints = pwlf_model.fit_breaks
            self.breakpoints[0] = np.min(X)

            #if self.breakpoints[1] < (self.breakpoints[0] + self.breakpoints[2]) / 2:
                #self.breakpoints =

        else:
            pwlf_model = pwlf.PiecewiseLinFit(X, y)
            pwlf_model.fit(self.n_segments)
            self.breakpoints = pwlf_model.fit_breaks



        # مرحله 2: ساخت hinge features
        X_hinge = self._hinge_features(X)

        # مرحله 3: فیت QuantileRegressor
        self.model = QuantileRegressor(quantile=self.quantile, alpha=0.0)
        self.model.fit(X_hinge, y)
        self.fitted = True
        return self

    def predict(self, X):
        if not self.fitted:
            raise ValueError("Model not fitted yet")
        X = np.asarray(X).flatten()
        X_hinge = self._hinge_features(X)
        return self.model.predict(X_hinge)
