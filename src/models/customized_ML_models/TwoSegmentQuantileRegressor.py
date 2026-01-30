import numpy as np
import pwlf
from sklearn.base import BaseEstimator, RegressorMixin
from sklearn.linear_model import QuantileRegressor


class TwoSegmentQuantileRegressor(BaseEstimator, RegressorMixin):
    def __init__(self, quantiles, break_bound_temp):
        self.n_segments = 2
        self.quantiles = quantiles
        self.break_bound_temp = break_bound_temp

        self.breakpoints = None
        self.pwlf_model = None
        self.is_fitted = False

        self.info = {
            "slopes": None,
            "intersection_x": None,
        }

    def fit(self, X, y):
        X = np.asarray(X).flatten()
        y = np.asarray(y).flatten()

        # --------------------------------------------------
        # 1) Estimate breakpoint using pwlf (high X values)
        # --------------------------------------------------
        high_temp_mask = X > self.break_bound_temp
        pwlf_init = pwlf.PiecewiseLinFit(X[high_temp_mask], y[high_temp_mask])
        pwlf_init.fit(self.n_segments)

        self.breakpoints = pwlf_init.fit_breaks
        self.breakpoints[0] = np.min(X)

        breakpoint_x = self.breakpoints[1]

        # --------------------------------------------------
        # 2) Fit separate quantile regressions
        # --------------------------------------------------
        left_model = QuantileRegressor(solver='highs',
            quantile=self.quantiles[0],
            alpha=0.0
        )
        left_mask = X < breakpoint_x
        left_model.fit(X[left_mask].reshape(-1, 1), y[left_mask])

        right_model = QuantileRegressor(solver='highs',
            quantile=self.quantiles[1],
            alpha=0.0
        )
        right_mask = X >= breakpoint_x
        right_model.fit(X[right_mask].reshape(-1, 1), y[right_mask])

        # --------------------------------------------------
        # 3) Generate smooth piecewise target
        # --------------------------------------------------
        x_grid = np.linspace(self.breakpoints[0], self.breakpoints[2], 100)

        left_pred = left_model.predict(x_grid.reshape(-1, 1)) - 1
        right_pred = right_model.predict(x_grid.reshape(-1, 1))

        left_slope = left_model.coef_[0]
        right_slope = right_model.coef_[0]

        if left_slope > right_slope:
            merged_target = np.minimum(left_pred, right_pred)
        else:
            merged_target = np.maximum(left_pred, right_pred)

        # --------------------------------------------------
        # 4) Final pwlf fit
        # --------------------------------------------------
        final_pwlf = pwlf.PiecewiseLinFit(x_grid, merged_target)
        final_pwlf.fit(self.n_segments)

        self.pwlf_model = final_pwlf
        self.is_fitted = True

        # --------------------------------------------------
        # 5) Store diagnostics
        # --------------------------------------------------
        self.info["intersection_x"] = final_pwlf.fit_breaks[1]
        self.info["slopes"] = [left_slope, right_slope]

        return self

    def predict(self, X):
        if not self.is_fitted:
            raise ValueError("Model has not been fitted yet.")

        X = np.asarray(X).flatten()
        return self.pwlf_model.predict(X)
