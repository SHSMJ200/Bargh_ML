import numpy as np
from sklearn.linear_model import QuantileRegressor

from src.models.customized_ML_models.TwoSegmentQuantileRegressor import (
    TwoSegmentQuantileRegressor
)

CONFIG = {
    "linear_quantile": 0.85,
    "piecewise_quantiles": [0.98, 0.85],
    "breakpoint_temperature_bound": 25,

    "min_slope_diff": 0.25,
    "max_slope_diff": 2.5,
    "min_high_segment_ratio": 5
}


class AdaptiveQuantileRegressor:
    def __init__(self):
        self.model = None

    def fit(self, X, y):
        # ---------- linear quantile model ----------
        linear_model = QuantileRegressor(
            quantile=CONFIG["linear_quantile"],
            alpha=0
        )
        linear_model.fit(X, y)

        # ---------- piecewise quantile model ----------
        piecewise_model = TwoSegmentQuantileRegressor(
            quantiles=CONFIG["piecewise_quantiles"],
            break_bound_temp=CONFIG["breakpoint_temperature_bound"]
        )
        piecewise_model.fit(X, y)

        # ---------- extract model properties ----------
        first_slope = piecewise_model.info["slopes"][0]
        second_slope = piecewise_model.info["slopes"][1]

        intersection_x = piecewise_model.info["inter_point"][0]
        left_breakpoint = piecewise_model.breakpoints[0]
        right_breakpoint = piecewise_model.breakpoints[2]

        # ---------- data distribution check ----------
        X_flat = np.asarray(X).flatten()
        high_segment_ratio = int(
            (np.sum(X_flat > piecewise_model.breakpoints[1]) / len(X_flat)) * 100
        )

        # ---------- selection condition ----------
        use_piecewise_model = (
            0 > first_slope > second_slope and
            left_breakpoint < intersection_x < right_breakpoint and
            CONFIG["max_slope_diff"] > abs(first_slope - second_slope) >= CONFIG["min_slope_diff"] and
            high_segment_ratio > CONFIG["min_high_segment_ratio"]
        )

        self.model = piecewise_model if use_piecewise_model else linear_model

    def predict(self, X):
        return self.model.predict(X)
