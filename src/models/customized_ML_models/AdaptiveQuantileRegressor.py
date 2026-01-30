import numpy as np
from sklearn.linear_model import QuantileRegressor

from src.models.customized_ML_models.TwoSegmentQuantileRegressor import (
    TwoSegmentQuantileRegressor
)
from src.root import get_root
import yaml

train_model_config_path = get_root() + '/configs/train_model.yaml'
train_model_config = yaml.safe_load(open(train_model_config_path, 'r', encoding='utf-8'))


class AdaptiveQuantileRegressor:
    def __init__(self):
        self.model = None

    def fit(self, X, y):
        # ---------- linear quantile model ----------
        linear_model = QuantileRegressor(solver='highs',
                                         quantile=train_model_config["linear_quantile"],
                                         alpha=0
                                         )
        linear_model.fit(X, y)

        # ---------- piecewise quantile model ----------
        piecewise_model = TwoSegmentQuantileRegressor(
            quantiles=train_model_config["piecewise_quantiles"],
            break_bound_temp=train_model_config["breakpoint_temperature_bound"]
        )
        piecewise_model.fit(X, y)

        # ---------- extract model properties ----------
        first_slope = piecewise_model.info["slopes"][0]
        second_slope = piecewise_model.info["slopes"][1]

        intersection_x = piecewise_model.info["intersection_x"]
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
                train_model_config["max_slope_diff"] > abs(first_slope - second_slope) >= train_model_config["min_slope_diff"] and
                high_segment_ratio > train_model_config["min_high_segment_ratio"]
        )

        self.model = piecewise_model if use_piecewise_model else linear_model

    def predict(self, X):
        return self.model.predict(X)
