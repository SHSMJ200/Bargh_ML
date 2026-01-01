import numpy as np
from sklearn.linear_model import QuantileRegressor

from src.models.customized_ML_models.SeparateQuantilePiecewiseLinear import SeparateQuantilePiecewiseLinear


class SmartOneTwoQuantileLine:
    def __init__(self):
        self.model = None

    def fit(self, X, y):
        model1 = QuantileRegressor(quantile=0.85, alpha=0)
        model1.fit(X, y)

        model2 = SeparateQuantilePiecewiseLinear(quantiles=[0.98, 0.85], break_bound_temp=25)
        model2.fit(X, y)

        s1 = model2.info["slopes"][0]
        s2 = model2.info["slopes"][1]
        xp = model2.info["inter_point"][0]
        x1 = model2.breakpoints[0]
        x2 = model2.breakpoints[2]

        temp = np.array(X).flatten()
        z2 = int((len(temp[temp > model2.breakpoints[1]]) / len(X)) * 100)

        cc = (0 > s1 > s2) and (x1 < xp < x2) and 2.5 > abs(s1 - s2) >= 0.25 and z2 > 5

        if cc:
            self.model = model2
        else:
            self.model = model1

    def predict(self, X):
        return self.model.predict(X)
