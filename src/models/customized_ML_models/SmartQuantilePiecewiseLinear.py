import numpy as np
from src.models.customized_ML_models.QuantilePiecewiseLinear import QuantilePiecewiseLinear


class SmartQuantilePiecewiseLinear:
    def __init__(self, quantile=0.9, threshold=0.3, break_bound_temp=None):
        self.quantile = quantile
        self.threshold = threshold
        self.model = None
        self.break_bound_temp = break_bound_temp

    def fit(self, X, y):
        model1 = QuantilePiecewiseLinear(n_segments=1, quantile=0.95)
        model1.fit(X, y)
        y_pred = model1.predict(X)
        mse_model1 = np.mean((y - y_pred) ** 2)

        model2 = QuantilePiecewiseLinear(n_segments=2, quantile=0.95, break_bound_temp=self.break_bound_temp)
        model2.fit(X, y)
        y_pred = model2.predict(X)
        mse_model2 = np.mean((y - y_pred) ** 2)

        self.th=(mse_model1 - mse_model2) / mse_model1
        print(self.th)
        if (mse_model1 - mse_model2) / mse_model1 < self.threshold:
            self.model = model1
        else:
            self.model = model2
        return self

    def predict(self, X):
        return self.model.predict(X)
