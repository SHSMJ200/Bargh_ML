import numpy as np
import pwlf


class PiecewiseLinearWrapper:
    def __init__(self, n_segments=2):
        """
        n_segments : تعداد قطعات (خط‌ها) در مدل
        """
        self.n_segments = n_segments
        self.model = None
        self.fitted = False

    def fit(self, X, y):
        """
        X : array-like, شکل (n_samples, 1) یا (n_samples,)
        y : array-like, شکل (n_samples,)
        """
        # تبدیل به numpy array و اطمینان از شکل درست
        X = np.asarray(X).flatten()
        y = np.asarray(y).flatten()

        # ایجاد مدل pwlf و fit
        self.model = pwlf.PiecewiseLinFit(X, y)
        self.model.fit(self.n_segments)
        self.fitted = True
        return self

    def predict(self, X):
        """
        X : array-like
        return : پیش‌بینی numpy array
        """
        if not self.fitted:
            raise ValueError("Model is not fitted yet. Call fit first.")

        X = np.asarray(X).flatten()
        y_pred = self.model.predict(X)
        return y_pred

    @property
    def breakpoints(self):
        if not self.fitted:
            raise ValueError("Model is not fitted yet.")
        return self.model.fit_breaks

    @property
    def slopes(self):
        if not self.fitted:
            raise ValueError("Model is not fitted yet.")
        return self.model.slopes

    @property
    def intercepts(self):
        if not self.fitted:
            raise ValueError("Model is not fitted yet.")
        return self.model.intercepts
