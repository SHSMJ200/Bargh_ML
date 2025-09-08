class DelayModel:
    # This model assumes that X and y are pandas dataframes!!
    def __init__(self, feature, delay):
        self.feature = feature
        self.delay = delay

    def fit(self, X, y):
        pass

    def predict(self, X):
        f_name = f"{self.feature}_with_{self.delay}_delay"
        y = X[f_name].copy()
        return y
