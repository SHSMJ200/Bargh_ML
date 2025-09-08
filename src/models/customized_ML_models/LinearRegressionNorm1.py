import numpy as np


class CustomLinearRegression:
    def __init__(self, learning_rate=0.01, epochs=1000):
        self.w = None
        self.b = 0.0
        self.learning_rate = learning_rate
        self.epochs = epochs

    def fit(self, X, y, verbose=False):
        n_samples, n_features = X.shape
        self.w = np.zeros(n_features)

        for epoch in range(self.epochs):
            y_pred = X.dot(self.w) + self.b
            error = y_pred - y

            grad_w = np.mean(np.sign(error).reshape(-1, 1) * X, axis=0)
            grad_b = np.mean(np.sign(error))

            self.w -= self.learning_rate * grad_w
            self.b -= self.learning_rate * grad_b

            if verbose and epoch % 100 == 0:
                loss = np.mean(np.abs(error))
                # print(f"Epoch {epoch}: L1 loss = {loss:.4f}")

    def predict(self, X):
        return X.dot(self.w) + self.b
