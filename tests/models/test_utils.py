from src.models.utils import *


def test_split_X_and_y():
    X = np.arange(10).reshape(5, 2)
    y = np.arange(5)

    X_train, X_test, y_train, y_test = split_X_and_y(X, y, test_size=0.4, shuffle=False)

    np.testing.assert_array_equal(X_train, X[:3])
    np.testing.assert_array_equal(X_test, X[3:])
    np.testing.assert_array_equal(y_train, y[:3])
    np.testing.assert_array_equal(y_test, y[3:])
