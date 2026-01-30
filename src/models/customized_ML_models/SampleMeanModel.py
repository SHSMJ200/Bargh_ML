class SampleMeanModel:
    # This model assumes that X and y are pandas dataframes!!
    def __init__(self, clustering_features):
        self.clustering_features = clustering_features
        self.means = {}

    def fit(self, X, y):
        clusters = X[self.clustering_features].drop_duplicates()
        for _, row in clusters.iterrows():
            mask = (X[self.clustering_features] == row[self.clustering_features]).all(axis=1)
            key = tuple(row)
            self.means[key] = y.loc[mask].mean()

    def predict(self, X):
        y = X.apply(
            lambda row: self.means.get(tuple(row[self.clustering_features]), 0),
            axis=1
        )
        return y
