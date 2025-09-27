import pandas as pd
from src.models.data_selector import Data_selector
from logs.logger import CustomLogger

logger = CustomLogger(name="feature_selector").get_logger()


class Feature_selector:
    def __init__(self, df: pd.DataFrame, target):
        self.df = df
        self.target = target

    def select(self, features_to_select=None, features_to_drop=None):

        if features_to_drop is not None:
            self.df = self.df.drop(columns=features_to_drop, axis=1)
        if features_to_select is not None:
            self.df = self.df[features_to_select + [self.target]]

        logger.debug(f"Selected features : {self.df.columns}")

    def get_X_and_y(self, do_onehot=True, n_mimo=1):
        is_mimo = n_mimo > 1
        df = self.df.copy(deep=True)
        if is_mimo:
            dff, self.name_code_dictionary_index = get_dataframe_block(df, n_mimo)
            dic_col = get_index_dictionary(df, n_mimo)
            X = dff.drop(columns=dic_col[self.target])
            y = dff[dic_col[self.target]]
            name_code_df = X[[0, 1]]
        else:
            dic_col = None
            X = df.drop(columns=[self.target])
            y = df[self.target]
            X.drop(columns=['datetime'], inplace=True)
            name_code_df = X[["name", "code"]]

        if do_onehot:
            categorical_cols = X.select_dtypes(include=['object', 'category']).columns
            X = pd.get_dummies(X, columns=categorical_cols, drop_first=True)
            X.columns = X.columns.astype(str)

        return X, y, name_code_df, dic_col


def get_df_rep(df, name, code, n, index_ranges):
    df1 = df.drop(columns=["name", "code", "datetime"])
    rows = []
    index = []
    k1 = 0
    k2 = 0
    for i1, i2 in index_ranges:
        for i in range(i1, i2 - n + 1):
            part = df1.iloc[i:i + n].reset_index(drop=True)
            row = pd.Series(part.values.flatten()).to_list()
            row = [name, code] + row
            rows.append(row)
        k2 += i2 - i1 - n + 1
        index.append((k1, k2))
        k1 = k2
    return rows, index


def get_interval(df, l_min):
    # logger.info("df.columns " + str("datetime" in list(df.columns)) + str(df.columns))
    gap_mask = pd.to_datetime(df['datetime']).diff() != pd.Timedelta(hours=1)
    start_indices = df.index[gap_mask].tolist()
    if 0 not in start_indices:
        start_indices = [0] + start_indices

    end_indices = [i for i in start_indices[1:]] + [df.index[-1] + 1]

    index_ranges = [(start_indices[i], end_indices[i]) for i in range(len(start_indices)) if
                    end_indices[i] - start_indices[i] >= l_min]

    return index_ranges


def get_dataframe_block(df, n):
    df_modified = df.copy(deep=True)
    ds = Data_selector(df_modified)

    rows = []
    power_plants = df_modified[['name', 'code']].drop_duplicates()
    name_code_dictionary_index = {}
    for _, row in power_plants.iterrows():
        # logger.info("df.columns " + str("datetime" in list(df.columns)))
        df_name_code = ds.filter_name_code(row["name"], row["code"])
        df_name_code.reset_index(drop=True, inplace=True)
        index_range = get_interval(df_name_code, l_min=n)
        rowss, indexes = get_df_rep(df_name_code, row["name"], row["code"], n, index_range)
        # print(type(name_code_dictionary_index),(row["name"], row["code"]),len(indexes))
        name_code_dictionary_index[(row["name"], row["code"])] = indexes
        rows += rowss
    df_new = pd.DataFrame(rows)
    return df_new, name_code_dictionary_index


def get_index_dictionary(df, rep):
    cols = df.drop(columns=["name", "code", "datetime"]).columns
    n = len(cols)

    d = {}
    d["name"] = 0
    d["code"] = 1

    for i in range(n):
        d[cols[i]] = [2 + i + k * n for k in range(rep)]

    return d
