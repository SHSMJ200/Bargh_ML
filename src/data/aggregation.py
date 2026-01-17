import os

from logs.logger import CustomLogger
from src.root import get_root

logger = CustomLogger(__name__).get_logger()

import pandas as pd
import yaml


def left_join(df1, df2, column=None):
    if column is None:
        column = df1.columns.to_list()
    result = df1.merge(df2, on=column, how='left', indicator=True)
    result = result.drop('_merge', axis=1)
    return result


def integrated_aggregation():
    tables_config_path = get_root() + '/configs/tables_columns.yaml'
    feature_dict = yaml.load(open(tables_config_path), Loader=yaml.SafeLoader)
    columns_name_dictionary = {df_name: list(feature_dict[df_name]) for df_name in feature_dict}

    dfs = {}
    for name in list(columns_name_dictionary):
        if name == "declaration_check":
            continue
        dfs[name] = pd.read_csv(os.path.join(get_root(), "data", "interim", f"{name}.csv"))
        dfs[name].columns = columns_name_dictionary[name]

    dfs["commitment"].rename(columns={"name": "final_name"}, inplace=True)
    df1 = left_join(dfs["commitment"], dfs["weather"], column=["id", "date", "hour"])
    df2 = left_join(df1, dfs["bar"], column=["date", "hour"])
    df3 = left_join(df2, dfs["load"], column=["date", "hour"])
    df4 = left_join(df3, dfs["energy"], column=["id", "code", "date", "hour"]).drop(columns=["name_x", "name_y"])
    df5 = left_join(df4, dfs["selleroffer"], column=["id", "code", "date", "hour"])
    df6 = left_join(df5, dfs["status"], column=["id", "code", "date", "hour"])
    df7 = left_join(df6, dfs["plant_temp"], column=["id", "date", "hour"])
    df_final = df7.rename(columns={"final_name": "name"}).drop(columns=["name_x", "name_y"])

    df_final.to_csv(get_root() + "/data/processed/integrated.csv")

    logger.info(f"Aggregation has been successfully done")
