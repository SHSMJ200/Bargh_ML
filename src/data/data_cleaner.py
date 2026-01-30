import jdatetime
import pandas as pd
import yaml

from src.logs.logger import CustomLogger
from src.root import get_root

logger = CustomLogger(__name__).get_logger()

tables_config_path = get_root() + '/configs/tables_columns.yaml'
feature_dict = yaml.load(open(tables_config_path), Loader=yaml.SafeLoader)


def custom_clean_energy(df):
    df = pd.melt(
        df, id_vars=['DispPlantCode', 'UnitCode', 'Name', 'Date'],
        value_vars=[f'H{i}' for i in range(1, 25)],
        var_name='Hour',
        value_name='generation'
    )
    df['Hour'] = df['Hour'].str.replace('H', '')
    df['Hour'] = df['Hour'].astype(int)

    df["UnitCode"] = df["UnitCode"].apply(lambda x: x[1:] if x.startswith("C") else x)

    clock_change_hour_condition = (
            (df["Date"].dt.year <= 2022) & (df["Date"].dt.month == 9) & (df["Date"].dt.day == 21) & (df["Hour"] == 24))
    df = df[~clock_change_hour_condition]

    return df


def custom_clean_status(df):
    df[['Id', 'Code']] = df['FullUnitCode'].str.split('-', expand=True)
    df.drop(columns=['FullUnitCode'], axis=1, inplace=True)

    new_col_order = ['Id', 'Code'] + [c for c in df.columns if c not in ['Id', 'Code']]
    df = df.reindex(columns=new_col_order)
    return df


def custom_clean_temperature(df):
    df.drop(columns=['Name', 'InsertDateTime'], axis=1, inplace=True)
    df = df.pivot(index=['PowerPlantCode', 'PowerPlantName', 'Date', 'HourNo'], columns='Code', values='Value')
    df.columns.name = None
    df = df.reset_index()
    return df


raw_data_config_path = get_root() + '/configs/raw_data.yaml'
Raw_Data_Config = yaml.safe_load(open(raw_data_config_path, 'r', encoding='utf-8'))
CLEAN_FUNCTIONS = {
    "custom_clean_energy": custom_clean_energy,
    "custom_clean_temperature": custom_clean_temperature,
    "custom_clean_status": custom_clean_status,
}


def jalali_to_gregorian(date_str):
    try:
        jy, jm, jd = map(int, date_str.split('/'))
        jdate = jdatetime.date(jy, jm, jd)
        gdate = jdate.togregorian()
        return f"{gdate.year}-{gdate.month:02d}-{gdate.day:02d}"
    except Exception as e:
        logger.error(f"Couldn't convert date. Exception below occurred:\n{e}")


def load_and_clean_data(df: pd.DataFrame, latest_revision_col):
    try:
        df.dropna(axis=0, inplace=True)
        df.drop_duplicates(inplace=True)

        if latest_revision_col:
            unique_cols = df.columns.tolist()
            unique_cols.remove('Revision')
            unique_cols.remove(latest_revision_col)
            df = df.loc[df.groupby(unique_cols)['Revision'].idxmax()]
            df.drop(columns="Revision", inplace=True)

        if "Date" in df.columns:
            df['Date'] = df['Date'].apply(jalali_to_gregorian)
            df['Date'] = pd.to_datetime(df['Date'])

        if "Hour" in df.columns:
            df['Hour'] = pd.to_numeric(df['Hour'], errors='coerce')
            df['Hour'] = df['Hour'].astype(int)

        logger.debug(f'Data was cleaned successfully')
        return df

    except Exception as e:
        logger.error(f"Exception below occurred during cleaning the data:\n{e}")
        return pd.DataFrame()


def save_data_to_file(df, table_name):
    file_path = get_root() + f'/data/interim/{table_name}.csv'
    df.to_csv(file_path, sep=",", header=True, index=False, na_rep='NULL')


def process_all_csv_files():
    for name, file_config in Raw_Data_Config.items():
        file_path = get_root() + "/data/raw/" + file_config.get("file_path")
        latest_revision_col = file_config.get("latest_revision_col", None)

        df = pd.read_csv(file_path, low_memory=False)

        df = load_and_clean_data(df, latest_revision_col)

        clean_func_name = file_config.get("clean_func")
        if clean_func_name:
            custom_clean_func = CLEAN_FUNCTIONS.get(clean_func_name)
            df = custom_clean_func(df)

        save_data_to_file(df, file_config.get("table_name"))

        logger.info(f"Manipulation for {name} has been completed")