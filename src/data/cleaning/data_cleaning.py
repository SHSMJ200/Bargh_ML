import sys
import os



from src.data.dbconnection import Database
from logs.logger import CustomLogger
import pandas as pd
import convertdate
import jdatetime
from enum import Enum
from psycopg2 import Error as psyError
import os
import yaml

logger = CustomLogger(__name__, log_file_name='cleaning.log').get_logger()

feature_dict = yaml.load(open('U:/ML_project/bargh/configs/tables_columns.yaml'), Loader=yaml.SafeLoader)

RAW_DATA_PATH = 'U:/ML_project/bargh/data/raw/'


class RawData(Enum):
    BAR = RAW_DATA_PATH + 'LowMiddlePeakLoadHours_View.csv'
    PLANT = RAW_DATA_PATH + 'PlantData.csv'
    TEMPERATURE = RAW_DATA_PATH + 'PlantsTemperature_View.csv'
    ENERGY = RAW_DATA_PATH + 'PowerplantUnitEnergy_View.csv'
    SELLEROFFER = RAW_DATA_PATH + 'SellerOffer_View.csv'
    FACTORS = RAW_DATA_PATH + 'TemperatureFactors_View.csv'
    STATUS = RAW_DATA_PATH + 'UnitsStatusChangeLastRev_Hourly_View.csv'
    LOAD = RAW_DATA_PATH + 'LoadPredictionNetwork.csv'
    COMMITMENT = RAW_DATA_PATH + 'UnitCommitment.csv'


class CsvfileManipulation:
    def __init__(self):
        pass

    def clean(self, input_file: str, is_xlsx=False, melt=False) -> pd.DataFrame:
        try:
            if is_xlsx:
                df = pd.read_excel(input_file)
            else:
                df = pd.read_csv(input_file)

            logger.debug(msg=f'{input_file} successfully saved in pandas dataframe.')

            if melt:

                try:
                    df = pd.melt(
                        df, id_vars=['DispPlantCode', 'UnitCode', 'Name', 'Date'],
                        value_vars=['h{}'.format(i) for i in range(1, 10)] + ['H{}'.format(i) for i in range(10, 25)],
                        var_name='hour',
                        value_name='generation'
                    )

                    df['hour'] = df['hour'].str.replace('h', '')
                    df['hour'] = df['hour'].str.replace('H', '')
                    df['hour'] = df['hour'].astype(int)

                    logger.debug(msg=f'Successfully expanded the table.')
                except Exception as e:
                    logger.error(f'an Exception occurred:\n{e}\n')

            # df = df[columns]
            df.dropna(axis=0, inplace=True)
            df.drop_duplicates(inplace=True)

            if "date" in df.columns:
                df['date'] = df['date'].apply(self.jalali_to_gregorian_fast)
                df['date'] = pd.to_datetime(df['date'])

            elif "Date" in df.columns:
                df['Date'] = df['Date'].apply(self.jalali_to_gregorian_fast)
                df['Date'] = pd.to_datetime(df['Date'])

            logger.debug(msg=f'Actions below applied successfully:\nDropping Nulls.\nDropping duplicates.')
            return df

        except Exception as e:
            logger.warning(msg=f'Exception --""{e}""-- occurred and couldnt get the data.')
            return None

    def get_innermost_dict(self, nested_dict: dict):
        if not isinstance(nested_dict, dict):
            return None

        if not nested_dict:
            return None

        first_value = next(iter(nested_dict.values()))

        if isinstance(first_value, dict):
            return self.get_innermost_dict(first_value)
        else:
            return nested_dict

    def jalali_to_gregorian(self, date_str):
        try:
            year, month, day = map(int, date_str.split('/'))
            g_year, g_month, g_day = convertdate.persian.to_gregorian(year, month, day)
            return f'{g_year}-{g_month:02d}-{g_day:02d}'
        except Exception as e:
            logger.error(f'Exception occurred: {e}')

    def jalali_to_gregorian_fast(self, date_str):
        try:
            jy, jm, jd = map(int, date_str.split('/'))
            jdate = jdatetime.date(jy, jm, jd)
            gdate = jdate.togregorian()
            return f"{gdate.year}-{gdate.month:02d}-{gdate.day:02d}"
        except Exception as e:
            logger.error(f"Could not convert date. Exception below occurred:\n{e}")

    def process(self, file: RawData):
        db = Database()
        db.connect()

        match file:


            case RawData.BAR:
                table_name = 'bar'
                df = self.clean(input_file=file.value, is_xlsx=False)
                try:

                    df.to_csv(path_or_buf='U:/ML_project/bargh/temp/bartemp.csv', sep=',', header=True, index=False,
                              na_rep='NULL')

                    db.create_table(
                        table_name=table_name,
                        columns=self.get_innermost_dict(feature_dict['bar'])
                    )

                    db.commit()

                    path_file = 'U:/ML_project/bargh/temp/bartemp.csv'

                    db.lazy_copy_expert(
                        table_name=table_name,
                        file=path_file,
                        mode='r',
                        into_db=True
                    )

                    db.commit()

                    logger.debug(msg=f'Table {table_name} created successfully in the database and data copied into it.')

                    db.__exit__()
                except psyError as e:
                    logger.error(f'an psycopg2 error occurred.\n{e}\n'f'an psycopg2 error occurred.\n{e}\n')
                    if db.connection:
                        db.rollback()

                try:
                    db.__enter__()

                    path_file = 'U:/ML_project/bargh/data/interim/bar.csv'

                    db.lazy_copy_expert(
                        table_name=table_name,
                        file=path_file,
                        mode='w',
                        into_local=True
                    )

                    db.commit()

                    logger.debug(msg=f'Data successfully copied from database into local system.')

                    db.__exit__()
                except psyError as e:
                    logger.error(f'an psycopg2 error occurred.\n{e}\n')
                    if db.connection:
                        db.rollback()

                os.remove('U:/ML_project/bargh/temp/bartemp.csv')

            case RawData.PLANT:
                table_name = 'plant_data'
                # df = self.clean(input_file=file.value, columns=['DispPlantCode', 'PlantName', 'PlantType', 'UTM'], is_xlsx=True)
                df = self.clean(input_file=file.value, is_xlsx=False)

                try:

                    df.to_csv(path_or_buf='U:/ML_project/bargh/temp/plantdata.csv', sep=',', header=True, index=False,
                              na_rep='NULL')
                    db.execute(
                        query=f'create table if not exists {table_name} (id varchar(50), name varchar(100), type varchar(10), UTM text)',
                        do_return=False
                    )
                    db.commit()

                    db.copy_expert(
                        query=f"copy {table_name} from stdin with delimiter ',' csv header NULL as 'NULL'",
                        file='U:/ML_project/bargh/temp/plantdata.csv',
                        mode='r'
                    )
                    db.commit()

                    db.__exit__()
                except psyError as e:
                    logger.error(f'an psycopg2 error occurred.\n{e}\n')
                    if db.connection:
                        db.rollback()

                try:
                    db.__enter__()

                    db.copy_expert(
                        query=f"copy {table_name} to stdout with delimiter ',' csv header NULL as 'NULL'",
                        file='U:/ML_project/bargh/data/interim/plant_data.csv',
                        mode='w'
                    )
                    db.commit()

                    db.__exit__()
                except psyError as e:
                    logger.error(f'an psycopg2 error occurred.\n{e}\n')
                    if db.gconnection:
                        db.rollback()

                os.remove('U:/ML_project/bargh/temp/plantdata.csv')

            case RawData.TEMPERATURE:
                table_name = 'plant_temp'
                # df = self.clean(input_file=file.value, columns=['PowerPlantCode', 'PowerPlantName', 'Date', 'HourNo', 'Value'], is_xlsx=False)
                df = self.clean(input_file=file.value, is_xlsx=False)

                try:
                    df.to_csv(path_or_buf='U:/ML_project/bargh/temp/temperature.csv', sep=',', header=True, index=False,
                              na_rep='NULL')
                    db.execute(
                        query=f'create table if not exists {table_name} (id varchar(50), name varchar(100), date text, hour int, temperature float)',
                        do_return=False
                    )
                    db.commit()

                    db.copy_expert(
                        query=f"copy {table_name} from stdin with delimiter ',' csv header NULL as 'NULL'",
                        file='U:/ML_project/bargh/temp/temperature.csv',
                        mode='r'
                    )
                    db.commit()

                    db.__exit__()
                except psyError as e:
                    logger.error(f'an psycopg2 error occurred.\n{e}\n')
                    if db.connection:
                        db.rollback()

                try:
                    db.__enter__()
                    db.execute(
                        query=f'create table plants_mean_temp as (select id, name, date, hour, avg(temperature) as mean_temp from plant_temp group by id, name, date, hour)',
                        do_return=False
                    )
                    db.commit()
                    db.__exit__()
                except psyError as e:
                    logger.error(f'an psycopg2 error occurred.\n{e}\n')
                    if db.connection:
                        db.rollback()

                os.remove('U:/ML_project/bargh/temp/temperature.csv')

            case RawData.ENERGY:
                table_name = 'energy'
                df = self.clean(input_file=file.value, is_xlsx=False, melt=True)

                try:
                    df.to_csv(path_or_buf='U:/ML_project/bargh/temp/energy.csv', sep=',', header=True, index=False,
                              na_rep='NULL')
                    db.execute(
                        query=f'create table if not exists {table_name} (id varchar(50), code varchar(50), name varchar(100), date text, hour int, generation float);',
                        do_return=False
                    )
                    db.commit()

                    db.copy_expert(
                        query=f"copy {table_name} from stdin delimiter ',' csv header NULL as 'NULL'",
                        file='U:/ML_project/bargh/temp/energy.csv',
                        mode='r'
                    )
                    db.commit()

                    db.__exit__()
                except psyError as e:
                    logger.error(f'an psycopg2 error occurred.\n{e}\n')
                    if db.connection:
                        db.rollback()

                try:
                    db.__enter__()

                    db.copy_expert(
                        query=f"copy {table_name} to stdout with delimiter ',' csv header NULL as 'NULL'",
                        file='U:/ML_project/bargh/data/interim/energy.csv',
                        mode='w'
                    )

                    db.commit()

                    db.__exit__()

                except psyError as e:
                    logger.error(f'an psycopg2 error occurred.\n{e}\n')
                    if db.connection:
                        db.rollback()

                os.remove('U:/ML_project/bargh/temp/energy.csv')

            case RawData.SELLEROFFER:
                table_name = 'selleroffer'
                df = self.clean(input_file=file.value, is_xlsx=False)

                try:
                    max_indices = df.groupby(['PowerPlantCode', 'PowerPlantName', 'UnitCode', 'Date', 'HourNo'])[
                        'Revision'].idxmax()
                    result = df.loc[max_indices]
                    result.drop(columns=['Revision'], axis=1, inplace=True)
                    result.to_csv(path_or_buf='U:/ML_project/bargh/temp/selleroffer.csv', sep=',', header=True,
                                  index=False, na_rep='NULL')

                    db.execute(
                        query=f"create table if not exists {table_name} (id varchar(10), name varchar(100), code varchar(10), date text, hour int, declare float)",
                        do_return=False
                    )
                    db.commit()

                    db.copy_expert(
                        query=f"copy {table_name} from stdin delimiter ',' csv header NULL as 'NULL'",
                        file='U:/ML_project/bargh/temp/selleroffer.csv',
                        mode='r'
                    )
                    db.commit()

                    db.__exit__()
                except Exception as e:
                    logger.error(f'an psycopg2 error occurred.\n{e}\n')
                    if db.connection:
                        db.rollback()

                try:
                    db.__enter__()

                    db.copy_expert(
                        query=f"copy {table_name} to stdout with delimiter ',' csv header NULL as 'NULL'",
                        file='U:/ML_project/bargh/data/interim/selleroffer.csv',
                        mode='w'
                    )

                    db.commit()

                    db.__exit__()

                except psyError as e:
                    logger.error(f'an psycopg2 error occurred.\n{e}\n')
                    if db.connection:
                        db.rollback()

                os.remove('U:/ML_project/bargh/temp/selleroffer.csv')

            case RawData.FACTORS:
                table_name = 'factors'
                df = self.clean(input_file=file.value, is_xlsx=False)

                try:
                    df.to_csv(path_or_buf='U:/ML_project/bargh/temp/factors.csv', sep=',', header=True, index=False,
                              na_rep='NULL')
                    db.execute(
                        query=f'create table if not exists {table_name} (id varchar(50), name varchar(100), code varchar(50), date text, a float, b float);',
                        do_return=False
                    )
                    db.commit()

                    db.copy_expert(
                        query=f"copy {table_name} from stdin delimiter ',' csv header NULL as 'NULL'",
                        file='U:/ML_project/bargh/temp/factors.csv',
                        mode='r'
                    )
                    db.commit()

                    db.__exit__()
                except psyError as e:
                    logger.error(f'an psycopg2 error occurred.\n{e}\n')
                    if db.connection:
                        db.rollback()

                try:
                    db.__enter__()

                    db.copy_expert(
                        query=f"copy {table_name} to stdout with delimiter ',' csv header NULL as 'NULL'",
                        file='U:/ML_project/bargh/data/interim/factors.csv',
                        mode='w'
                    )

                    db.commit()

                    db.__exit__()

                except psyError as e:
                    logger.error(f'an psycopg2 error occurred.\n{e}\n')
                    if db.connection:
                        db.rollback()

                os.remove('U:/ML_project/bargh/temp/factors.csv')

            case RawData.STATUS:
                table_name = 'status'
                df = self.clean(input_file=file.value, is_xlsx=False)

                try:
                    df[['id', 'code']] = df['FullUnitCode'].str.split('-', expand=True)
                    df.drop(columns=['FullUnitCode'], axis=1, inplace=True)
                    df = df[['id', 'code'] + [col for col in df.columns if col not in ['id', 'code']]]
                    df['Hour'] = df['Hour'].astype(int)
                    df.to_csv(path_or_buf='U:/ML_project/bargh/temp/status.csv', sep=',', header=True, index=False,
                              na_rep='NULL')

                    db.execute(
                        query=f'create table if not exists {table_name} (id varchar(50), code varchar(50), date text, hour int, status varchar(10));',
                        do_return=False
                    )
                    db.commit()

                    db.copy_expert(
                        query=f"copy {table_name} from stdin delimiter ',' csv header NULL as 'NULL'",
                        file='U:/ML_project/bargh/temp/status.csv',
                        mode='r'
                    )
                    db.commit()

                    db.__exit__()
                except psyError as e:
                    logger.error(f'an psycopg2 error occurred.\n{e}\n')
                    if db.connection:
                        db.rollback()
                except Exception as e:
                    logger.error(f'Exception below occurred:\n{e}\n')
                    if db.connection:
                        db.rollback()

                try:
                    db.__enter__()

                    db.copy_expert(
                        query=f"copy {table_name} to stdout with delimiter ',' csv header NULL as 'NULL'",
                        file='U:/ML_project/bargh/data/interim/status.csv',
                        mode='w'
                    )

                    db.commit()

                    db.__exit__()

                except psyError as e:
                    logger.error(f'an psycopg2 error occurred.\n{e}\n')
                    if db.connection:
                        db.rollback()

                os.remove('U:/ML_project/bargh/temp/status.csv')

            case RawData.LOAD:
                table_name = 'load'
                df = self.clean(input_file=file.value, is_xlsx=False)

                try:
                    max_indices = df.groupby(['Date', 'HourNo', 'ForcastedValue'])['Revision'].idxmax()
                    result = df.loc[max_indices]
                    result.drop(columns=['Revision'], axis=1, inplace=True)
                    result.to_csv(path_or_buf='U:/ML_project/bargh/temp/load.csv', sep=',', header=True, index=False,
                                  na_rep='NULL')

                    db.execute(
                        query=f"create table if not exists {table_name} (date text, hour int, forecast float)",
                        do_return=False
                    )
                    db.commit()

                    db.copy_expert(
                        query=f"copy {table_name} from stdin delimiter ',' csv header NULL as 'NULL'",
                        file='U:/ML_project/bargh/temp/load.csv',
                        mode='r'
                    )
                    db.commit()

                    db.__exit__()
                except Exception as e:
                    logger.error(f'an psycopg2 error occurred.\n{e}\n')
                    if db.connection:
                        db.rollback()

                try:
                    db.__enter__()

                    db.copy_expert(
                        query=f"copy {table_name} to stdout with delimiter ',' csv header NULL as 'NULL'",
                        file='U:/ML_project/bargh/data/interim/load.csv',
                        mode='w'
                    )

                    db.commit()

                    db.__exit__()

                except psyError as e:
                    logger.error(f'an psycopg2 error occurred.\n{e}\n')
                    if db.connection:
                        db.rollback()

                os.remove('U:/ML_project/bargh/temp/load.csv')

            case RawData.COMMITMENT:
                table_name = 'commitment'
                df = self.clean(input_file=file.value, is_xlsx=False)

                try:
                    max_indices = \
                    df.groupby(['PowerPlantCode', 'PowerPlantName', 'UnitCode', 'Date', 'HourNo', 'Required'])[
                        'Revision'].idxmax()
                    result = df.loc[max_indices]
                    result.drop(columns=['Revision'], axis=1, inplace=True)
                    result.to_csv(path_or_buf='U:/ML_project/bargh/temp/commitment.csv', sep=',', header=True,
                                  index=False, na_rep='NULL')

                    db.execute(
                        query=f"create table if not exists {table_name} (id varchar(10), name varchar(100), code varchar(10), date text, hour int, require float)",
                        do_return=False
                    )
                    db.commit()

                    db.copy_expert(
                        query=f"copy {table_name} from stdin delimiter ',' csv header NULL as 'NULL'",
                        file='U:/ML_project/bargh/temp/commitment.csv',
                        mode='r'
                    )
                    db.commit()

                    db.__exit__()
                except Exception as e:
                    logger.error(f'an psycopg2 error occurred.\n{e}\n')
                    if db.connection:
                        db.rollback()

                try:
                    db.__enter__()

                    db.copy_expert(
                        query=f"copy {table_name} to stdout with delimiter ',' csv header NULL as 'NULL'",
                        file='U:/ML_project/bargh/data/interim/commitment.csv',
                        mode='w'
                    )

                    db.commit()

                    db.__exit__()

                except psyError as e:
                    logger.error(f'an psycopg2 error occurred.\n{e}\n')
                    if db.connection:
                        db.rollback()

                os.remove('U:/ML_project/bargh/temp/commitment.csv')
