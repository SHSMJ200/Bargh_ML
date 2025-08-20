from src.root import get_root
import openmeteo_requests
import pandas as pd
import requests_cache
import yaml
from retry_requests import retry

from logs.logger import CustomLogger
from src.data.dbconnection import Database

logger = CustomLogger('Crawler', log_file_name='crawler.log').get_logger()

db = Database()

feature_dict = yaml.load(open(get_root()  + '/configs/tables_columns.yaml'), Loader=yaml.SafeLoader)


def get_innermost_dict(nested_dict: dict):
    if not isinstance(nested_dict, dict):
        return None

    if not nested_dict:
        return None

    first_value = next(iter(nested_dict.values()))

    if isinstance(first_value, dict):
        return get_innermost_dict(first_value)
    else:
        return nested_dict


def get_plants():
    data = pd.read_csv(get_root()  + '/data/raw/PlantsTemperature_View.csv')
    plants = list(map(str, list(data['PowerPlantCode'].drop_duplicates())))
    return plants


def prepare_datetime_columns(data):
    data['date'] = pd.to_datetime(data['date'])
    logger.debug(msg=f"Date column converted to datetime type")
    data['date_only'] = data['date'].dt.date
    data['time'] = data['date'].dt.time
    data['time'] = data['time'].apply(lambda x: int(str(x).split(':')[0]))
    logger.debug(msg=f"Create the date and time columns from date column")
    data.loc[data['time'] == 0, 'time'] = 24
    data.loc[data['time'] == 24, 'date_only'] = data['date_only'] - pd.Timedelta(days=1)
    logger.debug(msg=f"Time column corrected")
    data.drop(columns=['date'], axis=1, inplace=True)
    logger.debug(msg=f"Column date deleted")
    logger.debug(msg=f"Date column converted from Ad to Persian")
    data.rename(columns={'date_only': 'date'}, inplace=True)
    columns_to_move = ['unitid', 'date', 'time']
    new_columns = columns_to_move + [col for col in data.columns if col not in columns_to_move]
    data = data[new_columns]
    return data


def fetch_hourly_weather_data(openmeteo, params, unitid, url):
    responses = openmeteo.weather_api(url, params=params)
    # Process first location. Add a for-loop for multiple locations or weather models
    response = responses[0]
    print(f"Coordinates {response.Latitude()}°N {response.Longitude()}°E")
    print(f"Elevation {response.Elevation()} m asl")
    print(f"Timezone {response.Timezone()} {response.TimezoneAbbreviation()}")
    print(f"Timezone difference to GMT+0 {response.UtcOffsetSeconds()} s")
    # Process hourly data. The order of variables needs to be the same as requested.
    hourly = response.Hourly()
    hourly_temperature_2m = hourly.Variables(0).ValuesAsNumpy()
    hourly_relative_humidity_2m = hourly.Variables(1).ValuesAsNumpy()
    hourly_dew_point_2m = hourly.Variables(2).ValuesAsNumpy()
    hourly_apparent_temperature = hourly.Variables(3).ValuesAsNumpy()
    hourly_precipitation = hourly.Variables(5).ValuesAsNumpy()
    hourly_rain = hourly.Variables(6).ValuesAsNumpy()
    hourly_snowfall = hourly.Variables(7).ValuesAsNumpy()
    hourly_surface_pressure = hourly.Variables(8).ValuesAsNumpy()
    hourly_evapotranspiration = hourly.Variables(9).ValuesAsNumpy()
    hourly_wind_speed_10m = hourly.Variables(10).ValuesAsNumpy()
    hourly_wind_direction_10m = hourly.Variables(11).ValuesAsNumpy()
    hourly_data = {"date": pd.date_range(
        start=pd.to_datetime(hourly.Time(), unit="s", utc=True),
        end=pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True),
        freq=pd.Timedelta(seconds=hourly.Interval()),
        inclusive="left"
    ), "temperature_2m": hourly_temperature_2m, "relative_humidity_2m": hourly_relative_humidity_2m,
        "dew_point_2m": hourly_dew_point_2m, "apparent_temperature": hourly_apparent_temperature,
        "precipitation": hourly_precipitation, "rain": hourly_rain, "snowfall": hourly_snowfall,
        "surface_pressure": hourly_surface_pressure, "evapotranspiration": hourly_evapotranspiration,
        "wind_speed_10m": hourly_wind_speed_10m, "wind_direction_10m": hourly_wind_direction_10m,
        'unitid': [unitid for _ in range(len(hourly_temperature_2m))]}
    hourly_dataframe = pd.DataFrame(data=hourly_data)
    return hourly_dataframe


class Crawler:
    def __init__(self, file: str):
        self.file = file


class HistoryCrawler(Crawler):

    def crawl(self, start_date: str, end_date: str):

        try:
            plants = pd.read_csv(filepath_or_buffer=self.file)
            my_plants = get_plants()
            plants = plants[plants['DispPlantCode'].isin(my_plants)]

            logger.info(msg=f'Plants data successfully read from {self.file}')
            logger.info(msg=f"Plants to crawl:\n{plants['PlantName'].drop_duplicates()}")

            do_continue = input("\nContinue with these plants?\n")
            if do_continue == "no":
                return None

            with open(get_root()  + '/configs/crawling.yaml', 'r') as file:
                data = yaml.safe_load(file)
                url = data['url-historical']
                hourly_features = data['hourly']

            logger.info(msg=f'Successfully read data from crawling config file')

            cache_session = requests_cache.CachedSession('.cache', expire_after=3600)
            retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
            openmeteo = openmeteo_requests.Client(session=retry_session)

            data = pd.DataFrame()

            for utm, unitid in zip(plants['UTM'], plants['DispPlantCode']):
                lat, longit = utm.split(',')
                params = {
                    "latitude": float(lat),
                    "longitude": float(longit),
                    "start_date": start_date,
                    "end_date": end_date,
                    "hourly": hourly_features
                }
                hourly_dataframe = fetch_hourly_weather_data(openmeteo, params, unitid, url)

                data = pd.concat([data, hourly_dataframe], ignore_index=True)
                logger.debug(msg=f'Data with latitude: {lat} and longitude: {longit} added to the dataframe')

            data = prepare_datetime_columns(data)

            length = len(data.index)

            data.dropna(inplace=True)

            logger.debug(f"{length - len(data.index)} number of data removed because of being null.")

            logger.debug(msg=f"Reorder the columns as the id, date and time comes to first.")

            file_path = get_root()  + '/data/interim/weather.csv'

            data.to_csv(file_path, index=False)

            logger.info(msg=f"Number of columns of dataframe is: {len(data.columns)}")

            db.connect()

            db.create_table(
                table_name='weather',
                columns=get_innermost_dict(feature_dict['weather'])
            )
            db.commit()

            db.lazy_copy_expert(
                table_name='weather',
                file=file_path,
                mode='r',
                into_db=True
            )
            db.commit()
            db.close()

        except Exception as e:
            logger.error(msg=f"Couldn't complete the crawling due to below Exception:\n{e}\n")


class ForecastCrawler(Crawler):

    def crawl(self):
        try:

            plants = pd.read_csv(self.file)

            logger.info(msg=f'Plants data successfully read from {self.file}')

            with open(get_root()  + '/configs/crawling.yaml', 'r') as file:
                data = yaml.safe_load(file)
                url = data['url-forecast']
                hourly_features = data['hourly']

            logger.info(msg=f'Successfully read data from crawling config file')

            cache_session = requests_cache.CachedSession('.cache', expire_after=3600)
            retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
            openmeteo = openmeteo_requests.Client(session=retry_session)

            data = pd.DataFrame()

            for utm, unitid in zip(plants['UTM'], plants['DispPlantCode']):
                lat, longit = utm.split(',')

                params = {
                    "latitude": float(lat),
                    "longitude": float(longit),
                    "hourly": hourly_features,
                    "forecast_days": 2
                }
                hourly_dataframe = fetch_hourly_weather_data(openmeteo, params, unitid, url)

                hourly_dataframe = hourly_dataframe.tail(24).reset_index(drop=True)
                logger.debug(msg=f"Split the last 24 rows.")

                data = pd.concat([data, hourly_dataframe], ignore_index=True)
                logger.debug(msg=f'Data with latitude: {lat} and longitude: {longit} added to the dataframe')

            data = prepare_datetime_columns(data)

            logger.debug(msg=f"Reorder the columns as the id, date and time comes to first.")

            file_path = get_root()  + '/data/interim/weather-forecast.csv'

            data.to_csv(file_path, index=False)

            logger.info(msg=f"Number of columns of dataframe is: {len(data.columns)}")

            db.connect()

            db.create_table(
                table_name='forecast',
                columns=get_innermost_dict(feature_dict['weather'])
            )
            db.commit()

            db.lazy_copy_expert(
                table_name='forecast',
                file=file_path,
                mode='r',
                into_db=True
            )
            db.commit()
            db.close()

        except Exception as e:
            logger.error(msg=f"Couldn't complete the crawling due to below Exception:\n{e}\n")
