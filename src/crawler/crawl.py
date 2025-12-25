import openmeteo_requests
import pandas as pd
import requests_cache
import retry_requests
import yaml

from logs.logger import CustomLogger
from src.data.data_cleaner import RawDataConfig
from src.data.dbconnection import Database
from src.root import get_root

logger = CustomLogger(__name__).get_logger()

tables_config_path = get_root() + '/configs/tables_columns.yaml'
feature_dict = yaml.load(open(tables_config_path), Loader=yaml.SafeLoader)

crawl_config_path = get_root() + '/configs/crawling.yaml'
crawl_config = yaml.safe_load(open(crawl_config_path, 'r'))


def get_plants_info():
    plants_temperature_path = RawDataConfig.TEMPERATURE.value["file_path"]
    temperature_df = pd.read_csv(plants_temperature_path)
    all_plants = temperature_df['PowerPlantCode'].astype(str).drop_duplicates().tolist()

    plants_data_path = RawDataConfig.PLANT.value["file_path"]
    plants_df = pd.read_csv(plants_data_path)
    available_plants_df = plants_df[plants_df['DispPlantCode'].isin(all_plants)]
    return available_plants_df[['DispPlantCode', 'PlantName', 'UTM']]


def create_open_meteo_client():
    cache_session = requests_cache.CachedSession('.cache', expire_after=3600)
    retry_session = retry_requests.retry(cache_session, retries=5, backoff_factor=0.2)
    open_meteo = openmeteo_requests.Client(session=retry_session)
    return open_meteo


def fetch_hourly_weather_data(open_meteo, params, url):
    response = open_meteo.weather_api(url, params=params)[0]
    logger.debug(f"Target location info:\n"
                 f"Coordinates {response.Latitude()}°N {response.Longitude()}°E\n"
                 f"Elevation {response.Elevation()} m asl\n"
                 f"Timezone {response.Timezone()} {response.TimezoneAbbreviation()}\n"
                 f"Timezone difference to GMT+0 {response.UtcOffsetSeconds()} s")

    return response.Hourly()


def parse_hourly_weather(hourly, unit_id, unit_name):
    # Process hourly data. The order of variables needs to be the same as requested.
    variable_names = ["Temperature", "Humidity", "Dew", "ApparentTemperature",
                      "Precipitation", "Rain", "Snow", "SurfacePressure",
                      "Evapotranspiration", "WindSpeed", "WindDirection"]
    hourly_data = {var_name: hourly.Variables(i).ValuesAsNumpy() for i, var_name in enumerate(variable_names)}

    date_range = pd.date_range(
        start=pd.to_datetime(hourly.Time(), unit="s", utc=True),
        end=pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True),
        freq=pd.Timedelta(seconds=hourly.Interval()),
        inclusive="left"
    )
    hourly_data["Datetime"] = date_range
    hourly_data["UnitId"] = [unit_id] * len(date_range)
    hourly_data["Name"] = [unit_name] * len(date_range)

    return pd.DataFrame(data=hourly_data)


def extract_hour(full_time):
    hour = int(str(full_time).split(':')[0])
    return hour + 1  # Add 1 hour to adjust values where h is used for the interval (h-1, h]


def preprocess_weather_df(df):
    df['Datetime'] = pd.to_datetime(df['Datetime'])
    df['Datetime'] = df['Datetime'] + pd.to_timedelta("3:30:00")  # Because of GMT: +3:30

    df['Date'] = df['Datetime'].dt.date
    df['Hour'] = df['Datetime'].dt.time.apply(extract_hour)

    df.drop(columns=['Datetime'], axis=1, inplace=True)

    primary_cols = ['UnitId', 'Name', 'Date', 'Hour']
    new_col_order = primary_cols + [col for col in df.columns if col not in primary_cols]
    df = df.reindex(columns=new_col_order)

    return df


def crawl_history(start_date: str, end_date: str):
    try:
        open_meteo = create_open_meteo_client()

        hourly_df_list = []
        plants_info_df = get_plants_info()

        for row in plants_info_df.itertuples(index=False):
            hourly_features = crawl_config['hourly_features']
            f_lat, f_longit = map(float, row.UTM.split(','))
            url = crawl_config['url_historical']
            params = {"latitude": f_lat, "longitude": f_longit, "start_date": start_date, "end_date": end_date,
                      "hourly": hourly_features, "timezone": "auto"}
            hourly = fetch_hourly_weather_data(open_meteo, params, url)
            hourly_df = parse_hourly_weather(hourly, row.DispPlantCode, row.PlantName)
            hourly_df_list.append(hourly_df)
            logger.info(f'Crawling the data with latitude:{f_lat: 0.2f} & longitude:{f_longit: 0.2f} done')

        weather_df = pd.concat(hourly_df_list, ignore_index=True)
        weather_df = preprocess_weather_df(weather_df)
        weather_path = get_root() + '/data/interim/weather.csv'
        weather_df.to_csv(weather_path, index=False, na_rep='NULL')

        with Database() as db:
            db.create_table(table_name='weather', col_names_and_types=feature_dict['weather'])
            db.commit()
            db.copy_expert(table_name='weather', file=weather_path, into_db=True)
            db.commit()

    except Exception as e:
        logger.error(f"Couldn't complete the crawling due to below Exception:\n{e}\n")


def crawl_future():
    try:
        open_meteo = create_open_meteo_client()

        hourly_df_list = []
        plants_info_df = get_plants_info()

        for row in plants_info_df.itertuples(index=False):
            hourly_features = crawl_config['hourly_features']
            f_lat, f_longit = map(float, row.UTM.split(','))
            url = crawl_config['url_forecast']
            params = {"latitude": f_lat, "longitude": f_longit, "hourly": hourly_features, "forecast_days": 2,
                      "timezone": "auto"}
            hourly = fetch_hourly_weather_data(open_meteo, params, url)
            hourly_df = parse_hourly_weather(hourly, row.DispPlantCode, row.PlantName)
            hourly_df_list.append(hourly_df)
            logger.info(f'Crawling the data with latitude:{f_lat: 0.2f} & longitude:{f_longit: 0.2f} done')

        w_forecast_df = pd.concat(hourly_df_list, ignore_index=True)
        w_forecast_df = preprocess_weather_df(w_forecast_df)
        w_forecast_path = get_root() + '/data/interim/weather_forecast.csv'
        w_forecast_df.to_csv(w_forecast_path, index=False, na_rep='NULL')

        with Database() as db:
            db.create_table(table_name='weather_forecast', col_names_and_types=feature_dict['weather'])
            db.commit()
            db.copy_expert(table_name='weather_forecast', file=w_forecast_path, into_db=True)
            db.commit()

    except Exception as e:
        logger.error(f"Couldn't complete the crawling due to below Exception:\n{e}\n")
