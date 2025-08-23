from src.root import get_root
import pandas as pd
from logs.logger import CustomLogger
import numpy as np
from data_selector import Data_selector
from logs.logger import CustomLogger

logger = CustomLogger(name="model_main", log_file_name='model_main.log').get_logger()

logger = CustomLogger(name="feature_modifier", log_file_name='feature_modifier.log').get_logger()


class Feature_selector:
    def __init__(self, df: pd.DataFrame, target):
        self.df = df
        self.target = target

    def select(self, features_to_drop=None):
        df = self.df
        if features_to_drop is not None:
            df.drop(columns=features_to_drop, axis=1, inplace=True)

        categorical_cols = df.select_dtypes(include=['object', 'category']).columns
        df = pd.get_dummies(df, columns=categorical_cols, drop_first=True)

        df = df.dropna()
        
        logger.debug(f"Features training is applied on: {df.columns}")

        X = df.drop(columns=[self.target])
        y = df[self.target]
        self.df = df

        return X, y


class Feature_adder:
    def __init__(self, df: pd.DataFrame):
        self.df = df

    def create_feature_with_delay(self, feature, n_delay, drop_null = True):
        new_feature = f"{feature}_with_{n_delay}_delay"
        temp = self.df.sort_values(by=['code', 'name', 'date', 'hour'])
        self.df[new_feature] = temp.groupby(['code', 'name'])[feature].shift(n_delay)
        if drop_null : self.df.dropna(inplace=True)
        
        logger.debug(f"A new column created: {feature} with {n_delay} hours delay")

    def add_season(self):
        self.df['date'] = pd.to_datetime(self.df['date'])
        self.df['season'] = self.df['date'].apply(get_season)
        
        logger.debug(f"Season column was created")
        
    def add_date_time(self):
        self.df['date'] = pd.to_datetime(self.df['date'])
        self.df['datetime'] = self.df['date'] + pd.to_timedelta(self.df['hour'], unit='h')

    def add_is_good_peak(self,threshold,add_col = True):
        df_modified = self.df[["name","code","date","hour","status",'value',"generation",'season']].copy(deep=True)
        df_modified = Data_selector(df_modified).select_peaks(m_in_summer=True)
        Feature_adder(df_modified).add_date_time()
        
        if add_col : self.df["is_good_peak"] = 0
        
        time_ranges_by_name_code = {}
        power_plants = df_modified[['name', 'code']].drop_duplicates()
        for _, row in power_plants.iterrows():
            df_name_code_smooth = Data_selector(df_modified).filter_name_code(row["name"],row["code"])
            time_ranges = get_interval(df_name_code_smooth,l_min=threshold)
            
            time_ranges_by_name_code[(row["name"],row["code"])] = time_ranges
            if add_col : self.labeling_point(df_name_code_smooth,time_ranges,label=2)
            
        return time_ranges_by_name_code
    
    def labeling_point(self,df_n_c,date,label):
        for date1,date2 in date:
            flag_array = Data_selector(df_n_c).filter_time(date1,date2,get_bool=True)
            self.df.loc[flag_array.index[flag_array],"is_good_peak"] = label

    def add_difference_column(self,feature,order=1):
        df = self.df
        self.add_date_time()
        
        new_feature = f"{feature}_difference_{order}_order"
        df[new_feature] = None
        power_plants = df[['name', 'code']].drop_duplicates()
        for _, row in power_plants.iterrows():
            df_name_code_smooth = Data_selector(df).filter_name_code(row["name"],row["code"])
            df.loc[df_name_code_smooth.index,new_feature] = df_name_code_smooth[feature].diff()
            
        df[new_feature] = df[new_feature].astype(float)
        self.df.reset_index(drop=True,inplace=True)

def get_season(date):
    month = date.month
    if month in [12, 1, 2]:
        return 'winter'
    elif month in [3, 4, 5]:
        return 'spring'
    elif month in [6, 7, 8]:
        return 'summer'
    else:
        return 'fall'

def get_interval(df,l_min):
    df_s = df.reset_index(drop=True)
    gap_mask_time = df_s['datetime'].diff() != pd.Timedelta(hours=1)  # هر جایی اختلاف دقیقاً 1 ساعت نیست، مرز بازه جدید است
    gap_mask_generatiion = df_s['generation'].diff().abs() > 4
    gap_mask = gap_mask_time | gap_mask_generatiion
    # ایندکس شروع بازه‌ها
    start_indices = df_s.index[gap_mask].tolist()
    # چون اولین ایندکس هم ابتدای یک بازه است، اگر نیست اضافه می‌کنیم
    if 0 not in start_indices: start_indices = [0] + start_indices
    # ایندکس پایان بازه‌ها یکی قبل از شروع بازه بعدی است
    end_indices = [i-1 for i in start_indices[1:]] + [df_s.index[-1]]
    
    # ساخت لیست بازه‌های (i1, i2)
    index_ranges = [(start_indices[i],end_indices[i]) for i in range(len(start_indices)) if end_indices[i] - start_indices[i] >= l_min-1]
    # ساخت لیست بازه‌های زمانی (t1, t2)
    time_ranges = [(df_s.loc[i1, 'datetime'], df_s.loc[i2, 'datetime']) for i1, i2 in index_ranges]
    
    return time_ranges