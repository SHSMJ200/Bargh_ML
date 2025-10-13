import numpy as np
import pandas as pd
from scipy.stats import pearsonr
from sklearn.linear_model import LinearRegression

from src.models.data_selection.data_selector import Data_selector
from logs.logger import CustomLogger

logger = CustomLogger(__name__).get_logger()


class Feature_adder:
    def __init__(self, df: pd.DataFrame, add_label_column=True):
        self.df = df
        self.add_time_features()
        if add_label_column: self.df["is_good_peak"] = 0
        self.time_ranges_by_name_code = {}
        self.c_time_ranges_by_name_code = {}

    def create_feature_with_delay(self, feature, n_delay):
        new_feature = f"{feature}_with_{n_delay}_delay"
        temp = self.df.sort_values(by=['code', 'name', 'date', 'hour'])
        self.df[new_feature] = temp.groupby(['code', 'name'])[feature].shift(n_delay)

        logger.debug(f"A new column created: {feature} with {n_delay} hours delay")

    def add_time_features(self):
        self.df['date'] = pd.to_datetime(self.df['date'])
        self.df['datetime'] = self.df['date'] + pd.to_timedelta(self.df['hour'], unit='h')
        self.df['season'] = self.df['date'].apply(get_season)
        self.df['day_of_week'] = self.df['datetime'].dt.dayofweek
        self.df['month'] = self.df['datetime'].dt.month
        self.df.sort_values(by=['name','code','datetime'],inplace=True)

        logger.debug(f"Season column was created")

    def filter1(self):
        start_md = "05-22"
        end_md = "09-22"
        # We assume that start_md < end_md
        statusM_mask = (self.df['datetime'].dt.strftime('%m-%d') >= start_md) & (
                self.df['datetime'].dt.strftime('%m-%d') <= end_md)
        peak_condition = (self.df['load_level'] == 'P') | (self.df['load_level'] == 'M') & statusM_mask
        peak_condition = peak_condition & ((self.df['status'] == 'SO') | (self.df['status'] == 'LF1'))

        self.df.loc[peak_condition, "is_good_peak"] = 1

        self.log_filter_ratio(label=1)

    def filter2(self, l_min, max_diff):
        features = ["name", "code", "datetime", "generation", "is_good_peak"]
        df_modified = self.df[features].copy(deep=True)
        df_modified = df_modified[df_modified["is_good_peak"] >= 1]

        power_plants = df_modified[['name', 'code']].drop_duplicates()
        ds = Data_selector(df_modified)

        for _, row in power_plants.iterrows():
            name, code = row['name'], row['code']
            df_name_code = ds.filter_name_code(name, code)
            time_ranges = get_interval(df_name_code, l_min, max_diff)
            self.time_ranges_by_name_code[(name, code)] = time_ranges

        self.label_points(ds, power_plants, self.time_ranges_by_name_code, label=2)

        self.log_filter_ratio(label=2)

    def filter3(self, feature1, c_thresh=0.9):
        features = ["name", "code", "datetime", "generation", "is_good_peak"] + [feature1]
        df_modified = self.df[features].copy(deep=True)
        df_modified = df_modified[df_modified["is_good_peak"] >= 2]

        corr_pearsons = []
        power_plants = df_modified[['name', 'code']].drop_duplicates()
        ds = Data_selector(df_modified)

        for _, row in power_plants.iterrows():
            name, code = row['name'], row['code']
            df_name_code = ds.filter_name_code(name, code)
            time_ranges = self.time_ranges_by_name_code[(name, code)]
            consistent_time_ranges = find_consistency(df_name_code, feature1, time_ranges, c_thresh, corr_pearsons)
            self.c_time_ranges_by_name_code[(name, code)] = consistent_time_ranges

        self.label_points(ds, power_plants, self.c_time_ranges_by_name_code, label=3)

        self.log_filter_ratio(label=3)

    def filter4(self,th = 0.9,k_filter = 6,n_filter = 4,l_min = 3):
        feature = ['name','code',"datetime", "generation","temperature", "is_good_peak"]
        df_modified = self.df[feature].copy(deep=True)
        df_modified = df_modified[df_modified['is_good_peak']>=3]
        df_modified['datetime'] = pd.to_datetime(df_modified['datetime'])

        power_plants = df_modified[['name', 'code']].drop_duplicates()
        ds = Data_selector(df_modified)

        dates_by_name_code={}
        for _, row in power_plants.iterrows():
            name, code = row['name'], row['code']
            
            if code.startswith("S"):
                dates_by_name_code[(name,code)] = []
                continue
            
            df_name_code = ds.filter_name_code(name,code)
            if len(df_name_code) == 0:
                dates_by_name_code[(name,code)] = []
                continue
            
            df_year_month,min_date,max_date = get_df_of_year_and_month(df_name_code)
            weigth,accuracies,dates = compare_months(df_name_code,df_year_month)
            dates_by_name_code[(name,code)] = get_date_interval(accuracies,dates,weigth,min_date,max_date,th,k_filter,n_filter,l_min)
            
        self.label_points(ds, power_plants, dates_by_name_code, label=4)

        self.log_filter_ratio(label=3)

        
    def filter5(self):
        features = ["name", "code", "generation", "temp_sens", "is_good_peak"]
        df_modified = self.df[features].copy(deep=True)
        df_modified = df_modified[df_modified["is_good_peak"] >= 3]  # TODO: should be 4
        df_modified = df_modified.dropna()
        power_plants = df_modified[['name', 'code']].drop_duplicates()
        gas_plants = power_plants[power_plants['code'].str.startswith("G")]
        for row in gas_plants.itertuples():
            name = row.name
            code = row.code
            one_unit_df = df_modified[(df_modified['name'] == name) & (df_modified['code'] == code)]

            senstemps = one_unit_df['temp_sens'].values
            gens = one_unit_df['generation'].values

            sorted_idx = np.argsort(senstemps)
            senstemps, gens = senstemps[sorted_idx], gens[sorted_idx]

            hist, bin_edges = np.histogram(senstemps, bins=101)
            group_indices = np.digitize(senstemps, bin_edges) - 1
            tuples = []

            for g in np.unique(group_indices):
                mask = group_indices == g
                tuples.append((senstemps[mask].mean(), find_95_max(gens[mask])))

            X = np.array([a for a, b in tuples]).reshape(-1, 1)
            y = np.array([b for a, b in tuples])
            model = LinearRegression()
            model.fit(X, y)

            X_all = senstemps.reshape(-1, 1)
            y_pred_all = model.predict(X_all)

            alpha = 2
            beta = 5
            is_in_area = (y_pred_all + alpha >= gens) & (gens >= y_pred_all - beta)

            good_indices = one_unit_df[is_in_area].index
            self.df.loc[good_indices, "is_good_peak"] = 5


    def label_points(self, ds, power_plants, dates_by_name_code, label):
        all_indices = []
        for _, row in power_plants.iterrows():
            name, code = row['name'], row['code']
            df_name_code = ds.filter_name_code(name, code)
            dates = dates_by_name_code[(name, code)]

            interval_ds = Data_selector(df_name_code)
            for date1, date2 in dates:
                flag_array = interval_ds.filter_time(date1, date2, get_mask=True)
                indices = flag_array.index[flag_array]
                all_indices.extend(indices)
        self.df.loc[all_indices, "is_good_peak"] = label

    def add_interval_id(self):
        df_modified = self.df[self.df["is_good_peak"] >= 3]

        power_plants = df_modified[['name', 'code']].drop_duplicates()
        ds = Data_selector(self.df)

        mapping = {}

        for _, row in power_plants.iterrows():
            name, code = row['name'], row['code']
            df_name_code = ds.filter_name_code(name, code)
            consistent_time_ranges = self.c_time_ranges_by_name_code[(name, code)]
            interval_ds = Data_selector(df_name_code)
            for interval_id, (date1, date2) in enumerate(consistent_time_ranges):
                indices = interval_ds.filter_time(date1, date2).index
                mapping.update({idx: interval_id for idx in indices})

        interval_series = pd.Series(mapping, name="interval_id")
        self.df = self.df.join(interval_series, how="left")

    def log_filter_ratio(self, label):
        count_new_label = len(self.df[self.df["is_good_peak"] == label])
        count_old_label = len(self.df[self.df["is_good_peak"] >= label - 1])
        consistency_percentage = count_new_label / count_old_label * 100
        logger.info(f"{consistency_percentage:0.2f}% of rows have been chosen by filter{label}")

    def add_difference_column(self, feature, order=1):
        df = self.df

        new_feature = f"{feature}_difference_{order}_order"
        df[new_feature] = None
        power_plants = df[['name', 'code']].drop_duplicates()
        for _, row in power_plants.iterrows():
            df_name_code = Data_selector(df).filter_name_code(row["name"], row["code"])
            df.loc[df_name_code.index, new_feature] = df_name_code[feature].diff()

        df[new_feature] = df[new_feature].astype(float)


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


def get_interval(df, l_min, max_diff):
    df_s = df.reset_index(drop=True)
    gap_mask_time = df_s['datetime'].diff() != pd.Timedelta(hours=1)
    gap_mask_generation_up = (df_s['generation'].diff() > max_diff) & (~gap_mask_time)
    gap_mask_generation_down = (df_s['generation'].diff() < -max_diff) & (~gap_mask_time)
    gap_mask = gap_mask_time | gap_mask_generation_up | gap_mask_generation_down

    start_indices = df_s.index[gap_mask].tolist()
    if 0 not in start_indices: start_indices = [0] + start_indices
    end_indices = [i for i in start_indices[1:]] + [df_s.index[-1]]

    index_ranges = []
    for i in range(len(start_indices)):
        if end_indices[i] - start_indices[i] >= l_min:
            if not gap_mask_generation_down[start_indices[i]] and not gap_mask_generation_up[end_indices[i]]:
                index_ranges.append((start_indices[i], end_indices[i] - 1))

    time_ranges = [(df_s.loc[i1, 'datetime'], df_s.loc[i2, 'datetime']) for i1, i2 in index_ranges]

    return time_ranges


def find_consistency(df, feature1, time_ranges, c_thresh, corr_pearsons):
    consistent_time_ranges = []
    data_selector = Data_selector(df)
    for start, end in time_ranges:
        find_interval_consistency(consistent_time_ranges, corr_pearsons, data_selector, feature1, start, end, c_thresh)

    return consistent_time_ranges


def find_interval_consistency(consistent_time_ranges, corr_pearsons, data_selector, feature1, start, end, c_thresh):
    df_sliced = data_selector.filter_time(start, end)

    temperatures = df_sliced[feature1].values
    generations = df_sliced["generation"].values

    corr_pearson, are_similar = are_trends_similar(temperatures, generations, c_thresh, opposite_trend=True)
    corr_pearsons.append(corr_pearson)
    if are_similar:
        consistent_time_ranges.append((start, end))


def are_trends_similar(arr1, arr2, threshold, opposite_trend=False):
    if np.std(arr1) == 0 or np.std(arr2) == 0:
        return 0, np.std(arr1) == np.std(arr2)

    corr_pearson, _ = pearsonr(arr1, arr2)

    if not opposite_trend:
        return corr_pearson, (corr_pearson > threshold)
    return corr_pearson, (corr_pearson < -threshold)


def find_95_max(array):
    array = np.array(array)
    sorted_array = np.sort(array)
    idx = int(len(array) * 0.95)
    return sorted_array[idx]







def get_df_of_year_and_month(df):
    datetime = pd.to_datetime(df['datetime'])
    start = datetime.min().replace(day=1)
    end = datetime.max().replace(day=1)
    
    full_range = pd.date_range(start=start, end=end, freq='MS')
    df_full_range = pd.DataFrame({'year_month': full_range})['year_month']
    df_year_month =  pd.DataFrame({'year': df_full_range.dt.year, 'month': df_full_range.dt.month})
    
    return df_year_month,start,end


def get_date_interval(result,dates,weigth,min_date,max_date,th,k_filter=6,n_filter=1,l_min=3):
    result1 = f_n(result,k=k_filter,n=n_filter,weigth=weigth)
    result2 = np.array([my_func(x,th) for x in result1])
    intervals = find_intervals(result2, k=l_min)
    dates_interval = find_time_intervals(intervals,dates)
    
    time_e = [min_date] + dates_interval + [max_date+pd.DateOffset(months=1)]
    time_intervals = [(time_e[i],time_e[i+1]) for i in range(len(time_e)-1)]
    
    if time_intervals != [] : time_intervals = [time_intervals[-1]]
    return time_intervals

def f_n(result,k,n,weigth=None):
    for ii in range(n):
        result = np.array([normal_filter(result,i,k=k,weigth=weigth) for i in range(len(result))])
    return result

def normal_filter(array,index,k,weigth=None):
    k = min(index+1,k)
    k = min(len(array)-index,k)
    m = 0
    n = 0
    for i in range(index-k,index+k+1):
        mm,nn = get_el(array,i,i==index,weigth)
        m += mm
        n += nn
        
    return m/n if n != 0 else None

def get_el(a,i,f,weigth):
    
    if i < 0 or i >= len(a):
        return 0,0
    if a[i] == None:
        return 0,0
    
    if type(weigth) != np.ndarray: w = 1
    else: w = weigth[i]
    
    return a[i]*w,w

def my_func(x,th):
    if x == None:
        return None
    if x > th:
        return 1
    return 0

def find_intervals(arr, k):
    start = None
    count = 0
    intervals = []
    for i, num in enumerate(arr):
        if num == 1:
            if start is None:
                start = i
            count += 1
        elif num == 0:
            if count > k:
                intervals.append((start, i))
            start = None
            count = 0

    if count >= k or count == len(arr):
        intervals.append((start, len(arr)))
    
    return intervals

def find_time_intervals(intervals,dates):
        
    end = len(dates)
    last_time = max(dates)
    k = []
    y1 = np.timedelta64(1,'Y').astype('timedelta64[ns]')#pd.DateOffset(years=1)#np.datesdelta64(1,'M').astype('datesdelta64[ns]')
    m1 = np.timedelta64(1,'M').astype('timedelta64[ns]')
    for i1,i2 in intervals:
        if i2 != end:
            k.append(dates[i2-1]+m1)
        else :
            k.append(min(dates[i1],last_time)+y1)
    return k




from sklearn.metrics import classification_report
def compare_months(df_name_code,df_year_month):
    weigth = []
    accuracies = []
    dates = []
    
    df_year_month = df_year_month.iloc[0:-12]
    for _,row in df_year_month.drop_duplicates().iterrows():
        month = row['month']
        year = row['year']
        year1 = year
        year2 = year + 1     
        datetime = pd.to_datetime({'year': [year], 'month': [month], 'day': [1]})
        
        df_month,n1,n2 = select_month(df_name_code,month,month,year1,year2)
        weigth.append(n1+n2)
    
        if n1 != 0 and n2 != 0:    
            X = df_month[["generation"]+["temperature"]]
            y = df_month["label_month"]
            y_pred = check(X,y)
            d = classification_report(y, y_pred,output_dict=True,zero_division=0)
            accuracies.append(d['accuracy'])
            dates.append(datetime[0])
        else:
            accuracies.append(None)
            dates.append(datetime[0])
            
    accuracies = np.array(accuracies)
    dates = np.array(dates)
    weigth = np.array(weigth)/sum(weigth)
    return weigth,accuracies,dates

def select_month(df,m1,m2,y1,y2):
    df["datetime"] = pd.to_datetime(df["datetime"])
    year_s  = df["datetime"].dt.year
    month_s = df["datetime"].dt.month
    mask1 = (year_s == y1) & (month_s == m1)
    mask2 = (year_s == y2) & (month_s == m2)
    df_month = df[mask1|mask2]
    df_month["label_month"] = 1
    mm1 = mask1[mask1]
    mm2 = mask2[mask2]
    df_month.loc[mm1.index,"label_month"] = 1
    df_month.loc[mm2.index,"label_month"] = 2
    return df_month,len(mm1),len(mm2)

from sklearn.svm import SVC
def check(X,y):
    model = SVC(kernel='linear')
    model.fit(X, y)
    y_pred = model.predict(X)
    return y_pred







