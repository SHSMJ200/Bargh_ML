import sys, os

current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = current_dir[:current_dir.find("src") - 1]
sys.path.insert(0, project_root)

import pandas as pd
import numpy as np
from data_selector import Data_selector
from feature_adder import Feature_adder
from feature_selector import Feature_selector
from logs.logger import CustomLogger
from models import Random_Forest, Linear, Polynomial, XGBoost, LinearL1, Neural_network

logger = CustomLogger(name="model_main", log_file_name='model_main.log').get_logger()


def add_features_and_filter(l_min, max_diff, c_thresh, read_from_integrated=False, write_on_csv=None):
    if write_on_csv == None: write_on_csv = read_from_integrated

    csv_read_path = os.path.join(project_root, "data", "processed", "integrated.csv")
    csv_semi_write_path = os.path.join(project_root, "data", "processed", "semi_processed.csv")

    if read_from_integrated:
        df = pd.read_csv(csv_read_path, encoding='utf-8')
        feature_adder = Feature_adder(df)
        feature_adder.create_feature_with_delay("temperature", 5)
        for hour in range(1, 49):
            feature_adder.create_feature_with_delay("generation", hour)
        feature_adder.filter1()
        feature_adder.filter2(l_min=l_min, max_diff=max_diff)
        feature_adder.filter3("temperature_with_5_delay", c_thresh=c_thresh, plot_pearsons_hist=True)
        df = feature_adder.df
        if write_on_csv:
            df.to_csv(csv_semi_write_path, index=False)
    else:
        df = pd.read_csv(csv_semi_write_path, encoding='utf-8')

    return df


def test_model(model, do_inverse_scale=True):
    rmse_error_train, rmse_error_test = model.rescale_and_compute_error(do_inverse_scale)
    logger.info(f"Train Error: {rmse_error_train:0.2f}%, Test Error: {rmse_error_test:0.2f}%")


def write_result(df):
    csv_write_path = os.path.join(project_root, "data", "processed", "data_for_plot.csv")
    df.to_csv(csv_write_path, index=False)
    

def select_features_and_get_X_and_y(df, is_mimo=False, number_mimo=None):
    feature_selector = Feature_selector(df, target="generation")
    features_to_be_select = ["name", "code", "temperature", "humidity", "dew", "surface_pressure", "value", "forecast",
                             "status", "season", "temperature_with_5_delay"] + ["datetime"]
    features_to_be_select.append(f"generation_with_{24}_delay")
    feature_selector.select(features_to_select=features_to_be_select)
    X, y,name_code_df = feature_selector.get_X_and_y(is_mimo=is_mimo, number_mimo=number_mimo)
    return X, y,feature_selector,name_code_df

def get_y_inverse_mimo(df,number_mimo,name_code_df,y,dic):
    df_modified = df
    n = number_mimo
    ds = Data_selector(df_modified.reset_index(drop=True))
    series_y = pd.Series([np.nan] * len(df_modified))
    name_column = name_code_df.columns[0]
    code_column = name_code_df.columns[1]
    

    power_plants = df_modified[['name', 'code']].drop_duplicates()
    for _, row in power_plants.iterrows():
        df_name_code = ds.filter_name_code(row["name"], row["code"])
        y_name_code = (y[(name_code_df[name_column] == row["name"]) & (name_code_df[code_column] == row["code"])])
        indexes = dic[(row["name"], row["code"])]
        
        ll = []
        z = 0
        for (i1,i2) in indexes:
            z += 1
            arr = y_name_code[i1:i2]
            m = i2-i1
            l = [0]*(m+n-1)
            for i in range(m):
                for j in range(n):
                    l[i+j] += arr[i,j]

            mn = min(m,n)
            for k in range(m+n-1):
                kk = min(k+1,m+n-(k+1))
                l[k] /= min(kk,mn)
            
            ll += l
        try:
            series_y[df_name_code.index] = ll  
        except:
            print(len(ll),len(series_y),len(df_name_code)) 
            series_y[df_name_code.index] = ll
    return series_y.to_numpy()

if __name__ == "__main__":
    # TODO: for mimo > 1 doesn't work
    write_predictions = True

    number_mimo = 4
    is_mimo = number_mimo > 1
    y_is_flat = not is_mimo

    l_min = 4
    max_diff = 3
    c_thresh = 0.9

    df = add_features_and_filter(l_min, max_diff, c_thresh, read_from_integrated=False)
    logger.info(f"Csv file has bean labeled successfully")

    ds = Data_selector(df)
    df_modified = ds.select_peaks(goodness=3)
    logger.info(f"Rows have been selected successfully")

    X, y,fs,name_code_df = select_features_and_get_X_and_y(df_modified, is_mimo=is_mimo, number_mimo=number_mimo)
    logger.info(f"Some features have been dropped successfully")

    # model = Random_Forest(n_estimators=100, max_depth=1000)
    # model = Linear()
    # model = Polynomial(degree=2)
    # model = XGBoost(n_estimators=1000, max_depth=5)
    # model = Neural_network(input_dim=X.shape[1], epochs=100, verbose=1)

    model = XGBoost(n_estimators=1000, max_depth=5)
    model.scale_and_split_data(X, y, y_is_flat=y_is_flat)
    model.fit()
    logger.info(f"Model has been trained successfully")

    test_model(model)

    if is_mimo:
        y_pred_mimo = model.pred(X)
        dic = fs.name_code_dictionary_index
        y_pred = get_y_inverse_mimo(df_modified,number_mimo,name_code_df,y_pred_mimo,dic)
    else:
        y_pred = model.pred(X)

    df_modified["prediction"] = y_pred
    
    if write_predictions:
        df.loc[df_modified.index,"prediction"] = y_pred
        write_result(df)

