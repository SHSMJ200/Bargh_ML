import os
import sys

current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = current_dir[:current_dir.find("src") - 1]
sys.path.insert(0, project_root)

from sklearn.preprocessing import PolynomialFeatures
from src.models.customized_ML_models.AdaptiveQuantileRegressor import AdaptiveQuantileRegressor


from joblib import dump

from src.models.data_selection.data_selector import Data_selector
from src.models.data_selection.feature_selector import Feature_selector
from src.logs.logger import CustomLogger
from sklearn.linear_model import LinearRegression
from sklearn.pipeline import make_pipeline
from src.models.utils import *

logger = CustomLogger(__name__).get_logger()


def select_data(goodness):
    csv_semi_processed_path = os.path.join(project_root, "data", "processed", "semi_processed.csv")
    df = pd.read_csv(csv_semi_processed_path, encoding='utf-8')

    df_r_selected = Data_selector(df).select_peaks(goodness)
    logger.info(f"Rows with goodness={goodness} have been selected")
    return df_r_selected


def select_features(df_r_selected, features, target="generation"):
    feature_selector = Feature_selector(df_r_selected, target=target)
    feature_selector.filter_features(features_to_select=features)
    df_f_selected = feature_selector.df
    logger.info(f"Features have been selected successfully")
    return df_f_selected


def train_unit_model(X, y, folder_path, name, code, is_turbo=False):
    X_train = X
    y_train = y
    
    if is_turbo:
        model = make_pipeline(PolynomialFeatures(degree=2), LinearRegression())
    else:
        model = AdaptiveQuantileRegressor()
    
    model.fit(X_train, y_train)
    model_path = f"{folder_path}/{'turbo' if is_turbo else 'normal'}/{name}_{code}.joblib"
    dump(model, model_path)
    
    y_pred_train = model.predict(X_train)
    rmse_error_train = compute_relative_rmse(y_pred_train, y_train)

    return rmse_error_train


def print_report(data_sizes, train_errors):
    data_sizes = np.array(data_sizes)
    train_errors = np.array(train_errors)
    weighted_train_error = np.average(train_errors, weights=data_sizes)
    logger.info(f"Weighted train error: {weighted_train_error:0.3f}")


def train_all_unit_models(df_f_selected, is_turbo, save_model_folder):
    ds_n_c = Data_selector(df_f_selected)

    power_plants = ds_n_c.df[['name', 'code']].drop_duplicates()
    train_errors = []
    data_sizes = []
    for row in power_plants.itertuples():
        name, code = row.name, row.code
        logger.info(f"Train and test data related to {name}_{code}:")
        df_n_c = ds_n_c.filter_name_code(name, code)

        fs_n_c = Feature_selector(df_n_c, "generation")
        fs_n_c.filter_features(features_to_drop=["name", "code"])
        X, y = fs_n_c.get_X_and_y()

        train_error = train_unit_model(X, y, save_model_folder, name, code, is_turbo)
        logger.info(f"Train rmse error: {train_error:.3f}% , Size of data: {len(y)}")

        train_errors.append(train_error)
        data_sizes.append(len(y))

    print_report(data_sizes, train_errors)


def train_model():
    save_model_folder = os.path.join(project_root, "src", "models", "fitted_models")
    features = ["name", "code", "temperature"]

    logger.info(f"\n****************\ntrain models on normal data:")
    goodness_to_select = 5
    is_turbo = False
    df_row_selected = select_data(goodness_to_select)
    df_feature_selected = select_features(df_row_selected, features)
    train_all_unit_models(df_feature_selected, is_turbo, save_model_folder)

    logger.info(f"\n****************\ntrain models on turbo data:")
    goodness_to_select = 6
    is_turbo = True
    df_row_selected = select_data(goodness_to_select)
    df_feature_selected = select_features(df_row_selected, features)
    train_all_unit_models(df_feature_selected, is_turbo, save_model_folder)

if __name__ == "__main__":
    train_model()
