import os
import sys

current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = current_dir[:current_dir.find("src") - 1]
sys.path.insert(0, project_root)

import pandas as pd

from src.logs.logger import CustomLogger
from src.models.filter_data.feature_adder import Feature_adder

logger = CustomLogger(__name__).get_logger()



split_dates_by_name_code = {
    ('سبلان', 'G11'): pd.Timestamp('2021-10-31'),
    ('سبلان', 'G14'): pd.Timestamp('2022-05-01'),

    ('سیکل ترکیبی ارومیه', 'G11'): pd.Timestamp('2022-01-31'),

    ('سیکل ترکیبی یزد', 'G11'): pd.Timestamp('2021-08-01'),
    ('سیکل ترکیبی یزد', 'G15'): pd.Timestamp('2022-03-01'),

    ('شهدای پاکدشت - دماوند', 'G13'): pd.Timestamp('2022-01-01'),
    ('شهدای پاکدشت - دماوند', 'G22'): pd.Timestamp('2022-07-01'),

    ('عسلویه', 'G11'): pd.Timestamp('2023-07-01'),
    ('عسلویه', 'G12'): pd.Timestamp('2022-03-01'),

    ('قم', 'G11'): pd.Timestamp('2021-06-01'),

    ('پرند', 'G11'): pd.Timestamp('2022-03-31'),
    ('پرند', 'G12'): pd.Timestamp('2022-05-01'),

    ('گیلان', 'G15'): pd.Timestamp('2022-05-01'),
}

turbo_dict = {
    "قم": {
        "G11": True,
        "G12": True,
        "G13": True,
        "G14": True
    },
    "سیکل ترکیبی ارومیه": {
        "G11": False,
        "G12": False,
        "G13": False,
        "G14": False,
        "G15": False,
        "G16": False
    },
    "سیکل ترکیبی شیروان": {
        "G11": True,
        "G12": True,
        "G13": True,
        "G14": True,
        "G15": True,
        "G16": True
    },
    "سیکل ترکیبی یزد": {
        "G11": False,
        "G12": False,
        "G13": False,
        "G14": True,
        "G15": True,
        "G16": True
    },
    "شهدای پاکدشت - دماوند": {
        "G11": True,
        "G12": True,
        "G13": True,
        "G14": True,
        "G15": True,
        "G16": True,
        "G17": True,
        "G18": True,
        "G19": True,
        "G20": True,
        "G21": True,
        "G22": True
    },
    "شهدای پیروز - بهبهان": {
        "G11": False,
        "G12": False
    },
    "سبلان": {
        "G11": False,
        "G12": False,
        "G13": False,
        "G14": False,
        "G15": False,
        "G16": False
    },
    "حافظ": {
        "G11": False,
        "G12": False,
        "G13": False,
        "G14": False,
        "G15": False,
        "G16": False
    },
    "عسلویه": {
        "G11": False,
        "G12": False,
        "G13": False,
        "G14": False,
        "G15": False,
        "G16": False
    },
    "گیلان": {
        "G11": False,
        "G12": False,
        "G13": False,
        "G14": False,
        "G15": False,
        "G16": False
    },
    "جنوب اصفهان - چهلستون": {
        "G11": False,
        "G12": False,
        "G13": False,
        "G14": False,
        "G15": False,
        "G16": False
    },
    "پرند": {
        "G11": False,
        "G12": False,
        "G13": False,
        "G14": False,
        "G15": False,
        "G16": False
    }
}

FEATURE_SELECTION_CONFIG = {
    "turbo_hours": {
        "p_min": 0.8,
        "p_max": 0.99,
        "delta": 1,
        "interval": (0, 20)
    },
    "envelope": {
        "p": 2,
        "q": 0.02,
        "dt": 1,
        "min_turbo_temp": 20
    }
}


def get_coefs(df_factors):
    df_factors["Date"] = pd.to_datetime(df_factors["Date"])
    coefs = {}
    grouped = df_factors.groupby(['PowerPlantCode', 'PowerPlantName', "UnitCode"])
    for (pp_code, pp_name, unit_code), g in grouped:
        latest_row = g.sort_values("Date", ascending=False).iloc[0]
        coefs[(pp_name, unit_code)] = (latest_row["a1IndexGas"], latest_row["b1IndexGas"])
    return coefs


def add_features_and_filter(df, coefs):
    feature_adder = Feature_adder(df)

    feature_adder.select_gas_plants()

    feature_adder.select_active_hours(init_label=0, final_label=1)

    feature_adder.select_peak_level_hours(init_label=1, final_label=2)

    feature_adder.select_last_updated_plants_generation_function(split_dates_by_name_code, init_label=2, final_label=3)

    feature_adder.select_turbo_hours(
        coefs,
        turbo_dict,
        p_min=FEATURE_SELECTION_CONFIG["turbo_hours"]["p_min"],
        p_max=FEATURE_SELECTION_CONFIG["turbo_hours"]["p_max"],
        delta=FEATURE_SELECTION_CONFIG["turbo_hours"]["delta"],
        interval=FEATURE_SELECTION_CONFIG["turbo_hours"]["interval"],
        init_label=3,
        final_label=4
    )

    feature_adder.select_envelope(
        p=FEATURE_SELECTION_CONFIG["envelope"]["p"],
        q=FEATURE_SELECTION_CONFIG["envelope"]["q"],
        dt=FEATURE_SELECTION_CONFIG["envelope"]["dt"],
        init_label=3,
        final_label=5
    )

    feature_adder.select_envelope(
        p=FEATURE_SELECTION_CONFIG["envelope"]["p"],
        q=FEATURE_SELECTION_CONFIG["envelope"]["q"],
        dt=FEATURE_SELECTION_CONFIG["envelope"]["dt"],
        min_temp=FEATURE_SELECTION_CONFIG["envelope"]["min_turbo_temp"],
        init_label=4,
        final_label=6
    )

    return feature_adder.df


def filter_data():
    csv_read_path = os.path.join(project_root, "data", "processed", "integrated.csv")
    df = pd.read_csv(csv_read_path, encoding='utf-8')

    necessary_features = ['name', 'code', "date", "hour", "generation", "temperature", "load_level", "status"]
    df.dropna(subset=necessary_features, inplace=True)

    csv_read_path = os.path.join(project_root, "data", "interim", "factors.csv")
    df_factors = pd.read_csv(csv_read_path)
    coefs = get_coefs(df_factors)

    filtered_df = add_features_and_filter(df, coefs)

    csv_semi_write_path = os.path.join(project_root, "data", "processed", "semi_processed.csv")
    filtered_df.to_csv(csv_semi_write_path, index=False)


if __name__ == "__main__":
    filter_data()
