import os
import sys

current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = current_dir[:current_dir.find("src") - 1]
sys.path.insert(0, project_root)

import pandas as pd
from plotUnit import UnitPlotter

if __name__ == "__main__":

    csv_path = os.path.join(project_root, "data", "processed", "semi_processed.csv")
    df = pd.read_csv(csv_path, encoding='utf-8')

    up = UnitPlotter(df)

    power_plants = df[['name', 'code']].drop_duplicates()
    for _, row in power_plants.iterrows():
        # up.temperature_and_generation_flag_marker_over_time(name=row["name"], code=row["code"])
        # up.temperature_change_and_generation_change_flag_marker_over_time(name = row["name"], code = row["code"])
        # up.temperature_and_generation_over_time(name=row["name"], code=row["code"])
        up.generation_over_time(name=row["name"], code=row["code"])
        # up.prediction_and_generation_flag_marker_over_time(name=row["name"], code=row["code"])
        # up.generation_and_generation_with_24_delay_flag_marker_over_time(name=row["name"], code=row["code"])
        # up.generation_and_mean_generation_and_generation_with_24_delay_flag_marker_over_time(name=row["name"], code=row["code"])
        # up.prediction_and_declare_and_generation_flag_marker_over_time(name=row["name"], code=row["code"])

