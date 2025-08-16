import sys
import os

import pandas as pd
from plotUnit import UnitPlotter

project_root = os.path.abspath("U:/ML_project/bargh/")
sys.path.insert(0, project_root)

if __name__ == "__main__":

    csv_path = os.path.join(project_root, "data", "processed", "with_prediction.csv")
    df = pd.read_csv(csv_path, encoding='utf-8')
    # Modify date column and create datetime column:
    df['date'] = pd.to_datetime(df['date'])
    df['datetime'] = df['date'] + pd.to_timedelta(df['hour'], unit='h')

    # Draw Customize plot
    up = UnitPlotter(df)

    power_plants = df[['name', 'code']].drop_duplicates()
    for _, row in power_plants.iterrows():
        up.temperature_and_generation_over_time(name=row["name"], code=row["code"])
        up.generation_over_time(name=row["name"], code=row["code"])
        up.prediction_and_generation_over_time(name=row["name"], code=row["code"])

