import os
import sys

current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = current_dir[:current_dir.find("src") - 1]
sys.path.insert(0, project_root)

from src.models.data_selection.data_selector import Data_selector
from src.models.filter_data.filter_data import get_coefs
from src.logs.logger import CustomLogger

logger = CustomLogger(__name__).get_logger()

import pandas as pd
from plotUnit import draw_gen_temp_plot, draw_gen_date_plot


def plot():
    csv_read_path = os.path.join(project_root, "data", "interim", "factors.csv")
    df_factors = pd.read_csv(csv_read_path)
    coefs = get_coefs(df_factors)

    csv_path = os.path.join(project_root, "data", "processed", "semi_processed.csv")
    df = pd.read_csv(csv_path, encoding='utf-8')
    power_plants = df[['name', 'code']].drop_duplicates()

    for row in power_plants.itertuples():
        name, code = row.name, row.code
        ds_n_c_plot = Data_selector(Data_selector(df).select_peaks(goodness=2, is_tight=False))
        df_n_c_plot = ds_n_c_plot.filter_name_code(name, code)

        try:
            draw_gen_temp_plot(df_n_c_plot, coefs, name, code)
        except Exception as e:
            logger.error(f"This error occurred while drawing diagram related to {name}-{code}-temp:\n {e}")

        try:
            draw_gen_date_plot(df_n_c_plot, name, code)
        except Exception as e:
            logger.error(f"This error occurred while drawing diagram related to {name}-{code}-date :\n {e}")

    logger.info("Plot generation completed successfully.")


if __name__ == "__main__":
    plot()
