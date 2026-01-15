import os
import sys

from src.data.aggregation import integrated_aggregation
from src.data.data_cleaner import process_all_csv_files

current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = current_dir[:current_dir.find("src") - 1]
sys.path.insert(0, project_root)


def preprocess_data():
    process_all_csv_files()
    integrated_aggregation()


if __name__ == "__main__":
    preprocess_data()
