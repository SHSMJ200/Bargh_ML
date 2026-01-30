import os
import sys

current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = current_dir[:current_dir.find("src") - 1]
sys.path.insert(0, project_root)

<<<<<<< HEAD
from src.data.aggregation import integrated_aggregation
from src.data.data_cleaner import process_all_csv_files



=======

>>>>>>> 164075d33a64a17fab7647ce332420262a563800
def preprocess_data():
    process_all_csv_files()
    integrated_aggregation()


if __name__ == "__main__":
    preprocess_data()
