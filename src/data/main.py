import sys, os

current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = current_dir[:current_dir.find("src") - 1]
sys.path.insert(0, project_root)

from logs.logger import CustomLogger
from cleaning.data_cleaning import CsvfileManipulation, RawData
from aggregation import Aggregator

logger = CustomLogger(name="model_main", log_file_name='model_main.log').get_logger()


def process_all_csv_files():
    manipulator = CsvfileManipulation()
    for raw in RawData:
        logger.info(f"Manipulation relating to {raw} has been done")
        manipulator.process(file=raw)


if __name__ == "__main__":
    process_all_csv_files()

    Aggregator(name='Jasbi').integrated_aggregation()
