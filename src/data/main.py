import sys, os

current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = current_dir[:current_dir.find("src") - 1]
sys.path.insert(0, project_root)

from cleaning.data_cleaning import CsvfileManipulation, RawData
from aggregation import Aggregator


def process_all_csv_files():
    manipulator = CsvfileManipulation()
    for raw in RawData:
        manipulator.process(file=raw)


if __name__ == "__main__":
    # process_all_csv_files()

    Aggregator(name='Jasbi').integrated_aggregation()
