import sys
import os

from cleaning.data_cleaning import CsvfileManipulation, RawData
from aggregation import Aggregator

project_root = os.path.abspath("U:/ML_project/bargh/")
sys.path.insert(0, project_root)


def process_all_csv_files():
    manipulator = CsvfileManipulation()
    for raw in RawData:
        manipulator.process(file=raw)


if __name__ == "__main__":
    # process_all_csv_files()

    Aggregator(name='Jasbi').integrated_aggregation()
