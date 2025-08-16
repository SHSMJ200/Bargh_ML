import sys
import os
from crawl import HistoryCrawler, ForecastCrawler
from src.data.cleaning.data_cleaning import RawData

project_root = os.path.abspath("U:/ML_project/bargh/")
sys.path.insert(0, project_root)

if __name__ == "__main__":
    HistoryCrawler(file=RawData.PLANT.value).crawl(start_date='2021-03-21', end_date="2025-03-20")

    # ForecastCrawler(file=RawData.PLANT.value).crawl()
