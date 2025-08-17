import sys,os
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = current_dir[:current_dir.find("src")-1]
sys.path.insert(0, project_root)

from crawl import HistoryCrawler, ForecastCrawler
from src.data.cleaning.data_cleaning import RawData

if __name__ == "__main__":
    HistoryCrawler(file=RawData.PLANT.value).crawl(start_date='2021-03-21', end_date="2025-03-20")

    # ForecastCrawler(file=RawData.PLANT.value).crawl()
