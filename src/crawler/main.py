import os
import sys

from src.crawler.crawl import crawl_history

current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = current_dir[:current_dir.find("src") - 1]
sys.path.insert(0, project_root)

if __name__ == "__main__":
    crawl_history(start_date='2021-03-21', end_date="2025-03-20")
