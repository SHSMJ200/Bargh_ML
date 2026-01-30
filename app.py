import os
import sys

current_dir = os.path.dirname(os.path.abspath(__file__))
src_path = os.path.join(current_dir, "src")
sys.path.insert(0, src_path)

from src.models.prediction.prediction import predict_generation
from src.logs.logger import CustomLogger
from src.root import get_root

import yaml

logger = CustomLogger(__name__).get_logger()
prediction_config_path = get_root() + '/configs/prediction.yaml'
prediction_config = yaml.safe_load(open(prediction_config_path, 'r', encoding='utf-8'))

if __name__ == "__main__":
    xlsx_input_path = prediction_config["xlsx_input_path"]
    xlsx_output_path = prediction_config["xlsx_output_path"]

    try:
        predict_generation(xlsx_input_path, xlsx_output_path)
    except Exception as e:
        logger.error(f"Prediction error occurred:\n{e}\nPossible causes:\n"
                     "- Column names are incorrect\n"
                     "- Dates do not correspond to tomorrow\n"
                     "- Date format is not yyyy/mm/dd\n"
                     "- Output Excel file is open in another program")
