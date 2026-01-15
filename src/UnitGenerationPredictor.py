from src.crawler.main import crawl_data
from src.data.main import preprocess_data
from src.models.filter_data.filter_data import filter_data
from src.models.prediction.prediction import predict_generation
from src.models.train_model.train_model import train_model


class UnitGenerationPredictor:
    def crawl_data(self):
        crawl_data()

    def preprocess_data(self):
        preprocess_data()

    def filter_data(self):
        filter_data()

    def train_model(self):
        train_model()

    def predict(self, xlsx_input_path, xlsx_output_path):
        predict_generation(xlsx_input_path, xlsx_output_path)
