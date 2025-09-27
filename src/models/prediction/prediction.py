import sys, os

current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = current_dir[:current_dir.find("src") - 1]
sys.path.insert(0, project_root)

import pandas as pd
from model import Model

if __name__ == "__main__":

    path_input = "" #TODO
    path_output = "" # TODO
    
    data = pd.read_csv(path_input)
    
    predicator = Model().load()
    result = predicator.predict(data)
    
    data["prediction"] = result
    data.to_csv(path_output)