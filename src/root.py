import os


def get_root():
    current_dir = os.path.dirname(os.path.abspath(__file__))
    return current_dir[:-4]  # remove src
