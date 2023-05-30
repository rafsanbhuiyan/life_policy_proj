import os
import yaml
from yaml import Loader


def load_yaml(file):
    return yaml.load(file, Loader=Loader)


def get_resouce(filename, parser):
    cur_path = os.path.abspath(__file__)
    cur_dir = os.path.dirname(cur_path)

    path = os.path.join(cur_dir, "..", "resources", filename)

    with open(path, "rb") as file:
        return parser(file)

