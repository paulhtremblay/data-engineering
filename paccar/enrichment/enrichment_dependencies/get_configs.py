import yaml
import os

class ConfigError(Exception):
    pass

def get_configs():
    current_dir = os.path.dirname(os.path.abspath(__file__))
    with open(os.path.join(current_dir, "config.yaml")) as read_obj:
        try:
            data = yaml.safe_load(read_obj)
            return data
        except yaml.YAMLError as exc:
            raise(ConfigError(str(exc)))

