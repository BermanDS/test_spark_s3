import os
import yaml

root_dir = os.getcwd()

with open(os.path.join(root_dir, 'configs','auth_config.yml')) as f:
    auth_config = yaml.load(f, Loader = yaml.Loader)

with open(os.path.join(root_dir, 'configs','running_config.yml')) as f:
    running_config = yaml.load(f, Loader = yaml.Loader)