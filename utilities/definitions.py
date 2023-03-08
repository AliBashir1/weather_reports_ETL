"""
This file is used to get root directory of project so that can be used to fetch config_file.ini and
other resource files during runtime.
https://towardsdatascience.com/simple-trick-to-work-with-relative-paths-in-python-c072cdc9acb9
"""
import os
ROOT_DIR = os.path.realpath(os.path.join(os.path.dirname(__file__), ".."))
