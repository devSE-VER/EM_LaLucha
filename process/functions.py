# Functions

import re
from os import scandir


def get_files(path_gen, regex_str):
    return [obj.name for obj in scandir(path_gen)
            if obj.is_file() and re.search(regex_str, obj.name) and obj.name.endswith('.xlsx')]