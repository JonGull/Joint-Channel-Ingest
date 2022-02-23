import pandas as pd
from os import listdir
from os.path import isfile, join

def get_dataframe_from_folder(path_to_folder, sep=',', header=None, names=None, dtype=None, skiprows = 0):
    filepath = get_file_from_folder(path_to_folder)
    return pd.read_csv(filepath, sep=sep, header=header, names=names, dtype=dtype, skiprows=skiprows, skip_blank_lines=True)

def get_file_from_folder(path_to_folder):
    path_to_file = [f for f in listdir(path_to_folder) if isfile(join(path_to_folder, f))]
    if len(path_to_file) > 1:
        print("WARNING: MORE FILES THAN EXPECTED.")
    return path_to_folder + '/' + path_to_file[0]
