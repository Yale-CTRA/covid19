import os
import pandas as pd
import pyarrow.parquet as pq
root = os.path.join('D:','Data_WilsonLab','omop-covid')

def parquet_reader(folder):
    data = []
    for file in os.listdir(folder):
        if os.path.splitext(file)[1] == '.parquet':
            file_path = os.path.join(folder, file)
            chunk = pq.ParquetFile(file_path).read().to_pandas()
            data.append(chunk)
    return pd.concat(data, axis = 0)


def read_data():
    data_dict = {folder: parquet_reader(os.path.join(root,folder)) 
                 for folder in os.listdir(root)}