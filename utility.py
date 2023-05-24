import pandas as pd
import numpy as np


def read_csv_file(path):
    df = pd.read_csv(path, index_col=False)
    return df

def read_text_file(path):
    df = pd.read_csv(path, sep="\t", index_col=False)
    return df

def add_tag(df, name):
    name_element = name.split("_")
    timestamp = name_element[-1].split(".")[0]
    df['tag'] = [timestamp]*len(df)
    return df

def filter_bill_period(BP_list):
    selected = [x for x in BP_list if "2022" in x]
    return selected

def rename_columns(df):
    """
        this function resolve the wrong column names, switching between Acc. Class and Period
    """
    input_df = df.copy()
    result_df = input_df.rename(columns={'Acc. Class': 'Period', 'Period': 'Acc. Class'})
    
    return result_df