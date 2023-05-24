import pandas as pd
import numpy as np
from collections import *

def verify_monthly_076(list_data_076):
    for df in list_data_076:
        if not len(df["BILL_PERIOD"].unique().tolist()) == 1:
            print("BILL PERIOD Error @verify_monthly_076")
            return False
    return True

def append_data(input_array, data):
    if len(input_array) == 0: # empty array cannot do vstack, hstack is available for empty array only
        output_array = np.hstack((input_array, data))
        return output_array
    else:
        output_array = np.vstack((input_array, data))
        return output_array
    
def summary_076(df):
    """
        keep the rows with the longest late duration for each CA 
    """
    input_df = df.copy()
    input_df = input_df.sort_values(by=['บัญชีแสดงสัญญา','BETRH','tag'], ascending = True)
    key_dict3 = defaultdict(int) # same ca, betrh -> multiple data -> same bill but different late duration
    key_dict1 = defaultdict(int) # ca has multiple rows if value > 1
    key_dict2 = defaultdict(set) # ca has multiple betrh if value > 1
    data_np = input_df.to_numpy()
    
    result_data = np.array([])
    
    for i in range(len(data_np)):
        key_dict3[(data_np[i][3], data_np[i][11])] += 1
        key_dict1[data_np[i][3]] += 1
        key_dict2[data_np[i][3]].add((data_np[i][3],data_np[i][11]))
        
    
    for k,v in key_dict1.items():
        if v > 1: # ca has multiple rows
            # check whether a ca has multiple betrh
            ca_betrh_pair_list = list(key_dict2[k])
            for pair in ca_betrh_pair_list:
                sub_df = input_df[(input_df["บัญชีแสดงสัญญา"] == pair[0]) & (input_df["BETRH"] == pair[1])]
                sub_df_data = sub_df.to_numpy()[-1] 
                result_data = append_data(result_data, sub_df_data)

        else: # ca has a single row
            sub_df_data = input_df[input_df["บัญชีแสดงสัญญา"] == k].to_numpy()[-1]
            result_data = append_data(result_data, sub_df_data)         
            
    result_df = pd.DataFrame(data=result_data, columns=input_df.columns)
    return result_df

def validate_betrh_and_debt(df):
    """
        betrh and debt must be equal, only one colummns from all late columns has a value.
    """
    input_df = df.copy()
    betrh_list = input_df["BETRH"].tolist()
    data_np = input_df.to_numpy()
    debt_list = [np.sum(x[-6:-1]) for x in data_np]
    
    count_anomaly = 0
    for i in range(len(betrh_list)):
        if not (betrh_list[i] == debt_list[i]):
            count_anomaly += 1
            print(i, betrh_list[i], debt_list[i])
    
    if count_anomaly > 0:
        return False
    else:
        return True

def get_late_score(x, late_score):
    if not (x == 0):
        return late_score
    else:
        return x

def map_late_duration_to_score(df):
    input_df = df.copy()
    duration_to_score_dict = {'1-30 วัน': -1,
                              '31-180 วัน': -2,
                              '181-365 วัน': -3,
                              '366-730วัน': -4,
                              '731วัน ขึ้นไป': -5}
    for k,v in duration_to_score_dict.items():
        input_df[k] = input_df[k].apply(get_late_score, args=(v,))
    return input_df

def append_late_score_columns(df):
    """
        reduce raw late duration columns to a single late score column
    """
    input_df = df.copy()
    data_np = input_df.to_numpy()
    late_score_list = [np.sum(x[-6:-1]) for x in data_np]
    
    assert len(data_np) == len(late_score_list), "length error @append_late_score_columns function"
    input_df = input_df.drop(['1-30 วัน', '31-180 วัน', '181-365 วัน', '366-730วัน', '731วัน ขึ้นไป'], axis = 1)
    input_df['late_score'] = late_score_list
    
    return input_df

def verify_076_final(df):
    input_df = df.copy()
    c = Counter(input_df["บัญชีแสดงสัญญา"].tolist())
    for k,v in c.items():
        if v > 1:
            return False
    
    return True

def change_column_name_076(df_076):
    input_df = df_076.copy()
    input_df.rename(columns = {'บัญชีแสดงสัญญา':'Contract Acc. M/I'}, inplace = True)
    return input_df