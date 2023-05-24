import pandas as pd
from collections import *
from datetime import datetime
from dateutil.relativedelta import relativedelta
import datetime

def add_data_to_dict(d, key, value):
    """
        d = input dictionary
    """
    if key not in d:
        d[key] = value
        return d
    else:
        return d
    
def summary_999(df):
    """
        keep customer information which its transaction has oldest membership
    """
    input_df = df.copy()
    input_df = input_df.sort_values(by=['Contract Acc. M/I','M/I Date'], ascending = True)
    cols = input_df.columns.tolist()
    
    ratecat_dict = {}
    tsic_dict = {}
    cus_name_dict = {}
    
    bill_period = input_df["Period"].unique().tolist()
    if len(bill_period) > 1:
        print("bill period error @summary_999 function")
    
    acc_class = input_df["Acc. Class"].unique().tolist()
    if len(acc_class) > 1:
        print("acc class error @summary_999 function")
    
    installation_dict = {}
    date_dict = {}
    
    sum_kwh_dict = defaultdict(float)
    sum_dep_cash_dict = defaultdict(float)
    sum_dep_non_cash_dict = defaultdict(float)
    data_np = input_df.to_numpy()

    for i in range(len(data_np)):
        ca = data_np[i][1]
        ratecat_dict  = add_data_to_dict(ratecat_dict, ca, data_np[i][2])
        tsic_dict = add_data_to_dict(tsic_dict, ca, data_np[i][3])
        cus_name_dict = add_data_to_dict(cus_name_dict, ca, data_np[i][4])
        installation_dict = add_data_to_dict(installation_dict, ca, data_np[i][7])
        date_dict = add_data_to_dict(date_dict, ca, data_np[i][8])
        dep_cash = data_np[i][9]
        dep_non_cash = data_np[i][10]
        kwh_tot = data_np[i][11]
        sum_kwh_dict[ca] += kwh_tot
        sum_dep_cash_dict[ca] += dep_cash
        sum_dep_non_cash_dict[ca] += dep_non_cash
        
    result_data_dict = defaultdict(list)
    result_cols = cols[1:]
    for k,v in ratecat_dict.items():
        result_data_dict[result_cols[0]].append(k) # add ca data
        result_data_dict[result_cols[1]].append(v) # add ratecat data
        result_data_dict[result_cols[2]].append(tsic_dict[k])
        result_data_dict[result_cols[3]].append(cus_name_dict[k])
        result_data_dict[result_cols[4]].append(bill_period[0])
        result_data_dict[result_cols[5]].append(acc_class[0])
        result_data_dict[result_cols[6]].append(installation_dict[k])
        result_data_dict[result_cols[7]].append(date_dict[k])
        result_data_dict[result_cols[8]].append(sum_dep_cash_dict[k])
        result_data_dict[result_cols[9]].append(sum_dep_non_cash_dict[k])
        result_data_dict[result_cols[10]].append(sum_kwh_dict[k])
    
    result_df = pd.DataFrame(result_data_dict)
        
    return result_df

def verify_999_unique_ca(df):
    c_999 = Counter(df["Contract Acc. M/I"].tolist())
    for k,v in c_999.items():
        if v > 1:
            return False
    return True

def add_age_column(df):
    input_df = df.copy()
    bill_period = input_df["Period"].tolist()
    meter_register_date = input_df["M/I Date"].tolist()
    ages = [] # in month unit
    for bp, mrd in zip(bill_period, meter_register_date):
        bp_datetime = datetime.datetime.strptime(str(bp), '%Y%m')
        mrd_datetime_element = mrd.split("-")
        mrd_datetime = datetime.datetime.strptime(mrd_datetime_element[0]+mrd_datetime_element[1], '%Y%m')
        delta = relativedelta(bp_datetime, mrd_datetime)
        age = (delta.years * 12) + delta.months
        ages.append(age)
    
    input_df["Age"] = ages
    
    return input_df

def sum_sec_dep(df):
    input_df = df.copy()
    sec_dep_cash = input_df["Sec. Dep. (CASH)"].tolist()
    sec_dep_non_cash = input_df["Sec. Dep. (NONCASH)"].tolist()
    assert len(sec_dep_cash) == len(sec_dep_non_cash), "length error @sum_sec_dep function"
    
    sum_ = [] # sum security deposit
    for a,b in zip(sec_dep_cash, sec_dep_non_cash):
        sum_.append(a+b)
    
    input_df["sum_sec_dep"] = sum_
    
    return input_df

def merge_999_076(df_999, df_076):
    input_df1 = df_999.copy()
    input_df2 = df_076.copy()
    result_df = input_df1.merge(input_df2, how='left', on='Contract Acc. M/I')
    return result_df

def verify_unique_merged(df_999, merged_df):
    if len(df_999) == len(merged_df):
        if len(df_999["Contract Acc. M/I"].unique().tolist()) == len(df_999) and len(merged_df["Contract Acc. M/I"].unique().tolist()) == len(merged_df):
            return True
        else: 
            return False
    else:
        return False

def convert_zero(x):
    if x == 0:
        return 1e-3
    else:
        return x

def add_betrh_dep_ratio(df):
    input_df = df.copy()
    betrh_dep_ratio = []
    betrh = input_df["avg_betrh"].tolist()
    sum_sec_dep = input_df["sum_sec_dep"].tolist()
    
    assert len(betrh) == len(sum_sec_dep), "length error @add_betrh_dep_ratio"
    
    for i in range(len(betrh)):
        ratio = betrh[i]/convert_zero(sum_sec_dep[i])
        betrh_dep_ratio.append(ratio)
    
    input_df["betrh_dep_ratio"] = betrh_dep_ratio
    
    return input_df