import pandas as pd
import numpy as np
import utility as u
import zblr999 as zblr
import zcanr076 as zcanr
import clustering as cl

import os
import argparse

parser = argparse.ArgumentParser(description='Customer scoring script')
parser.add_argument('--zblr', default=None, type=str,help='path to zlbr999 folder')
parser.add_argument('--zcanr', default=None, type=str,help='path to zcanr076 folder')

args = parser.parse_args()

path_999 = args.zblr
path_076 = args.zcanr

pd.options.mode.chained_assignment = None  # default='warn'

# zcanr076 data processing
list_076 = [f for f in os.listdir(path_076) if f.endswith('.txt')]
list_076.sort()

shelf_076 = [u.read_text_file(path_076+f) for f in list_076]
shelf_076_with_tag = [u.add_tag(df, name) for (df, name) in zip(shelf_076, list_076)]

full_076 = pd.concat(shelf_076_with_tag, axis=0)
full_076["BILL_PERIOD"] = full_076["BILL_PERIOD"].astype(str) # casting str fix type consistency (str + int)
full_076 = full_076[full_076["BILL_PERIOD"] != " "] # remove null bill period

BP_list = full_076['BILL_PERIOD'].unique().tolist() # BP = Bill period
BP_list.sort()
BP_selected_list = u.filter_bill_period(BP_list)

monthly_076 = [full_076[full_076['BILL_PERIOD'] == tag] for tag in BP_selected_list]

if not zcanr.verify_monthly_076(monthly_076):
    print('error @verify_monthly_076 function')
    exit(0)

summary_monthly_076 = [zcanr.summary_076(f) for f in monthly_076]

is_validate = [zcanr.validate_betrh_and_debt(f) for f in summary_monthly_076]

if sum(is_validate) != len(is_validate):
    print('error @validate_betrh_and_debt function')
    exit(0)

summary_monthly_076_duration_to_late_score = [zcanr.map_late_duration_to_score(f) for f in summary_monthly_076]

summary_monthly_076_for_clustering = [zcanr.append_late_score_columns(f) for f in summary_monthly_076_duration_to_late_score]

summary_monthly_076_for_clustering[0].to_csv('temp1.csv', index=False, encoding='utf-8-sig')

# zcanr076 clustering
kmean_shelf_betrh_076 = [cl.clustering(f, "BETRH", 5) for f in summary_monthly_076_for_clustering]
betrh_score_map_shelf = [cl.get_boundary_map(km, df,"BETRH",76) for km,df in zip(kmean_shelf_betrh_076, summary_monthly_076_for_clustering)]

summary_monthly_076_with_amount_score = [cl.add_score_column(f,amount_score_map, "BETRH", "amount_score") for f, amount_score_map in zip(summary_monthly_076_for_clustering, betrh_score_map_shelf)]
summary_monthly_076_final = [cl.get_unique_clustering_076(f) for f in summary_monthly_076_with_amount_score]

is_verify_076_final = [zcanr.verify_076_final(f) for f in summary_monthly_076_final]

if sum(is_verify_076_final) != len(is_verify_076_final):
    print('error @verify_076_final function')
    exit(0)

# zblr999 data preprocessing
list_999 = [f for f in os.listdir(path_999) if f.endswith('.csv')]
list_999.sort()

shelf_999 = [u.read_csv_file(path_999+f) for f in list_999]
cleaned_shelf_999 = [zblr.summary_999(f) for f in shelf_999]

is_verify_999_unique = [zblr.verify_999_unique_ca(f) for f in cleaned_shelf_999]

if sum(is_verify_999_unique) != len(is_verify_999_unique):
    print('error @verify_999_unique_ca function')
    exit(0)

# cleaned_shelf_999[0] = u.rename_columns(cleaned_shelf_999[0])

summary_monthly_999_for_clustering = [zblr.add_age_column(f) for f in cleaned_shelf_999]

# zblr999 clustering
kmean_shelf_age_999 = [cl.clustering(f, "Age", 5) for f in summary_monthly_999_for_clustering]
age_score_map_shelf = [cl.get_boundary_map(km, df,"Age", 999) for km,df in zip(kmean_shelf_age_999, summary_monthly_999_for_clustering)]

kmean_shelf_kwh_tot_999 = [cl.clustering(f, "KWH_TOT", 5) for f in summary_monthly_999_for_clustering]
kwh_tot_score_map_shelf = [cl.get_boundary_map(km, df,"KWH_TOT", 999) for km,df in zip(kmean_shelf_kwh_tot_999, summary_monthly_999_for_clustering)]

summary_monthly_999_add_age_score = [cl.add_score_column(f,score_map, "Age", "age_score") for f, score_map in zip(summary_monthly_999_for_clustering, age_score_map_shelf)]
summary_monthly_999_add_usage_score = [cl.add_score_column(f,score_map, "KWH_TOT", "usage_score") for f, score_map in zip(summary_monthly_999_add_age_score, kwh_tot_score_map_shelf)]

summary_monthly_999_final = [zblr.sum_sec_dep(f) for f in summary_monthly_999_add_usage_score]

# zcanr076 data proseccing before merge
summary_monthly_076_final = [zcanr.change_column_name_076(f) for f in summary_monthly_076_final]

# merge zblr999 and zcanr076
merged_data = [zblr.merge_999_076(f1,f2) for f1,f2 in zip(summary_monthly_999_final, summary_monthly_076_final)]

is_merged_unique = [zblr.verify_unique_merged(f1,f2) for f1,f2 in zip(summary_monthly_999_final,merged_data)]

if sum(is_merged_unique) != len(is_merged_unique):
    print('error @verify_unique_merged function')
    exit(0)

# compute betrh-sec dep ratio and ratio score
merged_data_no_na = [f.fillna(0) for f in merged_data]
merged_data_final = [zblr.add_betrh_dep_ratio(f) for f in merged_data_no_na]

kmean_shelf_ratio = [cl.clustering(f, "betrh_dep_ratio", 5) for f in merged_data_final]
ratio_map_shelf = [cl.get_boundary_map(km, df,"betrh_dep_ratio", "merged") for km,df in zip(kmean_shelf_ratio, merged_data_final)]
merged_data_final_with_ratio = [cl.add_score_column(f,score_map, "betrh_dep_ratio", "ratio_score") for f, score_map in zip(merged_data_final, ratio_map_shelf)]

# compute customer score
customer_data = [cl.compute_customer_score(f) for f in merged_data_final_with_ratio]

customer_data_yearly = pd.concat(customer_data, axis=0)
customer_data_yearly_with_avg_customer_score = cl.add_avg_customer_score(customer_data_yearly)
customer_data_yearly_with_avg_customer_score = customer_data_yearly_with_avg_customer_score.drop(["BILL_PERIOD"], axis=1)

customer_data_yearly_with_avg_customer_score.to_csv('customer_scoring.csv', index=False, encoding='utf-8-sig')