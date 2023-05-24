import numpy as np
import copy
from sklearn.cluster import KMeans
from collections import *


# common function

def clustering(clustering_data, col_name, num_cluster):
    X = np.array(clustering_data[col_name].tolist())
    X = X.reshape((len(X),1))
    kmeans = KMeans(n_clusters=num_cluster, random_state=0, n_init=10).fit(X)
    return kmeans

#common function
def get_boundary_map(kmeans, df, col_name, code):
    """
        input:
            kmeans model (sklearn.cluster._kmeans.KMeans)
            df = data_076 corresponding to the its kmeans
        cluster_dict: key=cluster label, value = list of value in that cluster
        label_center_pairs: element = (cluster label, cluster center)
        min_max_pair_list: min_max_pair_list[i][0] = lower boundary, min_max_pair_list[i][1] = upper boundary
    """
    cluster_dict = defaultdict(list)
    X = df[col_name].tolist()
    assert len(kmeans.labels_) == len(X), "data length error @get_boundary_map function"
    for i in range(len(X)):
        cluster_dict[kmeans.labels_[i]].append(X[i])
        
    # sort cluster labels by their centers ascendingly
    label_center_pairs = [(label,cluster_center) for (label,cluster_center) in enumerate(kmeans.cluster_centers_)]
    label_center_pairs.sort(key=lambda x: x[1])
    
    # get original min max boundary from cluster information 
    original_boundary = [] # store min-max range of each cluster
    for pair in label_center_pairs:
        min_max = [min(cluster_dict[pair[0]]), max(cluster_dict[pair[0]])]
        original_boundary.append(min_max)
        
    updated_boundary = copy.deepcopy(original_boundary)
    
    # define new boundary
    for i in range(len(updated_boundary)):
        if i == 0:
            updated_boundary[i][0] = 1e-2
            updated_boundary[i][1] = (updated_boundary[i+1][0] + updated_boundary[i][1])/2 #(upper_bound[i] + lower_bound[i+1])/2   
        if i > 0 and i < len(updated_boundary)-1:
            updated_boundary[i][0] = updated_boundary[i-1][1] #equal the boundary
            updated_boundary[i][1] = (updated_boundary[i+1][0] + updated_boundary[i][1])/2 
        if i == len(updated_boundary) - 1:
            updated_boundary[i][0] = updated_boundary[i-1][1]
            updated_boundary[i][1] += 1e-5
            
    assert len(updated_boundary) == 5, "boundary list error @get_boundary_map function"

    boundary_score_map = {}
    for i in range(len(updated_boundary)):
        if code == 76 or code == "merged":
            boundary_score_map[tuple(updated_boundary[i])] = -1-i # negative score
        if code == 999:
            boundary_score_map[tuple(updated_boundary[i])] = 1+i # positive score
    return boundary_score_map

def get_score(x, score_map):
    for k,v in score_map.items():
        if x >= k[0] and x < k[1]:
            return v
        if x == 0 or x < 0.01:
            return 0

def add_score_column(df, score_map, input_col, new_col):
    input_df = df.copy()
    input_df[new_col] = input_df[input_col].apply(get_score, args=(score_map,))
    return input_df

def get_unique_clustering_076(df):
    input_df = df.copy()
    input_df = input_df.sort_values(by=['บัญชีแสดงสัญญา','BETRH','tag'], ascending = True)
    ca_list = input_df["บัญชีแสดงสัญญา"].tolist()
    late_scores = input_df["late_score"].tolist()
    amount_scores = input_df["amount_score"].tolist()
    betrh_list = input_df["BETRH"].tolist()
    ca_count_dict = Counter(ca_list)
    
    avg_late_scores = []
    avg_amount_scores = []
    avg_betrhs = []
    for i in range(len(ca_list)):
        if ca_count_dict[ca_list[i]] == 1:
            avg_late_scores.append(late_scores[i])
            avg_amount_scores.append(amount_scores[i])
            avg_betrhs.append(betrh_list[i])
            
        else:
            avg_late_score = np.mean(input_df[input_df["บัญชีแสดงสัญญา"] == ca_list[i]]["late_score"].tolist())
            avg_late_scores.append(avg_late_score)
            avg_amount_score = np.mean(input_df[input_df["บัญชีแสดงสัญญา"] == ca_list[i]]["amount_score"].tolist())
            avg_amount_scores.append(avg_amount_score)
            avg_betrh = np.mean(input_df[input_df["บัญชีแสดงสัญญา"] == ca_list[i]]["BETRH"].tolist())
            avg_betrhs.append(avg_betrh)
            
    result_df = input_df.copy()
    result_df["avg_betrh"] = avg_betrhs
    result_df["avg_late_score"] = avg_late_scores
    result_df["avg_amount_score"] = avg_amount_scores
    
    # drop tag column -> can drop duplicates -> get unique ca/row
    result_df = result_df.drop(['BETRH','tag','late_score','amount_score','อัตราค่าไฟ'], axis=1)
    result_df = result_df.drop_duplicates()
    return result_df

def compute_customer_score(df):
    input_df = df.copy()
    scores = []
    age_scores = input_df["age_score"].tolist()
    usage_scores = input_df["usage_score"].tolist()
    avg_late_scores = input_df["avg_late_score"].tolist()
    avg_amount_scores = input_df["avg_amount_score"].tolist()
    ratio_score = input_df["ratio_score"].tolist()
    
    for x1,x2,x3,x4,x5 in zip(age_scores,usage_scores,avg_late_scores,avg_amount_scores,ratio_score):
        score = 0.3*x1 + 0.7*x2 + 0.2*x3 + 0.3*x4 + 0.5*x5
        scores.append(score)
        
    input_df["customer_score"] = scores
    
    return input_df

def add_avg_customer_score(df):
    input_df = df.copy()
    ca_list = input_df["Contract Acc. M/I"].unique().tolist()    
    avg_customer_score_dict = {}
    for ca in ca_list:
        sub_df = input_df[input_df["Contract Acc. M/I"] == ca]
        avg_customer_score = np.mean(sub_df["customer_score"].tolist())
        avg_customer_score_dict[ca] = avg_customer_score
    
    input_df["avg_customer_score"] = input_df["Contract Acc. M/I"].map(avg_customer_score_dict)
    
    return input_df