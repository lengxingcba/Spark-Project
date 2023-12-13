import pandas as pd
import numpy as np

# test_data_X=pd.read_csv("C:/Users/lengxingcb/Desktop/机器学习算法专项/test_dataset_X.csv")
# droped_indexs=pd.read_csv("drop_indexs.csv")
# indexs=[]
# for i in droped_indexs.loc[:,"0"]:
#     indexs.append(i)
# test_data_X=test_data_X.drop(labels=["SMILES"],axis=1)
# test_data_X=test_data_X.drop(labels=indexs,axis=1)
# print(len(test_data_X.columns))
# #test_data_X.to_csv("test_data_X.csv")

data = pd.read_csv("C:/Users/lengxingcb/Desktop/机器学习算法专项/train_dataset_X.csv")

data = data.drop(labels="SMILES", axis=1)
data.loc["col_sum"] = data.apply(lambda x: x.sum())
for indexs in data.columns:
    if data.loc["col_sum", indexs] == 0:  # 去除全为0的列
        # indexs_train.append(indexs)
        data = data.drop(columns=indexs)
# print(len(data.columns))  #留下504组列数据

# data.drop_duplicates(keep='first',inplace=True) #进行去重操作
# 预测部分
data = data.drop(labels="col_sum")
data.to_csv("test_data_X.csv")
