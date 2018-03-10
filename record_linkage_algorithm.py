# -*- coding: utf-8 -*-
"""
Created on Feb 18 11:22:53 2017

@author: VictorO
"""

#importing the required libraries
import pandas as pd
import recordlinkage as rl
import os
import numpy as np

#Setting the working directory
os.chdir('M:\\Projects\\Record Linkage\\Python\\data\\')
#importing the data
dfA = pd.read_csv('for_linkage_data1.csv', sep=',', encoding='utf-8', index_col='dummy_id')

# Indexation step - Block Index
indexer = rl.BlockIndex(on='surname' and 'gender' and 'date_of_birth') #you can apply and or suppose you want to block with two variables
pairs = indexer.index(dfA)
#Printing the number of pairs creating from the blocking step
print(len(pairs))

#Doing comparision
compare_cl = rl.Compare(pairs, dfA, dfA)
compare_cl.string('firstname', 'firstname', method='jarowinkler', threshold=0.85, name='firstname')
compare_cl.string('surname', 'surname', method='jarowinkler', threshold=0.85, name='surname')
compare_cl.exact('initial', 'initial', name='initial')
compare_cl.exact('gender', 'gender', name='gender')
compare_cl.exact('dob_day', 'dob_day', name='dob_day')
compare_cl.exact('dob_month', 'dob_month', name='dob_month')
compare_cl.exact('dob_year', 'dob_year', name='dob_year')
compare_cl.exact('facility_code', 'facility_code', name='facility_code');

#Comparing vectors
compare_cl.vectors
compare_cl.vectors.describe()
# Sum the comparison results.
compare_cl.vectors.sum(axis=1).value_counts().sort_index(ascending=False)

#saving the vectors
data = compare_cl.vectors
#saving the vectors, so that I dont have to repeat the blocking and comparsion step
data.to_csv('data_vectors.csv', sep=',', encoding='utf-8')

#getting a sample of the dataframe
data_sample = data.take(np.random.permutation(len(data))[:2000000])

#tentetive matches
matches = dfA_sample[dfA_sample.sum(axis=1) > 6]
#tentetive matches
#nonmatches = data_sample[data_sample.sum(axis=1) < 4]
#creating match index
match_index = matches.index

#creating a training dataset
golden_pairs = data_sample[0:2000000]
golden_matches_index = golden_pairs.index & match_index

# Train the classifier
svm = rl.SVMClassifier()
svm.learn(golden_pairs, golden_matches_index)
# Predict the match status for all record pairs
result_svm = svm.predict(data)
len(result_svm)

#creating a confusion matrix
conf_svm = rl.confusion_matrix(match_index, result_svm, len(data))
conf_svm

# The F-score for this classification is
rl.fscore(conf_svm)

m_last = pd.DataFrame(result_svm)

#loading data for review
dfB = pd.read_csv('for_linkage_data1.csv', sep=',',encoding='utf-8')

#after review the dataframe m_last
#to review the matches
dfB.loc[['LID000000000','LID000157274','LID000217044','LID000491999','LID000558481','LID000589541']]
