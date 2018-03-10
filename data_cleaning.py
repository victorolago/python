# -*- coding: utf-8 -*-
"""
Created on Tue Feb 12 09:30:29 2017

@author: VictorO
"""

#Importing the libraries
import time
import os
import pandas as pd
import numpy as np

start = time.time()
#Setting the working directory
os.chdir('M:\\Projects\\Data Cleaning\\') 
#Reading the Raw_hiv_data dataset
dfA = pd.read_csv('data1.csv', sep='\t', encoding='utf-8') #Take note of the csv columns separation
#deleting dummy ID the previous dummy id
del dfA['dummy_id']

#creating the value for duplicates
dfA['is_duplicated'] = dfA.duplicated(['episode_no'])
#keeping the duplicates in a separate dataframe
#dups = dfA[(dfA['is_duplicated'] == True)]
del dfA['is_duplicated']
#droping duplicates based on the episode Number
dfA = dfA.drop_duplicates('episode_no', keep='last')

#Creating an auto increamental number    
dfA = dfA.assign(New_ID=[000000000 + i for i in range(len(dfA))]) 
#adding leading zeros in the ID created above
dfA['New_ID'] = dfA['New_ID'].apply(lambda x: '{0:0>9}'.format(x))
#Creating a column with a constant entry LID - Record Level Identifier
dfA['New_ID2'] = 'LID'  
#Creating a dummy ID
dfA['Dummy_ID'] = dfA['New_ID2'] + dfA['New_ID'].astype(str)
#droping the ID's creating in the process
del dfA['New_ID'], dfA['New_ID2']
#Converting the columns headers to lower case
dfA.columns = dfA.columns.str.lower()    

#getting a sample of the dataframe
dfA_sample = dfA.take(np.random.permutation(len(dfA))[:200])

#removing title at the end
dfA['patient_name_lis'] = dfA['patient_name_lis'].str.replace(r'(MISS)$', '')
dfA['patient_name_lis'] = dfA['patient_name_lis'].str.replace(r'(MRS)$', '')
dfA['patient_name_lis'] = dfA['patient_name_lis'].str.replace(r'(MR)$', '')
dfA['patient_name_lis'] = dfA['patient_name_lis'].str.replace(r'(MS)$', '')
dfA['patient_name_lis'] = dfA['patient_name_lis'].str.replace(r'(DR)$', '')
dfA['patient_name_lis'] = dfA['patient_name_lis'].str.replace(r'(ME)$', '')
#names that starts with titles
dfA['patient_name_lis'] = dfA['patient_name_lis'].str.replace(r'^(MISS )', '')
dfA['patient_name_lis'] = dfA['patient_name_lis'].str.replace(r'^(MISS,)', '')
dfA['patient_name_lis'] = dfA['patient_name_lis'].str.replace(r'^(MRS )', '')
dfA['patient_name_lis'] = dfA['patient_name_lis'].str.replace(r'^(MRS,)', '')
dfA['patient_name_lis'] = dfA['patient_name_lis'].str.replace(r'^(MR )', '')
dfA['patient_name_lis'] = dfA['patient_name_lis'].str.replace(r'^(MR,)', '')
dfA['patient_name_lis'] = dfA['patient_name_lis'].str.replace(r'^(DR )', '')
dfA['patient_name_lis'] = dfA['patient_name_lis'].str.replace(r'^(DR,)', '')
dfA['patient_name_lis'] = dfA['patient_name_lis'].str.replace(r'^(DR.)', '')
dfA['patient_name_lis'] = dfA['patient_name_lis'].str.replace(r'^(MD.)', '')
dfA['patient_name_lis'] = dfA['patient_name_lis'].str.replace(r'^(DR, )', '')
dfA['patient_name_lis'] = dfA['patient_name_lis'].str.replace(r'^(MS )', '')
dfA['patient_name_lis'] = dfA['patient_name_lis'].str.replace(r'^(MS,)', '')

#removing apostrophy
dfA["patient_name_lis"] = dfA["patient_name_lis"].str.replace("'","")
dfA["patient_name_lis"] = dfA["patient_name_lis"].str.replace("~","")
dfA["patient_name_lis"] = dfA["patient_name_lis"].str.replace("`","")
dfA["patient_name_lis"] = dfA["patient_name_lis"].str.replace(";","")
dfA["patient_name_lis"] = dfA["patient_name_lis"].str.replace("=","")
dfA["patient_name_lis"] = dfA["patient_name_lis"].str.replace("\?","")
dfA["patient_name_lis"] = dfA["patient_name_lis"].str.replace("\+","")
#dfA["patient_name_lis"] = dfA["patient_name_lis"].str.replace("-","")
dfA["patient_name_lis"] = dfA["patient_name_lis"].str.replace("<","")
dfA["patient_name_lis"] = dfA["patient_name_lis"].str.replace(" ,",",")
dfA["patient_name_lis"] = dfA["patient_name_lis"].str.replace(", ",",")
dfA["patient_name_lis"] = dfA["patient_name_lis"].str.replace("\.",",")
dfA["patient_name_lis"] = dfA["patient_name_lis"].str.replace(",,,,,,,,,",",")
dfA["patient_name_lis"] = dfA["patient_name_lis"].str.replace(",,,,,,,,",",")
dfA["patient_name_lis"] = dfA["patient_name_lis"].str.replace(",,,,,,,",",")
dfA["patient_name_lis"] = dfA["patient_name_lis"].str.replace(",,,,,,",",")
dfA["patient_name_lis"] = dfA["patient_name_lis"].str.replace(",,,,,",",")
dfA["patient_name_lis"] = dfA["patient_name_lis"].str.replace(",,,,",",")
dfA["patient_name_lis"] = dfA["patient_name_lis"].str.replace(",,,",",")
dfA["patient_name_lis"] = dfA["patient_name_lis"].str.replace(",,",",")

#removing more garbage
dfA = dfA[~dfA['patient_name_lis'].astype(str).str.contains(r'^(\d+)$')]
dfA = dfA[~dfA['patient_name_lis'].astype(str).str.contains(r'^(\d+,\d+)$')]
dfA = dfA[~dfA['patient_name_lis'].astype(str).str.contains(r'^(\d+ \d+)$')]
dfA = dfA[~dfA['patient_name_lis'].astype(str).str.contains(r'^(\:\d+)$')]
dfA = dfA[~dfA['patient_name_lis'].astype(str).str.contains(r'^(\:\d+ \d+)$')]
dfA = dfA[~dfA['patient_name_lis'].astype(str).str.contains(r'^(\w+\:\d+)$')]
dfA = dfA[~dfA['patient_name_lis'].astype(str).str.contains(r'^(' ')$')]

#Stripping the patient_name_lis 
dfA['patient_name_lis'] = dfA['patient_name_lis'].str.rstrip(',')
dfA['patient_name_lis'] = dfA['patient_name_lis'].str.lstrip(',')
dfA['patient_name_lis'] = dfA['patient_name_lis'].str.rstrip('-')
dfA['patient_name_lis'] = dfA['patient_name_lis'].str.lstrip('-')
dfA['patient_name_lis'] = dfA['patient_name_lis'].str.rstrip('\.')
dfA['patient_name_lis'] = dfA['patient_name_lis'].str.lstrip('\.')
dfA['patient_name_lis'] = dfA['patient_name_lis'].str.strip()

#sub setting the dfA just on name list
dfB = pd.concat([dfA['dummy_id'],dfA['patient_surname'],dfA['patient_name_lis'], dfA['patient_name_lis'].str.split(',', expand=True)], axis=1)

#renaming the columns
dfB.rename(columns={0:'surname', 1:'name1', 2:'name2', 3:'name3', 4:'name4', 5:'name5', 6:'name6',
                    7:'name7', 8:'name8'}, inplace=True)

#keeping the names with numbers
dfB_Digits = dfB[dfB['patient_name_lis'].astype(str).str.contains(r'\d+')]

#Removing names with Number
dfB = dfB[~dfB['patient_name_lis'].astype(str).str.contains(r'\d+')]
#removing names based on the structures of the regular expressions below
dfB = dfB[~dfB['patient_name_lis'].astype(str).str.contains(r'^(\w{1,3})$')]
dfB = dfB[~dfB['patient_name_lis'].astype(str).str.contains(r'^(\w{1,2} \w{1,2})$')]
dfB = dfB[~dfB['patient_name_lis'].astype(str).str.contains(r'^(\w \w \w,\w \w)$')]
dfB = dfB[~dfB['patient_name_lis'].astype(str).str.contains(r'^(\w,\w,\w)$')]

''' SubA'''
#subset of dfB
dfB_subA = pd.concat([dfB['dummy_id'], dfB['patient_surname'], dfB['patient_name_lis'], dfB['surname'],
                      dfB['name1'], dfB['name2'], dfB['name3'], dfB['name4'], dfB['name5'], dfB['name6'], 
                      dfB['name7'], dfB['name8']], axis = 1)
#getting name list with surname, initial and firstname1
dfB_subA['patient_name_lis'] = dfB['patient_name_lis'].str.extract(r'^(\w+,.,\w+)$')
#deleting the empty rows
dfB_subA.dropna(subset=['patient_name_lis'], inplace=True)
#creating the remainder of the dfB
dfB_remA = dfB[~dfB['dummy_id'].isin(dfB_subA['dummy_id'])]
#droping empty variables in dfB_subA
del dfB_subA['name3'], dfB_subA['name4'], dfB_subA['name5'], dfB_subA['name6'], dfB_subA['name7'], dfB_subA['name8'], dfB_subA['name1']
#renaming the columns
dfB_subA.rename(columns={'name2':'firstname1'}, inplace=True)
#initial
dfB_subA['initial'] = dfB_subA['firstname1'].str.extract(r'^(\w)')
#droping patient_name_lis1
del dfB_subA['patient_name_lis']

''' SubB'''
#subset of dfB
dfB_subB = pd.concat([dfB_remA['dummy_id'], dfB_remA['patient_surname'], dfB_remA['patient_name_lis'], dfB_remA['surname'],
                      dfB_remA['name1'], dfB_remA['name2'], dfB_remA['name3'], dfB_remA['name4'], dfB_remA['name5'], 
                      dfB_remA['name6'], dfB_remA['name7'], dfB_remA['name8']], axis = 1)
#getting name list with surname, initial and firstname1
dfB_subB['patient_name_lis'] = dfB['patient_name_lis'].str.extract(r'^(\w+,\w{2,2},\w+)$')
#deleting the empty rows
dfB_subB.dropna(subset=['patient_name_lis'], inplace=True)
#creating the remainder
dfB_remB = dfB_remA[~dfB_remA['dummy_id'].isin(dfB_subB['dummy_id'])]
#droping empty variables in dfB_subA
del dfB_subB['name3'], dfB_subB['name4'], dfB_subB['name5'], dfB_subB['name1'], dfB_subB['name6'], dfB_subB['name7'], dfB_subB['name8']
#renaming the columns
dfB_subB.rename(columns={'name2':'firstname1'}, inplace=True)
#initial
dfB_subB['initial'] = dfB_subB['firstname1'].str.extract(r'^(\w)')
#droping patient_name_lis1
del dfB_subB['patient_name_lis']

''' SubC'''
#subset of dfB_subB
dfB_subC = pd.concat([dfB_remB['dummy_id'], dfB_remB['patient_surname'], dfB_remB['patient_name_lis'], 
                      dfB_remB['surname'], dfB_remB['name1'], dfB_remB['name2'], dfB_remB['name3'], dfB_remB['name8'],
                      dfB_remB['name4'], dfB_remB['name5'], dfB_remB['name6'], dfB_remB['name7']], axis = 1)
#getting name list with surname, initial, initial, firstname1 and firstname2
dfB_subC['patient_name_lis'] = dfB_remB['patient_name_lis'].str.extract(r'^(\w+,.,.,\w{2,},\w+)$')
#deleting the empty rows
dfB_subC.dropna(subset=['patient_name_lis'], inplace=True)
#creating the remainder
dfB_remC = dfB_remB[~dfB_remB['dummy_id'].isin(dfB_subC['dummy_id'])]
#droping empty variables in dfB_subA
del dfB_subC['name2'], dfB_subC['name5'], dfB_subC['name1'], dfB_subC['name6'], dfB_subC['name7'], dfB_subC['name8']
#renaming the columns
dfB_subC.rename(columns={'name3':'firstname1', 'name4':'firstname2'}, inplace=True)
#initial
dfB_subC['initial'] = dfB_subC['firstname1'].str.extract(r'^(\w)')
#droping patient_name_lis1
del dfB_subC['patient_name_lis']

''' SubD'''
#subset of dfB_subB
dfB_subD = pd.concat([dfB_remC['dummy_id'], dfB_remC['patient_surname'], dfB_remC['patient_name_lis'], 
                      dfB_remC['surname'], dfB_remC['name1'], dfB_remC['name2'], dfB_remC['name3'], 
                      dfB_remC['name4'], dfB_remC['name5']], axis = 1)
#getting name list with surname, and firstname1
dfB_subD['patient_name_lis'] = dfB_remC['patient_name_lis'].str.extract(r'^(\w+,\w+)$')
#deleting the empty rows
dfB_subD.dropna(subset=['patient_name_lis'], inplace=True)
#creating the remainder
dfB_remD = dfB_remC[~dfB_remC['dummy_id'].isin(dfB_subD['dummy_id'])]
#droping empty variables in dfB_subA
del dfB_subD["name2"], dfB_subD['name3'], dfB_subD['name4'], dfB_subD['name5']
#renaming the columns
dfB_subD.rename(columns={'name1':'firstname1'}, inplace=True)
#getting the initials only in the intial column
dfB_subD['initial'] = dfB_subD['firstname1'].str.extract(r'^(\w)')
#droping patient_name_lis1
del dfB_subD['patient_name_lis']

''' SubE'''
#subset of dfB_subB
dfB_subE = pd.concat([dfB_remD['dummy_id'], dfB_remD['patient_surname'], dfB_remD['patient_name_lis'], 
                      dfB_remD['surname'], dfB_remD['name1'], dfB_remD['name2'], dfB_remD['name3'], 
                      dfB_remD['name4'], dfB_remD['name5']], axis = 1)
#getting name list with surname, two word initial, firstname1
dfB_subE['patient_name_lis'] = dfB_remD['patient_name_lis'].str.extract(r'^(\w+,.,.,\w+)$')
#deleting the empty rows
dfB_subE.dropna(subset=['patient_name_lis'], inplace=True)
#creating the remainder
dfB_remE = dfB_remD[~dfB_remD['dummy_id'].isin(dfB_subE['dummy_id'])]
#droping empty variables in dfB_subA
del dfB_subE['name1'], dfB_subE['name2'], dfB_subE['name4'], dfB_subE['name5']
#renaming the columns
dfB_subE.rename(columns={'name3':'firstname1'}, inplace=True)
#initial
dfB_subE['initial'] = dfB_subE['firstname1'].str.extract(r'^(\w)')
#droping patient_name_lis1
del dfB_subE['patient_name_lis']

''' SubF'''
#subset of dfB_subB
dfB_subF = pd.concat([dfB_remE['dummy_id'], dfB_remE['patient_surname'], dfB_remE['patient_name_lis'], 
                      dfB_remE['surname'], dfB_remE['name1'], dfB_remE['name2'], dfB_remE['name3'], 
                      dfB_remE['name4'], dfB_remE['name5']], axis = 1)
#getting name list with surname, initial, initial, and firstname1
dfB_subF['patient_name_lis'] = dfB_remE['patient_name_lis'].str.extract(r'^(\w+,.,\w+,\w+)$')
#deleting the empty rows
dfB_subF.dropna(subset=['patient_name_lis'], inplace=True)
#creating the remainder
dfB_remF = dfB_remE[~dfB_remE['dummy_id'].isin(dfB_subF['dummy_id'])]
#droping empty variables in dfB_subA
del dfB_subF['name4'], dfB_subF['name5']
#renaming the columns
dfB_subF.rename(columns={'name1':'initial','name2':'firstname1','name3':'firstname2'}, inplace=True)
#removing digits in the firstname2
dfB_subF['firstname2'] = dfB_subF['firstname2'].str.replace(r'\d+', '')
#droping patient_name_lis1
del dfB_subF['patient_name_lis']

''' SubG'''
#subset of dfB_subB
dfB_subG = pd.concat([dfB_remF['dummy_id'], dfB_remF['patient_surname'], dfB_remF['patient_name_lis'], 
                      dfB_remF['surname'], dfB_remF['name1'], dfB_remF['name2'], dfB_remF['name3'], 
                      dfB_remF['name4'], dfB_remF['name5']], axis = 1)
#getting name list with surname, initial, initial, firstname1 and firstname2
dfB_subG['patient_name_lis'] = dfB_remF['patient_name_lis'].str.extract(r'^(\w+,\w{2,},\w+)$')
#deleting the empty rows
dfB_subG.dropna(subset=['patient_name_lis'], inplace=True)
#creating the remainder
dfB_remG = dfB_remF[~dfB_remF['dummy_id'].isin(dfB_subG['dummy_id'])]
#droping empty variables in dfB_subA
del dfB_subG['name3'], dfB_subG['name4'], dfB_subG['name5']
#renaming the columns
dfB_subG.rename(columns={'name2':'firstname2','name1':'firstname1'}, inplace=True)
#initial
dfB_subG['initial'] = dfB_subG['firstname1'].str.extract(r'^(\w)')
#droping patient_name_lis1
del dfB_subG['patient_name_lis']

''' SubH'''
#subset of dfB_subB
dfB_subH = pd.concat([dfB_remG['dummy_id'], dfB_remG['patient_surname'], dfB_remG['patient_name_lis'], 
                      dfB_remG['surname'], dfB_remG['name1'], dfB_remG['name2'], dfB_remG['name3'], 
                      dfB_remG['name4'], dfB_remG['name5']], axis = 1)
#getting name list with surname, initial, initial, firstname1 and firstname2
dfB_subH['patient_name_lis'] = dfB_remG['patient_name_lis'].str.extract(r'^(\w+,\w{2,2},.,\w+)$')
#deleting the empty rows
dfB_subH.dropna(subset=['patient_name_lis'], inplace=True)
#creating the remainder
dfB_remH = dfB_remG[~dfB_remG['dummy_id'].isin(dfB_subH['dummy_id'])]
#droping empty variables in dfB_subA
del dfB_subH['name1'], dfB_subH['name4'], dfB_subH['name5']
#renaming the columns
dfB_subH.rename(columns={'name3':'firstname1', 'name2':'initial'}, inplace=True)
#droping patient_name_lis1
del dfB_subH['patient_name_lis']


#subset of dfB_subB
dfB_subI = pd.concat([dfB_remH['dummy_id'], dfB_remH['patient_surname'], dfB_remH['patient_name_lis'], 
                      dfB_remH['surname'], dfB_remH['name1'], dfB_remH['name2'], dfB_remH['name3'], 
                      dfB_remH['name4'], dfB_remH['name5']], axis = 1)
#getting name list with surname, initial, initial, firstname1 and firstname2
dfB_subI['patient_name_lis'] = dfB_remH['patient_name_lis'].str.extract(r'^(\w+,. \w+)$')
#deleting the empty rows
dfB_subI.dropna(subset=['patient_name_lis'], inplace=True)
#creating the remainder
dfB_remI = dfB_remH[~dfB_remH['dummy_id'].isin(dfB_subI['dummy_id'])]
#spliting the name1 on spaces
dfB_subI = pd.concat([dfB_subI['dummy_id'],dfB_subI['patient_surname'],dfB_subI['patient_name_lis'], 
                      dfB_subI['surname'],dfB_subI['name1'].str.split(' ', expand=True)], axis=1)
#renaming the columns
dfB_subI.rename(columns={1:'firstname1'}, inplace=True)
#initials
dfB_subI['initial'] = dfB_subI['firstname1'].str.extract(r'^(\w)')
#droping patient_name_lis1
del dfB_subI['patient_name_lis'], dfB_subI[0]


''' SubJ'''
#subset of dfB_subB
dfB_subJ = pd.concat([dfB_remI['dummy_id'], dfB_remI['patient_surname'], dfB_remI['patient_name_lis'], 
                      dfB_remI['surname'], dfB_remI['name1'], dfB_remI['name2'], dfB_remI['name3'], 
                      dfB_remI['name4'], dfB_remI['name5']], axis = 1)

#getting name list with surname, initial, initial, firstname1 and firstname2
dfB_subJ['patient_name_lis'] = dfB_remI['patient_name_lis'].str.extract(r'^(\w+,\w{2,2},\w+ \w+)$')
#deleting the empty rows
dfB_subJ.dropna(subset=['patient_name_lis'], inplace=True)
#creating the remainder
dfB_remJ = dfB_remI[~dfB_remI['dummy_id'].isin(dfB_subJ['dummy_id'])]
#spliting the name1 on spaces
dfB_subJ = pd.concat([dfB_subJ['dummy_id'],dfB_subJ['patient_surname'],dfB_subJ['patient_name_lis'], 
                      dfB_subJ['surname'],dfB_subJ['name2'].str.split(' ', expand=True)], axis=1)
#renaming the columns
dfB_subJ.rename(columns={1:'firstname2', 0:'firstname1'}, inplace=True)
#initials
dfB_subJ['initial'] = dfB_subJ['firstname1'].str.extract(r'^(\w)')
#droping patient_name_lis1
del dfB_subJ['patient_name_lis']


''' SubK'''
#subset of dfB_subB
dfB_subK = pd.concat([dfB_remJ['dummy_id'], dfB_remJ['patient_surname'], dfB_remJ['patient_name_lis'], 
                      dfB_remJ['surname'], dfB_remJ['name1'], dfB_remJ['name2'], dfB_remJ['name3'], 
                      dfB_remJ['name4'], dfB_remJ['name5']], axis = 1)
#getting name list with surname, initial, initial, firstname1 and firstname2
dfB_subK['patient_name_lis'] = dfB_remJ['patient_name_lis'].str.extract(r'^(\w+ .,\w,\w+)$')
#deleting the empty rows
dfB_subK.dropna(subset=['patient_name_lis'], inplace=True)
#creating the remainder
dfB_remK = dfB_remJ[~dfB_remJ['dummy_id'].isin(dfB_subK['dummy_id'])]
#spliting the name1 on spaces
dfB_subK = pd.concat([dfB_subK['dummy_id'],dfB_subK['patient_surname'],dfB_subK['patient_name_lis'], 
                      dfB_subK['name2'],dfB_subK['surname'].str.split(' ', expand=True)], axis=1)
#renaming the columns
dfB_subK.rename(columns={'name2':'firstname1', 0:'surname'}, inplace=True)
#initials
dfB_subK['initial'] = dfB_subK['firstname1'].str.extract(r'^(\w)')
#droping patient_name_lis1
del dfB_subK['patient_name_lis'], dfB_subK[1]


''' SubL'''
#subset of dfB_subB
dfB_subL = pd.concat([dfB_remK['dummy_id'], dfB_remK['patient_surname'], dfB_remK['patient_name_lis'], 
                      dfB_remK['surname'], dfB_remK['name1'], dfB_remK['name2'], dfB_remK['name3'], 
                      dfB_remK['name4'], dfB_remK['name5']], axis = 1)
#getting name list with surname, initial, initial, firstname1 and firstname2
dfB_subL['patient_name_lis'] = dfB_remK['patient_name_lis'].str.extract(r'^(\w+,.,\w{2,} \w+)$')
#deleting the empty rows
dfB_subL.dropna(subset=['patient_name_lis'], inplace=True)
#creating the remainder
dfB_remL = dfB_remK[~dfB_remK['dummy_id'].isin(dfB_subL['dummy_id'])]
#splitting name2
dfB_subL = pd.concat([dfB_subL['dummy_id'],dfB_subL['patient_surname'],dfB_subL['patient_name_lis'], dfB_subL['surname'],
                      dfB_subL['name1'], dfB_subL['name2'].str.split(' ', expand=True)], axis=1)
#renaming the columns
dfB_subL.rename(columns={'name1':'initial',0:'firstname1', 1:'firstname2'}, inplace=True)
#droping patient_name_lis1
del dfB_subL['patient_name_lis']


''' SubM'''
#subset of dfB_subB
dfB_subM = pd.concat([dfB_remL['dummy_id'], dfB_remL['patient_surname'], dfB_remL['patient_name_lis'], 
                      dfB_remL['surname'], dfB_remL['name1'], dfB_remL['name2'], dfB_remL['name3'], 
                      dfB_remL['name4'], dfB_remL['name5']], axis = 1)
#getting name list with surname, initial, initial, firstname1 and firstname2
dfB_subM['patient_name_lis'] = dfB_remL['patient_name_lis'].str.extract(r'^(\w+ .,\w+)$')
#deleting the empty rows
dfB_subM.dropna(subset=['patient_name_lis'], inplace=True)
#creating the remainder
dfB_remM = dfB_remL[~dfB_remL['dummy_id'].isin(dfB_subM['dummy_id'])]
#splitting name2
dfB_subM = pd.concat([dfB_subM['dummy_id'],dfB_subM['patient_surname'],dfB_subM['patient_name_lis'], dfB_subM['name1'],
                      dfB_subM['surname'].str.split(' ', expand=True)], axis=1)
#renaming the columns
dfB_subM.rename(columns={'name1':'firstname1',0:'surname',1:'initial'}, inplace=True)
#droping patient_name_lis1
del dfB_subM['patient_name_lis']


''' SubN'''
#subset of dfB_subB
dfB_subN = pd.concat([dfB_remM['dummy_id'], dfB_remM['patient_surname'], dfB_remM['patient_name_lis'], 
                      dfB_remM['surname'], dfB_remM['name1'], dfB_remM['name2'], dfB_remM['name3'], 
                      dfB_remM['name4'], dfB_remM['name5']], axis = 1)
#getting name list with surname, initial, initial, firstname1 and firstname2
dfB_subN['patient_name_lis'] = dfB_remM['patient_name_lis'].str.extract(r'^(\w+ .,\w,\w{2,},\w+)$')
#deleting the empty rows
dfB_subN.dropna(subset=['patient_name_lis'], inplace=True)
#creating the remainder
dfB_remN = dfB_remM[~dfB_remM['dummy_id'].isin(dfB_subN['dummy_id'])]
#split name list
dfB_subN = pd.concat([dfB_subN['dummy_id'],dfB_subN['patient_surname'],dfB_subN['name2'], dfB_subN['name3'],
                      dfB_subN['surname'].str.split(' ', expand=True)], axis=1)
#renaming the columns
dfB_subN.rename(columns={'name2':'firstname1','name3':'firstname2',0:'surname'}, inplace=True)
#initials
dfB_subN['initial'] = dfB_subN['firstname1'].str.extract(r'^(\w)')
#droping patient_name_lis1
del dfB_subN[1]

''' SubO'''
#subset of dfB_subB
dfB_subO = pd.concat([dfB_remN['dummy_id'], dfB_remN['patient_surname'], dfB_remN['patient_name_lis'], 
                      dfB_remN['surname'], dfB_remN['name1'], dfB_remN['name2'], dfB_remN['name3'], 
                      dfB_remN['name4'], dfB_remN['name5']], axis = 1)
#getting name list with surname, initial, initial, firstname1 and firstname2
dfB_subO['patient_name_lis'] = dfB_remN['patient_name_lis'].str.extract(r'^(\w+)$')
#deleting the empty rows
dfB_subO.dropna(subset=['patient_name_lis'], inplace=True)
#creating the remainder
dfB_remO = dfB_remN[~dfB_remN['dummy_id'].isin(dfB_subO['dummy_id'])]
#droping empty variable
del dfB_subO['name1'], dfB_subO['name2'], dfB_subO['name3'], dfB_subO['name4'], dfB_subO['name5']
#droping patient_name_lis1
del dfB_subO['patient_name_lis']

''' SubP'''
#subset of dfB_subB
dfB_subP = pd.concat([dfB_remO['dummy_id'], dfB_remO['patient_surname'], dfB_remO['patient_name_lis'], 
                      dfB_remO['surname'], dfB_remO['name1'], dfB_remO['name2'], dfB_remO['name3'], 
                      dfB_remO['name4'], dfB_remO['name5']], axis = 1)
#getting name list with surname, initial, initial, firstname1 and firstname2
dfB_subP['patient_name_lis'] = dfB_remO['patient_name_lis'].str.extract(r'^(\w{2,} \w{2,} \w{2,})$')
#deleting the empty rows
dfB_subP.dropna(subset=['patient_name_lis'], inplace=True)
#creating the remainder
dfB_remP = dfB_remO[~dfB_remO['dummy_id'].isin(dfB_subP['dummy_id'])]
#splitting name2
dfB_subP = pd.concat([dfB_subP['dummy_id'],dfB_subP['patient_surname'],
                      dfB_subP['surname'].str.split(' ', expand=True)], axis=1)
#renaming the columns
dfB_subP.rename(columns={1:'firstname1',0:'surname',2:'firstname2'}, inplace=True)
#initials
dfB_subP['initial'] = dfB_subP['firstname1'].str.extract(r'^(\w)')


''' SubQ'''
#subset of dfB_subB
dfB_subQ = pd.concat([dfB_remP['dummy_id'], dfB_remP['patient_surname'], dfB_remP['patient_name_lis'], 
                      dfB_remP['surname'], dfB_remP['name1'], dfB_remP['name2'], dfB_remP['name3'], 
                      dfB_remP['name4'], dfB_remP['name5']], axis = 1)
#getting name list with surname, initial, initial, firstname1 and firstname2
dfB_subQ['patient_name_lis'] = dfB_remP['patient_name_lis'].str.extract(r'^(\w+,.,.,.,\w+)$')
#deleting the empty rows
dfB_subQ.dropna(subset=['patient_name_lis'], inplace=True)
#creating the remainder
dfB_remQ = dfB_remP[~dfB_remP['dummy_id'].isin(dfB_subQ['dummy_id'])]
#droping empty variable
del dfB_subQ['name3'], dfB_subQ['name2'], dfB_subQ['name5'], dfB_subQ['name1']
#renaming the columns
dfB_subQ.rename(columns={'name4':'firstname1'}, inplace=True)
#remving space in the surname
#dfB_subQ["surname"] = dfB_subQ["surname"].str.replace("-","")
#initials
dfB_subQ['initial'] = dfB_subQ['firstname1'].str.extract(r'^(\w)')
#droping patient_name_lis1
del dfB_subQ['patient_name_lis']

''' SubR'''
#subset of dfB_subB
dfB_subR = pd.concat([dfB_remQ['dummy_id'], dfB_remQ['patient_surname'], dfB_remQ['patient_name_lis'], 
                      dfB_remQ['surname'], dfB_remQ['name1'], dfB_remQ['name2'], dfB_remQ['name3'], 
                      dfB_remQ['name4'], dfB_remQ['name5']], axis = 1)
#getting name list with surname, initial, initial, firstname1 and firstname2
dfB_subR['patient_name_lis'] = dfB_remQ['patient_name_lis'].str.extract(r'^(\w+ \w+)$')
#deleting the empty rows
dfB_subR.dropna(subset=['patient_name_lis'], inplace=True)
#creating the remainder
dfB_remR = dfB_remQ[~dfB_remQ['dummy_id'].isin(dfB_subR['dummy_id'])]
#split name list
dfB_subR = pd.concat([dfB_subR['dummy_id'],dfB_subR['patient_surname'],
                      dfB_subR['patient_name_lis'].str.split(' ', expand=True)], axis=1)
#renaming the columns
dfB_subR.rename(columns={0:'surname',1:'firstname1'}, inplace=True)
#initials
dfB_subR['initial'] = dfB_subR['firstname1'].str.extract(r'^(\w)')

''' SubS'''
#subset of dfB_subB
dfB_subS = pd.concat([dfB_remR['dummy_id'], dfB_remR['patient_surname'], dfB_remR['patient_name_lis'], 
                      dfB_remR['surname'], dfB_remR['name1'], dfB_remR['name2'], dfB_remR['name3'], 
                      dfB_remR['name4'], dfB_remR['name5']], axis = 1)
#getting name list with surname, initial, initial, firstname1 and firstname2
dfB_subS['patient_name_lis'] = dfB_remR['patient_name_lis'].str.extract(r'^(\w+,\w{2,2},\w+,.,\w+)$')
#deleting the empty rows
dfB_subS.dropna(subset=['patient_name_lis'], inplace=True)
#creating the remainder
dfB_remS = dfB_remR[~dfB_remR['dummy_id'].isin(dfB_subS['dummy_id'])]
#droping empty variable
del dfB_subS['name1'], dfB_subS['name3'], dfB_subS['name5']
#renaming the columns
dfB_subS.rename(columns={'name2':'firstname1','name4':'firstname2'}, inplace=True)
#initials
dfB_subS['initial'] = dfB_subS['firstname1'].str.extract(r'^(\w)')
#droping patient_name_lis1
del dfB_subS['patient_name_lis']

''' SubT'''
#subset of dfB_subB
dfB_subT = pd.concat([dfB_remS['dummy_id'], dfB_remS['patient_surname'], dfB_remS['patient_name_lis'], 
                      dfB_remS['surname'], dfB_remS['name1'], dfB_remS['name2'], dfB_remS['name3'], 
                      dfB_remS['name4'], dfB_remS['name5']], axis = 1)
#getting name list with surname, initial, initial, firstname1 and firstname2
dfB_subT['patient_name_lis'] = dfB_remS['patient_name_lis'].str.extract(r'^(\w{2,3} \w+,.,\w+)$')
#deleting the empty rows
dfB_subT.dropna(subset=['patient_name_lis'], inplace=True)
#creating the remainder
dfB_remT = dfB_remS[~dfB_remS['dummy_id'].isin(dfB_subT['dummy_id'])]
#droping empty variable
del dfB_subT['name1'], dfB_subT['name3'], dfB_subT['name4'], dfB_subT['name5']
#renaming the columns
dfB_subT.rename(columns={'name2':'firstname1'}, inplace=True)
#initials
dfB_subT['initial'] = dfB_subT['firstname1'].str.extract(r'^(\w)')
#remving space in the surname
dfB_subT["surname"] = dfB_subT["surname"].str.replace(" ","")
#droping patient_name_lis1
del dfB_subT['patient_name_lis']

''' SubU'''
#subset of dfB_subB
dfB_subU = pd.concat([dfB_remT['dummy_id'], dfB_remT['patient_surname'], dfB_remT['patient_name_lis'], 
                      dfB_remT['surname'], dfB_remT['name1'], dfB_remT['name2'], dfB_remT['name3'], 
                      dfB_remT['name4'], dfB_remT['name5']], axis = 1)
#getting name list with surname, initial, initial, firstname1 and firstname2
dfB_subU['patient_name_lis'] = dfB_remT['patient_name_lis'].str.extract(r'^(\w{2,},\w{3,3},\w{2,} \w{2,} \w{2,})$')
#deleting the empty rows
dfB_subU.dropna(subset=['patient_name_lis'], inplace=True)
#creating the remainder
dfB_remU = dfB_remT[~dfB_remT['dummy_id'].isin(dfB_subU['dummy_id'])]
#splitting name 1
dfB_subU = pd.concat([dfB_subU['dummy_id'],dfB_subU['patient_surname'], dfB_subU['surname'],
                      dfB_subU['name2'].str.split(' ', expand=True)], axis=1)
#renaming the columns
dfB_subU.rename(columns={1:'firstname2', 0:'firstname1',2:'firstname3'}, inplace=True)
#initials
dfB_subU['initial'] = dfB_subU['firstname1'].str.extract(r'^(\w)')

''' SubV'''
#subset of dfB_subB
dfB_subV = pd.concat([dfB_remU['dummy_id'], dfB_remU['patient_surname'], dfB_remU['patient_name_lis'], 
                      dfB_remU['surname'], dfB_remU['name1'], dfB_remU['name2'], dfB_remU['name3'], 
                      dfB_remU['name4'], dfB_remU['name5']], axis = 1)
#getting name list with surname, initial, initial, firstname1 and firstname2
dfB_subV['patient_name_lis'] = dfB_remU['patient_name_lis'].str.extract(r'^(\w{2,3} \w{2,3} \w+,.,\w+-\w+)$')
#deleting the empty rows
dfB_subV.dropna(subset=['patient_name_lis'], inplace=True)
#creating the remainder
dfB_remV = dfB_remU[~dfB_remU['dummy_id'].isin(dfB_subV['dummy_id'])]
#droping empty variable
del dfB_subV['name3'], dfB_subV['name4'], dfB_subV['name5']
#renaming the columns
dfB_subV.rename(columns={'name1':'initial','name2':'firstname1'}, inplace=True)
#remving space in the surname
dfB_subV["surname"] = dfB_subV["surname"].str.replace(" ","")
#droping patient_name_lis1
del dfB_subV['patient_name_lis']

''' SubW'''
#subset of dfB_subB
dfB_subW = pd.concat([dfB_remV['dummy_id'], dfB_remV['patient_surname'], dfB_remV['patient_name_lis'], 
                      dfB_remV['surname'], dfB_remV['name1'], dfB_remV['name2'], dfB_remI['name3'], 
                      dfB_remV['name4'], dfB_remV['name5']], axis = 1)
#getting name list with surname, initial, initial, firstname1 and firstname2
dfB_subW['patient_name_lis'] = dfB_remV['patient_name_lis'].str.extract(r'^(\w+,.,\w{2,},.,\w+)$')
#deleting the empty rows
dfB_subW.dropna(subset=['patient_name_lis'], inplace=True)
#creating the remainder
dfB_remW = dfB_remV[~dfB_remV['dummy_id'].isin(dfB_subW['dummy_id'])]
#droping empty variable
del dfB_subW['name3'], dfB_subW['name5'], dfB_subW['name1']
#renaming the columns
dfB_subW.rename(columns={'name2':'firstname1','name4':'firstname2'}, inplace=True)
#initials
dfB_subW['initial'] = dfB_subW['firstname1'].str.extract(r'^(\w)')
#droping patient_name_lis1
del dfB_subW['patient_name_lis']

''' SubX'''
#subset of dfB_subB
dfB_subX = pd.concat([dfB_remW['dummy_id'], dfB_remW['patient_surname'], dfB_remW['patient_name_lis'], 
                      dfB_remW['surname'], dfB_remW['name1'], dfB_remW['name2'], dfB_remW['name3'], 
                      dfB_remW['name4'], dfB_remW['name5']], axis = 1)
#getting name list with surname, initial, initial, firstname1 and firstname2
dfB_subX['patient_name_lis'] = dfB_remW['patient_name_lis'].str.extract(r'^(\w{2,},\w{2,2},\w{2,} \w{2,} \w{2,})$')
#deleting the empty rows
dfB_subX.dropna(subset=['patient_name_lis'], inplace=True)
#creating the remainder
dfB_remX = dfB_remW[~dfB_remW['dummy_id'].isin(dfB_subX['dummy_id'])]
#splitting name 1
dfB_subX = pd.concat([dfB_subX['dummy_id'],dfB_subX['patient_surname'], dfB_subX['surname'],
                      dfB_subX['name2'].str.split(' ', expand=True)], axis=1)
#renaming the columns
dfB_subX.rename(columns={1:'firstname2', 0:'firstname1',2:'firstname3'}, inplace=True)
#initials
dfB_subX['initial'] = dfB_subX['firstname1'].str.extract(r'^(\w)')


''' SubY'''
#subset of dfB_subB
dfB_subY = pd.concat([dfB_remX['dummy_id'], dfB_remX['patient_surname'], dfB_remX['patient_name_lis'], 
                      dfB_remX['surname'], dfB_remX['name1'], dfB_remX['name2'], dfB_remX['name3'], 
                      dfB_remX['name4'], dfB_remX['name5']], axis = 1)
#getting name list with surname, initial, initial, firstname1 and firstname2
dfB_subY['patient_name_lis'] = dfB_remX['patient_name_lis'].str.extract(r'^(\w+,.,.,\w+ \w+)$')
#deleting the empty rows
dfB_subY.dropna(subset=['patient_name_lis'], inplace=True)
#creating the remainder
dfB_remY = dfB_remX[~dfB_remX['dummy_id'].isin(dfB_subY['dummy_id'])]
#splitting name 3
dfB_subY = pd.concat([dfB_subY['dummy_id'],dfB_subY['patient_surname'],dfB_subY['patient_name_lis'], dfB_subY['surname'],
                      dfB_subY['name3'].str.split(' ', expand=True)], axis=1)
#renaming the columns
dfB_subY.rename(columns={0:'firstname1', 1:'firstname2'}, inplace=True)
#droping patient_name_lis1
del dfB_subY['patient_name_lis']
#initials
dfB_subY['initial'] = dfB_subY['firstname1'].str.extract(r'^(\w)')

''' SubZ'''
#subset of dfB_subB
dfB_subZ = pd.concat([dfB_remY['dummy_id'], dfB_remY['patient_surname'], dfB_remY['patient_name_lis'], 
                      dfB_remY['surname'], dfB_remY['name1'], dfB_remY['name2'], dfB_remY['name3'], 
                      dfB_remY['name4'], dfB_remY['name5']], axis = 1)
#getting name list with surname, initial, initial, firstname1 and firstname2
dfB_subZ['patient_name_lis'] = dfB_remY['patient_name_lis'].str.extract(r'^(\w+,\w{2,2} \w+ \w+)$')
#deleting the empty rows
dfB_subZ.dropna(subset=['patient_name_lis'], inplace=True)
#creating the remainder
dfB_remZ = dfB_remY[~dfB_remY['dummy_id'].isin(dfB_subZ['dummy_id'])]
#splitting name 3
dfB_subZ = pd.concat([dfB_subZ['dummy_id'],dfB_subZ['patient_surname'],dfB_subZ['patient_name_lis'], dfB_subZ['surname'],
                      dfB_subZ['name1'].str.split(' ', expand=True)], axis=1)
#renaming the columns
dfB_subZ.rename(columns={1:'firstname1',2:'firstname2'}, inplace=True)
#initials
dfB_subZ['initial'] = dfB_subZ['firstname1'].str.extract(r'^(\w)')
#droping patient_name_lis1
del dfB_subZ['patient_name_lis'], dfB_subZ[0]

''' SubZA'''
#subset of dfB_subB
dfB_subZA = pd.concat([dfB_remZ['dummy_id'], dfB_remZ['patient_surname'], dfB_remZ['patient_name_lis'], 
                      dfB_remZ['surname'], dfB_remZ['name1'], dfB_remZ['name2'], dfB_remZ['name3'], 
                      dfB_remZ['name4'], dfB_remZ['name5']], axis = 1)
#getting name list with surname, initial, initial, firstname1 and firstname2
dfB_subZA['patient_name_lis'] = dfB_remZ['patient_name_lis'].str.extract(r'^(\w+,\w+ .)$')
#deleting the empty rows
dfB_subZA.dropna(subset=['patient_name_lis'], inplace=True)
#creating the remainder
dfB_remZA = dfB_remZ[~dfB_remZ['dummy_id'].isin(dfB_subZA['dummy_id'])]
#droping empty variable
del dfB_subZA['name3'], dfB_subZA['name5'], dfB_subZA['name2'], dfB_subZA['name4']
#renaming the columns
dfB_subZA.rename(columns={'name1':'firstname1'}, inplace=True)
#initials
dfB_subZA['initial'] = dfB_subZA['firstname1'].str.extract(r'^(\w)')
#droping patient_name_lis1
del dfB_subZA['patient_name_lis']
#remving space in the surname
dfB_subZA["firstname1"] = dfB_subZA["firstname1"].str.replace(" ","")

''' SubZB'''
#subset of dfB_subB
dfB_subZB = pd.concat([dfB_remZA['dummy_id'], dfB_remZA['patient_surname'], dfB_remZA['patient_name_lis'], 
                      dfB_remZA['surname'], dfB_remZA['name1'], dfB_remZA['name2'], dfB_remZA['name3'], 
                      dfB_remZA['name4'], dfB_remZA['name5']], axis = 1)
#getting name list with surname, initial, initial, firstname1 and firstname2
dfB_subZB['patient_name_lis'] = dfB_remZA['patient_name_lis'].str.extract(r'^(\w+,\w{2,2} \w+)$')
#deleting the empty rows
dfB_subZB.dropna(subset=['patient_name_lis'], inplace=True)
#creating the remainder
dfB_remZB = dfB_remZA[~dfB_remZA['dummy_id'].isin(dfB_subZB['dummy_id'])]
#splitting name 3
dfB_subZB = pd.concat([dfB_subZB['dummy_id'],dfB_subZB['patient_surname'],dfB_subZB['patient_name_lis'], dfB_subZB['surname'],
                      dfB_subZB['name1'].str.split(' ', expand=True)], axis=1)
#renaming the columns
dfB_subZB.rename(columns={1:'firstname1'}, inplace=True)
#initials
dfB_subZB['initial'] = dfB_subZB['firstname1'].str.extract(r'^(\w)')
#droping patient_name_lis1
del dfB_subZB['patient_name_lis'], dfB_subZB[0]

''' SubZC'''
#subset of dfB_subB
dfB_subZC = pd.concat([dfB_remZB['dummy_id'], dfB_remZB['patient_surname'], dfB_remZB['patient_name_lis'], 
                      dfB_remZB['surname'], dfB_remZB['name1'], dfB_remZB['name2'], dfB_remZB['name3'], 
                      dfB_remZB['name4'], dfB_remZB['name5']], axis = 1)
#getting name list with surname, initial, initial, firstname1 and firstname2
dfB_subZC['patient_name_lis'] = dfB_remZB['patient_name_lis'].str.extract(r'^(\w+,\w+ \w+)$')
#deleting the empty rows
dfB_subZC.dropna(subset=['patient_name_lis'], inplace=True)
#creating the remainder
dfB_remZC = dfB_remZB[~dfB_remZB['dummy_id'].isin(dfB_subZC['dummy_id'])]
#splitting name 1
dfB_subZC = pd.concat([dfB_subZC['dummy_id'],dfB_subZC['patient_surname'],dfB_subZC['patient_name_lis'], dfB_subZC['surname'],
                      dfB_subZC['name1'].str.split(' ', expand=True)], axis=1)
#renaming the columns
dfB_subZC.rename(columns={0:'firstname1',1:'firstname2'}, inplace=True)
#initials
dfB_subZC['initial'] = dfB_subZC['firstname1'].str.extract(r'^(\w)')
#droping patient_name_lis1
del dfB_subZC['patient_name_lis']

''' SubZD'''
#subset of dfB_subB
dfB_subZD = pd.concat([dfB_remZC['dummy_id'], dfB_remZC['patient_surname'], dfB_remZC['patient_name_lis'], 
                      dfB_remZC['surname'], dfB_remZC['name1'], dfB_remZC['name2'], dfB_remZC['name3'], 
                      dfB_remZC['name4'], dfB_remZC['name5']], axis = 1)
#getting name list with surname, initial, initial, firstname1 and firstname2
dfB_subZD['patient_name_lis'] = dfB_remZC['patient_name_lis'].str.extract(r'^(\w+,\w{2,2},\w+,\w+)$')
#deleting the empty rows
dfB_subZD.dropna(subset=['patient_name_lis'], inplace=True)
#creating the remainder
dfB_remZD = dfB_remZC[~dfB_remZC['dummy_id'].isin(dfB_subZD['dummy_id'])]
#droping empty variable
del dfB_subZD['name4'], dfB_subZD['name5'], dfB_subZD['name1']
#renaming the columns
dfB_subZD.rename(columns={'name2':'firstname1','name3':'firstname2'}, inplace=True)
#droping patient_name_lis1
del dfB_subZD['patient_name_lis']
#initials
dfB_subZD['initial'] = dfB_subZD['firstname1'].str.extract(r'^(\w)')

''' SubZE'''
#subset of dfB_subB
dfB_subZE = pd.concat([dfB_remZD['dummy_id'], dfB_remZD['patient_surname'], dfB_remZD['patient_name_lis'], 
                      dfB_remZD['surname'], dfB_remZD['name1'], dfB_remZD['name2'], dfB_remZD['name3'], 
                      dfB_remZD['name4'], dfB_remZD['name5']], axis = 1)
#getting name list with surname, initial, initial, firstname1 and firstname2
dfB_subZE['patient_name_lis'] = dfB_remZD['patient_name_lis'].str.extract(r'^(\w+,\w+,\w{2,2},\w+)$')
#deleting the empty rows
dfB_subZE.dropna(subset=['patient_name_lis'], inplace=True)
#creating the remainder
dfB_remZE = dfB_remZD[~dfB_remZD['dummy_id'].isin(dfB_subZE['dummy_id'])]
#droping empty variable
del dfB_subZE['name4'], dfB_subZE['name5'], dfB_subZE['name2']
#renaming the columns
dfB_subZE.rename(columns={'name1':'firstname1','name3':'firstname2'}, inplace=True)
#droping patient_name_lis1
del dfB_subZE['patient_name_lis']
#initials
dfB_subZE['initial'] = dfB_subZE['firstname1'].str.extract(r'^(\w)')

''' SubZF'''
#subset of dfB_subB
dfB_subZF = pd.concat([dfB_remZE['dummy_id'], dfB_remZE['patient_surname'], dfB_remZE['patient_name_lis'], 
                      dfB_remZE['surname'], dfB_remZE['name1'], dfB_remZE['name2'], dfB_remZE['name3'], 
                      dfB_remZE['name4'], dfB_remZE['name5']], axis = 1)
#getting name list with surname, initial, initial, firstname1 and firstname2
dfB_subZF['patient_name_lis'] = dfB_remZE['patient_name_lis'].str.extract(r'^(\w+ . \w+)$')
#deleting the empty rows
dfB_subZF.dropna(subset=['patient_name_lis'], inplace=True)
#creating the remainder
dfB_remZF = dfB_remZE[~dfB_remZE['dummy_id'].isin(dfB_subZF['dummy_id'])]
#splitting name 1
dfB_subZF = pd.concat([dfB_subZF['dummy_id'],dfB_subZF['patient_surname'],dfB_subZF['patient_name_lis'],
                      dfB_subZF['surname'].str.split(' ', expand=True)], axis=1)
#renaming the columns
dfB_subZF.rename(columns={2:'firstname1',0:'surname',1:'initial'}, inplace=True)
#droping patient_name_lis1
del dfB_subZF['patient_name_lis']

''' SubZG'''
#subset of dfB_subB
dfB_subZG = pd.concat([dfB_remZF['dummy_id'], dfB_remZF['patient_surname'], dfB_remZF['patient_name_lis'], 
                      dfB_remZF['surname'], dfB_remZF['name1'], dfB_remZF['name2'], dfB_remZF['name3'], 
                      dfB_remZF['name4'], dfB_remZF['name5']], axis = 1)
#getting name list with surname, initial, initial, firstname1 and firstname2
dfB_subZG['patient_name_lis'] = dfB_remZF['patient_name_lis'].str.extract(r'^(\w+ \w\w,\w+ \w+)$')
#deleting the empty rows
dfB_subZG.dropna(subset=['patient_name_lis'], inplace=True)
#creating the remainder
dfB_remZG = dfB_remZF[~dfB_remZF['dummy_id'].isin(dfB_subZG['dummy_id'])]
#splitting name 1
dfB_subZG = pd.concat([dfB_subZG['dummy_id'],dfB_subZG['patient_surname'],dfB_subZG['patient_name_lis'],dfB_subZG['name1'],
                      dfB_subZG['surname'].str.split(' ', expand=True)], axis=1)
#renaming the columns
dfB_subZG.rename(columns={0:'surname'}, inplace=True)
#splitting name 1
dfB_subZG = pd.concat([dfB_subZG['dummy_id'],dfB_subZG['patient_surname'],dfB_subZG['patient_name_lis'],dfB_subZG['surname'],
                      dfB_subZG['name1'].str.split(' ', expand=True)], axis=1)
#renaming the columns
dfB_subZG.rename(columns={0:'firstname1',1:'firstname2'}, inplace=True)
#droping patient_name_lis1
del dfB_subZG['patient_name_lis']
#initials
dfB_subZG['initial'] = dfB_subZG['firstname1'].str.extract(r'^(\w)')

''' SubZH'''
#subset of dfB_subB
dfB_subZH = pd.concat([dfB_remZG['dummy_id'], dfB_remZG['patient_surname'], dfB_remZG['patient_name_lis'], 
                      dfB_remZG['surname'], dfB_remZG['name1'], dfB_remZG['name2'], dfB_remZG['name3'], 
                      dfB_remZG['name4'], dfB_remZG['name5']], axis = 1)
#getting name list with surname, initial, initial, firstname1 and firstname2
dfB_subZH['patient_name_lis'] = dfB_remZG['patient_name_lis'].str.extract(r'^(\w+,. \w+ \w+)$')
#deleting the empty rows
dfB_subZH.dropna(subset=['patient_name_lis'], inplace=True)
#creating the remainder
dfB_remZH = dfB_remZG[~dfB_remZG['dummy_id'].isin(dfB_subZH['dummy_id'])]
#splitting name 1
dfB_subZH = pd.concat([dfB_subZH['dummy_id'],dfB_subZH['patient_surname'],dfB_subZH['patient_name_lis'],dfB_subZH['surname'],
                      dfB_subZH['name1'].str.split(' ', expand=True)], axis=1)
#renaming the columns
dfB_subZH.rename(columns={1:'firstname1',2:'firstname2'}, inplace=True)
#droping patient_name_lis1
del dfB_subZH['patient_name_lis'], dfB_subZH[0]
#initials
dfB_subZH['initial'] = dfB_subZH['firstname1'].str.extract(r'^(\w)')

''' SubZI'''
#subset of dfB_subB
dfB_subZI = pd.concat([dfB_remZH['dummy_id'], dfB_remZH['patient_surname'], dfB_remZH['patient_name_lis'], 
                      dfB_remZH['surname'], dfB_remZH['name1'], dfB_remZH['name2'], dfB_remZH['name3'], 
                      dfB_remZH['name4'], dfB_remZH['name5']], axis = 1)
#getting name list with surname, initial, initial, firstname1 and firstname2
dfB_subZI['patient_name_lis'] = dfB_remZH['patient_name_lis'].str.extract(r'^(\w+,...,\w+ \w+)$')
#deleting the empty rows
dfB_subZI.dropna(subset=['patient_name_lis'], inplace=True)
#creating the remainder
dfB_remZI = dfB_remZH[~dfB_remZH['dummy_id'].isin(dfB_subZI['dummy_id'])]
#splitting name 1
dfB_subZI = pd.concat([dfB_subZI['dummy_id'],dfB_subZI['patient_surname'],dfB_subZI['patient_name_lis'],dfB_subZI['surname'],
                      dfB_subZI['name2'].str.split(' ', expand=True)], axis=1)
#renaming the columns
dfB_subZI.rename(columns={0:'firstname1',1:'firstname2'}, inplace=True)
#droping patient_name_lis1
del dfB_subZI['patient_name_lis']
#initials
dfB_subZI['initial'] = dfB_subZI['firstname1'].str.extract(r'^(\w)')

''' SubZJ'''
#subset of dfB_subB
dfB_subZJ = pd.concat([dfB_remZI['dummy_id'], dfB_remZI['patient_surname'], dfB_remZI['patient_name_lis'], 
                      dfB_remZI['surname'], dfB_remZI['name1'], dfB_remZI['name2'], dfB_remZI['name3'], 
                      dfB_remZI['name4'], dfB_remZI['name5']], axis = 1)
#getting name list with surname, initial, initial, firstname1 and firstname2
dfB_subZJ['patient_name_lis'] = dfB_remZI['patient_name_lis'].str.extract(r'^(\w{1,3} \w+,\w+)$')
#deleting the empty rows
dfB_subZJ.dropna(subset=['patient_name_lis'], inplace=True)
#creating the remainder
dfB_remZJ = dfB_remZI[~dfB_remZI['dummy_id'].isin(dfB_subZJ['dummy_id'])]
#droping empty variable
del dfB_subZJ['name4'], dfB_subZJ['name5'], dfB_subZJ['name3'], dfB_subZJ['name2']
#renaming the columns
dfB_subZJ.rename(columns={'name1':'firstname1'}, inplace=True)
#droping patient_name_lis1
del dfB_subZJ['patient_name_lis']
#initials
dfB_subZJ['initial'] = dfB_subZJ['firstname1'].str.extract(r'^(\w)')
#remving space in the surname
dfB_subZJ["surname"] = dfB_subZJ["surname"].str.replace(" ","")

''' SubZK'''
#subset of dfB_subB
dfB_subZK = pd.concat([dfB_remZJ['dummy_id'], dfB_remZJ['patient_surname'], dfB_remZJ['patient_name_lis'], 
                      dfB_remZJ['surname'], dfB_remZJ['name1'], dfB_remZJ['name2'], dfB_remZJ['name3'], 
                      dfB_remZJ['name4'], dfB_remZJ['name5']], axis = 1)
#getting name list with surname, initial, initial, firstname1 and firstname2
dfB_subZK['patient_name_lis'] = dfB_remZJ['patient_name_lis'].str.extract(r'^(\w+,...,\w+)$')
#deleting the empty rows
dfB_subZK.dropna(subset=['patient_name_lis'], inplace=True)
#creating the remainder
dfB_remZK = dfB_remZJ[~dfB_remZJ['dummy_id'].isin(dfB_subZK['dummy_id'])]
#droping empty variable
del dfB_subZK['name4'], dfB_subZK['name5'], dfB_subZK['name3'], dfB_subZK['name1']
#renaming the columns
dfB_subZK.rename(columns={'name2':'firstname1'}, inplace=True)
#droping patient_name_lis1
del dfB_subZK['patient_name_lis']
#initials
dfB_subZK['initial'] = dfB_subZK['firstname1'].str.extract(r'^(\w)')

''' SubZL'''
#subset of dfB_subB
dfB_subZL = pd.concat([dfB_remZK['dummy_id'], dfB_remZK['patient_surname'], dfB_remZK['patient_name_lis'], 
                      dfB_remZK['surname'], dfB_remZK['name1'], dfB_remZK['name2'], dfB_remZK['name3'], 
                      dfB_remZK['name4'], dfB_remZK['name5']], axis = 1)
#getting name list with surname, initial, initial, firstname1 and firstname2
dfB_subZL['patient_name_lis'] = dfB_remZK['patient_name_lis'].str.extract(r'^(\w+,\w+,.,\w+)$')
#deleting the empty rows
dfB_subZL.dropna(subset=['patient_name_lis'], inplace=True)
#creating the remainder
dfB_remZL = dfB_remZK[~dfB_remZK['dummy_id'].isin(dfB_subZL['dummy_id'])]
#droping empty variable
del dfB_subZL['name4'], dfB_subZL['name5'], dfB_subZL['name2']
#renaming the columns
dfB_subZL.rename(columns={'name1':'firstname1','name3':'firstname2'}, inplace=True)
#droping patient_name_lis1
del dfB_subZL['patient_name_lis']
#initials
dfB_subZL['initial'] = dfB_subZL['firstname1'].str.extract(r'^(\w)')

''' SubZM'''
#subset of dfB_subB
dfB_subZM = pd.concat([dfB_remZL['dummy_id'], dfB_remZL['patient_surname'], dfB_remZL['patient_name_lis'], 
                      dfB_remZL['surname'], dfB_remZL['name1'], dfB_remZL['name2'], dfB_remZL['name3'], 
                      dfB_remZL['name4'], dfB_remZL['name5']], axis = 1)
#getting name list with surname, initial, initial, firstname1 and firstname2
dfB_subZM['patient_name_lis'] = dfB_remZL['patient_name_lis'].str.extract(r'^(\w+-\w+,\w+-\w+)$')
#deleting the empty rows
dfB_subZM.dropna(subset=['patient_name_lis'], inplace=True)
#creating the remainder
dfB_remZM = dfB_remZL[~dfB_remZL['dummy_id'].isin(dfB_subZM['dummy_id'])]
#splitting name 1
dfB_subZM = pd.concat([dfB_subZM['dummy_id'],dfB_subZM['patient_surname'],dfB_subZM['patient_name_lis'],dfB_subZM['name1'],
                      dfB_subZM['surname'].str.split('-', expand=True)], axis=1)
#renaming the columns
dfB_subZM.rename(columns={'name1':'firstname1',0:'surname',1:'surname1'}, inplace=True)
#droping patient_name_lis1
del dfB_subZM['patient_name_lis']
#initials
dfB_subZM['initial'] = dfB_subZM['firstname1'].str.extract(r'^(\w)')
#remving space in the surname
dfB_subZM["surname1"] = dfB_subZM["surname1"].str.replace("^(.)$","")

''' SubZN'''
#subset of dfB_subB
dfB_subZN = pd.concat([dfB_remZM['dummy_id'], dfB_remZM['patient_surname'], dfB_remZM['patient_name_lis'], 
                      dfB_remZM['surname'], dfB_remZM['name1'], dfB_remZM['name2'], dfB_remZM['name3'], 
                      dfB_remZM['name4'], dfB_remZM['name5']], axis = 1)
#getting name list with surname, initial, initial, firstname1 and firstname2
dfB_subZN['patient_name_lis'] = dfB_remZM['patient_name_lis'].str.extract(r'^(\w+,\w\w,\w{2,},\w+,\w+)$')
#deleting the empty rows
dfB_subZN.dropna(subset=['patient_name_lis'], inplace=True)
#creating the remainder
dfB_remZN = dfB_remZM[~dfB_remZM['dummy_id'].isin(dfB_subZN['dummy_id'])]
#droping empty variable
del dfB_subZN['name1'], dfB_subZN['name5']
#renaming the columns
dfB_subZN.rename(columns={'name2':'firstname1','name3':'firstname2','name4':'firstname3'}, inplace=True)
#droping patient_name_lis1
del dfB_subZN['patient_name_lis']
#initials
dfB_subZN['initial'] = dfB_subZN['firstname1'].str.extract(r'^(\w)')
#remving space in the surname
dfB_subZN["firstname3"] = dfB_subZN["firstname3"].str.replace("^(...)$","")

''' SubZO'''
#subset of dfB_subB
dfB_subZO = pd.concat([dfB_remZN['dummy_id'], dfB_remZN['patient_surname'], dfB_remZN['patient_name_lis'], 
                      dfB_remZN['surname'], dfB_remZN['name1'], dfB_remZN['name2'], dfB_remZN['name3'], 
                      dfB_remZN['name4'], dfB_remZN['name5']], axis = 1)
#getting name list with surname, initial, initial, firstname1 and firstname2
dfB_subZO['patient_name_lis'] = dfB_remZN['patient_name_lis'].str.extract(r'^(\w{2,3} \w{2,} .,\w{2,})$')
#deleting the empty rows
dfB_subZO.dropna(subset=['patient_name_lis'], inplace=True)
#creating the remainder
dfB_remZO = dfB_remZN[~dfB_remZN['dummy_id'].isin(dfB_subZO['dummy_id'])]
#droping empty variable
del dfB_subZO['name3'], dfB_subZO['name4'], dfB_subZO['name2'], dfB_subZO['name5']
#renaming the columns
dfB_subZO.rename(columns={'name1':'firstname1'}, inplace=True)
#droping patient_name_lis1
del dfB_subZO['patient_name_lis']
#initials
dfB_subZO['initial'] = dfB_subZO['firstname1'].str.extract(r'^(\w)')
#remving space in the surname
dfB_subZO["surname"] = dfB_subZO["surname"].str.replace(r'( \w)$',"")
dfB_subZO["surname"] = dfB_subZO["surname"].str.replace(' ',"")


''' SubZP'''
#subset of dfB_subB
dfB_subZP = pd.concat([dfB_remZO['dummy_id'], dfB_remZO['patient_surname'], dfB_remZO['patient_name_lis'], 
                      dfB_remZO['surname'], dfB_remZO['name1'], dfB_remZO['name2'], dfB_remZO['name3'], 
                      dfB_remZO['name4'], dfB_remZO['name5']], axis = 1)
#getting name list with surname, initial, initial, firstname1 and firstname2
dfB_subZP['patient_name_lis'] = dfB_remZO['patient_name_lis'].str.extract(r'^(\w{2,3} \w{2,},\w\w,\w{2,})$')
#deleting the empty rows
dfB_subZP.dropna(subset=['patient_name_lis'], inplace=True)
#creating the remainder
dfB_remZP = dfB_remZO[~dfB_remZO['dummy_id'].isin(dfB_subZP['dummy_id'])]
#droping empty variable
del dfB_subZP['name1'], dfB_subZP['name5'], dfB_subZP['name3'], dfB_subZP['name4']
#renaming the columns
dfB_subZP.rename(columns={'name2':'firstname1'}, inplace=True)
#droping patient_name_lis1
del dfB_subZP['patient_name_lis']
#initials
dfB_subZP['initial'] = dfB_subZP['firstname1'].str.extract(r'^(\w)')
#remving space in the surname
dfB_subZP["surname"] = dfB_subZP["surname"].str.replace(' ',"")

''' SubZU'''
#subset of dfB_subB
dfB_subZU = pd.concat([dfB_remZP['dummy_id'], dfB_remZP['patient_surname'], dfB_remZP['patient_name_lis'], 
                      dfB_remZP['surname'], dfB_remZP['name1'], dfB_remZP['name2'], dfB_remZP['name3'], 
                      dfB_remZP['name4'], dfB_remZP['name5']], axis = 1)
#getting name list with surname, initial, initial, firstname1 and firstname2
dfB_subZU['patient_name_lis'] = dfB_remZP['patient_name_lis'].str.extract(r'^(\w+ \w{3,} \w{3,},\w+)$')
#deleting the empty rows
dfB_subZU.dropna(subset=['patient_name_lis'], inplace=True)
#creating the remainder
dfB_remZU = dfB_remZP[~dfB_remZP['dummy_id'].isin(dfB_subZU['dummy_id'])]
#droping empty variables in dfB_subA
del dfB_subZU['name2'], dfB_subZU['name4'], dfB_subZU['name5'], dfB_subZU['name3']
#renaming the columns
dfB_subZU.rename(columns={'name1':'firstname1'}, inplace=True)
#droping patient_name_lis1
del dfB_subZU['patient_name_lis']
#initials
dfB_subZU['initial'] = dfB_subZU['firstname1'].str.extract(r'^(\w)')
#remving space in the surname
dfB_subZU["surname"] = dfB_subZU["surname"].str.replace(' ',"")
#remving space in the surname
dfB_subZU["firstname1"] = dfB_subZU["firstname1"].str.replace("^(\w{1,2})$","")


''' SubZU'''
#subset of dfB_subB
dfB_subZV = pd.concat([dfB_remZU['dummy_id'], dfB_remZU['patient_surname'], dfB_remZU['patient_name_lis'], 
                      dfB_remZU['surname'], dfB_remZU['name1'], dfB_remZU['name2'], dfB_remZU['name3'], 
                      dfB_remZU['name4'], dfB_remZU['name5']], axis = 1)
#getting name list with surname, initial, initial, firstname1 and firstname2
dfB_subZV['patient_name_lis'] = dfB_remZU['patient_name_lis'].str.extract(r'^(\w{2,3}-\w+,\w+)$')
#deleting the empty rows
dfB_subZV.dropna(subset=['patient_name_lis'], inplace=True)
#creating the remainder
dfB_remZV = dfB_remZU[~dfB_remZU['dummy_id'].isin(dfB_subZV['dummy_id'])]
#droping empty variables in dfB_subA
del dfB_subZV['name2'], dfB_subZV['name4'], dfB_subZV['name5'], dfB_subZV['name3']
#renaming the columns
dfB_subZV.rename(columns={'name1':'firstname1'}, inplace=True)
#droping patient_name_lis1
del dfB_subZV['patient_name_lis']
#initials
dfB_subZV['initial'] = dfB_subZV['firstname1'].str.extract(r'^(\w)')
#remving space in the surname
dfB_subZV["surname"] = dfB_subZV["surname"].str.replace('-',"")
#remving space in the surname
dfB_subZV["firstname1"] = dfB_subZV["firstname1"].str.replace("^(\w{1,2})$","")

''' SubZU'''
#subset of dfB_subB
dfB_subZW = pd.concat([dfB_remZV['dummy_id'], dfB_remZV['patient_surname'], dfB_remZV['patient_name_lis'], 
                      dfB_remZV['surname'], dfB_remZV['name1'], dfB_remZV['name2'], dfB_remZV['name3'], 
                      dfB_remZV['name4'], dfB_remZV['name5']], axis = 1)
#getting name list with surname, initial, initial, firstname1 and firstname2
dfB_subZW['patient_name_lis'] = dfB_remZV['patient_name_lis'].str.extract(r'^(\w+,\w+-\w+)$')
#deleting the empty rows
dfB_subZW.dropna(subset=['patient_name_lis'], inplace=True)
#creating the remainder
dfB_remZW = dfB_remZV[~dfB_remZV['dummy_id'].isin(dfB_subZW['dummy_id'])]
#droping empty variables in dfB_subA
del dfB_subZW['name2'], dfB_subZW['name4'], dfB_subZW['name5'], dfB_subZW['name3']
#renaming the columns
dfB_subZW.rename(columns={'name1':'firstname1'}, inplace=True)
#droping patient_name_lis1
del dfB_subZW['patient_name_lis']
#initials
dfB_subZW['initial'] = dfB_subZW['firstname1'].str.extract(r'^(\w)')
#remving space in the surname
dfB_subZW["firstname1"] = dfB_subZW["firstname1"].str.replace("^(\w-\w)$","")

''' SubZU'''
#subset of dfB_subB
dfB_subZX = pd.concat([dfB_remZW['dummy_id'], dfB_remZW['patient_surname'], dfB_remZW['patient_name_lis'], 
                      dfB_remZW['surname'], dfB_remZW['name1'], dfB_remZW['name2'], dfB_remZW['name3'], 
                      dfB_remZW['name4'], dfB_remZW['name5']], axis = 1)
#getting name list with surname, initial, initial, firstname1 and firstname2
dfB_subZX['patient_name_lis'] = dfB_remZW['patient_name_lis'].str.extract(r'^(\w+,.,\w+-\w+)$')
#deleting the empty rows
dfB_subZX.dropna(subset=['patient_name_lis'], inplace=True)
#creating the remainder
dfB_remZX = dfB_remZW[~dfB_remZW['dummy_id'].isin(dfB_subZX['dummy_id'])]
#droping empty variables in dfB_subA
del dfB_subZX['name1'], dfB_subZX['name4'], dfB_subZX['name5'], dfB_subZX['name3']
#renaming the columns
dfB_subZX.rename(columns={'name2':'firstname1'}, inplace=True)
#droping patient_name_lis1
del dfB_subZX['patient_name_lis']
#initials
dfB_subZX['initial'] = dfB_subZX['firstname1'].str.extract(r'^(\w)')

''' SubZU'''
#subset of dfB_subB
dfB_subZY = pd.concat([dfB_remZX['dummy_id'], dfB_remZX['patient_surname'], dfB_remZX['patient_name_lis'], 
                      dfB_remZX['surname'], dfB_remZX['name1'], dfB_remZX['name2'], dfB_remZX['name3'], 
                      dfB_remZX['name4'], dfB_remZX['name5']], axis = 1)
#getting name list with surname, initial, initial, firstname1 and firstname2
dfB_subZY['patient_name_lis'] = dfB_remZX['patient_name_lis'].str.extract(r'^(\w+-\w+,\w+)$')
#deleting the empty rows
dfB_subZY.dropna(subset=['patient_name_lis'], inplace=True)
#creating the remainder
dfB_remZY = dfB_remZX[~dfB_remZX['dummy_id'].isin(dfB_subZY['dummy_id'])]
#splitting name 1
dfB_subZY = pd.concat([dfB_subZY['dummy_id'],dfB_subZY['patient_surname'],dfB_subZY['patient_name_lis'],dfB_subZY['name1'],
                      dfB_subZY['surname'].str.split('-', expand=True)], axis=1)
#renaming the columns
dfB_subZY.rename(columns={'name1':'firstname1',0:'surname',1:'surname1'}, inplace=True)
#droping patient_name_lis1
del dfB_subZY['patient_name_lis']
#initials
dfB_subZY['initial'] = dfB_subZY['firstname1'].str.extract(r'^(\w)')
#removing one letter names
dfB_subZY["firstname1"] = dfB_subZY["firstname1"].str.replace("^(\w{1,2})$","")

''' SubZU'''
#subset of dfB_subB
dfB_subZZ = pd.concat([dfB_remZY['dummy_id'], dfB_remZY['patient_surname'], dfB_remZY['patient_name_lis'], 
                      dfB_remZY['surname'], dfB_remZY['name1'], dfB_remZY['name2'], dfB_remZY['name3'], 
                      dfB_remZY['name4'], dfB_remZY['name5']], axis = 1)
#getting name list with surname, initial, initial, firstname1 and firstname2
dfB_subZZ['patient_name_lis'] = dfB_remZY['patient_name_lis'].str.extract(r'^(\w{2,3}-\w+,.,\w+)$')
#deleting the empty rows
dfB_subZZ.dropna(subset=['patient_name_lis'], inplace=True)
#creating the remainder
dfB_remZZ = dfB_remZY[~dfB_remZY['dummy_id'].isin(dfB_subZZ['dummy_id'])]
#droping empty variables in dfB_subA
del dfB_subZZ['name1'], dfB_subZZ['name4'], dfB_subZZ['name5'], dfB_subZZ['name3']
#renaming the columns
dfB_subZZ.rename(columns={'name2':'firstname1'}, inplace=True)
#droping patient_name_lis1
del dfB_subZZ['patient_name_lis']
#initials
dfB_subZZ['initial'] = dfB_subZZ['firstname1'].str.extract(r'^(\w)')
#removing one letter names
dfB_subZZ["surname"] = dfB_subZZ["surname"].str.replace("-","")

''' SubZU'''
#subset of dfB_subB
dfB_subZZA = pd.concat([dfB_remZZ['dummy_id'], dfB_remZZ['patient_surname'], dfB_remZZ['patient_name_lis'], 
                      dfB_remZZ['surname'], dfB_remZZ['name1'], dfB_remZZ['name2'], dfB_remZZ['name3'], 
                      dfB_remZZ['name4'], dfB_remZZ['name5']], axis = 1)
#getting name list with surname, initial, initial, firstname1 and firstname2
dfB_subZZA['patient_name_lis'] = dfB_remZZ['patient_name_lis'].str.extract(r'^(\w+ \w+ \w+,.,\w+)$')
#deleting the empty rows
dfB_subZZA.dropna(subset=['patient_name_lis'], inplace=True)
#creating the remainder
dfB_remZZA = dfB_remZZ[~dfB_remZZ['dummy_id'].isin(dfB_subZZA['dummy_id'])]
#droping empty variables in dfB_subA
del dfB_subZZA['name1'], dfB_subZZA['name4'], dfB_subZZA['name5'], dfB_subZZA['name3']
#renaming the columns
dfB_subZZA.rename(columns={'name2':'firstname1'}, inplace=True)
#droping patient_name_lis1
del dfB_subZZA['patient_name_lis']
#initials
dfB_subZZA['initial'] = dfB_subZZA['firstname1'].str.extract(r'^(\w)')
#removing one letter names
dfB_subZZA["surname"] = dfB_subZZA["surname"].str.replace(" ","")

''' SubZU'''
#subset of dfB_subB
dfB_subZZB = pd.concat([dfB_remZZA['dummy_id'], dfB_remZZA['patient_surname'], dfB_remZZA['patient_name_lis'], 
                      dfB_remZZA['surname'], dfB_remZZA['name1'], dfB_remZZA['name2'], dfB_remZZA['name3'], 
                      dfB_remZZA['name4'], dfB_remZZA['name5']], axis = 1)
#getting name list with surname, initial, initial, firstname1 and firstname2
dfB_subZZB['patient_name_lis'] = dfB_remZZA['patient_name_lis'].str.extract(r'^(\w+ \w+,\w+-\w+)$')
#deleting the empty rows
dfB_subZZB.dropna(subset=['patient_name_lis'], inplace=True)
#creating the remainder
dfB_remZZB = dfB_remZZA[~dfB_remZZA['dummy_id'].isin(dfB_subZZB['dummy_id'])]
#droping empty variables in dfB_subA
del dfB_subZZB['name2'], dfB_subZZB['name4'], dfB_subZZB['name5'], dfB_subZZB['name3']
#renaming the columns
dfB_subZZB.rename(columns={'name1':'firstname1'}, inplace=True)
#droping patient_name_lis1
del dfB_subZZB['patient_name_lis']
#initials
dfB_subZZB['initial'] = dfB_subZZB['firstname1'].str.extract(r'^(\w)')
#removing one letter names
dfB_subZZB["surname"] = dfB_subZZB["surname"].str.replace(r'( \w)$',"")
#removing one letter names
dfB_subZZB["surname"] = dfB_subZZB["surname"].str.replace(" ","")

''' SubZU'''
#subset of dfB_subB
dfB_subZZC = pd.concat([dfB_remZZB['dummy_id'], dfB_remZZB['patient_surname'], dfB_remZZB['patient_name_lis'], 
                      dfB_remZZB['surname'], dfB_remZZB['name1'], dfB_remZZB['name2'], dfB_remZZB['name3'], 
                      dfB_remZZB['name4'], dfB_remZZB['name5']], axis = 1)
#getting name list with surname, initial, initial, firstname1 and firstname2
dfB_subZZC['patient_name_lis'] = dfB_remZZB['patient_name_lis'].str.extract(r'^(\w+,\w\w,\w+-\w+)$')
#deleting the empty rows
dfB_subZZC.dropna(subset=['patient_name_lis'], inplace=True)
#creating the remainder
dfB_remZZC = dfB_remZZB[~dfB_remZZB['dummy_id'].isin(dfB_subZZC['dummy_id'])]
#droping empty variables in dfB_subA
del dfB_subZZC['name1'], dfB_subZZC['name4'], dfB_subZZC['name5'], dfB_subZZC['name3']
#renaming the columns
dfB_subZZC.rename(columns={'name2':'firstname1'}, inplace=True)
#droping patient_name_lis1
del dfB_subZZC['patient_name_lis']
#initials
dfB_subZZC['initial'] = dfB_subZZC['firstname1'].str.extract(r'^(\w)')

''' SubZU'''
#subset of dfB_subB
dfB_subZZD = pd.concat([dfB_remZZC['dummy_id'], dfB_remZZC['patient_surname'], dfB_remZZC['patient_name_lis'], 
                      dfB_remZZC['surname'], dfB_remZZC['name1'], dfB_remZZC['name2'], dfB_remZZC['name3'], 
                      dfB_remZZC['name4'], dfB_remZZC['name5']], axis = 1)
#getting name list with surname, initial, initial, firstname1 and firstname2
dfB_subZZD['patient_name_lis'] = dfB_remZZC['patient_name_lis'].str.extract(r'^(\w+ \w+,\w\w,\w+ \w+)$')
#deleting the empty rows
dfB_subZZD.dropna(subset=['patient_name_lis'], inplace=True)
#creating the remainder
dfB_remZZD = dfB_remZZC[~dfB_remZZC['dummy_id'].isin(dfB_subZZD['dummy_id'])]
#splitting name 1
dfB_subZZD = pd.concat([dfB_subZZD['dummy_id'],dfB_subZZD['patient_surname'],dfB_subZZD['surname'],
                      dfB_subZZD['name2'].str.split(' ', expand=True)], axis=1)
#renaming the columns
dfB_subZZD.rename(columns={0:'firstname1',1:'firstname2'}, inplace=True)
#droping patient_name_lis1
#del dfB_subZZD['patient_name_lis']
#initials
dfB_subZZD['initial'] = dfB_subZZD['firstname1'].str.extract(r'^(\w)')
#remving space in the surname
dfB_subZZD["surname"] = dfB_subZZD["surname"].str.replace(' ',"")

''' SubZU'''
#subset of dfB_subB
dfB_subZZE = pd.concat([dfB_remZZD['dummy_id'], dfB_remZZD['patient_surname'], dfB_remZZD['patient_name_lis'], 
                      dfB_remZZD['surname'], dfB_remZZD['name1'], dfB_remZZD['name2'], dfB_remZZD['name3'], 
                      dfB_remZZD['name4'], dfB_remZZD['name5']], axis = 1)
#getting name list with surname, initial, initial, firstname1 and firstname2
dfB_subZZE['patient_name_lis'] = dfB_remZZD['patient_name_lis'].str.extract(r'^(\w+ \w+ \w+,\w\w,\w+)$')
#deleting the empty rows
dfB_subZZE.dropna(subset=['patient_name_lis'], inplace=True)
#creating the remainder
dfB_remZZE = dfB_remZZD[~dfB_remZZD['dummy_id'].isin(dfB_subZZE['dummy_id'])]
#droping empty variables in dfB_subA
del dfB_subZZE['name1'], dfB_subZZE['name4'], dfB_subZZE['name5'], dfB_subZZE['name3']
#renaming the columns
dfB_subZZE.rename(columns={'name2':'firstname1'}, inplace=True)
#droping patient_name_lis1
del dfB_subZZE['patient_name_lis']
#initials
dfB_subZZE['initial'] = dfB_subZZE['firstname1'].str.extract(r'^(\w)')
#remving space in the surname
dfB_subZZE["surname"] = dfB_subZZE["surname"].str.replace(' ',"")

''' SubZU'''
#subset of dfB_subB
dfB_subZZF = pd.concat([dfB_remZZE['dummy_id'], dfB_remZZE['patient_surname'], dfB_remZZE['patient_name_lis'], 
                      dfB_remZZE['surname'], dfB_remZZE['name1'], dfB_remZZE['name2'], dfB_remZZE['name3'], 
                      dfB_remZZE['name4'], dfB_remZZE['name5']], axis = 1)
#getting name list with surname, initial, initial, firstname1 and firstname2
dfB_subZZF['patient_name_lis'] = dfB_remZZE['patient_name_lis'].str.extract(r'^(\w+-\w+,\w\w,\w+ \w+)$')
#deleting the empty rows
dfB_subZZF.dropna(subset=['patient_name_lis'], inplace=True)
#creating the remainder
dfB_remZZF = dfB_remZZE[~dfB_remZZE['dummy_id'].isin(dfB_subZZF['dummy_id'])]
#splitting name 1
dfB_subZZF = pd.concat([dfB_subZZF['dummy_id'],dfB_subZZF['patient_surname'],dfB_subZZF['surname'],
                      dfB_subZZF['name2'].str.split(' ', expand=True)], axis=1)
#renaming the columns
dfB_subZZF.rename(columns={0:'firstname1',1:'firstname2'}, inplace=True)
#droping patient_name_lis1
#del dfB_subZZF['patient_name_lis']
#initials
dfB_subZZF['initial'] = dfB_subZZF['firstname1'].str.extract(r'^(\w)')
#remving space in the surname
dfB_subZZF["surname"] = dfB_subZZF["surname"].str.replace('-',"")

''' SubZU'''
#subset of dfB_subB
dfB_subZZG = pd.concat([dfB_remZZF['dummy_id'], dfB_remZZF['patient_surname'], dfB_remZZF['patient_name_lis'], 
                      dfB_remZZF['surname'], dfB_remZZF['name1'], dfB_remZZF['name2'], dfB_remZZF['name3'], 
                      dfB_remZZF['name4'], dfB_remZZF['name5']], axis = 1)
#getting name list with surname, initial, initial, firstname1 and firstname2
dfB_subZZG['patient_name_lis'] = dfB_remZZF['patient_name_lis'].str.extract(r'^(\w+ \w+ \w+,\w,\w+ \w+)$')
#deleting the empty rows
dfB_subZZG.dropna(subset=['patient_name_lis'], inplace=True)
#creating the remainder
dfB_remZZG = dfB_remZZF[~dfB_remZZF['dummy_id'].isin(dfB_subZZG['dummy_id'])]
#splitting name 1
dfB_subZZG = pd.concat([dfB_subZZG['dummy_id'],dfB_subZZG['patient_surname'],dfB_subZZG['surname'],
                      dfB_subZZG['name2'].str.split(' ', expand=True)], axis=1)
#renaming the columns
dfB_subZZG.rename(columns={0:'firstname1',1:'firstname2'}, inplace=True)
#droping patient_name_lis1
#del dfB_subZZF['patient_name_lis']
#initials
dfB_subZZG['initial'] = dfB_subZZG['firstname1'].str.extract(r'^(\w)')
#remving space in the surname
dfB_subZZG["surname"] = dfB_subZZG["surname"].str.replace(' ',"")

''' SubZU'''
#subset of dfB_subB
dfB_subZZH = pd.concat([dfB_remZZG['dummy_id'], dfB_remZZG['patient_surname'], dfB_remZZG['patient_name_lis'], 
                      dfB_remZZG['surname'], dfB_remZZG['name1'], dfB_remZZG['name2'], dfB_remZZG['name3'], 
                      dfB_remZZG['name4'], dfB_remZZG['name5']], axis = 1)
#getting name list with surname, initial, initial, firstname1 and firstname2
dfB_subZZH['patient_name_lis'] = dfB_remZZG['patient_name_lis'].str.extract(r'^(\w+ \w+,\w\w,\w+,\w+)$')
#deleting the empty rows
dfB_subZZH.dropna(subset=['patient_name_lis'], inplace=True)
#creating the remainder
dfB_remZZH = dfB_remZZG[~dfB_remZZG['dummy_id'].isin(dfB_subZZH['dummy_id'])]
#droping empty variables in dfB_subA
del dfB_subZZH['name1'], dfB_subZZH['name4'], dfB_subZZH['name5']
#renaming the columns
dfB_subZZH.rename(columns={'name2':'firstname1','name3':'firstname2'}, inplace=True)
#droping patient_name_lis1
del dfB_subZZH['patient_name_lis']
#initials
dfB_subZZH['initial'] = dfB_subZZH['firstname1'].str.extract(r'^(\w)')
#remving space in the surname
dfB_subZZH["surname"] = dfB_subZZH["surname"].str.replace(' ',"")

''' SubZU'''
#subset of dfB_subB
dfB_subZZI = pd.concat([dfB_remZZH['dummy_id'], dfB_remZZH['patient_surname'], dfB_remZZH['patient_name_lis'], 
                      dfB_remZZH['surname'], dfB_remZZH['name1'], dfB_remZZH['name2'], dfB_remZZH['name3'], 
                      dfB_remZZH['name4'], dfB_remZZH['name5']], axis = 1)
#getting name list with surname, initial, initial, firstname1 and firstname2
dfB_subZZI['patient_name_lis'] = dfB_remZZH['patient_name_lis'].str.extract(r'^(\w+ \w+,\w,\w+)$')
#deleting the empty rows
dfB_subZZI.dropna(subset=['patient_name_lis'], inplace=True)
#creating the remainder
dfB_remZZI = dfB_remZZH[~dfB_remZZH['dummy_id'].isin(dfB_subZZI['dummy_id'])]
#droping empty variables in dfB_subA
del dfB_subZZI['name1'], dfB_subZZI['name4'], dfB_subZZI['name3'], dfB_subZZI['name5']
#renaming the columns
dfB_subZZI.rename(columns={'name2':'firstname1'}, inplace=True)
#droping patient_name_lis1
del dfB_subZZI['patient_name_lis']
#initials
dfB_subZZI['initial'] = dfB_subZZI['firstname1'].str.extract(r'^(\w)')
#remving space in the surname
dfB_subZZI["surname"] = dfB_subZZI["surname"].str.replace(' ',"")

''' SubZU'''
#subset of dfB_subB
dfB_subZZJ = pd.concat([dfB_remZZI['dummy_id'], dfB_remZZI['patient_surname'], dfB_remZZI['patient_name_lis'], 
                      dfB_remZZI['surname'], dfB_remZZI['name1'], dfB_remZZI['name2'], dfB_remZZI['name3'], 
                      dfB_remZZI['name4'], dfB_remZZI['name5']], axis = 1)
#getting name list with surname, initial, initial, firstname1 and firstname2
dfB_subZZJ['patient_name_lis'] = dfB_remZZI['patient_name_lis'].str.extract(r'^(\w+ \w+,\w+)$')
#deleting the empty rows
dfB_subZZJ.dropna(subset=['patient_name_lis'], inplace=True)
#creating the remainder
dfB_remZZJ = dfB_remZZI[~dfB_remZZI['dummy_id'].isin(dfB_subZZJ['dummy_id'])]
#droping empty variables in dfB_subA
del dfB_subZZJ['name2'], dfB_subZZJ['name4'], dfB_subZZJ['name3'], dfB_subZZJ['name5']
#renaming the columns
dfB_subZZJ.rename(columns={'name1':'firstname1'}, inplace=True)
#droping patient_name_lis1
del dfB_subZZJ['patient_name_lis']
#initials
dfB_subZZJ['initial'] = dfB_subZZJ['firstname1'].str.extract(r'^(\w)')
#remving space in the surname
dfB_subZZJ["surname"] = dfB_subZZJ["surname"].str.replace(' ',"")

''' SubZU'''
#subset of dfB_subB
dfB_subZZK = pd.concat([dfB_remZZJ['dummy_id'], dfB_remZZJ['patient_surname'], dfB_remZZJ['patient_name_lis'], 
                      dfB_remZZJ['surname'], dfB_remZZJ['name1'], dfB_remZZJ['name2'], dfB_remZZJ['name3'], 
                      dfB_remZZJ['name4'], dfB_remZZJ['name5']], axis = 1)
#getting name list with surname, initial, initial, firstname1 and firstname2
dfB_subZZK['patient_name_lis'] = dfB_remZZJ['patient_name_lis'].str.extract(r'^(\w+ \w+,.,\w+ \w+)$')
#deleting the empty rows
dfB_subZZK.dropna(subset=['patient_name_lis'], inplace=True)
#creating the remainder
dfB_remZZK = dfB_remZZJ[~dfB_remZZJ['dummy_id'].isin(dfB_subZZK['dummy_id'])]
#droping empty variables in dfB_subA
del dfB_subZZK['name1'], dfB_subZZK['name4'], dfB_subZZK['name3'], dfB_subZZK['name5']
#renaming the columns
dfB_subZZK.rename(columns={'name2':'firstname1'}, inplace=True)
#droping patient_name_lis1
del dfB_subZZK['patient_name_lis']
#initials
dfB_subZZK['initial'] = dfB_subZZK['firstname1'].str.extract(r'^(\w)')
#remving space in the surname
dfB_subZZK["surname"] = dfB_subZZK["surname"].str.replace(' ',"")
#remving space in the surname
dfB_subZZK["firstname1"] = dfB_subZZK["firstname1"].str.replace(' ',"")

''' SubZU'''
#subset of dfB_subB
dfB_subZZL = pd.concat([dfB_remZZK['dummy_id'], dfB_remZZK['patient_surname'], dfB_remZZK['patient_name_lis'], 
                      dfB_remZZK['surname'], dfB_remZZK['name1'], dfB_remZZK['name2'], dfB_remZZK['name3'], 
                      dfB_remZZK['name4'], dfB_remZZK['name5']], axis = 1)
#getting name list with surname, initial, initial, firstname1 and firstname2
dfB_subZZL['patient_name_lis'] = dfB_remZZK['patient_name_lis'].str.extract(r'^(\w+ \w+,.,\w+-\w+)$')
#deleting the empty rows
dfB_subZZL.dropna(subset=['patient_name_lis'], inplace=True)
#creating the remainder
dfB_remZZL = dfB_remZZK[~dfB_remZZK['dummy_id'].isin(dfB_subZZL['dummy_id'])]
#droping empty variables in dfB_subA
del dfB_subZZL['name1'], dfB_subZZL['name4'], dfB_subZZL['name3'], dfB_subZZL['name5']
#renaming the columns
dfB_subZZL.rename(columns={'name2':'firstname1'}, inplace=True)
#droping patient_name_lis1
del dfB_subZZL['patient_name_lis']
#initials
dfB_subZZL['initial'] = dfB_subZZL['firstname1'].str.extract(r'^(\w)')
#remving space in the surname
dfB_subZZL["surname"] = dfB_subZZL["surname"].str.replace(' ',"")

''' SubZU'''
#subset of dfB_subB
dfB_subZZM = pd.concat([dfB_remZZL['dummy_id'], dfB_remZZL['patient_surname'], dfB_remZZL['patient_name_lis'], 
                      dfB_remZZL['surname'], dfB_remZZL['name1'], dfB_remZZL['name2'], dfB_remZZL['name3'], 
                      dfB_remZZL['name4'], dfB_remZZL['name5']], axis = 1)
#getting name list with surname, initial, initial, firstname1 and firstname2
dfB_subZZM['patient_name_lis'] = dfB_remZZL['patient_name_lis'].str.extract(r'^(\w+-\w+,.,\w+)$')
#deleting the empty rows
dfB_subZZM.dropna(subset=['patient_name_lis'], inplace=True)
#creating the remainder
dfB_remZZM = dfB_remZZL[~dfB_remZZL['dummy_id'].isin(dfB_subZZM['dummy_id'])]
#splitting name 1
dfB_subZZM = pd.concat([dfB_subZZM['dummy_id'],dfB_subZZM['patient_surname'],dfB_subZZM['name2'],
                      dfB_subZZM['surname'].str.split('-', expand=True)], axis=1)
#renaming the columns
dfB_subZZM.rename(columns={'name2':'firstname1',0:'surname',1:'surname1'}, inplace=True)
#droping patient_name_lis1
#del dfB_subZZM['patient_name_lis']
#initials
dfB_subZZM['initial'] = dfB_subZZM['firstname1'].str.extract(r'^(\w)')
#remving space in the surname
#dfB_subZZM["surname"] = dfB_subZZM["surname"].str.replace(' ',"")


#digits
''' SubA'''
#subset of dfB
dfB_DigitsA = pd.concat([dfB_Digits['dummy_id'], dfB_Digits['patient_surname'], dfB_Digits['patient_name_lis'], dfB_Digits['surname'],
                      dfB_Digits['name1'], dfB_Digits['name2'], dfB_Digits['name3'], dfB_Digits['name4'], dfB_Digits['name5']], axis = 1)
#getting name list with surname, initial and firstname1
dfB_DigitsA['patient_name_lis'] = dfB_Digits['patient_name_lis'].str.extract(r'^(\w+,.,\w+-\w+)$')
#deleting the empty rows
dfB_DigitsA.dropna(subset=['patient_name_lis'], inplace=True)
#creating the remainder of the dfB
dfB_rem_digits_A = dfB_Digits[~dfB_Digits['dummy_id'].isin(dfB_DigitsA['dummy_id'])]
#droping empty variables in dfB_subA
del dfB_DigitsA['name3'], dfB_DigitsA['name4'], dfB_DigitsA['name5']
#renaming the columns
dfB_DigitsA.rename(columns={'name1':'initial', 'name2':'firstname1'}, inplace=True)
#droping patient_name_lis1
del dfB_DigitsA['patient_name_lis']
#cleaning the first name
dfB_DigitsA['firstname1'] = dfB_DigitsA['firstname1'].str.replace(r'(-\w+)$', '')

''' SubB'''
#subset of dfB
dfB_DigitsB = pd.concat([dfB_rem_digits_A['dummy_id'], dfB_rem_digits_A['patient_surname'], dfB_rem_digits_A['patient_name_lis'], dfB_rem_digits_A['surname'],
                      dfB_rem_digits_A['name1'], dfB_rem_digits_A['name2'], dfB_rem_digits_A['name3'], dfB_rem_digits_A['name4'], dfB_rem_digits_A['name5']], axis = 1)
#getting name list with surname, initial and firstname1
dfB_DigitsB['patient_name_lis'] = dfB_rem_digits_A['patient_name_lis'].str.extract(r'^(\w+,.,\w+ \w+)$')
#deleting the empty rows
dfB_DigitsB.dropna(subset=['patient_name_lis'], inplace=True)
#creating the remainder
dfB_rem_digits_B = dfB_rem_digits_A[~dfB_rem_digits_A['dummy_id'].isin(dfB_DigitsB['dummy_id'])]
#droping empty variables in dfB_subA
del dfB_DigitsB['name3'], dfB_DigitsB['name4'], dfB_DigitsB['name5']
#renaming the columns
dfB_DigitsB.rename(columns={'name1':'initial', 'name2':'firstname1'}, inplace=True)
#droping patient_name_lis1
del dfB_DigitsB['patient_name_lis']
#cleaning the first name
dfB_DigitsB['firstname1'] = dfB_DigitsB['firstname1'].str.replace(r'( \w+)$', '')

#subset of dfB
dfB_DigitsC = pd.concat([dfB_rem_digits_B['dummy_id'], dfB_rem_digits_B['patient_surname'], dfB_rem_digits_B['patient_name_lis'], dfB_rem_digits_B['surname'],
                      dfB_rem_digits_B['name1'], dfB_rem_digits_B['name2'], dfB_rem_digits_B['name3'], dfB_rem_digits_B['name4'], dfB_rem_digits_B['name5']], axis = 1)
#getting name list with surname, initial and firstname1
dfB_DigitsC['patient_name_lis'] = dfB_rem_digits_B['patient_name_lis'].str.extract(r'^(\w+,\w\w,\w+-\w+)$')
#deleting the empty rows
dfB_DigitsC.dropna(subset=['patient_name_lis'], inplace=True)
#creating the remainder of the dfB
dfB_rem_digits_C = dfB_rem_digits_B[~dfB_rem_digits_B['dummy_id'].isin(dfB_DigitsC['dummy_id'])]
#droping empty variables in dfB_subA
del dfB_DigitsC['name3'], dfB_DigitsC['name4'], dfB_DigitsC['name5'], dfB_DigitsC['name1']
#renaming the columns
dfB_DigitsC.rename(columns={'name2':'firstname1'}, inplace=True)
#droping patient_name_lis1
del dfB_DigitsC['patient_name_lis']
#cleaning the first name
dfB_DigitsC['firstname1'] = dfB_DigitsC['firstname1'].str.replace(r'(-\w+)$', '')
#initials
dfB_DigitsC['initial'] = dfB_DigitsC['firstname1'].str.extract(r'^(\w)')

#subset of dfB
dfB_DigitsD = pd.concat([dfB_rem_digits_C['dummy_id'], dfB_rem_digits_C['patient_surname'], dfB_rem_digits_C['patient_name_lis'], dfB_rem_digits_C['surname'],
                      dfB_rem_digits_C['name1'], dfB_rem_digits_C['name2'], dfB_rem_digits_C['name3'], dfB_rem_digits_C['name4'], dfB_rem_digits_C['name5']], axis = 1)
#getting name list with surname, initial and firstname1
dfB_DigitsD['patient_name_lis'] = dfB_rem_digits_C['patient_name_lis'].str.extract(r'^(\w+ \w+,.,\w+-\w+)$')
#deleting the empty rows
dfB_DigitsD.dropna(subset=['patient_name_lis'], inplace=True)
#creating the remainder of the dfB
dfB_rem_digits_D = dfB_rem_digits_C[~dfB_rem_digits_C['dummy_id'].isin(dfB_DigitsD['dummy_id'])]
#droping empty variables in dfB_subA
del dfB_DigitsD['name3'], dfB_DigitsD['name4'], dfB_DigitsD['name5'], dfB_DigitsD['name1']
#renaming the columns
dfB_DigitsD.rename(columns={'name2':'firstname1'}, inplace=True)
#droping patient_name_lis1
del dfB_DigitsD['patient_name_lis']
#cleaning the first name
dfB_DigitsD['firstname1'] = dfB_DigitsD['firstname1'].str.replace(r'(-\w+)$', '')
#cleaning the first name
dfB_DigitsD['surname'] = dfB_DigitsD['surname'].str.replace(' ', '')
#initials
dfB_DigitsD['initial'] = dfB_DigitsD['firstname1'].str.extract(r'^(\w)')

#subset of dfB
dfB_DigitsE = pd.concat([dfB_rem_digits_D['dummy_id'], dfB_rem_digits_D['patient_surname'], dfB_rem_digits_D['patient_name_lis'], dfB_rem_digits_C['surname'],
                      dfB_rem_digits_D['name1'], dfB_rem_digits_D['name2'], dfB_rem_digits_D['name3'], dfB_rem_digits_D['name4'], dfB_rem_digits_C['name5']], axis = 1)
#getting name list with surname, initial and firstname1
dfB_DigitsE['patient_name_lis'] = dfB_rem_digits_D['patient_name_lis'].str.extract(r'^(\w+,\w+ \w+)$')
#deleting the empty rows
dfB_DigitsE.dropna(subset=['patient_name_lis'], inplace=True)
#creating the remainder of the dfB
dfB_rem_digits_E = dfB_rem_digits_D[~dfB_rem_digits_D['dummy_id'].isin(dfB_DigitsE['dummy_id'])]
#droping empty variables in dfB_subA
del dfB_DigitsE['name3'], dfB_DigitsE['name4'], dfB_DigitsE['name5'], dfB_DigitsE['name2']
#cleaning the first name
dfB_DigitsE = dfB_DigitsE[~dfB_DigitsE['patient_name_lis'].astype(str).str.contains(r'1,00 INF')]
dfB_DigitsE = dfB_DigitsE[~dfB_DigitsE['patient_name_lis'].astype(str).str.contains(r'SOSHL,1113 KK')]
dfB_DigitsE = dfB_DigitsE[~dfB_DigitsE['patient_name_lis'].astype(str).str.contains(r'BABY NO2')]
#cleaning the names
dfB_DigitsE['surname'] = dfB_DigitsE['surname'].str.replace(r'\d+', '')
dfB_DigitsE['name1'] = dfB_DigitsE['name1'].str.replace(r'\d+', '')
dfB_DigitsE['name1'] = dfB_DigitsE['name1'].str.replace(r'( \w\w)$', '')
#renaming the columns
dfB_DigitsE.rename(columns={'name1':'firstname1'}, inplace=True)
#droping patient_name_lis1
del dfB_DigitsE['patient_name_lis']
#initials
dfB_DigitsE['initial'] = dfB_DigitsE['firstname1'].str.extract(r'^(\w)')


#stop here with the names

#consolidatig the data so far
dfB_names = pd.concat([dfB_subA, dfB_subB, dfB_subC, dfB_subD, dfB_subE, dfB_subF, dfB_subG, dfB_subH, dfB_subI, dfB_subJ, dfB_subK, 
                        dfB_subL, dfB_subM, dfB_subN, dfB_subO, dfB_subP, dfB_subQ, dfB_subR, dfB_subS, dfB_subT, dfB_subU, dfB_subV, 
                        dfB_subW, dfB_subX, dfB_subY, dfB_subZ, dfB_subZA, dfB_subZB, dfB_subZC, dfB_subZD, dfB_subZE, dfB_subZF,
                        dfB_subZG, dfB_subZH, dfB_subZI, dfB_subZJ, dfB_subZK, dfB_subZL, dfB_subZM, dfB_subZN, dfB_subZO, dfB_subZP,
                        dfB_subZU, dfB_subZV, dfB_subZW, dfB_subZX, dfB_subZY, dfB_subZZ, dfB_subZZA, dfB_subZZB, dfB_subZZC, dfB_subZZD,
                        dfB_subZZE, dfB_subZZF, dfB_subZZG, dfB_subZZH, dfB_subZZI, dfB_subZZJ, dfB_subZZK, dfB_subZZL, dfB_subZZM, 
                        dfB_DigitsA, dfB_DigitsB, dfB_DigitsC, dfB_DigitsD, dfB_DigitsE], ignore_index=True)
   
del dfB_names["patient_surname"]  
##saving the dataset
dfB_names.to_csv('pp_gauteng_names.csv', sep=',', encoding='windows-1252')                 
#dealing with dates
#sub setting the dates
dfC = pd.concat([dfA['dummy_id'], dfA['date_of_birth'], dfA['date_specimen_registered']], axis=1)
#Creating system date
dfC['date_of_birth'] = dfC['date_of_birth'].str.replace(r'1800-01-01', '')
#Converting birthday in date time
dfC["dob"] = pd.to_datetime(dfC["date_of_birth"])
dfC["dsr"] = pd.to_datetime(dfC["date_specimen_registered"])
#diference in years
dfC['time_diference'] = round(((dfC["dsr"] - dfC["dob"]).dt.days)/365, 0)
#Splitting the date time into separate columns
#date_of_birth
dfC["dob_day"] = dfC['dob'].map(lambda x: x.day)
dfC["dob_month"] = dfC['dob'].map(lambda x: x.month)
dfC["dob_year"] = dfC['dob'].map(lambda x: x.year)
##date_specimen_registered
#dfC["dsr_day"] = dfC['dsr'].map(lambda x: x.day)
#dfC["dsr_month"] = dfC['dsr'].map(lambda x: x.month)
#dfC["dsr_year"] = dfC['dsr'].map(lambda x: x.year)
#deleting variables not in use
del dfC["dob"], dfC["date_of_birth"], dfC["dsr"], dfC["date_specimen_registered"]

#Merging dfA and dfC
dfD = pd.merge(dfB_names, dfC, on='dummy_id')
#delete patient surname from cdw


dfD1 = pd.merge(dfA, dfD, on='dummy_id')

#two years diference with name baby
dfE = dfD1[(dfD1['time_diference'] <= 2) & dfD1['patient_name_lis'].astype(str).str.contains(r'BABY') ]
#dfE1 = dfD1[(dfD1['time_diference'] == -1)]

#records remaining
dfF = dfD1[~dfD1['dummy_id'].isin(dfE['dummy_id'])]
#del dfF['patient_surname_x'], dfF['patient_surname_y']

dfF.loc[dfF['firstname1'].isnull(), 'firstname1'] = ''
dfF.loc[dfF['firstname2'].isnull(), 'firstname2'] = ''
dfF.loc[dfF['firstname3'].isnull(), 'firstname3'] = ''
dfF.loc[dfF['initial'].isnull(), 'initial'] = ''

#keeping the linkage data on a separate data frame
dfF_linkage_data = pd.concat([dfF['dummy_id'],dfF['surname'],dfF['firstname1'],dfF['firstname2'],dfF['gender'],dfF['initial'],
                              dfF['dob_day'],dfF['dob_month'],dfF['dob_year'],dfF['facility_code'],dfF['date_of_birth']], axis=1)
#changing the index coulm
dfF_linkage_data= dfF_linkage_data.set_index('dummy_id')

#saving the dataset for linkage
dfF_linkage_data.to_csv('for_linkage_data1.csv', sep=',', encoding='utf-8')

##saving the dataset
dfF.to_csv('pp_data1.csv', sep=',', encoding='utf-8')

print('It took', (time.time()-start)/60, 'minutes.')
