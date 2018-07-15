
# coding: utf-8

# In[1]:

import pandas as pd
import numpy as np
import datetime

# Files to read
data_files = ['FILE1', 'FILE11',
              'FILE21', 'FILE22', 
              'FILE31', 'FILE32', 
              'FILE41', 'FILE42', 
              'FILE51', 'FILE52']

# New dict to organize individual file data
data = {}
for file in data_files:
    file_contents = pd.read_csv('{0}.txt'.format(file), delimiter=',')
    data[file] = file_contents

# Check out data
#for table in data:
#    print(table, ':\n-------------------')
#    print(data[table].head(), '\n', data[table].shape, '\n', data[table].dtypes)
#    print('\n\n')


# In[2]:

# Encoding Hebrew to something we can work with
def decode_heb(input_str):
    coded_str = input_str.encode('utf-8')
    coded_str = coded_str.decode('utf-8')
    return coded_str

# Parsing age col to datetime obj
def parse_bday(input_str):
    bday = input_str.split(' ')[0]
    bday = datetime.datetime.strptime(bday, '%d/%m/%Y')
    analysis_date = datetime.datetime(year = 2018, month = 5, day = 1)
    age = analysis_date - bday
    age = age
    return age

# File: 'FILE1'
data['FILE1']['pid'] = data['FILE1']['pID'].astype(str)
data['FILE1']['index'] = 1
data['FILE1']['sex'] = data['FILE1']['SexCode'].apply(decode_heb)
data['FILE1']['sex'] = data['FILE1']['sex'].replace("ז", 1) # male=1
data['FILE1']['sex'] = data['FILE1']['sex'].replace("נ", 0) # female=0
data['FILE1']['sex'] = data['FILE1']['sex'].astype(int)
data['FILE1']['age'] = data['FILE1']['BirthDate'].apply(parse_bday)
data['FILE1']['age'] = data['FILE1']['age'].dt.total_seconds() / (365 * 24 * 60 * 60)
data['FILE1']['origin'] = data['FILE1']['NationID'].apply(decode_heb)
data['FILE1']['origin'] = data['FILE1']['origin'].replace("י", 1) # jewish=1
data['FILE1']['origin'] = data['FILE1']['origin'].replace("ב", 0) # bedouin=0
data['FILE1']['origin'] = data['FILE1']['origin'].astype(int)
data['FILE1']['cntrlpid1'] = data['FILE1']['CntrlpID1'].astype(str)
data['FILE1']['cntrlpid2'] = data['FILE1']['CntrlpID2'].astype(str)
data['FILE1']['cntrlpid3'] = data['FILE1']['CntrlpID3'].astype(str)
data['FILE1']['cntrlpid4'] = data['FILE1']['CntrlpID4'].astype(str)
data['FILE1']['cntrlpid5'] = data['FILE1']['CntrlpID5'].astype(str)
#print('FILE1:\n\n', data['FILE1'].head(), '\n', data['FILE1'].shape, '\n', data['FILE1'].dtypes)

# File: 'FILE11'
data['FILE11']['pid'] = data['FILE11']['pID'].astype(str)
data['FILE11']['index'] = 0
data['FILE11']['sex'] = data['FILE11']['SexCode'].apply(decode_heb)
data['FILE11']['sex'] = data['FILE11']['sex'].replace("ז", 1) # male=1
data['FILE11']['sex'] = data['FILE11']['sex'].replace("נ", 0) # female=0
data['FILE11']['sex'] = data['FILE11']['sex'].astype(int)
data['FILE11']['age'] = data['FILE11']['BirthDate'].apply(parse_bday)
data['FILE11']['age'] = data['FILE11']['age'].dt.total_seconds() / (365 * 24 * 60 * 60)
data['FILE11']['origin'] = data['FILE11']['NationID'].apply(decode_heb)
data['FILE11']['origin'] = data['FILE11']['origin'].replace("י", 1) # jewish=1
data['FILE11']['origin'] = data['FILE11']['origin'].replace("ב", 0) # bedouin=0
data['FILE11']['origin'] = data['FILE11']['origin'].astype(int)
#print('FILE11:\n\n', data['FILE11'].head(), '\n', data['FILE11'].shape, '\n', data['FILE11'].dtypes)


# In[3]:

# Organizing files
cols_for_final_table = ['pid', 'index', 'sex', 'age', 'origin', 
                        'cntrlpid1', 'cntrlpid2', 'cntrlpid3', 'cntrlpid4', 'cntrlpid5', 
                        'diag1-50',
                        'num_enc_total', 'days_enc_total', 'enc1-6', 
                        'clinic_visit_count', 'er_references_count']

# Combining files 1 & 11 to final_table
final_table = data['FILE1'].filter(cols_for_final_table)
final_table = final_table.merge(data['FILE11'].filter(cols_for_final_table), how='outer', sort=False)
final_table = final_table.fillna(0)

#print(final_table.head(), '\n\n', final_table.shape, '\n\n', final_table.dtypes, '\n\n')


# In[4]:

# Chronic conditions (Diags)

# Fixing dtypes of data
#print(data['FILE21'].dtypes, '\n\n', data['FILE22'].dtypes)
data['FILE22']['pID'] = data['FILE22']['pid']

# Files: 'FILE21', 'FILE22'
conditions_combined = data['FILE21']
conditions_combined = conditions_combined.merge(data['FILE22'], how='outer', sort=False)
conditions_combined = conditions_combined.fillna(0)
conditions_combined['pid'] = conditions_combined['pID'].astype(int).astype(str)

# Mapping top chronic conditions to cols in final_table
conditions_combined['orig_cond_diagnosis'] = conditions_combined["אבחנה קבועה מקורית"] # Just for ease of use without Hebrew
conditions_combined = conditions_combined.filter(['pid', 'orig_cond_diagnosis'])
top_conditions = conditions_combined['orig_cond_diagnosis'].value_counts()
condition_cutoff = 50 # cutoff value as discussed
top_conditions = top_conditions[:condition_cutoff]
for condition, num_occurances in top_conditions.items():
    final_table[condition] = 0

# Count each chronic top_condition to relevant col
for pid in final_table['pid']:
    pid_conditions = conditions_combined.loc[conditions_combined['pid'] == pid]
    if (not pid_conditions.empty):
         for condition, num_occurances in top_conditions.items():
                for cond, num in pid_conditions['orig_cond_diagnosis'].value_counts().items():
                    if (condition == cond):
                        final_table[condition].loc[final_table['pid'] == pid] = num

#print(final_table.head())


# In[5]:

# Making sure conditions were written
#for condition, num_occurances in top_conditions.items():
#    print(final_table[condition].value_counts())


# In[7]:

# Encounters (Enc)
# Files: 'FILE51', 'FILE52'
encounters_combined = data['FILE51']
encounters_combined = encounters_combined.merge(data['FILE52'], how='outer', sort=False)
encounters_combined = encounters_combined.fillna(0)
encounters_combined['pid'] = encounters_combined['pID'].astype(int).astype(str)
encounters_combined['EncounterDate'] = encounters_combined['EncounterDate'].astype(str)
encounters_combined['DischargeDate'] = encounters_combined['DischargeDate'].astype(str)

# Parsing 'EncounterDate' and 'DischargeDate' to 'enc_length'
def parse_date(input_str):
    date = datetime.datetime.strptime(input_str, '%d/%m/%Y %H:%M:%S')
    return date
encounters_combined['EncounterDate'] = encounters_combined['EncounterDate'].apply(parse_date)
encounters_combined['DischargeDate'] = encounters_combined['DischargeDate'].apply(parse_date)
encounters_combined['enc_length'] = encounters_combined['DischargeDate'] - encounters_combined['EncounterDate']
encounters_combined['enc_length_in_days'] = encounters_combined['enc_length'].dt.total_seconds() / (24 * 60 * 60) # timedelta to days

encounters_combined = encounters_combined.filter(['pid', 'enc_length_in_days', 'UnitName'])
enc_cols = ['num_enc_total', 'days_enc_total', 'enc_kids', 'enc_chirurgy', 'enc_chirurgy_other', 'enc_er', 'enc_hemato_onc', 'enc_neonatal']
for enc_col in enc_cols:
    final_table[enc_col] = 0
enc_division = {
    'enc_kids' : ["ילדים ד", "ילדים א", "ילדים ב", "ילדים ב' כוויות", "ילדים ד -כוויות", "ילד.ד-א.א.ג."],
    'enc_chirurgy' : ["מח כיר ילדים א", "כירור ילדים"],
    'enc_chirurgy_other' : ["כירור פלסטית", "כירור-פלסטיקה", "כירלד-נוירוכירורגיה", "כירלד-אורתופדית", "כירלד -עיניים", "אף אוזן וגרון", "כירלד -אורולוגית", "עיניים"],
    'enc_er' : ["מח טפול נמרץ ילדים", "טיפול נמרץ ילדים"],
    'enc_hemato_onc' : ["המטואונקולוגיה-ילדים"],
    'enc_neonatal' : ["פגים ב"]
}
for division in enc_division:
    final_table[division + '_days'] = 0

for pid in final_table['pid']:
    pid_encs = encounters_combined.loc[encounters_combined['pid'] == pid]
    if (not pid_encs.empty):
        final_table['num_enc_total'].loc[final_table['pid'] == pid] = pid_encs.shape[0]
        final_table['days_enc_total'].loc[final_table['pid'] == pid] = pid_encs['enc_length_in_days'].sum()
        for idx, row in pid_encs.iterrows():
            for division in enc_division:
                if row['UnitName'] in enc_division[division]:
                    final_table[division].loc[final_table['pid'] == pid] += 1
                    final_table[division + '_days'].loc[final_table['pid'] == pid] += row['enc_length_in_days']


# In[8]:

# Clinics
# Files: 'FILE31', 'FILE32'
clinic_combined = data['FILE31']
clinic_combined = clinic_combined.merge(data['FILE32'], how='outer', sort=False)
clinic_combined = clinic_combined.fillna(0)
clinic_combined['pid'] = clinic_combined['pID'].astype(int).astype(str)

final_table['num_clinic_visits'] = 0

# Count num of clinic visits for each pid
for pid in final_table['pid']:
    pid_clinics = clinic_combined.loc[clinic_combined['pid'] == pid]
    if (not pid_clinics.empty):
         final_table['num_clinic_visits'].loc[final_table['pid'] == pid] = pid_clinics.shape[0]


# In[9]:

# Emergency department (ER)
# Files: 'FILE41', 'FILE42'
er_combined = data['FILE41']
er_combined = er_combined.merge(data['FILE42'], how='outer', sort=False)
er_combined = er_combined.fillna(0)
er_combined['pid'] = er_combined['pID'].astype(int).astype(str)

final_table['num_er_visits'] = 0

# Count num of clinic visits for each pid
for pid in final_table['pid']:
    pid_er = er_combined.loc[er_combined['pid'] == pid]
    if (not pid_er.empty):
         final_table['num_er_visits'].loc[final_table['pid'] == pid] = pid_er.shape[0]


# In[10]:

# Output as csv file
final_table.to_csv('output.csv')