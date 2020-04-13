import numpy as np
import pandas as pd
import re

df = pd.read_csv(r'C:\Users\acevedoh\Downloads\ITRACKTrackingNeurod_DATA_2019-07-03_0907.csv')

df = df.set_index(['study_id', 'redcap_event_name'])
subs, _ = df.index.levels

new_index = pd.MultiIndex.from_product([subs, ['2018_arm_1', '2019_arm_1']])
df = df.reindex(new_index).reset_index().rename(columns={'level_0': 'study_id', 'level_1': 'redcap_event_name'})

df['start_year'] = df['study_id'].apply(lambda x: re.match(r'WOLF_(\d{4})_', x).group(1))
df['clinic_year'] = df['redcap_event_name'].apply(lambda x: re.match(r'(\d{4})_arm_1', x).group(1))

df['wolfram_sessionnumber'] = (df['clinic_year'].astype(int) - df['start_year'].astype(int)) + 1
df['wolfram_sessionnumber'] = df['wolfram_sessionnumber'].replace(0, np.nan)
print(df)

df.to_csv('wfs_2019_sess_import.csv')
