import numpy as np
import pandas as pd

def get_typed_option(opt):
    opt = opt.strip()
    try:
        return float(opt)
    except ValueError as e:
        print(opt)
        return opt

def get_data_dict_options_map(change_df, variable):
    choices_str = change_df.xs(variable)['opt_replacements']
    options = [ choice.split(',', 1) for choice in choices_str.split('|') ]
    options = [ opt for opt in options if opt != ['']] # handle extra '|' at end of choice specification
    replacement_map = { option[0].strip(): option[-1].strip() for option in options }
    return replacement_map
    #return { get_typed_option(opt[0]): get_typed_option(opt[-1]) for opt in options }


def replace_values(df, change_df, variable):
    replacement_map = get_data_dict_options_map(change_df, variable)
    print(variable, replacement_map)
    df[variable] = df[variable].replace(replacement_map)
    return df


def get_complete_varlist(df, varlist):
    new_varlist = []
    for var in varlist:
        new_varlist += [ col for col in df.columns if col == var or col.startswith(var + '___') ]
    return new_varlist


datafile = r'H:\H\Users\Haley Acevedo\WolframSyndromeClini_DATA_2019-06-07_1431.csv'

df = pd.read_csv(datafile, dtype=object)
df = df[df['study_id'].str.contains('WOLF_\d{4}_.+')] # remove Test and Wolf_AN rows # FIXME
df['redcap_event_name'] = df['redcap_event_name'].str.replace('wolframclinic_', '')
df = df.set_index(['study_id', 'redcap_event_name'])
change_df = pd.read_csv(r'C:\Users\acevedoh\Documents\new_wfs_db_variables.csv')
dd_df = pd.read_csv(r'H:\H\Wolfram Research Clinic\All_Data\REDCap database materials\ITRACKTrackingNeurodegeneratio_DataDictionary_2019-06-06.csv')

checkbox_cols = dd_df[dd_df['Field Type'] == 'checkbox']['Variable / Field Name'].values

# change variable names
change_df['new_var'] = change_df['new_var'].fillna(change_df['old_var'])
rename_map = {}
for idx, row in change_df[pd.notnull(change_df['new_var'])].iterrows():
    # if checkbox, then iterate over all matching columns to create rename map entries
    if row['new_var'] in checkbox_cols:
        var_cols = [ col for col in df.columns if col.startswith(row['old_var'] + '___')]
        for col in var_cols:
            rename_map[col] = col.replace(row['old_var'], row['new_var'])
    else:
        rename_map[row['old_var']] = row['new_var']
df = df.rename(columns=rename_map)

# get columns to drop
drop_vars = change_df[change_df['drop'] == 1]['new_var'].values
drop_cols = []
for var in drop_vars:
    drop_cols += [ col for col in df.columns if col == var or col.startswith(var + '___') ]
drop_cols += list(dd_df[dd_df['Field Type'] == 'calc']['Variable / Field Name'].values)
df = df.drop(columns=drop_cols, errors='ignore')

# replace variable values
replace_value_vars = list(change_df.dropna(subset=['opt_replacements'])['new_var'].values)
change_df = change_df.set_index('new_var')
for var in replace_value_vars:
    if not var in df:
        continue
    df = replace_values(df, change_df, var)

df = df.drop(columns=['mri_contraindication AND mri_other'])

## Handle stable_char special cases

# backfill stable demographic information
demo_vars = get_complete_varlist(df, dd_df[dd_df['Form Name'] == 'patient_demographics']['Variable / Field Name'].values)
df[demo_vars] = df.groupby('study_id')[demo_vars].apply(lambda x: x.bfill()) # back fill demographics form

stable_char_forms = [ 'patient_demographics', 'ses_related_variables', 'clinical_mutations', 'clinical_dx_summary', 'parent_wtar', 'medical_history']
stable_vars = [ form + '_complete' for form in stable_char_forms if form + '_complete' in df.columns ] + \
    get_complete_varlist(df, dd_df[dd_df['Form Name'].isin(stable_char_forms)]['Variable / Field Name'].values)

df.loc[~df.index.isin(['stable_patient_cha_arm_1'], level=1), stable_vars] = np.nan

text_cols = get_complete_varlist(df, dd_df[dd_df['Field Type'] == 'text']['Variable / Field Name'].values)
for col in text_cols:
    try:
        if pd.to_numeric(df[col], errors='coerce').isnull().all() or col.startswith('compass31'):
            continue
        df[col] = pd.to_numeric(df[col], errors='coerce')
        if all(x.is_integer() or pd.isnull(x) for x in df[col]):
            df[col] = df[col].astype('Int64')
    except TypeError as e:
        print(col, e)
        pass

df.to_csv(r'H:\H\Users\Haley Acevedo\WolframSyndromeClini_DATA_2019-06-07_1431_migration.csv')
