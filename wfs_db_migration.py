import argparse
import numpy as np
import os
import pandas as pd
import re


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


def replace_values(df, change_df, index_col='new_var'):
    replace_value_vars = list(change_df.dropna(subset=['opt_replacements'])[index_col].values)
    change_df = change_df.set_index(index_col)
    for var in replace_value_vars:
        if not var in df:
            continue
        replacement_map = get_data_dict_options_map(change_df, var)
        print(var, replacement_map)
        df[var] = df[var].replace(replacement_map)
    return df


def get_complete_varlist(df, varlist):
    new_varlist = []
    for var in varlist:
        new_varlist += [ col for col in df.columns if col == var or col.startswith(var + '___') ]
    return new_varlist


def merge_columns(df, var_dict):
    for k,v in var_dict.items():
        print(k,v)
        if k == 'pan_chorea':
            chorea_arm_cols = [ col for col in v if re.search('_(l|r)', col) ]
            chorea_tng_col = [ col for col in v if col not in chorea_arm_cols ]
            df[k] = np.where(df[chorea_arm_cols].astype(float).sum(axis=1, min_count=1) > 0, 2, 0)
            df[k] = df.apply(lambda x: x[k] + x[chorea_tng_col].astype(float), axis=1).astype('Int64')
        else:
            df[k] = df[v].astype(float).any(axis=1)
            df[k] = df[k].mask(pd.isnull(df[v].astype(float)).all(axis=1)).astype('Int64') # mark rows that were all null as NaN instead of False

        df = df.drop(columns=v)
    return df


def migrate(data_file, var_file):
    for (datafile, varfile) in zip(data_file, var_file):
        print(datafile, varfile)
        df = pd.read_csv(datafile, dtype=object)
        df = df[df['study_id'].str.contains('WOLF_\d{4}_.+')] # remove Test and Wolf_AN rows # FIXME
        df['redcap_event_name'] = df['redcap_event_name'].str.replace('wolframclinic_', '')
        df = df.set_index(['study_id', 'redcap_event_name']).dropna(how='all')
        change_df = pd.read_csv(varfile)
        dd_df = pd.read_csv(r'H:\H\Wolfram Research Clinic\All_Data\REDCap database materials\ITRACKTrackingNeurodegeneratio_DataDictionary_2019-06-06.csv')

        # get columns that will need to merged (should be exluded from renaming step)
        merge_dict = change_df[pd.notnull(change_df['merge_var'])].groupby('merge_var')['old_var'].apply(list).to_dict()
        merge_cols = [ item for sublist in merge_dict.values() for item in sublist ]
        print(merge_dict)
        print('pan_choreiform_invol_mov_right' in df.columns)

        # change variable names
        checkbox_cols = dd_df[dd_df['Field Type'] == 'checkbox']['Variable / Field Name'].values
        change_df['new_var'] = change_df['new_var'].fillna(change_df['old_var'])
        rename_map = {}
        for idx, row in change_df[pd.notnull(change_df['new_var'])].iterrows():
            if row['new_var'] in merge_dict.keys():
                continue

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
        drop_cols += [ col for col in df.columns if col not in dd_df['Variable / Field Name'].values and col not in merge_cols ]
        df = df.drop(columns=drop_cols, errors='ignore')

        # replace variable values
        df = replace_values(df, change_df)

        df = df.drop(columns=['mri_contraindication AND mri_other'], errors='ignore')

        # merge L/R that are now overall y/n
        df = merge_columns(df, merge_dict)

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

        df.dropna(how='all').to_csv('{}_migration.csv'.format(os.path.splitext(datafile)[0]))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--datafile', nargs='+', required=True)
    parser.add_argument('--varfile', nargs='+', required=True)
    args = parser.parse_args()

    if len(args.datafile) != len(args.varfile):
        parser.error('Must provide equal number of data and variable files')

    migrate(args.datafile, args.varfile)
