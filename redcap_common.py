import numpy as np
import pandas as pd
from pandas.api.types import is_numeric_dtype
import re

from getpass import getpass
from itertools import groupby, chain
from redcap import Project
from subprocess import Popen
from sys import exit, stderr

# API constants
DB_PATH = r'//neuroimage.wustl.edu/nil/hershey/H/REDCap Scripts/api_tokens.accdb'
URL = 'https://redcap.wustl.edu/redcap/srvrs/prod_v3_1_0_001/redcap/api/'

STUDY_ID = 'study_id'
SESSION_YEAR = 'session_year'
SESSION_NUMBER = 'session_number'
DOB = 'dob'
SESSION_DATE = 'session_date'
SESSION_AGE = 'session_age'

COMMON_COLS = [STUDY_ID, SESSION_YEAR, DOB, SESSION_DATE, SESSION_NUMBER]

def create_df(input_file):
    return pd.read_csv(input_file)


# Rename common columns used in shared functions
def rename_common_columns(df, renames, reset):
    old_cols = COMMON_COLS if reset else renames
    new_cols = renames if reset else COMMON_COLS
    rename_dict = { old_col: new_col for old_col, new_col in zip(old_cols, new_cols) if old_col is not None and new_col is not None }
    return df.rename(columns=rename_dict)


# create separate dataframe with demographic/diagnosis info from API export
def get_redcap_project(project, api_token=None):
    if not api_token:
        print('\nRequested action requires API access. Enter API token to continue.')
        api_token = getpass()

    project = Project(URL, api_token)
    return project


def merge_api_data(df, project, fields, left_merge_fields):
    project = project if project else get_redcap_project()
    demo_dx_df = project.export_records(fields=fields, format='df')
    df = df.merge(demo_dx_df, how='left', left_on=left_merge_fields, right_index=True, suffixes=('_original', ''))
    return df


def cleanup_api_merge(df, fields):
    df = df.drop(fields, axis=1, errors='ignore') # supress error if field already has been removed from df (i.e. dropped because entirely empty)
    df = df.rename(columns={ col + '_original': col for col in fields if col + '_original' in df.columns})
    return df


def string_to_float(s):
    try:
        return float(s)
    except (ValueError, TypeError):
        return np.nan


# Helper function to calculate fractional age (date1 should be dob)
def get_age(date1, date2):
    return (date2 - date1).days / float(365.25)


def prepare_age_calc(df):
    # put new session_age right after session_date in df
    df[SESSION_DATE] = pd.to_datetime(df[SESSION_DATE], errors='coerce')
    df[DOB] = pd.to_datetime(df.groupby([STUDY_ID])[DOB].transform(lambda x: x.loc[x.first_valid_index()] if x.first_valid_index() is not None else np.nan)) # fills in dob for missing years using first-found dob for participant
    session_age_column = df.apply(lambda x: get_age(x[DOB], x[SESSION_DATE]), axis=1)
    df.insert(df.columns.get_loc(SESSION_DATE), SESSION_AGE, session_age_column)
    return df


# Determine age at diagnosis
# Uses birthday, session date, session age and diagnosis age
def get_diagnosis_age(row, dx_vars):
    # diagnosis_date = row[dx_vars['dx_date']]
    # diagnosis_age = np.nan
    diagnosis_age = row[dx_vars['dx_age']]

    # if no date was provided, fall back on age (if provided)
    # if pd.isnull(diagnosis_date):
    #    diagnosis_age = string_to_float(row[dx_vars['dx_age']]) # float since we may have fractional age
    #else:
    #    try:
    #        diagnosis_age = get_age(row['dob'], diagnosis_date)
    #    except ValueError:
    #        pass

    row['dx_age'] = diagnosis_age
    return row[[STUDY_ID, 'dx_age']]


# Determines duration of diagnosis for each participant at each session_number
# Relies on precalculated diagnosis age (based on single year (2016) diagnosis information due to database structure)
def calculate_diagnosis_duration(group, dx_type, dx_age_df, age_field):
    diagnosis_age = dx_age_df.loc[dx_age_df[STUDY_ID] == group.name]['dx_age']
    if diagnosis_age.values:
        new_column = '_'.join(['dx', dx_type, 'duration'])
        group[new_column] = group[age_field] - float(diagnosis_age.values[0]) # duration is session age minus diagnosis age

    return group


# Filter down to consecutive year data (at least n years) for participants

#  default params (added to make generalizable to other scripts (finding "best" range across variables, optic nerve special condition, etc.)):
#   - skip, number between years to look for
#   - keep_all, return all rows that are consecutive (instead of just the first n)
#   - return_rows, return row numbers in instead of actual rows
def get_consecutive_years(group, n, skip=1, keep_all=False, return_rows=False):
    # for REDCap longitudinal studies - there may be a year-agnostic event that should still be included in output,
    #   but should not be considered when calculating consecutive years
    #   -- if it exists, shift indices accordingly and add it in before returning
    year_agnostic_row = pd.isnull(group.iloc[0][SESSION_YEAR])

    # remove participant data if they haven't been even involved for n years
    if (len(group) - year_agnostic_row) < n:
        return

    # get list of differences in years between each session for a participants
    # identify consecutive duplicates in that list (https://stackoverflow.com/questions/6352425/whats-the-most-pythonic-way-to-identify-consecutive-duplicates-in-a-list)
    #   -- only consider rows with non-null years for calculation
    diff_count = [ (k, sum(1 for i in g)) for k,g in groupby(np.diff(group[pd.notnull(group[SESSION_YEAR])][SESSION_YEAR].astype(int).values)) ]

    index = 0 + year_agnostic_row
    consecutive_rows = [] if not year_agnostic_row else [0] # add first row if it's year-agnostic
    prev_consecutive = False
    # iterate through to identify rows that make up consecutive ranges
    for k, count in diff_count:
        if prev_consecutive: # if the previous one was consecutive, we know this one is not consecutive
            index += count-1
            prev_consecutive = False
        elif k != skip or count < n-1: # not consecutive if the difference between years was greater than 1 or if the range did not meet the specified length
            index += count
        else:
            prev_consecutive = True
            range_end = index + count + 1
            consecutive_rows += range(index, range_end, 1)
            index = range_end

    rows = consecutive_rows[:n] if not keep_all else consecutive_rows
    return group.iloc[rows,:] if not return_rows else rows


def get_matching_columns(columns, pattern):
    return [ col for col in columns if re.match(pattern, col) ]


def get_variable_column_lists(columns, variables, project):
    # make sure that all the required variables actually appear in dataset (otherwise all will be filtered out)
    missing_args = [ var for var in variables if not get_matching_columns(columns, var) ]
    if missing_args:
        stderr.write('Specified variable(s) not included in data export: {}'.format(missing_args))
        exit(1)

    exact_match_columns, other_columns = [], []
    for var in variables:
        if var in columns: # if variable name exactly matches column label
            exact_match_columns.append(var)
            continue

        if var in project.forms:
            other_columns.append([ field['field_name'] for field in project.export_metadata(forms=[var]) ])
        else:
            other_columns.append(get_matching_columns(columns, var))

    # if no data exists for a field, it's automatically dropped from output, so remove these from our search
    other_columns = [ [ col for col in l if col in columns ] for l in other_columns ]
    return exact_match_columns, other_columns



def check_for_all(df, all_vars, project, cast_numeric=False):
    exact_match_columns, other_columns = get_variable_column_lists(df.columns, all_vars, project)
    df = df.drop(df[df[exact_match_columns].apply(lambda x: pd.to_numeric(x, errors='coerce') if cast_numeric else x).isnull().any(axis=1)].index)
    # for all, we have to make sure each var's column isn't all null
    for col_list in other_columns:
        df = df.drop(df[df[col_list].apply(lambda x: pd.to_numeric(x, errors='coerce') if cast_numeric else x).isnull().all(axis=1)].index)
    return df


def check_for_any(df, any_vars, project, cast_numeric=False):
    exact_match_columns, other_columns = get_variable_column_lists(df.columns, any_vars, project)
    # flatten because for 'any' we can search all the columns together (just one overall has to be non-null)
    flattened_other_columns = list(chain.from_iterable(other_columns))
    df = df.drop(df[
        df[exact_match_columns].apply(lambda x: pd.to_numeric(x, errors='coerce') if cast_numeric else x).isnull().all(axis=1) & \
        df[flattened_other_columns].apply(lambda x: pd.to_numeric(x, errors='coerce') if cast_numeric else x).isnull().all(axis=1)
    ].index)
    return df


# re-shape dataframe such that there is one row per participant (each row contains all sessions)
def flatten(df, flatten_by_column, sort=True, prefix=''):
    # assume session_number = 0 is "stable" arm (things that don't change over time)
    # assume session_number > 0 is some longitudinal variable (like session number or clinic year)
  
    # select session_number = 0 into df_stable
    
    if is_numeric_dtype(df[flatten_by_column]):
        df_stable = df[df[flatten_by_column]==0]
    else:
        df_stable = df[df[flatten_by_column]=='0']

    # drop blank columns???
    df_stable = df_stable.dropna(axis=1, how='all')
    
    df_stable.set_index([STUDY_ID, flatten_by_column], inplace=True)
    df_stable = df_stable.unstack()

    # select session_number != 0 into df_long
    if is_numeric_dtype(df[flatten_by_column]):
        df_long = df[df[flatten_by_column]>0]
    else:
        df_long = df[df[flatten_by_column]!='0']

    # drop blank columns???
    df_long = df_long.dropna(axis=1, how='all')

    df_long.set_index([STUDY_ID, flatten_by_column], inplace=True)
    df_long = df_long.unstack()

    if sort:
        df_stable = df_stable.sort_index(1, level=1)
        df_long = df_long.sort_index(1, level=1)
    df_stable.columns = [ '_'.join([prefix + str(tup[1]), tup[0]]) for tup in df_stable.columns ] # append unstacked index to front of column name
    df_long.columns = [ '_'.join([prefix + str(tup[1]), tup[0]]) for tup in df_long.columns ]

    # merge stable and long back together
    # Use a left join so participants with stable (session 0) data but no longitudinal
    # sessions are retained instead of being dropped (previously 'inner' dropped them).
    df = pd.merge(df_stable, df_long, how='left', on=STUDY_ID)

    # remove s0_clinic_year and s0_session_number
    df = df.drop(['{}0_clinic_year'.format(prefix)], axis=1, errors='ignore')
    df = df.drop(['{}0_wolfram_sessionnumber'.format(prefix)], axis=1, errors='ignore')

    return df

# re-shape dataframe such that there is one row per participant (each row contains all sessions)
def simple_flatten(df, sort=True, prefix=''):
    df = df.unstack()
    if sort:
        df = df.sort_index(axis=1, level=1)
    df.columns = [ '_'.join([prefix + str(tup[1]), tup[0]]) for tup in df.columns ] # append unstacked index to front of column name
    df = df.dropna(axis=1, how='all')
    return df

# reshape dataframe such that there is one row per participant per session
def expand(df):
    non_session_cols = { col: 's1_' + col for col in df.columns if not re.match(r's\d_', col) }
    df.rename(columns=non_session_cols, inplace=True)

    # extract session number as new index level and then reshape to have one row per subject per session
    df.columns = df.columns.str.split('_', n=1, expand=True)
    df = df.stack(level=0).reset_index()  # creates new level and labels it 'level_1' (actually represents session_number)
    return df.rename(columns={'level_0': STUDY_ID, 'level_1': SESSION_NUMBER}), non_session_cols


def write_results_and_open(df, output_file):
    try:
        df.to_csv(output_file)
        Popen(output_file, shell=True)
    except PermissionError:
        stderr.write('Output file is currently open. Please close the file before trying again.')
        exit(1)
