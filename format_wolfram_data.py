import argparse
import datetime
import re
import time
import pyodbc

import numpy as np
import pandas as pd

from getpass import getpass, getuser
from itertools import chain, groupby
from redcap import Project, RedcapError
from subprocess import Popen
from sys import exit, stderr

# API constants
DB_PATH = 'H:/REDCap Scripts/api_tokens.accdb'
URL = 'https://redcap.wustl.edu/redcap/srvrs/prod_v3_1_0_001/redcap/api/'

# Redcap constants
STUDY_ID = 'study_id'
CLINIC_YEAR = 'wfs_clinic_year'
SESSION_NUMBER = 'wolfram_sessionnumber'

ALL_DX_TYPES = ['wfs', 'dm', 'di', 'hearloss', 'oa', 'bladder']
NON_DX_FIELDS_FOR_DURATION = ['dob', 'clinic_date']

def string_to_float(s):
    try:
        return float(s)
    except (ValueError, TypeError):
        return np.nan


def get_dx_column(dx_type, measure):
    return '_'.join(['clinichx', 'dx', dx_type, measure])


# Determine date of diagnosis based on information provided
# Uses actual date if provided, otherwise falls back on year and month and/or day
def get_diagnosis_date(row, dx_type):
    date_col = get_dx_column(dx_type, 'date')
    try:
        return pd.to_datetime(row[date_col])
    except (ValueError, TypeError):
        pass

    year_col = get_dx_column(dx_type, 'year')
    if row[year_col].isdigit():
        year = int(row[year_col])
        try:
            month = datetime.datetime.strptime(row[get_dx_column(dx_type, 'mnth')], '%b').month
            day = row[date_col] if row[date_col].isdigit() else 1 # date column filled out as date or day
            return datetime.datetime(year, month, day)
        except:
            pass

    return np.nan


def get_age(date1, date2):
    return (date2 - date1).days / 365


# Determine age at diagnosis
# Uses birthday and diagnosis date if possible, otherwise uses provided age
def get_diagnosis_age(row, dx_type):
    diagnosis_date = get_diagnosis_date(row, dx_type)
    diagnosis_age = np.nan

    # if no date was determined, fall back on age (if provided)
    if pd.isnull(diagnosis_date):
        diagnosis_age = string_to_float(row[get_dx_column(dx_type, 'age')]) # float since we may have fractional age
    else:
        try:
            diagnosis_age = get_age(row['dob'], diagnosis_date)
        except ValueError:
            pass

    row['dx_age'] = diagnosis_age
    return row[[STUDY_ID, 'dx_age']]


# Determines duration of diagnosis for each participant at each session_number
# Relies on precalculated diagnosis age (based on single year (2016) diagnosis information due to database structure)
def calculate_diasnosis_duration(group, dx_type, dx_age_df):
    diagnosis_age = dx_age_df.loc[dx_age_df[STUDY_ID] == group.name]['dx_age']
    if diagnosis_age.values:
        new_column = '_'.join(['dx', dx_type, 'duration'])
        group[new_column] = group['session_age'] - float(diagnosis_age.values[0]) # duration is session age minus diagnosis age

    return group


# Filter down to consecutive year data (at least n years) for participants
# Keeps all consecutive ranges of data for each particpant
def get_consecutive_years(group, n):
    # remove participant data if they haven't been even involved for n years
    if len(group) < n:
        return

    # get list of differences in years between each session for a participants
    # identify consecutive duplicates in that list (https://stackoverflow.com/questions/6352425/whats-the-most-pythonic-way-to-identify-consecutive-duplicates-in-a-list)
    diff_count = [ (k, sum(1 for i in g)) for k,g in groupby(np.diff(group[CLINIC_YEAR].values)) ]

    index = 0
    consecutive_rows = []
    prev_consecutive = False
    # iterate through to identify rows that make up consecutive ranges
    for k, count in diff_count:
        if prev_consecutive: # if the previous one was consecutive, we know this one is not consecutive
            index += count-1
            prev_consecutive = False
        elif k != 1 or count < n-1: # not consecutive if the difference between years was greater than 1 or if the range did not meet the specified length
            index += count
        else:
            prev_consecutive = True
            range_end = index + count + 1
            consecutive_rows += range(index, range_end)
            index = range_end

    return group.iloc[consecutive_rows,:] # return only rows that were in consecutive ranges


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


# create separate dataframe with demographic/diagnosis info from API export
def get_redcap_project():
    print('\nRequested action requires API access. Enter access database password to continue.')

    try:
        conn_str = (
            r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};'
            r'DBQ=' + DB_PATH + ';'
            r'PWD=' + getpass()
        )
        conn = pyodbc.connect(conn_str)
    except pyodbc.Error:
        stderr.write('Entered incorrect password for database.')
        exit(1)

    cursor = conn.cursor()
    sql = 'SELECT api_token FROM wolfram_api_tokens WHERE userid = ?'
    cursor.execute(sql, (getuser(),))
    api_token = cursor.fetchone()[0]
    project = Project(URL, api_token)
    return project


def format_wolfram_data():
    # set up expected arguments and associated help text
    parser = argparse.ArgumentParser(description='Formats data from REDCap csv export')
    parser.add_argument('input_file', help='exported file to be formatted')
    parser.add_argument('output_file', help='full filepath where formatted data should be stored (if file does not exist in location, it will be created)')
    parser.add_argument('-c', '--consecutive', type=int, metavar='num_years', help='limit data to particpants with data for a number of consecutive years')
    parser.add_argument('-s', '--by-session', action='store_true', help='organize data by session (default is by year)')
    parser.add_argument('-f', '--flatten', action='store_true', help='arrange all session data in single row for participant (default is one row per session)')
    parser.add_argument('-d', '--duration', nargs='*', metavar='dx_type', dest='dx_types', default=None, choices=ALL_DX_TYPES, help='calculate diagnosis duration for specified diagnosis types (all if none specified)')
    parser.add_argument('-t', '--transpose', action='store_true', help='transpose the data')
    parser.add_argument('--all', nargs='+', metavar='var', default=None, help='limit data to participants with data (in export) for every specified variables (can be category, column prefix, or specific variable)')
    parser.add_argument('--any', nargs='+', metavar='var', default=None, help='limit data to participants with data (in export) for at least one of the specified variables (can be category, column prefix, or specific variable)')
    args = parser.parse_args()

    if not args.input_file.endswith('.csv') or not args.output_file.endswith('.csv'):
        parser.error('Input and output files must be of type csv')

    # create dataframe from REDCap data
    df = pd.read_csv(args.input_file)
    df = df[df.study_id.str.contains('WOLF_\d{4}_.+')] # remove Test and Wolf_AN rows

    # only create API project if actions require it and data needed is not already present
    project = None
    fields = [SESSION_NUMBER] if SESSION_NUMBER not in df.columns else [] # always need to get session number if not in data (used to determine which rows to keep)
    if any(arg is not None for arg in [args.dx_types, args.all, args.any]): # all of these args require api project info
        project = project if project else get_redcap_project()
        if args.dx_types is not None:
            fields += NON_DX_FIELDS_FOR_DURATION
            fields += get_matching_columns(project.field_names, 'clinichx_dx_') # if doing date calculation, always bring in all dates to prevent possible date-shift errors
    if fields:
        project = project if project else get_redcap_project()
        demo_dx_df = project.export_records(fields=fields, format='df')
        df = df.merge(demo_dx_df, how='left', left_on=[STUDY_ID, 'redcap_event_name'], right_index=True, suffixes=('_original', ''))

    # if clinic year is already in data, drop it - will extract year from redcap_event_name instead
    # a bit redundant, but makes the diagnosis duration logic more managable (clinic year is only
    #   specified for years attended, but dx info is in 2016 always - need to be able to indentify this)
    if CLINIC_YEAR in df.columns:
        df.drop([CLINIC_YEAR], inplace=True, axis=1)
    df.rename(columns={ 'redcap_event_name': CLINIC_YEAR }, inplace=True)
    df[CLINIC_YEAR] = df[CLINIC_YEAR].str.extract('(\d{4})', expand=False).astype(int)

    num_clinic_years = len(df[CLINIC_YEAR].unique()) # FIXME: should be counting max number of sessions for participants (still may cause error because they might not be consecutive)
    if args.consecutive is not None and args.consecutive not in range(2, num_clinic_years + 1):
        parser.error('Consecutive years must be greater than 1 and cannot exceed number of clinic years ({})'.format(num_clinic_years))

    # Temporarily update rows for 2016 that have no session number to be -1 (will remove after calculation)
    df.loc[(df[CLINIC_YEAR] == 2016), [SESSION_NUMBER]] = df.loc[(df[CLINIC_YEAR] == 2016), [SESSION_NUMBER]].fillna(-1)
    df = df[pd.notnull(df[SESSION_NUMBER])] # remove other rows for non-attended sessions
    df[SESSION_NUMBER] = df[SESSION_NUMBER].astype(int) # once NANs are gone, we can cast as int (nicer for flatten display)

    # if duration argument specified, calculate diagnosis duration for types specified or all (if none specified)
    if args.dx_types is not None: # explicit None check because empty array is valid
        df['dob'] = pd.to_datetime(df.groupby([STUDY_ID])['dob'].transform(lambda x: x.loc[x.first_valid_index()])) # fills in dob for missing years using first-found dob for participant
        df['session_age'] = df.apply(lambda x: get_age(x['dob'], pd.to_datetime(x['clinic_date'])), axis=1)
        dx_types = args.dx_types if args.dx_types else ALL_DX_TYPES
        for dx_type in dx_types:
            dx_age_df = df.loc[df[CLINIC_YEAR] == 2016].apply(get_diagnosis_age, args=(dx_type,), axis=1)
            df = df.groupby([STUDY_ID]).apply(calculate_diasnosis_duration, dx_type, dx_age_df)


    # after calculation, we can remove the 2016 rows for participants who did not attend (reassigned earlier to session_id -1)
    df = df[df[SESSION_NUMBER] != -1]

    # if we have brought in dx info/demographics from the API, remove it after the calculation and rename columns that were suffixed due to merge
    if len(fields) > 1: # don't need to go through deletion logic if only field is session number
        demo_dx_drop_cols = [ col for col in demo_dx_df.columns if col not in [CLINIC_YEAR, SESSION_NUMBER] ]
        df = df.drop(demo_dx_drop_cols, axis=1)
        suffixed_columns = { col: col[:-9] for col in df.columns if col.endswith('_original') }
        df.rename(columns=suffixed_columns, inplace=True)

    # if varaibles are specified, filter out rows that don't have data for them (if null or non-numeric)
    if args.all:
        exact_match_columns, other_columns = get_variable_column_lists(df.columns, args.all, project)
        df = df.drop(df[df[exact_match_columns].apply(lambda x: pd.to_numeric(x, errors='coerce')).isnull().any(axis=1)].index)
        # for all, we have to make sure each var's column isn't all null
        for col_list in other_columns:
            df = df.drop(df[df[col_list].apply(lambda x: pd.to_numeric(x, errors='coerce')).isnull().all(axis=1)].index)
    if args.any:
        exact_match_columns, other_columns = get_variable_column_lists(df.columns, args.any, project)
        # flatten because for 'any' we can search all the columns together (just one overall has to be non-null)
        flattened_other_columns = list(chain.from_iterable(other_columns))
        df = df.drop(df[
            df[exact_match_columns].apply(lambda x: pd.to_numeric(x, errors='coerce')).isnull().all(axis=1) & \
            df[flattened_other_columns].apply(lambda x: pd.to_numeric(x, errors='coerce')).isnull().all(axis=1)
        ].index)

    # remove session data for participants that did not occur in consecutive years
    if args.consecutive:
        df = df.groupby([STUDY_ID]).apply(get_consecutive_years, args.consecutive)

    if df.empty:
        stderr.write('No data to return. Selections have filtered out all rows.')
        exit(1)

    index_cols = [STUDY_ID, SESSION_NUMBER] if args.by_session else [STUDY_ID, CLINIC_YEAR]
    df.set_index(index_cols, inplace=True)

    # puts all sessions/clinic years for a participant on one line (suffixed with year/session)
    if args.flatten:
        df = df.unstack().sort_index(1, level=1)
        df.columns = [ '_'.join(map(str,i)) for i in df.columns ]

    if args.transpose:
        df = df.transpose()

    try:
        df.to_csv(args.output_file)
        Popen(args.output_file, shell=True)
    except PermissionError:
        stderr.write('Output file is currently open. Please close the file before trying again.')
        exit(1)


format_wolfram_data()
