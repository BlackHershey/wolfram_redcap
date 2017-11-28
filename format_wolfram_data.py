import argparse # availble only for python 3.x
import datetime
import re
import numpy as np
import pandas as pd

from itertools import groupby
from sys import argv

STUDY_ID = 'study_id'
CLINIC_YEAR = 'wfs_clinic_year'
SESSION_NUMBER = 'wolfram_sessionnumber'
ALL_DX_TYPES = ['wfs', 'dm', 'di', 'hearloss', 'oa', 'bladder']

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
            dob = pd.to_datetime(row['dob'])
            diagnosis_age = (diagnosis_date - dob).days / 365
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
        group[new_column] = group['wolf_age'] - float(diagnosis_age.values[0]) # duration is session age minus diagnosis age

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
    delete_rows = []
    # iterate through to identify rows that make up consecutive ranges
    for k, count in diff_count:
        # not considered consecutive if the difference between years was greater than 1 or if the range did not meet the specified length
        if k != 1 or count < n-1:
            delete_rows += range(index, index + count + 1)
            index += count # add count because we still want to check if last index in the range is the start of a new consecutive range
        else:
            if index in delete_rows:
                delete_rows.remove(index) # if a consecutive range is preceded by a non-consecutive one, we need to remove the index from the deletion list
            index += count + 1 # add count+1 because we know the last index is not consecutive with next one

    if delete_rows:
        group = group.drop(group.index[delete_rows])

    return group


def get_matching_columns(columns, pattern):
    return [ col for col in columns if re.match(pattern, col) ]


def get_column_lists_for_variables(columns, variables):
    var_options = '|'.join(variables)
    var_completed_columns = get_matching_columns(columns, '(' + var_options + ')(_completed|complete|_collected)') # get columns indicating form completion
    var_columns = get_matching_columns(columns, var_options) # get exact column name matches or column prefix matches
    return var_completed_columns, var_columns


# TODO: either delete FALSE 'meet_reqs' row when done (and meets reqs col) OR figure out how to delete in place
def has_required_variables(row, all, var_completed_columns, var_columns):
    col = 'meets_reqs'
    row[col] = True

    if all and not row[var_completed_columns].isin([1]).all():
        row[col] = False
    elif not all and not row[var_completed_columns].isin([1]).any():
        row[col] = False

    if not all and row[var_columns].isnull().all():
        row[col] = False
    elif all and row[var_columns].isnull().values.any():
        row[col] = False

    return row


def format_wolfram_data():
    # set up expected arguments and associated help text
    parser = argparse.ArgumentParser(description='Formats data from REDCap csv export')
    parser.add_argument('file', type=argparse.FileType('r', encoding='utf-8-sig'), help='exported file to be formatted') # utf-8-sig encoding to remove UTF-8 byte order mark
    parser.add_argument('-c', '--consecutive', type=int, metavar='num_years', help='limit data to particpants with data for a number of consecutive years')
    parser.add_argument('-s', '--by-session', action='store_true', help='organize data by session (default is by year)')
    parser.add_argument('-f', '--flatten', action='store_true', help='arrange all session data in single row for participant (default is one row per session)')
    parser.add_argument('-d', '--duration', nargs='*', metavar='dx_type', dest='dx_types', default=None, choices=ALL_DX_TYPES, help='calculate diagnosis duration for specified diagnosis types (all if none specified)')
    parser.add_argument('--all', nargs='+', metavar='var', default=None, help='limit data to participants with data (in export) for all specified variables (can be category, column prefix, or specific variable)')
    parser.add_argument('--any', nargs='+', metavar='var', default=None, help='limit data to participants with data (in export) for any specified variables (can be category, column prefix, or specific variable)')
    args = parser.parse_args()

    # create dataframe from REDCap data
    df = pd.read_csv(args.file)
    df = df[df.study_id.str.contains('WOLF_\d{4}_.+')] # remove Test and Wolf_AN rows

    # if clinic year is already in data, drop it - will extract year from redcap_event_name instead
    # a bit redundant, but makes the diagnosis duration logic more managable (clinic year is only
    #   specified for years attended, but dx info is in 2016 always - need to be able to indentify this)
    if CLINIC_YEAR in df.columns:
        df.drop([CLINIC_YEAR], inplace=True, axis=1)
    df.rename(columns={ 'redcap_event_name': CLINIC_YEAR }, inplace=True)
    df[CLINIC_YEAR] = df[CLINIC_YEAR].str.extract('(\d{4})', expand=False).astype(int)

    # Temporarily update rows for 2016 that have no session number to be -1 (will remove after calculation)
    df.loc[(df[CLINIC_YEAR] == 2016), [SESSION_NUMBER]] = df.loc[(df[CLINIC_YEAR] == 2016), [SESSION_NUMBER]].fillna(-1)
    df = df[pd.notnull(df[SESSION_NUMBER])] # remove other rows for non-attended sessions
    df[SESSION_NUMBER] = df[SESSION_NUMBER].astype(int) # once NANs are gone, we can cast as int (nicer for flatten display)

    # if duration argument specified, calculate diagnosis duration for types specified or all (if none specified)
    if args.dx_types != None: # explicit None check because empty array is valid
        df['dob'] = df.groupby([STUDY_ID])['dob'].transform(lambda x: x.loc[x.first_valid_index()]) # fills in dob for missing years using first-found dob for participant
        dx_types = args.dx_types if args.dx_types else ALL_DX_TYPES
        for dx_type in dx_types:
            dx_age_df = df.loc[df[CLINIC_YEAR] == 2016].apply(get_diagnosis_age, args=(dx_type,), axis=1)
            df = df.groupby([STUDY_ID]).apply(calculate_diasnosis_duration, dx_type, dx_age_df)

        # drop columns included for calculation from final dataframe
        drop_cols = [ col for col in df.columns if col.startswith('clinichx_dx') ]
        drop_cols = drop_cols + [ 'clinic_history_complete', 'dx_notes' ]
        df = df.drop(drop_cols, axis=1)

    # after calculation, we can remove the 2016 rows for participants who did not attend (reassigned earlier to session_id -1)
    df = df[df[SESSION_NUMBER] != -1]

    # TODO: add basic contains regex check on all column names (for all and maybe any??) to ensure each var appears at least once in dataset
    # if varaibles are specified, filter out rows that don't have data for them
    if args.all:
        df = df.apply(has_required_variables, args=((True,) + get_column_lists_for_variables(df.columns, args.all)), axis=1)
    if args.any:
        df = df.apply(has_required_variables, args=((False,) + get_column_lists_for_variables(df.columns, args.any)), axis=1)

    # remove session data for participants that did not occur in consecutive years
    if args.consecutive:
        df = df.groupby([STUDY_ID]).apply(get_consecutive_years, args.consecutive)

    index_cols = [STUDY_ID, SESSION_NUMBER] if args.by_session else [STUDY_ID, CLINIC_YEAR]
    df.set_index(index_cols, inplace=True)

    # puts all sessions/clinic years for a participant on one line (suffixed with year/session)
    if args.flatten:
        df = df.unstack().sort_index(1, level=1)
        df.columns = [ '_'.join(map(str,i)) for i in df.columns ]

    df.to_csv('formatted_data.csv')


format_wolfram_data()
