import redcap_common

import numpy as np
import pandas as pd
import re

from gooey import Gooey, GooeyParser
from sys import exit, stderr

# Redcap constants
WFS_STUDY_ID = 'study_id'
WFS_CLINIC_YEAR = 'wfs_clinic_year'
WFS_SESSION_NUMBER = 'wolfram_sessionnumber'
MISSED_SESSION = 'missed_session'

RENAMES = [None, WFS_CLINIC_YEAR, None, 'clinic_date', WFS_SESSION_NUMBER]

MRI_DATE = 'mri_date'
MRI_AGE = 'mri_age'

def mri_age_calc(df):
    df[MRI_DATE] = pd.to_datetime(df[MRI_DATE], errors='coerce')
    df['dob'] = pd.to_datetime(df.groupby(['study_id'])['dob'].transform(lambda x: x.loc[x.first_valid_index()] if x.first_valid_index() is not None else np.nan)) # fills in dob for missing years using first-found dob for participant
    mri_age_column = df.apply(lambda x: redcap_common.get_age(x['dob'], x[MRI_DATE]), axis=1)
    df.insert(df.columns.get_loc(MRI_DATE), MRI_AGE, mri_age_column)
    return df

def select_best_age(row):
    if row['mri_age'] > 0:
        return row['mri_age']
    else:
        return row['session_age']

def get_clinic_year_label(row):
    clinic_year = row['redcap_event_name'][0:4]
    if clinic_year == 'stab':
        clinic_label = '0'
    elif clinic_year == 'mini':
        clinic_label = 'mc{}'.format(row['redcap_event_name'][12])
    else:
        clinic_label = '{}'.format(clinic_year)
    return clinic_label


@Gooey(default_size=(700,600))
def format_wolfram_data():
    # set up expected arguments and associated help text
    parser = GooeyParser(description='Formats Wolfram data from REDCap csv export\n********************\nNOTE: Input REDCap file must contain both stable (e.g. sex) and clinic-year data, and also include wolfram_sessionnumber.\n********************')
    required = parser.add_argument_group('Required Arguments', gooey_options={'columns':1})
    required.add_argument('--input_file', required=True, widget='FileChooser', gooey_options={'wildcard':"Comma separated file (*.csv)|*.csv|"}, help='REDCap-exported csv file')
    # required.add_argument('--output_file', required=True, widget='FileChooser', help='CSV file to store formatted data in')

    optional = parser.add_argument_group('Optional Arguments', gooey_options={'columns':1})
    optional.add_argument('-c', '--consecutive', type=int, metavar='num_consecutive_years', help='Limit results to particpants with data for a number of consecutive years')
    optional.add_argument('--drop_non_mri', action='store_true', help='Drop all sessions that do not have an "mri_date" entry.')
    # optional.add_argument('--api_token', widget='PasswordField', help='REDCap API token (if not specified, will not pull anything from REDCap)')

    # variable_options = parser.add_argument_group('Variable options', 'Space-separated lists of data points (category, column prefix, and/or variable) participants must have data for in export', gooey_options={'columns':1, 'show_border':True})
    # variable_options.add_argument('--all', nargs='+', default=None, help='All specified data points required for participant to be included in result')
    # variable_options.add_argument('--any', nargs='+', default=None, help='At least one specified data point required for participant to be included in result')

    format_options = parser.add_argument_group('Formatting options', gooey_options={'columns':2, 'show_border':True})
    format_options.add_argument('-f', '--flatten', action='store_true', help='Arrange all session data in single row for participant')
    format_options.add_argument('--flatten_by', default='session number', choices=['session number', 'clinic year'], help='Flatten data by session number or clinic year')
    format_options.add_argument('-t', '--transpose', action='store_true', help='Transpose the data')
    format_options.add_argument('-s', '--sort_by', default='variable', choices=['variable', 'session/clinic'], help='Sort flattened data by session or variable')

    args = parser.parse_args()

    dur_label = ''
    flatten_label = ''
    mri_label = ''

    if not args.input_file.endswith('.csv'):
        parser.error('ERROR: Input file must be a csv exported from REDCap')

    # create dataframe from REDCap data
    df = redcap_common.create_df(args.input_file)
    df = df.drop(['redcap_repeat_instrument'], axis=1, errors="ignore")
    df = df.drop(['redcap_repeat_instance'], axis=1, errors="ignore")
    # df = df[df[WFS_STUDY_ID].str.contains(r'^(WOLF|SIB)_\d{4}_.+')] # remove Test and Wolf_AN rows
    df = df[df[WFS_STUDY_ID].str.contains(r'^(WOLF|SIB|DT)')] # remove Test and Wolf_AN rows
    df = df.rename(columns={WFS_SESSION_NUMBER: redcap_common.SESSION_NUMBER, WFS_STUDY_ID: redcap_common.STUDY_ID})
    arm_tail_pattern = re.compile(r'_arm_.')
    df['redcap_event_name'] = df['redcap_event_name'].str.replace(arm_tail_pattern,'', regex=True)
    num_clinic_years = len(df['redcap_event_name'].unique())-1  # FIXME: should be counting max number of sessions for participants (still may cause error because they might not be consecutive)
    print('### Number of clinic years detected in file = {} ###'.format(num_clinic_years))

    # get number of subjects in dataframe
    num_subjects = len(df[WFS_STUDY_ID].unique())
    print('### Number of subjects detected in {} = {} ###'.format(args.input_file,num_subjects))

    if args.consecutive is not None and args.consecutive not in range(2, num_clinic_years + 1):
        parser.error('Consecutive years must be greater than 1 and cannot exceed number of clinic years ({})'.format(num_clinic_years))

    df.loc[(df['redcap_event_name'] == 'stable'), [redcap_common.SESSION_NUMBER]] = df.loc[(df['redcap_event_name'] == 'stable'), [redcap_common.SESSION_NUMBER]].fillna(0)
    # remove rows for sessions not attended (will have a flag saying they did not attend)
    df = df[pd.notnull(df[redcap_common.SESSION_NUMBER])]
    df = df[pd.isnull(df[MISSED_SESSION])]
    df[redcap_common.SESSION_NUMBER] = df[redcap_common.SESSION_NUMBER].astype(int) # once NANs are gone, we can cast as int (nicer for flatten display)

    # if varaibles are specified, filter out rows that don't have data for them (if null or non-numeric)
    # if args.all:
    #    df = redcap_common.check_for_all(df, args.all, project, True)
    # if args.any:
    #    df = redcap_common.check_for_any(df, args.any, project, True)

    # remove session data for participants that did not occur in consecutive years
    if args.consecutive:
        df = df.groupby([redcap_common.STUDY_ID]).apply(redcap_common.get_consecutive_years, args.consecutive)

    if df.empty:
        stderr.write('No data to return. Selections have filtered out all rows.')
        exit(1)

    # add clinic_year
    df['clinic_year'] = df.apply(lambda row: get_clinic_year_label(row), axis = 1)

    # rename common columns back to original names
    df = redcap_common.rename_common_columns(df, RENAMES, True)

    # rename session_age to clinic_age
    df = df.rename(columns={"session_age": "clinic_age"})

    # remove dob, clinic date and MRI date
    df = df.drop(['dob'], axis=1, errors="ignore")
    df = df.drop(['clinic_date'], axis=1, errors="ignore")
    df = df.drop(['mri_date'], axis=1, errors="ignore")
    df = df.drop(['redcap_event_name'], axis=1, errors="ignore")

    # drop non-MRI sessions
    if args.drop_non_mri:
        df = df[(df[MRI_AGE]>0.0) | (df['clinic_year']==0)]
        mri_label = '_just_mri'

    # puts all sessions/clinic years for a participant on one line (suffixed with year/session)
    if args.flatten:
        # multi-index column for flattening
        if args.flatten_by == 'session number':
            flatten_by_column = 'wolfram_sessionnumber'
            flatten_label = '_flattened_by_session'
            # df.set_index([redcap_common.STUDY_ID, redcap_common.SESSION_NUMBER], inplace=True)
            flatten_group_prefix = 's'
        elif args.flatten_by == 'clinic year':
            flatten_by_column = 'clinic_year'
            flatten_label = '_flattened_by_clinic'
            # df.set_index([redcap_common.STUDY_ID, 'clinic_year'], inplace=True)
            flatten_group_prefix = 'c'
        else:
            raise Exception('ERROR: flatten_by check failed')

        sort = args.sort_by == 'session'
        df = redcap_common.flatten(df, flatten_by_column, sort, flatten_group_prefix)

    if args.transpose:
        df = df.transpose()

    # make output_file name
    output_file = args.input_file.replace('.csv','{}{}{}.csv'.format(dur_label, flatten_label, mri_label))

    redcap_common.write_results_and_open(df, output_file)

format_wolfram_data()
