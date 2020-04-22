import datetime
import re
import time
import redcap_common

import numpy as np
import pandas as pd

from getpass import getpass, getuser
from gooey import Gooey, GooeyParser
from itertools import chain, groupby
from redcap import Project, RedcapError
from subprocess import Popen
from sys import exit, stderr

# API constants
URL = 'https://redcap.wustl.edu/redcap/srvrs/prod_v3_1_0_001/redcap/api/'

# Redcap constants
WFS_STUDY_ID = 'study_id'
WFS_CLINIC_YEAR = 'wfs_clinic_year'
WFS_SESSION_NUMBER = 'wolfram_sessionnumber'
MISSED_SESSION = 'missed_session'

ALL_DX_TYPES = ['wfs', 'dm', 'di', 'hearloss', 'oa', 'bladder']
NON_DX_FIELDS_FOR_DURATION = ['dob', 'clinic_date']

RENAMES = [None, WFS_CLINIC_YEAR, None, 'clinic_date', WFS_SESSION_NUMBER]

MRI_DATE = 'mri_date'
MRI_AGE = 'mri_age'

def get_dx_column(dx_type, measure):
    return '_'.join(['dx', dx_type, measure])

def mri_age_calc(df):
    df[MRI_DATE] = pd.to_datetime(df[MRI_DATE], errors='coerce')
    df['dob'] = pd.to_datetime(df.groupby(['study_id'])['dob'].transform(lambda x: x.loc[x.first_valid_index()] if x.first_valid_index() is not None else np.nan)) # fills in dob for missing years using first-found dob for participant
    df[MRI_AGE] = df.apply(lambda x: redcap_common.get_age(x['dob'], x[MRI_DATE]), axis=1)
    return df

def select_best_age(row):
    if row['mri_age'] > 0:
        return row['mri_age']
    else:
        return row['session_age']

def get_clinic_year(row):
    clinic_year = row['redcap_event_name'][0:4]
    if clinic_year == 'stab':
        clinic_year = '0'
    return clinic_year


@Gooey(default_size=(700,600))
def format_wolfram_data():
    # set up expected arguments and associated help text
    parser = GooeyParser(description='Formats Wolfram data from REDCap csv export')
    required = parser.add_argument_group('Required Arguments', gooey_options={'columns':1})
    required.add_argument('--input_file', required=True, widget='FileChooser', help='REDCap export file')
    required.add_argument('--output_file', required=True, widget='FileChooser', help='CSV file to store formatted data in')

    optional = parser.add_argument_group('Optional Arguments', gooey_options={'columns':1})
    optional.add_argument('-c', '--consecutive', type=int, metavar='num_consecutive_years', help='Limit results to particpants with data for a number of consecutive years')
    optional.add_argument('-d', '--duration', nargs='*', dest='dx_types',  widget='Listbox', default=None, choices=ALL_DX_TYPES, help='Calculate diagnosis duration for specified diagnosis types')
    optional.add_argument('--duration-type', dest='duration_type', default='clinic date', choices=['clinic date','MRI date','MRI date if available, otherwise clinic date ("mri_or_clinic")'], help='Visit date to use when calculating dx durations')
    optional.add_argument('--old-db', action='store_true', help='whether data was sourced from old Wolfram database')
    optional.add_argument('--api_token', widget='PasswordField', help='REDCap API token (if not specified, will not pull anything from REDCap)')

    variable_options = parser.add_argument_group('Variable options', 'Space-separated lists of data points (category, column prefix, and/or variable) participants must have data for in export', gooey_options={'columns':1, 'show_border':True})
    variable_options.add_argument('--all', nargs='+', default=None, help='All specified data points required for participant to be included in result')
    variable_options.add_argument('--any', nargs='+', default=None, help='At least one specified data point required for participant to be included in result')

    format_options = parser.add_argument_group('Formatting options', gooey_options={'columns':2, 'show_border':True})
    format_options.add_argument('-f', '--flatten', action='store_true', help='Arrange all session data in single row for participant')
    format_options.add_argument('--flatten_by', default='session number', choices=['session number', 'clinic year'], help='Flatten data by session number or clinic year')
    format_options.add_argument('-t', '--transpose', action='store_true', help='Transpose the data')
    format_options.add_argument('-s', '--sort_by', default='variable', choices=['variable', 'session'], help='Sort flattened data by session or variable')

    args = parser.parse_args()

    if not args.old_db:
        print('### "old_db" not checked, only pulling data from the "new" database ###')

    if not args.input_file.endswith('.csv') or not args.output_file.endswith('.csv'):
        parser.error('Input and output files must be of type csv')

    # create dataframe from REDCap data
    df = redcap_common.create_df(args.input_file)
    df = df[df[WFS_STUDY_ID].str.contains(r'WOLF_\d{4}_.+')] # remove Test and Wolf_AN rows
    num_clinic_years = len(df['redcap_event_name'].unique())-1  # FIXME: should be counting max number of sessions for participants (still may cause error because they might not be consecutive)
    print('### Number of clinic years detected = {} ###'.format(num_clinic_years))

    # get number of subjects in dataframe
    num_subjects = len(df[WFS_STUDY_ID].unique())
    print('### Number of subjects detected in {} = {} ###'.format(args.input_file,num_subjects))

    # only create API project if actions require it and data needed is not already present, AND if API token is given
    project = None
    # check for fields missing from csv df
    fields = [WFS_SESSION_NUMBER, WFS_CLINIC_YEAR] if WFS_SESSION_NUMBER not in df.columns else [] # always need to get session number if not in data (used to determine which rows to keep)
    if MISSED_SESSION not in df.columns:
        fields.append(MISSED_SESSION) # need missed_session var to remove rows for unattended session
    if args.dx_types is not None:
        for dx_type in args.dx_types:
            dx_age_field = get_dx_column(dx_type, 'best_age_calc')
            if dx_age_field not in df.columns:
                fields.append(dx_age_field)
        for non_dx_field in NON_DX_FIELDS_FOR_DURATION:
            if non_dx_field not in df.columns:
                fields.append(non_dx_field)
    if fields: # missing some fields, go get from REDCap
        print('### need to get some fields from REDCap ###')
        if args.api_token == "":
            raise RuntimeError("Thre are missing fields in the input csv, so we need to get data from REDCap, but no API token is given. Ask Jon about REDCap API access.")
        else:
            redcap_project_key = 'itrack' if not args.old_db else 'wolfram'
            project = project if project else redcap_common.get_redcap_project(redcap_project_key, args.api_token)
            df = redcap_common.merge_api_data(df, project, fields, [WFS_STUDY_ID, 'redcap_event_name'])

    # rename common columns after api merge to ensure column names match up
    df = redcap_common.rename_common_columns(df, RENAMES, False)

    if args.consecutive is not None and args.consecutive not in range(2, num_clinic_years + 1):
        parser.error('Consecutive years must be greater than 1 and cannot exceed number of clinic years ({})'.format(num_clinic_years))

    df.loc[(df['redcap_event_name'] == 'stable_patient_cha_arm_1'), [redcap_common.SESSION_NUMBER]] = df.loc[(df['redcap_event_name'] == 'stable_patient_cha_arm_1'), [redcap_common.SESSION_NUMBER]].fillna(0)
    # remove rows for sessions not attended (will have a flag saying they did not attend)
    df = df[pd.notnull(df[redcap_common.SESSION_NUMBER])]
    df = df[pd.isnull(df[MISSED_SESSION])]
    df[redcap_common.SESSION_NUMBER] = df[redcap_common.SESSION_NUMBER].astype(int) # once NANs are gone, we can cast as int (nicer for flatten display)

    # if duration argument specified, calculate diagnosis duration for types specified or all (if none specified)
    if args.dx_types is not None: # explicit None check because empty array is valid
        # this puts a 'session_age' field into the df using dob and session_date (where session_date is from clinic_date)
        df = redcap_common.prepare_age_calc(df)
        df = mri_age_calc(df)
        df['mri_or_clinic_age'] = df.apply(lambda row: select_best_age(row), axis = 1)
        for dx_type in args.dx_types:
            dx_vars = { 'dx_age': get_dx_column(dx_type, 'best_age_calc') }
            # df[dx_vars['dx_date']] = pd.to_datetime(df[dx_vars['dx_date']], errors='coerce')
            dx_age_df = df.loc[df['redcap_event_name'] == 'stable_patient_cha_arm_1'].apply(redcap_common.get_diagnosis_age, args=(dx_vars,), axis=1)
            if args.duration_type == 'clinic date':
                dx_type_clinic = '_'.join([dx_type, 'clinic'])
                df = df.groupby([redcap_common.STUDY_ID]).apply(redcap_common.calculate_diagnosis_duration, dx_type_clinic, dx_age_df, 'session_age')
                dx_dur_field = get_dx_column(dx_type, 'clinic_duration')
                df.loc[~(df[dx_dur_field] > 0), dx_dur_field]=np.nan
            elif args.duration_type == 'MRI date':
                dx_type_mri = '_'.join([dx_type, 'mri'])
                df = df.groupby([redcap_common.STUDY_ID]).apply(redcap_common.calculate_diagnosis_duration, dx_type_mri, dx_age_df, 'mri_age')
                dx_mri_dur_field = get_dx_column(dx_type, 'mri_duration')
                df.loc[~(df[dx_mri_dur_field] > 0), dx_mri_dur_field]=np.nan
            elif args.duration_type == 'MRI date if available, otherwise clinic date ("mri_or_clinic")':
                dx_type_mri_or_clinic = '_'.join([dx_type, 'mri_or_clinic'])
                df = df.groupby([redcap_common.STUDY_ID]).apply(redcap_common.calculate_diagnosis_duration, dx_type_mri_or_clinic, dx_age_df, 'mri_or_clinic_age')
                dx_best_dur_field = get_dx_column(dx_type, 'mri_or_clinic_duration')
                df.loc[~(df[dx_best_dur_field] > 0), dx_best_dur_field]=np.nan
            else:
                raise Exception("ERROR: dx_types chosen, but no duration_type chosen")
        # df = df.drop(['session_age', 'redcap_event_name'], axis=1)

    # if varaibles are specified, filter out rows that don't have data for them (if null or non-numeric)
    if args.all:
        df = redcap_common.check_for_all(df, args.all, project, True)
    if args.any:
        df = redcap_common.check_for_any(df, args.any, project, True)

    # remove session data for participants that did not occur in consecutive years
    if args.consecutive:
        df = df.groupby([redcap_common.STUDY_ID]).apply(redcap_common.get_consecutive_years, args.consecutive)

    if df.empty:
        stderr.write('No data to return. Selections have filtered out all rows.')
        exit(1)

    # add clinic_year
    df['clinic_year'] = df.apply(lambda row: get_clinic_year(row), axis = 1)

    if args.flatten_by == 'session number':
        df.set_index([redcap_common.STUDY_ID, redcap_common.SESSION_NUMBER], inplace=True)
        flatten_group_prefix = 's'
    elif args.flatten_by == 'clinic year':
        df.set_index([redcap_common.STUDY_ID, 'clinic_year'], inplace=True)
        flatten_group_prefix = 'c'
    else:
        raise Exception('ERROR: flatten_by check failed')

    df = redcap_common.rename_common_columns(df, RENAMES, True) # rename common columns back to original names
    # if we have brought in dx info/demographics from the API, remove it after the calculation and rename columns that were suffixed due to merge
    if not fields == [WFS_SESSION_NUMBER, WFS_CLINIC_YEAR] and args.api_token: # don't need to go through deletion logic if only field is session number
        if WFS_SESSION_NUMBER in fields:
            fields.remove(WFS_SESSION_NUMBER) # remove session number from fields
        df = redcap_common.cleanup_api_merge(df, fields)

    # remove dob, clinic date and MRI date
    df = df.drop(['dob'], axis=1)
    df = df.drop(['clinic_date'], axis=1)
    df = df.drop(['mri_date'], axis=1)

    # df.to_csv(r'C:\temp\df_before_flatten.csv')

    # puts all sessions/clinic years for a participant on one line (suffixed with year/session)
    if args.flatten:
        sort = args.sort_by == 'session'
        df = redcap_common.flatten(df, sort, flatten_group_prefix)

    if args.transpose:
        df = df.transpose()

    # df.to_csv(r'C:\temp\df_right_before_save.csv')

    redcap_common.write_results_and_open(df, args.output_file)


format_wolfram_data()
