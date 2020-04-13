import datetime
import re
import time
import pyodbc
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
DB_PATH = 'H:/REDCap Scripts/api_tokens.accdb'
URL = 'https://redcap.wustl.edu/redcap/srvrs/prod_v3_1_0_001/redcap/api/'

# Redcap constants
WFS_STUDY_ID = 'study_id'
WFS_CLINIC_YEAR = 'wfs_clinic_year'
WFS_SESSION_NUMBER = 'wolfram_sessionnumber'
MISSED_SESSION = 'missed_session'

ALL_DX_TYPES = ['wfs', 'dm', 'di', 'hearloss', 'oa', 'bladder']
NON_DX_FIELDS_FOR_DURATION = ['dob', 'clinic_date']

RENAMES = [None, WFS_CLINIC_YEAR, None, 'clinic_date', WFS_SESSION_NUMBER]

def get_dx_column(dx_type, measure):
    return '_'.join(['clinichx', 'dx', dx_type, measure])


@Gooey(default_size=(700,600))
def format_wolfram_data():
    # set up expected arguments and associated help text
    parser = GooeyParser(description='Formats Wolfram data from REDCap csv export')
    required = parser.add_argument_group('Required Arguments', gooey_options={'columns':1})
    required.add_argument('--input_file', required=True, widget='FileChooser', help='REDCap export file')
    required.add_argument('--output_file', required=True, widget='FileChooser', help='CSV file to store formatted data in')
    required.add_argument('--api_password', required=True, widget='PasswordField', help='Password to access API token')

    optional = parser.add_argument_group('Optional Arguments', gooey_options={'columns':1})
    optional.add_argument('-c', '--consecutive', type=int, metavar='num_consecutive_years', help='Limit results to particpants with data for a number of consecutive years')
    optional.add_argument('-d', '--duration', nargs='*', dest='dx_types',  widget='Listbox', default=None, choices=ALL_DX_TYPES, help='Calculate diagnosis duration for specified diagnosis types')
    optional.add_argument('--old-db', action='store_true', help='whether data was sourced from old Wolfram database')

    variable_options = parser.add_argument_group('Variable options', 'Space-separated lists of data points (category, column prefix, and/or variable) participants must have data for in export', gooey_options={'columns':1, 'show_border':True})
    variable_options.add_argument('--all', nargs='+', default=None, help='All specified data points required for participant to be included in result')
    variable_options.add_argument('--any', nargs='+', default=None, help='At least one specified data point required for participant to be included in result')

    format_options = parser.add_argument_group('Formatting options', gooey_options={'columns':2, 'show_border':True})
    format_options.add_argument('-f', '--flatten', action='store_true', help='Arrange all session data in single row for participant')
    format_options.add_argument('-t', '--transpose', action='store_true', help='Transpose the data')
    format_options.add_argument('-s', '--sort_by', default='session', choices=['session','variable'], help='Sort flattened data by session or variable')

    args = parser.parse_args()

    if not args.input_file.endswith('.csv') or not args.output_file.endswith('.csv'):
        parser.error('Input and output files must be of type csv')

    # create dataframe from REDCap data
    df = redcap_common.create_df(args.input_file)
    df = df[df[WFS_STUDY_ID].str.contains(r'WOLF_\d{4}_.+')] # remove Test and Wolf_AN rows
    num_clinic_years = len(df['redcap_event_name'].unique())-1  # FIXME: should be counting max number of sessions for participants (still may cause error because they might not be consecutive)

    # only create API project if actions require it and data needed is not already present
    project = None
    fields = [WFS_SESSION_NUMBER, WFS_CLINIC_YEAR] if WFS_SESSION_NUMBER not in df.columns else [] # always need to get session number if not in data (used to determine which rows to keep)
    if MISSED_SESSION not in df.columns:
        fields.append(MISSED_SESSION) # need missed_session var to remove rows for unattended session
    if any(arg is not None for arg in [args.dx_types, args.all, args.any]): # all of these args require api project info
        project = redcap_common.get_redcap_project('wolfram', args.api_password)
        if args.dx_types is not None:
            fields += NON_DX_FIELDS_FOR_DURATION
            fields += redcap_common.get_matching_columns(project.field_names, 'clinichx_dx_') # if doing date calculation, always bring in all dates to prevent possible date-shift errors
    if fields:
        redcap_project_key = 'itrack' if not args.old_db else 'wolfram'
        project = project if project else redcap_common.get_redcap_project(redcap_project_key, args.api_password)
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
        df = redcap_common.prepare_age_calc(df)
        for dx_type in args.dx_types:
            dx_vars = { 'dx_date': get_dx_column(dx_type, 'date'), 'dx_age': get_dx_column(dx_type, 'age') }
            df[dx_vars['dx_date']] = pd.to_datetime(df[dx_vars['dx_date']], errors='coerce')
            dx_age_df = df.loc[df['redcap_event_name'] == 'stable_patient_cha_arm_1'].apply(redcap_common.get_diagnosis_age, args=(dx_vars,), axis=1)
            df = df.groupby([redcap_common.STUDY_ID]).apply(redcap_common.calculate_diasnosis_duration, dx_type, dx_age_df)
            #df.to_csv(r'C:\Users\acevedoh\Downloads\test.csv')
        df = df.drop(['session_age', 'redcap_event_name'], axis=1)

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

    df.set_index([redcap_common.STUDY_ID, redcap_common.SESSION_NUMBER], inplace=True)

    df = redcap_common.rename_common_columns(df, RENAMES, True) # rename common columns back to original names
    # if we have brought in dx info/demographics from the API, remove it after the calculation and rename columns that were suffixed due to merge
    if not fields == [WFS_SESSION_NUMBER, WFS_CLINIC_YEAR]: # don't need to go through deletion logic if only field is session number
        if WFS_SESSION_NUMBER in fields:
            fields.remove(WFS_SESSION_NUMBER) # remove session number from fields
        df = redcap_common.cleanup_api_merge(df, fields)


    # puts all sessions/clinic years for a participant on one line (suffixed with year/session)
    if args.flatten:
        sort = args.sort_by == 'session'
        df = redcap_common.flatten(df, sort, 's')

    if args.transpose:
        df = df.transpose()

    redcap_common.write_results_and_open(df, args.output_file)


format_wolfram_data()
