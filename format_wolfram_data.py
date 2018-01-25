import argparse
import datetime
import re
import time
import pyodbc
import redcap_common

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
WFS_STUDY_ID = 'study_id'
WFS_CLINIC_YEAR = 'wfs_clinic_year'
WFS_SESSION_NUMBER = 'wolfram_sessionnumber'
MISSED_SESSION = 'missed_session'

ALL_DX_TYPES = ['wfs', 'dm', 'di', 'hearloss', 'oa', 'bladder']
NON_DX_FIELDS_FOR_DURATION = ['dob', 'clinic_date']

RENAMES = [None, 'redcap_event_name', None, 'clinic_date', 'wolfram_sessionnumber']

def get_dx_column(dx_type, measure):
    return '_'.join(['clinichx', 'dx', dx_type, measure])


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
    df = redcap_common.create_df(args.input_file)
    df = df[df[WFS_STUDY_ID].str.contains('WOLF_\d{4}_.+')] # remove Test and Wolf_AN rows

    # only create API project if actions require it and data needed is not already present
    project = None
    fields = [WFS_SESSION_NUMBER,] if WFS_SESSION_NUMBER not in df.columns else [] # always need to get session number if not in data (used to determine which rows to keep)
    if MISSED_SESSION not in df.columns:
        fields.append(MISSED_SESSION) # need missed_session var to remove rows for unattended session
    if any(arg is not None for arg in [args.dx_types, args.all, args.any]): # all of these args require api project info
        project = redcap_common.get_redcap_project('wolfram')
        if args.dx_types is not None:
            fields += NON_DX_FIELDS_FOR_DURATION
            fields += redcap_common.get_matching_columns(project.field_names, 'clinichx_dx_') # if doing date calculation, always bring in all dates to prevent possible date-shift errors
    if fields:
        project = project if project else redcap_common.get_redcap_project('wolfram')
        df = redcap_common.merge_api_data(df, project, fields, [WFS_STUDY_ID, 'redcap_event_name'])

    # rename common columns after api merge to ensure column names match up
    df = redcap_common.rename_common_columns(df, RENAMES, False)

    # if clinic year is already in data, drop it - will extract year from redcap_event_name instead
    # a bit redundant, but makes the diagnosis duration logic more managable (clinic year is only
    #   specified for years attended, but dx info is in 2016 always - need to be able to indentify this)
    if WFS_CLINIC_YEAR in df.columns:
        df.drop([WFS_CLINIC_YEAR], inplace=True, axis=1)
    df[redcap_common.SESSION_YEAR] = df[redcap_common.SESSION_YEAR].str.extract('(\d{4})', expand=False).astype(int)

    num_clinic_years = len(df[redcap_common.SESSION_YEAR].unique()) # FIXME: should be counting max number of sessions for participants (still may cause error because they might not be consecutive)
    if args.consecutive is not None and args.consecutive not in range(2, num_clinic_years + 1):
        parser.error('Consecutive years must be greater than 1 and cannot exceed number of clinic years ({})'.format(num_clinic_years))

    # Temporarily update rows for 2016 that have no session number to be -1 (will remove after calculation)
    df.loc[(df[redcap_common.SESSION_YEAR] == 2016), [redcap_common.SESSION_NUMBER]] = df.loc[(df[redcap_common.SESSION_YEAR] == 2016), [redcap_common.SESSION_NUMBER]].fillna(-1)
    # remove rows for sessions not attended (either will have no session number or will have a flag saying they did not attend)
    df = df[pd.notnull(df[redcap_common.SESSION_NUMBER])]
    df = df[pd.isnull(df[MISSED_SESSION])]
    df[redcap_common.SESSION_NUMBER] = df[redcap_common.SESSION_NUMBER].astype(int) # once NANs are gone, we can cast as int (nicer for flatten display)

    # if duration argument specified, calculate diagnosis duration for types specified or all (if none specified)
    if args.dx_types is not None: # explicit None check because empty array is valid
        df = redcap_common.prepare_age_calc(df)
        dx_types = args.dx_types if args.dx_types else ALL_DX_TYPES
        for dx_type in dx_types:
            dx_vars = { 'dx_date': get_dx_column(dx_type, 'date'), 'dx_age': get_dx_column(dx_type, 'age') }
            df[dx_vars['dx_date']] = pd.to_datetime(df[dx_vars['dx_date']], errors='coerce')
            dx_age_df = df.loc[df[redcap_common.SESSION_YEAR] == 2016].apply(redcap_common.get_diagnosis_age, args=(dx_vars,), axis=1)
            df = df.groupby([redcap_common.STUDY_ID]).apply(redcap_common.calculate_diasnosis_duration, dx_type, dx_age_df)
            df = df.drop('session_age', axis=1)

    # after calculation, we can remove the 2016 rows for participants who did not attend (reassigned earlier to session_id -1)
    df = df[df[redcap_common.SESSION_NUMBER] != -1]

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

    index_cols = [redcap_common.STUDY_ID, redcap_common.SESSION_NUMBER] if args.by_session else [redcap_common.STUDY_ID, redcap_common.SESSION_YEAR]
    df.set_index(index_cols, inplace=True)

    df = redcap_common.rename_common_columns(df, RENAMES, True) # rename common columns back to original names
    # if we have brought in dx info/demographics from the API, remove it after the calculation and rename columns that were suffixed due to merge
    if not fields == [WFS_SESSION_NUMBER]: # don't need to go through deletion logic if only field is session number
        if WFS_SESSION_NUMBER in fields:
            fields.remove(WFS_SESSION_NUMBER) # remove session number from fields
        df = redcap_common.cleanup_api_merge(df, fields)


    # puts all sessions/clinic years for a participant on one line (suffixed with year/session)
    if args.flatten:
        prefix = 's' if args.by_session else ''
        df = redcap_common.flatten(df, prefix)

    if args.transpose:
        df = df.transpose()

    redcap_common.write_results_and_open(df, args.output_file)


format_wolfram_data()
