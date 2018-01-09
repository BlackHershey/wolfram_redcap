import argparse
import redcap_common

import pandas as pd

import pdb

TRACK_STUDY_ID = 'track_id'
TRACK_EXAM_DATE = 'physicalexam_date'

RENAMES = [TRACK_STUDY_ID, None, None, TRACK_EXAM_DATE, None]
DURATION_FIELDS = ['dob', 'db_dx_date', 'physicalexam_date']

def format_track_data():
    # set up expected arguments and associated help text
    parser = argparse.ArgumentParser(description='Formats data from REDCap csv export')
    parser.add_argument('input_file', help='exported file to be formatted')
    parser.add_argument('output_file', help='full filepath where formatted data should be stored (if file does not exist in location, it will be created)')
    parser.add_argument('-c', '--consecutive', type=int, metavar='num_years', help='limit data to particpants with data for a number of consecutive years')
    parser.add_argument('-e', '--expand', action='store_true', help='arrange data with one row per subject per session')
    parser.add_argument('-d', '--duration', action='store_true', help='calculate diabetes diagnosis duration')
    parser.add_argument('-t', '--transpose', action='store_true', help='transpose the data')
    parser.add_argument('--all', nargs='+', metavar='var', default=None, help='limit data to participants with data (in export) for every specified variables (can be category, column prefix, or specific variable)')
    parser.add_argument('--any', nargs='+', metavar='var', default=None, help='limit data to participants with data (in export) for at least one of the specified variables (can be category, column prefix, or specific variable)')
    args = parser.parse_args()

    if not args.input_file.endswith('.csv') or not args.output_file.endswith('.csv'):
        parser.error('Input and output files must be of type csv')

    # create initial dataframe structure
    df = redcap_common.create_df(args.input_file)
    df = df[df[TRACK_STUDY_ID].str.contains('TRACK\d+')] # remove test rows

    project = None
    if any(arg is not None for arg in [args.all, args.any, args.duration, args.consecutive]):
        project = redcap_common.get_redcap_project('track')

    if args.all:
        df = redcap_common.check_for_all(df, args.all, project)

    if args.any:
        df = redcap_common.check_for_any(df, args.any, project)

    fields = None
    if args.duration or args.consecutive:
        fields = redcap_common.get_matching_columns(project.field_names, '\w*(' + '|'.join(DURATION_FIELDS) + ')')
        df = redcap_common.merge_api_data(df, project, fields, [TRACK_STUDY_ID])

    #pdb.set_trace()

    # expand/rename after api merge to ensure column names match up
    df, non_session_cols = redcap_common.expand(df.set_index(TRACK_STUDY_ID))
    df = redcap_common.rename_common_columns(df, RENAMES, False)

    df[redcap_common.SESSION_DATE] = pd.to_datetime(df[redcap_common.SESSION_DATE])
    df = df[pd.notnull(df[redcap_common.SESSION_DATE])] # remove rows for non-attended sessions

    if args.duration:
        df = redcap_common.prepare_age_calc(df)
        dx_vars = { 'dx_date': 'db_dx_date', 'dx_age': 'db_onset_age' }
        df['db_dx_date'] = pd.to_datetime(df['db_dx_date'])
        dx_age_df = df.loc[df[redcap_common.SESSION_NUMBER] == 's1'].apply(redcap_common.get_diagnosis_age, args=(dx_vars,), axis=1)
        df = df.groupby([redcap_common.STUDY_ID]).apply(redcap_common.calculate_diasnosis_duration, 'db', dx_age_df)
        df = df.drop('session_age', axis=1)

    if args.consecutive:
        df[redcap_common.SESSION_YEAR] = df[redcap_common.SESSION_DATE].apply(lambda x: x.year if x else None)
        df = df.groupby([redcap_common.STUDY_ID]).apply(redcap_common.get_consecutive_years, args.consecutive)
        df = df.drop([redcap_common.SESSION_YEAR], axis=1)

    df = redcap_common.rename_common_columns(df, RENAMES, True) # rename common columns back to original names pre-flattening

    if not args.expand:
        df = df.set_index([TRACK_STUDY_ID, redcap_common.SESSION_NUMBER])
        df = redcap_common.flatten(df) # always reflatten at end, unless expand flag is set

    if args.transpose:
        df = df.transpose()

    # clean up dataframe
    revert_non_session_cols = { 's1_' + col: col for col in non_session_cols.keys() }
    df = df.rename(columns=revert_non_session_cols)

    if fields:
        drop_fields = fields if not args.expand else DURATION_FIELDS # if leaving expanded, then the columns we brought in don't match the current columns
        df = redcap_common.cleanup_api_merge(df, drop_fields)

    redcap_common.write_results_and_open(df, args.output_file)


format_track_data()
