from gooey import Gooey, GooeyParser

import pandas as pd
import redcap_common

TRACK_STUDY_ID = 'track_id'
TRACK_EXAM_DATE = 'physicalexam_date'

RENAMES = [TRACK_STUDY_ID, None, None, TRACK_EXAM_DATE, None]
DURATION_FIELDS = ['dob', 'db_dx_date', 'physicalexam_date']

@Gooey(default_size=(700,600))
def format_track_data():
    # set up expected arguments and associated help text
    parser = GooeyParser(description='Formats TRACK data from REDCap csv export')

    required = parser.add_argument_group('Required Arguments', gooey_options={'columns':1})
    required.add_argument('--input_file', required=True, widget='FileChooser', help='REDCap export file')
    required.add_argument('--output_file', required=True, widget='FileChooser', help='CSV file to store formatted data in')
    required.add_argument('--api_password', required=True, widget='PasswordField', help='Password to access API token')

    optional = parser.add_argument_group('Optional Arguments', gooey_options={'columns':2})
    optional.add_argument('-c', '--consecutive', type=int, metavar='num_consecutive_years', help='Limit results to particpants with data for a number of consecutive years')
    optional.add_argument('-d', '--duration', action='store_true', help='Calculate diabetes diagnosis duration')

    variable_options = parser.add_argument_group('Variable options', 'Space-separated lists of data points (category, column prefix, and/or variable) participants must have data for in export', gooey_options={'columns':1, 'show_border':True})
    variable_options.add_argument('--all', nargs='+', default=None, help='All specified data points required for participant to be included in result')
    variable_options.add_argument('--any', nargs='+', default=None, help='At least one specified data point required for participant to be included in result')

    format_options = parser.add_argument_group('Formatting options', gooey_options={'columns':2, 'show_border':True})
    format_options.add_argument('-e', '--expand', action='store_true', help='Arrange data with one row per subject per session')
    format_options.add_argument('-t', '--transpose', action='store_true', help='Transpose the data')

    args = parser.parse_args()

    if not args.input_file.endswith('.csv') or not args.output_file.endswith('.csv'):
        parser.error('Input and output files must be of type csv')

    # create initial dataframe structure
    df = redcap_common.create_df(args.input_file)
    df = df[df[TRACK_STUDY_ID].str.contains('TRACK\d+')] # remove test rows

    project = None
    if any(arg is not None for arg in [args.all, args.any, args.duration, args.consecutive]):
        project = redcap_common.get_redcap_project('track', args.api_password)

    if args.all:
        df = redcap_common.check_for_all(df, args.all, project)

    if args.any:
        df = redcap_common.check_for_any(df, args.any, project)

    fields = None
    if args.duration or args.consecutive:
        fields = redcap_common.get_matching_columns(project.field_names, '\w*(' + '|'.join(DURATION_FIELDS) + ')')
        df = redcap_common.merge_api_data(df, project, fields, [TRACK_STUDY_ID])

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
    df = df.set_index([TRACK_STUDY_ID, redcap_common.SESSION_NUMBER])

    if not args.expand:
        non_session_cols = { col: 's1_' + col for col in df.columns if not re.match('s\d_', col) }
        df = df.rename(columns=non_session_cols)
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
