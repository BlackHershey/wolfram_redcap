import json
import numpy as np
import os
import pandas as pd
import redcap_common
import shutil

from gooey import Gooey, GooeyParser
from wfs_db_migration import replace_values
from zipfile import ZipFile

STATIC_FOLDER = r'//neuroimage.wustl.edu/nil/hershey/H/REDCap Scripts/static/'
VARFILE_TEMPLATE = '{}_{}_column_map.csv'

ASEBA_ID = 'AssessedPersonId'
STUDY_ID_MAP = {
    'NEWT': 'newt_id',
    'NT': 'demo_study_id'
}

def gen_import_file(datafile, varfile, study_name, form_type, flatten=False):
    df = pd.read_excel(datafile)
    change_df = pd.read_csv(varfile)

    # Remove irrelevant columns
    drop_cols = [ col for col in df.columns if col not in change_df['aseba_var'].values ]
    df = df.drop(columns=drop_cols)

    # Determine clinical/borderline checkbox values
    checkbox_lut = np.genfromtxt(os.path.join(STATIC_FOLDER, 'ASEBA_checkbox_map.csv'), delimiter=',', dtype='str')
    for (var, trange) in checkbox_lut:
        if not var in change_df['aseba_var'].values:
            continue
        tscore_col = var.split('.')[0]
        min, max = map(int, trange.split('-'))
        df[var] = df[tscore_col].apply(lambda x: int(x >= min and x <= max))

    # Rename columns to match REDCap variables
    df = df.rename(columns={ row['aseba_var']: row['redcap_var'] for _, row in change_df.iterrows() if pd.notnull(row['redcap_var'])})

    # Assign value to static columns that need to be present
    assign_col_df = change_df[pd.notnull(change_df['fill_value'])]
    for _, row in assign_col_df.iterrows():
        df[row['redcap_var']] = row['fill_value']

    # Extract study_id/redcap_event_name and rename
    study_id_col = STUDY_ID_MAP[study_name] if study_name in STUDY_ID_MAP else 'record_id'
    index_cols = [study_id_col, 'redcap_event_name']
    df[index_cols] = df[ASEBA_ID].str.split('_', 1, expand=True)
    df = df.drop(columns=[ASEBA_ID]).set_index(index_cols)

    # Flatten dataframe for multi-session databases NOT in longitudinal format
    if flatten:
        df = redcap_common.flatten(df)

    outroot = os.path.splitext(datafile)[0]
    df.to_csv(outroot + '_import.csv')


@Gooey()
def parse_args():
    parser = GooeyParser(description='Format ASEBA score export for REDCap import')

    required = parser.add_argument_group('Required arguments')
    required.add_argument('--aseba_export', required=True, widget='FileChooser', help='Excel scores export from ASEBA')
    required.add_argument('--study_name', required=True, choices=['NEWT', 'NT', 'other'])
    required.add_argument('--form_type', required=True, choices=['cbcl', 'ycbcl'])

    other = parser.add_argument_group('Other study options (can ignore if using named study)', gooey_options={'columns':1})
    other.add_argument('--varfile', widget='FileChooser', help='csv file with REDCap to ASEBA mapping (see H:/REDCap Scripts/static/*cbcl_column_map.csv for examples)')
    other.add_argument('--wide', action='store_true', help='if REDCap has multiple sessions per row (vs each session on own line)')
    args = parser.parse_args()

    if args.study_name == 'other' and not args.varfile:
        parser.error('Must supply varfile if not using named study')

    return args

if __name__ == '__main__':
    args = parse_args()

    varfile = os.path.join(STATIC_FOLDER, '{}_{}_column_map.csv'.format(args.study_name, args.form_type)) if not args.varfile else args.varfile
    flatten = args.wide or args.study_name in ['NEWT']

    gen_import_file(args.aseba_export, varfile, args.study_name, args.form_type, flatten)
