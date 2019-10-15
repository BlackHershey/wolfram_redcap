import json
import numpy as np
import os
import pandas as pd
import shutil

from gooey import Gooey, GooeyParser
from redcap_common import flatten
from wfs_db_migration import replace_values
from zipfile import ZipFile

STATIC_FOLDER = r'//neuroimage.wustl.edu/nil/hershey/H/REDCap Scripts/static/'
VARFILE_TEMPLATE = '{}_{}_column_map.csv'

ASEBA_ID = 'AssessedPersonId'
STUDY_ID_MAP = {
    'NEWT': 'newt_id',
    'NT': 'demo_study_id'
}


def gen_import_file(datafile, study_name, form_type):
    df = pd.read_excel(datafile)
    varfile = os.path.join(STATIC_FOLDER, VARFILE_TEMPLATE.format(study_name, form_type))
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
    index_cols = [STUDY_ID_MAP[study_name], 'redcap_event_name']
    df[index_cols] = df[ASEBA_ID].str.split('_', 1, expand=True)
    df = df.drop(columns=[ASEBA_ID]).set_index(index_cols)

    # Flatten dataframe for multi-session databases NOT in longitudinal format
    if study_name in ['NEWT']:
        df = flatten(df)

    outroot = os.path.splitext(datafile)[0]
    df.to_csv(outroot + '_import.csv')


@Gooey()
def parse_args():
    parser = GooeyParser(description='Format ASEBA score export for REDCap import')
    parser.add_argument('--aseba_export', required=True, widget='FileChooser', help='Excel scores export from ASEBA')
    parser.add_argument('--study_name', required=True, choices=['NEWT', 'NT'])
    parser.add_argument('--form_type', required=True, choices=['cbcl', 'ycbcl'])
    return parser.parse_args()


if __name__ == '__main__':
    args = parse_args()
    gen_import_file(args.aseba_export, args.study_name, args.form_type)
