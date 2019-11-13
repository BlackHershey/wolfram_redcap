import json
import os
import pandas as pd
import redcap_common
import shutil

from gooey import Gooey, GooeyParser
from wfs_db_migration import replace_values
from zipfile import ZipFile

STATIC_FOLDER = r'//neuroimage.wustl.edu/nil/hershey/H/REDCap Scripts/static/'
JSON_MAPFILE = os.path.join(STATIC_FOLDER, 'ASEBA_json_mapping.csv')
CONTENT_TYPES_XML = os.path.join(STATIC_FOLDER, '[Content_Types].xml')

FORM_LUT = {
    'cbcl': {
        'FormInstrumentId': '07855877-88cf-458a-8d4e-93be0af21fa6',
        'fname_id': 3001
    },
    'ycbcl': {
        'FormInstrumentId': '35aebf67-0ec4-407a-a240-1966a4852031',
        'fname_id': 6001
    }
}

def write_json(row, var_lut, outdir, form_ins_id):
    print('name', row.name)
    patid = '_'.join(row.name)

    answers = []
    for col in row.axes[0].tolist():
        if col not in var_lut or pd.isnull(row[col]):
            continue

        try:
            val = float(row[col])
            val = int(val)
        except:
            val = row[col]

        answers.append({
            'QuestionId': var_lut[col],
            'Value': str(val),
            'Comments': []
        })

    data = {
        'PersonInformation': {
            'IdentificationCode': patid
        },
        'Forms': [
            {
                'FormInstrument': {
                    'Id': form_ins_id,
                    'Answers': answers
                },
                'Society': {
                    'Id': '622adb6d-dede-48e3-ae34-e80cdeb37ed8'
                }
            }
        ]
    }

    with open(os.path.join(outdir, patid + '.json'), 'w') as f:
        json.dump(data, f)
    return


def gen_import_file(datafile, varfile, form_type, outdir, expand=False):
    df = pd.read_csv(datafile, dtype="object")
    change_df = pd.read_csv(varfile)
    json_df = pd.read_csv(JSON_MAPFILE)
    change_df = change_df.merge(json_df, left_on='aseba_var', right_on='aseba_var')

    df.columns.values[0] = redcap_common.STUDY_ID
    if expand:
        df, non_session_cols = redcap_common.expand(df.set_index(redcap_common.STUDY_ID))
        stable_cols = list(non_session_cols.keys())
    else:
        df.columns.values[1] = redcap_common.SESSION_NUMBER
        aseba_sex_var = next(var for var in change_df['aseba_var'].values if var.endswith('_sex'))
        stable_cols = change_df[change_df['aseba_var'] == aseba_sex_var]['redcap_var']

    df[stable_cols] = df.groupby(redcap_common.STUDY_ID)[stable_cols].fillna(method='ffill')
    df = df.set_index([redcap_common.STUDY_ID, redcap_common.SESSION_NUMBER])

    # handle backfill of gender -- only gets input for cbcl not ycbcl

    drop_cols = [ col for col in df if col not in change_df['redcap_var'].values ]
    df = df.drop(columns=drop_cols)
    df = df.dropna(how='all', subset=[col for col in df.columns if 'cbcl' in col])
    df = replace_values(df, change_df, index_col='redcap_var')

    outroot = 'aseba_import'
    outpath = os.path.join(outdir, outroot)
    if not os.path.exists(outpath):
        os.mkdir(outpath)
    shutil.copyfile(CONTENT_TYPES_XML, os.path.join(outpath, os.path.basename(CONTENT_TYPES_XML)))

    var_lut = { row['redcap_var']: row['json_id'] for _, row in change_df.iterrows() if pd.notnull(row['json_id']) }
    fname_key = next((key for key, val in var_lut.items() if val == FORM_LUT[form_type]['fname_id']), None)
    form_ins_id = FORM_LUT[form_type]['FormInstrumentId']
    df.apply(write_json, args=(var_lut, outpath, form_ins_id), axis=1)

    shutil.make_archive(outpath, 'zip', os.path.dirname(outpath), outroot)


@Gooey()
def parse_args():
    parser = GooeyParser(description='Format redcap CBCL export for ASEBA import')
    required = parser.add_argument_group('Required Arguments')
    required.add_argument('--redcap_export', required=True, widget='FileChooser', help='Demographics + CBCL export from REDCap')
    required.add_argument('--study_name', required=True, choices=['NEWT', 'NT', 'other'])
    required.add_argument('--form_type', required=True, choices=['cbcl', 'ycbcl'])
    required.add_argument('--outdir', widget='DirChooser', required=True, help='where to store output zip')

    other = parser.add_argument_group('Other study options (can ignore if using named study)', gooey_options={'columns':1})
    other.add_argument('--varfile', widget='FileChooser', help='csv file with REDCap to ASEBA mapping (see H:/REDCap Scripts/static/*cbcl_column_map.csv for examples)')
    other.add_argument('--wide', action='store_true', help='if REDCap export contains multiple sessions per row (vs each session on own line)')

    args = parser.parse_args()

    if args.study_name == 'other' and not args.varfile:
        parser.error('Must supply varfile if not using named study')

    return args

if __name__ == '__main__':
    args = parse_args()

    varfile = os.path.join(STATIC_FOLDER, '{}_{}_column_map.csv'.format(args.study_name, args.form_type)) if not args.varfile else args.varfile
    expand = args.wide or args.study_name in ['NEWT']

    gen_import_file(args.redcap_export, varfile, args.form_type, args.outdir, expand)
