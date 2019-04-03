import argparse
import pandas as pd
import re
import redcap_common

from gooey import Gooey, GooeyParser
from os.path import basename, splitext

def extract_form_fields(input_file, fields, merge=None, output_file=None):
    df = pd.read_csv(input_file, index_col=[0,1])
    columns = [ col for field in fields for col in df.columns if re.match(field, col) ]
    df = df[columns]

    default_output = splitext(input_file)[0] + '_extract.csv'
    if merge:
        merge_df = pd.read_csv(merge, index_col=[0,1])
        df = merge_df.join(df, how='outer', rsuffix='_duplicate')
        shared_cols = [ col for col in df if col.endswith('_duplicate') ]
        for col in shared_cols:
            orig_col = col.rsplit('_', 1)[0]
            df[orig_col] = df[orig_col].fillna(df[col])
        df = df.drop(columns=shared_cols)
        default_output = splitext(merge)[0] + '_merged.csv'

    output_file = output_file if output_file and splitext(output_file)[1] == '.csv' else default_output
    redcap_common.write_results_and_open(df, output_file)

@Gooey()
def parse_args():
    parser = GooeyParser(description='Extracts certain fields from a REDCap data export')

    required_group = parser.add_argument_group('Required Arguments', gooey_options={'columns': 1})
    required_group.add_argument('--input_file', required=True, widget='FileChooser', help='exported file to extract fields from')
    required_group.add_argument('--fields', required=True, nargs='+', help='Space-separated list of fields to extract from data export (can be exact column names or regexes)')

    optional_group = parser.add_argument_group('Optional Arguments', gooey_options={'columns': 1})
    optional_group.add_argument('-m', '--merge', widget='FileChooser', metavar='merge_with_file', help='merge extracted fields with data from specified file')
    optional_group.add_argument('-o', '--output_file', widget='FileChooser', help='file (csv) to write results to (default: <input_file>_extract.csv or <merge_with_file>_merged.csv)')

    return parser.parse_args()

if __name__ == '__main__':
    args = parse_args()
    extract_form_fields(args.input_file, args.fields, args.merge, args.output_file)
