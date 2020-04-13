import pandas as pd
import re
import redcap_common

from gooey import Gooey, GooeyParser

@Gooey
def parse_args():
    parser = GooeyParser(description='Converts data in REDCap format (one row per session) to SPSS format (one row per subject) and vice versa')
    parser.add_argument('input_file', widget='FileChooser', help='csv file (full path) to be formatted (first column should contain subject ids)')
    parser.add_argument('output_file', help='csv file (full path) to store formatted data (if file does not exist, it will be created)')

    args = parser.parse_args()
    if not args.input_file.endswith('.csv') or not args.output_file.endswith('.csv'):
        parser.error('Input and output files must be of type csv')

    return args


def redcap2spss(input_file, output_file):
    df = redcap_common.create_df(input_file)

    if df.shape[0] == len(df.iloc[:, 0].unique()): # if the row count is the same as the unique indentifiers, assume spss to redcap
        # get columns that have session as a suffix and make it a prefix instead (this is the format expand expects)
        suffixed_cols = { col: '_'.join([col[-2:], col[:-3]]) for col in df.columns if re.search(r'_s\d$', col) }
        if suffixed_cols:
            df = df.rename(columns=suffixed_cols)

        df, non_session_cols = redcap_common.expand(df.set_index(df.columns[0]))
        df= df.set_index(df.columns[0])
    else: # assume redcap to spss
        prefix = 's' if df[df.columns[1]].dtype == 'int64' else ''
        df = redcap_common.flatten(df.set_index([df.columns[0], df.columns[1]]), sort=True, prefix=prefix)

    redcap_common.write_results_and_open(df, output_file)


if __name__ == '__main__':
    args = parse_args()
    redcap2spss(args.input_file, args.output_file)
