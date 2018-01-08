import argparse
import pandas as pd
import redcap_common

from os.path import basename, splitext

def extract_form_fields():
    parser = argparse.ArgumentParser(description='Extracts certain fields from a REDCap data export')
    parser.add_argument('input_file', help='exported file to extract fields from')
    parser.add_argument('fields', nargs='+', help='fields to extract from data export')
    parser.add_argument('-m', '--merge', metavar='merge_with_file', help='merge extracted fields with data from specified file')
    parser.add_argument('-o', '--output_file', help='file (csv) to write results to (default: <input_file>_extract.csv or <merge_with_file>_merged.csv)')
    args = parser.parse_args()

    df = pd.read_csv(args.input_file, index_col=[0,1])[args.fields]

    default_output = splitext(basename(args.input_file))[0] + '_extract.csv'
    if args.merge:
        merge_df = pd.read_csv(args.merge, index_col=[0,1])
        df = pd.concat([merge_df, df], axis=1)
        default_output = splitext(basename(args.merge))[0] + '_merged.csv'

    output_file = args.output_file if args.output_file and splitext(args.output_file)[1] == 'csv' else default_output
    redcap_common.write_results_and_open(df, output_file)


extract_form_fields()
