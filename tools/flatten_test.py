import pandas as pd
import argparse
import os

# Set up argument parser
parser = argparse.ArgumentParser(description='Process and pivot a CSV file.')
parser.add_argument('input_csv', type=str, help='Path to the input CSV file')
args = parser.parse_args()

# Read the CSV file into a DataFrame
df = pd.read_csv(args.input_csv)

# Filter out rows where 'study_id' starts with 'z_' or is 'WOLF_SAMPLE'
df_cleaned = df[~df['study_id'].str.startswith('z_') & (df['study_id'] != 'WOLF_SAMPLE')]

# Rename values in 'redcap_event_name' that start with "stable" to "stable"
df_cleaned['redcap_event_name'] = df_cleaned['redcap_event_name'].str.replace(r'^stable.*', 'stable', regex=True)

# Extract records where 'redcap_event_name' equals "stable"
df_stable = df_cleaned[df_cleaned['redcap_event_name'] == 'stable']

# Drop 'redcap_event_name' column from df_stable
df_stable = df_stable.drop(columns=['redcap_event_name'])

# Drop all empty columns from df_stable
df_stable = df_stable.dropna(axis=1, how='all')

# Rename the cleaned DataFrame without 'stable' records to df_long
df_long = df_cleaned[df_cleaned['redcap_event_name'] != 'stable']

# Drop records from df_long where 'nfl_sample_index' is less than 1
df_long = df_long[df_long['nfl_sample_index'] >= 1]

# Rename 'redcap_event_name' to 'clinic_year' in df_long
df_long = df_long.rename(columns={'redcap_event_name': 'clinic_year'})

# Change clinic_year values as specified: 4-digit year to the year, mini_clinic_1 to mc1, mini_clinic_2 to mc2
df_long['clinic_year'] = df_long['clinic_year'].str.replace(r'^(\d{4}).*', r'\1', regex=True)
df_long['clinic_year'] = df_long['clinic_year'].str.replace(r'^mini_clinic_1.*', 'mc1', regex=True)
df_long['clinic_year'] = df_long['clinic_year'].str.replace(r'^mini_clinic_2.*', 'mc2', regex=True)

# Determine columns to pivot other than 'study_id' and 'nfl_sample_index'
value_columns = [col for col in df_long.columns if col not in ['study_id', 'nfl_sample_index']]

# Pivot the df_long DataFrame based on nfl_sample_index
df_pivoted = df_long.pivot_table(index='study_id',
                                 columns='nfl_sample_index',
                                 values=value_columns,
                                 aggfunc='first',
                                 sort=False).reset_index()

# Flatten the MultiIndex in columns and rename them to have the '_sX' suffix
df_pivoted.columns = [f'{col[0]}_s{int(col[1])}' if isinstance(col, tuple) and col[1] != '' else col[0]
                      for col in df_pivoted.columns]

# Merge df_stable with df_pivoted on the study_id key (df_stable first)
df_merged = pd.merge(df_stable, df_pivoted, on='study_id', how='left')

# Create the output file path by adding '_flattened' to the filename
input_csv_path = args.input_csv
base, ext = os.path.splitext(input_csv_path)
output_csv_path = f"{base}_flattened{ext}"

# Save the merged DataFrame to the new CSV file
df_merged.to_csv(output_csv_path, index=False)

print("Stable records have been extracted into a separate DataFrame named 'df_stable' and the 'redcap_event_name' column has been dropped.")
print("The 'df_long' DataFrame has been flattened and pivoted, and merged with 'df_stable'.")
print(f"The merged DataFrame has been saved to: {output_csv_path}")