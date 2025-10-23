from flask import Flask, request, render_template, send_file
import pandas as pd
import os
import io

flatten = Flask(__name__)

def process_csv(input_csv_path, pivot_field):
    suffix = f"flattened_by_{pivot_field}"
    
    # Read the CSV file into a DataFrame
    df = pd.read_csv(input_csv_path)

    # Filter out rows where 'study_id' starts with 'z_' or is 'WOLF_SAMPLE'
    df_cleaned = df[~df['study_id'].str.startswith('z_') & (df['study_id'] != 'WOLF_SAMPLE')]

    # Sort df_cleaned by 'study_id' and the specified 'pivot_field'
    df_cleaned = df_cleaned.sort_values(by=['study_id', pivot_field])

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

    # Drop records from df_long where the pivot_field is less than 1 (if the pivot_field is numeric)
    if pd.api.types.is_numeric_dtype(df_long[pivot_field]):
        df_long = df_long[df_long[pivot_field] >= 1]

    # Ensure pivot_field values are integers
    df_long[pivot_field] = df_long[pivot_field].astype(int)

    # Determine the maximum pivot_field value
    max_pivot = df_long[pivot_field].max()

    # Create a DataFrame with all study_id and pivot_field combinations up to max_pivot
    all_combinations = pd.MultiIndex.from_product([df_long['study_id'].unique(), range(1, max_pivot + 1)], names=['study_id', pivot_field])

    # Reindex df_long to include all combinations of study_id and pivot_field, filling missing values with NaN
    df_long = df_long.set_index(['study_id', pivot_field]).reindex(all_combinations).reset_index()

    # Sort df_long by 'study_id' and the specified 'pivot_field' again after reindexing
    df_long = df_long.sort_values(by=['study_id', pivot_field])

    # Rename 'redcap_event_name' to 'clinic_year' in df_long
    df_long = df_long.rename(columns={'redcap_event_name': 'clinic_year'})

    # Change clinic_year values as specified: 4-digit year to the year, mini_clinic_1 to mc1, mini_clinic_2 to mc2
    df_long['clinic_year'] = df_long['clinic_year'].str.replace(r'^(\d{4}).*', r'\1', regex=True)
    df_long['clinic_year'] = df_long['clinic_year'].str.replace(r'^mini_clinic_1.*', 'mc1', regex=True)
    df_long['clinic_year'] = df_long['clinic_year'].str.replace(r'^mini_clinic_2.*', 'mc2', regex=True)

    # Determine columns to pivot other than 'study_id' and the pivot_field
    value_columns = [col for col in df_long.columns if col not in ['study_id', pivot_field]]

    # Pivot the df_long DataFrame based on the pivot_field
    df_pivoted = df_long.pivot_table(index='study_id',
                                     columns=pivot_field,
                                     values=value_columns,
                                     aggfunc='first',
                                     sort=False).reset_index()

    # Flatten the MultiIndex in columns and rename them to have the '_sX' suffix
    df_pivoted.columns = [f'{col[0]}_s{int(col[1])}' if isinstance(col, tuple) and col[1] != '' else col[0]
                          for col in df_pivoted.columns]

    # Merge df_stable with df_pivoted on the study_id key (df_stable first)
    df_merged = pd.merge(df_stable, df_pivoted, on='study_id', how='left')

    # Create the output file path by adding the specified suffix to the filename
    output_csv_path = os.path.splitext(input_csv_path)[0] + f"_{suffix}.csv"

    # Save the merged DataFrame to the new CSV file
    df_merged.to_csv(output_csv_path, index=False)

    return output_csv_path

@flatten.route('/')
def index():
    return render_template('index.html')

@flatten.route('/upload', methods=['POST'])
def upload():
    if 'file' not in request.files:
        return 'No file part'
    file = request.files['file']
    if file.filename == '':
        return 'No selected file'
    
    pivot_field = request.form.get('pivot_field', 'wolfram_sessionnumber')
    
    if file:
        input_csv_path = os.path.join('uploads', file.filename)

        # Save uploaded file to server
        file.save(input_csv_path)
        
        # Process CSV
        output_csv_path = process_csv(input_csv_path, pivot_field)
        
        # Send processed file back to user
        return send_file(output_csv_path, as_attachment=True)
        
if __name__ == '__main__':
    # Ensure the uploads directory exists
    os.makedirs('uploads', exist_ok=True)
    flatten.run(debug=True, host='0.0.0.0')