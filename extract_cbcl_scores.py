import argparse
import csv
import numpy as np
import pandas as pd
import re
import redcap_common

from datetime import datetime
from gooey import Gooey, GooeyParser
from os import listdir, stat
from os.path import join, splitext
from subprocess import call

TABULA_JAR = r'\\neuroimage.wustl.edu\nil\Hershey\H\REDCap Scripts\dependencies\tabula-1.0.2-jar-with-dependencies.jar'

COMP_SCALE1 = [ 'activities', 'social', 'school' ]
COMP_SCALE2 = [ 'totalcomp' ]
SYN_SCALE = [ 'anxdepr', 'withdepr', 'somcomp', 'socprob', 'thoprob', 'attenprob', 'rulebreak', 'aggbeh' ]
INT_EXT_PROBS = [ 'intprob', 'extprob', 'totalprob' ]
DSM_SCALE = [ 'affecprob', 'anxprob', 'somprob', 'adhdprob', 'opdefprob', 'condprob' ]

ALL_MEASURES = COMP_SCALE1 + COMP_SCALE2 + SYN_SCALE + INT_EXT_PROBS + DSM_SCALE

def extract_scores(input_folder, output_file, study_id_var, subject_prefix, subjects=[], from_date=None, non_long=False):
	filename_format = '(({}\d+)_(\w+).\w+)'.format(subject_prefix) # get all the converted score reports (capturing the filename and the visit type)
	all_files = [ re.search(filename_format, f, flags=re.IGNORECASE) for f in listdir(input_folder) ]
	all_files = [ result.groups() for result in all_files if result ]
	pdf_files = [ f for f in all_files if re.search('.pdf$', f[0], flags=re.IGNORECASE) ]

	index_cols = [ study_id_var, 'redcap_event_name', 'score_type']
	results = [] # array of arrays of scores (raw/t/percentile) for each CBCL measure
	for file_info in pdf_files:
		result = {}
		if subjects and file_info[1] not in subjects:
			continue
		if from_date and datetime.fromtimestamp(stat(join(input_folder, file_info[0])).st_mtime) < from_date:
			continue

		root, ext = splitext(file_info[0])
		csv_file = root + '.csv'
		if csv_file not in [ f[0] for f in all_files ]:
			print('No csv file: {}. Converting pdf to csv...'.format(csv_file))
			call(['java', '-jar', TABULA_JAR, '-l', '-o', join(input_folder, csv_file), '--pages', 'all', join(input_folder, file_info[0])])

		with open(join(input_folder, csv_file), 'r') as f:
			print('Processing file: {}'.format(csv_file))
			reader = csv.reader(f)
			for row in reader:
				search_res = re.match('(Total Score|T Score|Percentile)', row[0])
				if search_res:
					score_type = search_res.group(1)
					if score_type not in result:
						result[score_type] = []
					result[score_type] += list(filter(None, row[1:]))

		ranges = [ 'clinical', 'borderline' ]
		for range in ranges:
			result[range] = [ int('-' + range[0].upper() in score) for score in result['T Score'] ] # check if -C or -B in in T score

		result['T Score'] = [ score.split('-')[0] for score in result['T Score'] ]
		for type in result.keys():
			results.append([file_info[1], file_info[2], type] + result[type])

	columns = index_cols + ALL_MEASURES
	if len(columns) - len(results[0]) == 4:
		print('Assuming that competence scale measures were not collected....')
		columns = [ col for col in columns if col in index_cols or col not in COMP_SCALE1 + COMP_SCALE2 ]

	df = pd.DataFrame(data=results, columns=columns).set_index(index_cols)
	df = df.rename(index={'Total Score': 'raw', 'T Score': 't', 'Percentile': 'per'})
	df = df.replace('nc', np.nan).dropna(axis=1, how='all') # drop columns that are all NaN (not all studies collect competence scale data)
	df = redcap_common.flatten(df, sort=False, prefix='cbcl_')

	checkbox_cols = [ re.search('(cbcl_(?:borderline|clinical)_(\w+))', col) for col in df.columns ]
	checkbox_col_info = [ result.groups() for result in checkbox_cols if result ]
	checkbox_col_renames = { group[0]: '{}___{}'.format(group[0].rsplit('_', 1)[0], ALL_MEASURES.index(group[1])+1) for group in checkbox_col_info }
	df = df.rename(columns=checkbox_col_renames)

	if non_long:
		df = redcap_common.flatten(df) # flatten again to add session prefix to columns if non-longitudinally stored

	df.to_csv(output_file)


@Gooey()
def parse_args():
	parser = GooeyParser(description='Generate REDCap import for CBCL scores from PDF reports')

	required_group = parser.add_argument_group('Required Arguments', gooey_options={'columns': 1})
	required_group.add_argument('--folder', widget='DirChooser', required=True, help='Folder containing score reports to be processed')
	required_group.add_argument('--outfile', widget='FileChooser', required=True, help='File to output results to')
	required_group.add_argument('--study_id_var', help='REDCap variable for study id (i.e. demo_study_id, newt_id)')
	required_group.add_argument('--subject_prefix', required=True, help='prefix for subject identifier string (i.e. NT, NEWT, WOLF)')

	optional_group = parser.add_argument_group('Optional Arguments', gooey_options={'columns': 1})
	optional_group.add_argument('-s', '--subjects', nargs='+', help='Space-separated list of subject ids to run for (if blank, runs all in folder)')
	optional_group.add_argument('--from_date', type=lambda d: datetime.strptime(d, '%Y-%m-%d'), widget='DateChooser', help='Process only files that were modified on/after specified date')
	optional_group.add_argument('--flatten_sessions', action='store_true', help='flatten session names (only needed if REDCap is set up non-longitudinally)')

	return parser.parse_args()


if __name__ == '__main__':
	args = parse_args()
	extract_scores(args.folder, args.outfile, args.study_id_var, args.subject_prefix, args.subjects, args.from_date, args.flatten_sessions)
