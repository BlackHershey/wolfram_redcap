from gooey import Gooey, GooeyParser
from os import listdir
from os.path import join, exists

import pandas as pd
import numpy as np
import re
import redcap_common

COG_FOLDER = 'H:\\NEWT\\Data\\Cognitive'

UNADJUSTED = 'Uncorrected Standard Score'
ADJUSTED = 'Age-Corrected Standard Score'

parent_vars = {
	'Picture Vocabulary': 'parent_pic_vocab',
	'Oral Reading': 'parent_oral_reading',
	'Crystallized Composite': 'parent_verbal_iq',
	'Neuro-QoL': 'parent_neuroqol'
}

subject_vars = {
	'Picture Vocabulary': 'vocab',
	'Flanker Inhibitory Control': 'flanker',
	'List Sorting': 'listsort',
	'Dimensional Change': 'dccs',
	'Pattern Comparison': 'pattern',
	'Picture Sequence': 'picseq',
	'Oral Reading': 'oral_reading',
	'Fluid Composite': 'fluid_cog',
	'Crystallized Composite': 'crystal_cog', # for unadjusted, cog_crystal for adjusted
	'Total Composite': 'cog_composite',
	'Early Childhood': 'cog_earlychild',
	'Neuro-QoL': 'self_neuroqol'
}

def replace_variables(df, var_map):
	renames = { inst: v for k,v in var_map.items() for inst in df['Inst'] if k in inst }
	return df.replace(list(renames.keys()), list(renames.values()))


def get_session_number(pin):
	session = re.search(r'(s\d?)', pin, flags=re.IGNORECASE)
	return session.group().lower() if session else '1'


def extract_id_and_session(df):
	# df['session_number'] = df['newt_id'].apply(get_session_number)
	# df['newt_id'] = df['newt_id'].apply(lambda x: re.match('(NEWT *\d{3}?)', x, flags=re.IGNORECASE).group().upper().replace(' ', ''))
	# print('newt_id = {}'.format(df['newt_id']))
	df[['newt_id', 'session_number']] = df['newt_id'].str.extract(r'(?P<newt_id>\w{0,4} ?\d{1,4})?(?:_| |-)?(?:s?)?(?P<session_number>\d?)', flags=re.IGNORECASE)
	df['newt_id'] = df['newt_id'].apply(lambda x: x.replace(' ', '').upper())
	df['session_number'] = df['session_number'].replace(r'^\s*$', np.NaN, regex=True).fillna('1').str.lower()
	# df.to_csv('after_replace_s1.csv')
	return df


def nih_toolbox_import(exports_folder, subjects):
	result = None
	exports = [ f for f in listdir(exports_folder) if f.endswith('.csv') ]
	for export in exports:
		df = pd.read_csv(join(exports_folder, export)).dropna(how='all')
		if UNADJUSTED not in df.columns:
			print('WARNING: SKIPPING "{}", Column named "{}" not found'.format(export,UNADJUSTED))
			continue
		else:
			print('Processing: "{}"'.format(export))

		# df.to_csv('df_before_rename.csv')
		df = df.rename(columns={'PIN': 'newt_id',  'RawScore': 'raw', 'TScore': 'tscore'})

		# check for S2 mismatch
		if ( 'S2' in export.upper()):
			S2_mismatch = True in ( 'S2' not in newt_id.upper() for newt_id in df['newt_id'].drop_duplicates().tolist() )
			if S2_mismatch:
				print('WARNING: S2 mismatch detected in "{}", check csv file'.format(export))

		# df.to_csv('df_after_rename.csv')
		parent_df = df[df['newt_id'].str.contains('parent', flags=re.IGNORECASE)]
		# parent_df.to_csv('parent_df.csv')
		subject_df = df[~df['newt_id'].isin(parent_df['newt_id'])]

		if not parent_df.empty:
			parent_df = replace_variables(parent_df, parent_vars)
			parent_df = extract_id_and_session(parent_df)
			# print(export, " contains parent data")
		else:
			parent_df = None

		if not subject_df.empty:
			subject_df = subject_df.groupby('newt_id').apply(lambda x: replace_variables(x, subject_vars) if len(x) > 4 else replace_variables(x, parent_vars))
			# subject_df.to_csv('subject_df_before_extract.csv')
			subject_df = extract_id_and_session(subject_df)
		else:
			subject_df = None

		export_df = pd.concat([subject_df, parent_df], axis=0)
		result = pd.concat([result, export_df], axis=0)

	print('Formatting...')

	if subjects:
		result = result[result['newt_id'].isin(subjects)]
	result.rename(columns={UNADJUSTED: 'unadj', ADJUSTED: 'ageadj'}, inplace=True)
	result = result.drop_duplicates(subset=['newt_id', 'Inst', 'unadj'], keep='last')
	result.set_index(['newt_id', 'session_number', 'Inst'], inplace=True)
	score_cols = ['raw', 'tscore', 'unadj', 'ageadj']
	result = result[score_cols]

	result = result.dropna(how='all', subset=score_cols)
	# result.to_csv('result_before_flatten.csv')
	result = redcap_common.simple_flatten(redcap_common.simple_flatten(result),True,'s')

	# perform renames - no session numbers and changes suffixes for parent iq columns, cog_crystal order change for ageadj column,
	#	remove raw suffix for self_neuroqol column
	result.rename(columns={ col: col.replace('unadj', 'unc').replace('ageadj', 'ac') for col in result.columns if 'parent' in col }, inplace=True)
	result.rename(columns={ col: col.replace('crystal_cog', 'cog_crystal') for col in result.columns if 'crystal_cog_ageadj' in col }, inplace=True)
	result.rename(columns={ col: col[3:] for col in result.columns if 'parent' in col and 'neuroqol' not in col }, inplace=True)
	result.rename(columns={ col: col[:-4] for col in result.columns if 'self_neuroqol_raw' in col }, inplace=True)

	drop_cols = [ col for col in result.columns if not 'neuroqol' in col and re.match(r'\w*_(raw|tscore)$', col) ]
	result = result.drop(drop_cols, axis=1)

	redcap_common.write_results_and_open(result, 'nih_result.csv')

	return

@Gooey
def parse_args():
	parser = GooeyParser(description='Format NIH toolbox cognitive data for redcap import')
	required_group = parser.add_argument_group('Required Arguments', gooey_options={'columns': 1})
	required_group.add_argument('--exports_folder', widget='DirChooser', required=True, help='Folder containing export files to be processed')
	optional_group = parser.add_argument_group('Optional Arguments', gooey_options={'columns': 1})
	optional_group.add_argument('-s', '--subjects', nargs='+', help='Space separated list of subject ids to run for (if blank, runs all in folder)')
	return parser.parse_args()


if __name__ == '__main__':
	args = parse_args()
	if not exists(args.exports_folder):
		parse_args.error('Specified folder does not exist.')

	nih_toolbox_import(args.exports_folder, args.subjects)
