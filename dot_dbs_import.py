from gooey import Gooey, GooeyParser
from numpy import nan
from os import chdir, getcwd, listdir
from os.path import join, exists
from stringcase import titlecase
from sys import exit, stderr

import pandas as pd
import re
import redcap_common

# UNUSED_VARS = [ 'fwhm', 'b11h' ]

@Gooey
def dot_dbs_import():
    parser = GooeyParser(description='Formats kinematics data for redcap import')
    required_group = parser.add_argument_group('Required Arguments', gooey_options={'columns': 1})
    required_group.add_argument('--folder', widget='DirChooser', required=True, help='Folder containing subject directories to be processed')
    optional_group = parser.add_argument_group('Optional Arguments', gooey_options={'columns': 1})
    optional_group.add_argument('-s', '--subjects', nargs='+', help='Space-separated list of subject ids to run for (if blank, runs all in folder)')
    args = parser.parse_args()

    if not exists(args.folder):
        parser.error('Specified folder does not exist.')

    script_dir = getcwd()
    chdir(args.folder)

    result = None
    subject_dirs = [ d for d in listdir(getcwd()) if re.match('DOTDBS(\d)+$', d) ] if not args.subjects else args.subjects

    if not subject_dirs:
        stderr.write('No subject directories found matching pattern: {}'.format('DOTDBS##'))
        exit(1)

    for i, subject in enumerate(subject_dirs):
        chdir(subject)
        summary_files = [ f for f in listdir(getcwd()) if f.endswith('.xlsx') ]
        if not summary_files:
            print('Subject {} does not have a summary file'.format(subject))
            chdir('..')
            continue

        subject_df = pd.read_excel(summary_files[0], index_col=0, sheet_name=[0,1,2])
        temp_df = None
        for sheet in subject_df.keys():
            print('Processing: {}, block {}'.format(subject, sheet+1))
            subject_df[sheet] = subject_df[sheet][subject_df[sheet].index.notnull()]
            measures = subject_df[sheet].index
            measures = [ ''.join([w[0].lower() for w in s ])
                            for s in [titlecase(m).split() for m in measures ]] # get strings of lowercased first letters for each word in each measure
            measures = [ '_'.join([m[:2], m[2:]]).replace('rt', 'at').replace('ft_pvcov', 'ft_pvcv').replace('b11h', '')
                            for m in measures ] # split var into action type and measure and rename mismatched var names
            subject_df[sheet].index = measures
            subject_df[sheet].insert(0, 'record_id', subject)
            subject_df[sheet].insert(1, 'block', 'Block' + str(sheet+1))
            subject_df[sheet].set_index(['record_id', 'block'], append=True, inplace=True)
            subject_df[sheet] = subject_df[sheet].reorder_levels([1,2,0])
            subject_df[sheet] = subject_df[sheet].dropna(axis=1, how='all')
            subject_df[sheet].columns = [ col.lower() for col in subject_df[sheet].columns ]
            subject_df[sheet] = subject_df[sheet].rename(columns={'left avg': 'Left', 'right avg': 'Right'})
            subject_df[sheet] = subject_df[sheet].drop([col for col in subject_df[sheet].columns if col not in ['Left', 'Right']], axis=1)
            subject_df[sheet] = subject_df[sheet].unstack([1,2]) # .sort_index(1, level=1)
            subject_df[sheet].columns = [ ('_'.join([tup[1], tup[2], tup[0]])).lower() for tup in subject_df[sheet].columns ]
            temp_df = subject_df[sheet] if sheet == 0 else pd.concat([temp_df, subject_df[sheet]], axis=1)

        result = temp_df if i == 0 else pd.concat([result, temp_df], axis=0)
        chdir('..')

    ft_mpv_columns = { col: col.replace('right', 'left_right') for col in result.columns if col.endswith('ft_mpv_right') }
    result.rename(columns=ft_mpv_columns, inplace=True) # hack to fix deviation from regular naming convention ('_left_right' instead of '_right')

    # unused_cols = [ col for col in result.columns if any(var in col for var in UNUSED_VARS) ]
    # result.drop(unused_cols, axis=1, inplace=True)

    for i in range(1,4):
        result['_'.join(['block', str(i), 'kinematics_complete'])] = 1

    print('here')

    redcap_common.write_results_and_open(result, join(script_dir, 'formatted_dotdbs.csv'))
    return


dot_dbs_import()
