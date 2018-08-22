import argparse
import numpy as np
import pandas as pd
import re
import sys

sys.path.append(r'H:\REDCap Scripts')

from collections import OrderedDict
from gooey import Gooey, GooeyParser
from os.path import join
from redcap_common import expand

def card1_specific_changes(card1):
    incorrectly_numbered = [ col for col in card1.axes[0].tolist() if col.startswith('cbcl_number') or col == 'cbcl_times_a_week_friends' ]
    na_5only = [ col for col in card1.axes[0].tolist() if col.startswith('cbcl_other_sub') ]
    na_4and5 = [ col for col in card1[:'cbcl_chores_well3'].axes[0].tolist() if not col.startswith('cbcl_number') ]
    yes_no = ['cbcl_special_edu', 'cbcl_repeat_grade', 'cbcl_acad_prob']

    card1[incorrectly_numbered] = card1[incorrectly_numbered] - 1
    card1[na_5only] = card1[na_5only].replace(5, 9)
    card1[na_4and5] = card1[na_4and5].replace([4, 5], 9)
    card1[yes_no] = card1[yes_no].apply(lambda x: 1 if x == 2 else 2)
    card1[['cbcl_along_w_sibs']] = card1[['cbcl_along_w_sibs']].replace(4, 0) # no sibs is incorrectly coded as 4 instead of 0
    card1[['cbcl_times_a_week_friends']] = card1[['cbcl_times_a_week_friends']].replace(0, 1) # extra option of "none" in redcap, should be "less than 1"

    return card1


def card(card_number, row, card_bounds):
    line = ''
    card = row[card_bounds[0]:card_bounds[1]]

    if card_number == '01':
        line += ' ' * 18
        card = card1_specific_changes(card)
    elif card.min() != 0 or card.max == 3:
        card = card - 1

    card = card.fillna(9)
    line += ''.join([str(int(n)) for n in card.values])

    return line

def get_matching_col(row, pattern):
    return [ col for col in row.axes[0].tolist() if re.match(pattern, col) ][0]


def gen_subject_file(row, outfolder, question_prefix, card1_bounds, demo_cols):
    study_id = row.name
    print('Processing ', study_id)

    if row[list(demo_cols.values())].isnull().any():
        print('Skipping subject {} due to missing age and/or gender'.format(study_id))
        return

    study_id = '_'.join([study_id, row['session_number'].replace('_', '')])
    subj_str = '{: >10}{}{}{:02}'.format(study_id[:10], '{}', int(row[demo_cols['gender']]), int(row[demo_cols['age']]))

    cards = OrderedDict([
        ('01', card1_bounds),
        ('02', (question_prefix + '1',  question_prefix + '58')),
        ('03', (question_prefix + '59', get_matching_col(row, question_prefix + '113')))
    ])

    line = ''
    for num, col_tuple in cards.items():
        line += subj_str.format(num)
        if num == '01' and not all(card1_bounds):
            line += ' ' * 18 + '9' * 38
        else:
            line += card(num, row, col_tuple)

        line_end = 'CBC' if num == '01' else ''
        line_end = 'END' if num == '03' else line_end

        line += line_end + '\n'

    outfile = '{}.CBC'.format(study_id)
    with open(join(outfolder, outfile), 'w') as f:
        f.write(line)
    return


def cbcl_import(file, data_dict, outfolder, question_prefix, card1_bounds=(None,None)):
    df = pd.read_csv(file, index_col=0)

    data_dict_df = pd.read_csv(data_dict, index_col=0)
    cbclq_df = data_dict_df[data_dict_df['Field Type'] == 'radio']
    keep_qcols = [ col for col in list(cbclq_df.index.values) if col.startswith(question_prefix) and col in df.columns ]
    df.drop(columns=[ col for col in df.columns if col.startswith(question_prefix) and col not in keep_qcols], inplace=True)

    # if non-longitudinal, expand the df; otherwise, rename the event column to match expanded naming scheme
    df = redcap_common.expand(df) if 'redcap_event_name' not in df.columns else df.rename(columns={'redcap_event_name': 'session_number'})

    gender_cols = [ col for col in df.columns if 'gender' in col ]
    age_cols = [ col for col in df.columns if 'age' in col ]
    if len(gender_cols) < 1 or len(age_cols) < 1:
        sys.stderr.write('Age and gender columns must be included in REDCap export.')
        sys.exit(2)
    demo_cols = { 'gender': gender_cols[0], 'age': age_cols[0] }

    df = df[df[keep_qcols].sum(axis=1) != 0] # get rid of empty rows (all nan or 0)
    print(df)
    df.apply(gen_subject_file, args=(outfolder, question_prefix, card1_bounds, demo_cols), axis=1)


@Gooey()
def parse_args():
    parser = GooeyParser(description='Format redcap export for ADM import\n(Assumes REDCap form is in order of actual CBCL questionnaire)')

    required_group = parser.add_argument_group('Required Arguments', gooey_options={'columns': 1})
    required_group.add_argument('--export_file', required=True, widget='FileChooser', help='Demographics + CBCL export from REDCap')
    required_group.add_argument('--data_dict', required=True, widget='FileChooser', help='REDCap data dictionary export for CBCL')
    required_group.add_argument('--output_folder', required=True, widget='DirChooser', help='Folder to store output CBC files')
    required_group.add_argument('--question_prefix', required=True, help='Common prefix for REDCap variables for the numbered CBCL questions (i.e. cbcl_, s1_cbcl_q)')

    card1_group = parser.add_argument_group('Competence Scale Arguments', 'Leave blank if this section of CBCL was not collected (otherwise, must specify both)')
    card1_group.add_argument('--num_sports_var', help='REDCap variable for number of sports')
    card1_group.add_argument('--academic_problems_var', help='REDCap variable for academic/school problems')

    return parser

if __name__ == '__main__':
    parser = parse_args()
    args = parser.parse_args()

    card1_bounds = (args.num_sports_var, args.academic_problems_var)
    if any(card1_bounds) and not all(card1_bounds):
        parser.error('Both num_sports_var and academic_problems_var need to be provided to identify the sports/activities section of the CBCL')
        sys.exit(1)

    cbcl_import(args.export_file, args.data_dict, args.output_folder, args.question_prefix, card1_bounds)
