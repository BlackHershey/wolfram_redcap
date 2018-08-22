import argparse
import numpy as np
import pandas as pd
import re

from collections import OrderedDict
from gooey import Gooey, GooeyParser
from os.path import join

# NOTE: do not try to expand the df for non-longitudal projects - it screws up the column order which this code heavily relies on
# from redcap_common import expand

var_search_map = {
    'Number of favorite sports listed': 'num_sports',
    'any academic or other problems in school': 'acad_prob',
    '1. Acts too young for his/her age': 'q1',
    '58. Picks nose, skin, or other parts of body': 'q58',
    '59. Plays with own sex parts in public': 'q59',
    '113': 'q113'
}

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


def gen_subject_file(row, outfolder, var_map):
    study_id = row.name + '_'
    study_id += row['redcap_event_name'].replace('_', '') if 'redcap_event_name' in row.axes[0] else var_map['q1'].split('_')[0]
    print('Processing ', study_id)

    if row[[var_map['gender'], var_map['age']]].isnull().any():
        print('Skipping subject {} due to missing age and/or gender'.format(study_id))
        return

    subj_str = '{: >10}{}{}{:02}'.format(study_id[:10], '{}', int(row[var_map['gender']]), int(row[var_map['age']]))

    cards = OrderedDict([
        ('01', (var_map['num_sports'] if 'num_sports' in var_map else None, var_map['acad_prob'] if 'acad_prob' in var_map else None)),
        ('02', (var_map['q1'], var_map['q58'])),
        ('03', (var_map['q59'], var_map['q113']))
    ])

    line = ''
    for num, col_tuple in cards.items():
        line += subj_str.format(num)
        if num == '01' and not all(col_tuple):
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


def cbcl_import(file, data_dict, outfolder, age_col):
    df = pd.read_csv(file, index_col=0)

    data_dict_df = pd.read_csv(data_dict, index_col=0)

    # construct map of card bound variables
    #   var_map example: { 'q1': [ 's1s1_cbcl_q1', 's2_cbcl_q2'], 'q58': ['s1_cbcl_q58', 's2_cbcl_q58'] }
    var_map = { v: data_dict_df.index[(data_dict_df['Field Label'].str.contains(k, na=False)) & (data_dict_df['Field Type'] == 'radio')].tolist() for k,v in var_search_map.items() }
    var_map = { k: v for k,v in var_map.items() if v }

    gender_cols = [ col for col in df.columns if 'gender' in col ]
    age_cols = [ col for col in df.columns if re.search(age_col, col) ]
    if len(gender_cols) < 1 or len(age_cols) < 1:
        sys.stderr.write('Age and gender columns must be included in REDCap export.')
        sys.exit(2)

    # convert var_map to be a list of dicts (where each dict has same set of keys, but each with one of the lists values)
    #   needed to handle non-longitudal REDCap structure which will have multiple matching variables
    #   num_var_list structure: [ { 'q1': 's1_cbcl_q1', 'q58': 's1_cbcl_q58' }, {'q1': 's2_cbcl_q1', 'q58': 's2_cbcl_q58' } ]
    num_var_lists = len(var_map['q1'])
    var_map_list = [ {} for i in range(num_var_lists) ]
    for k,v in var_map.items():
        for i in range(num_var_lists):
            var_map_list[i][k] = v[i]
            var_map_list[i]['gender'] = gender_cols[0]
            var_map_list[i]['age'] = age_cols[i]

    for i, map in enumerate(var_map_list):
        print(map)
        start_q = map['num_sports'] if 'num_sports' in map else map['q1']
        cbclq_df = data_dict_df[start_q:map['q113']]
        keep_qcols = cbclq_df[cbclq_df['Field Type'] == 'radio'].index.tolist()
        df.drop(columns=[ col for col in cbclq_df.index.tolist() if col in df and col not in keep_qcols], inplace=True)

        df = df[df[keep_qcols].sum(axis=1) != 0] # get rid of empty rows (all nan or 0)
        df.apply(gen_subject_file, args=(outfolder, map), axis=1)


@Gooey()
def parse_args():
    parser = GooeyParser(description='Format redcap export for ADM import\n(Assumes REDCap form is in order of actual CBCL questionnaire)')

    required_group = parser.add_argument_group('Required Arguments', gooey_options={'columns': 1})
    required_group.add_argument('--export_file', required=True, widget='FileChooser', help='Demographics + CBCL export from REDCap')
    required_group.add_argument('--data_dict', required=True, widget='FileChooser', help='REDCap data dictionary export for CBCL')
    required_group.add_argument('--output_folder', required=True, widget='DirChooser', help='Folder to store output CBC files')
    required_group.add_argument('--age_var', required=True, help='REDCap variable containing subject age (i.e. childs_age, age_decimal)')
    return parser

if __name__ == '__main__':
    parser = parse_args()
    args = parser.parse_args()

    cbcl_import(args.export_file, args.data_dict, args.output_folder, args.age_var)
