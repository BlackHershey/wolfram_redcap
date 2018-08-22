import pandas as pd
import preschool_cbcl_scales as cbcl
import re

from builtins import input
from gooey import Gooey, GooeyParser

raw_col_template = '{}ycbcl_raw_{}'
tscore_col_template = '{}ycbcl_t_{}'
pctile_col_template = '{}ycbcl_per_{}'
borderline_col_template = '{}cbcl_borderline___{}'
clinical_col_template = '{}cbcl_clinical___{}'

internal = ['emoreact', 'anxdep', 'somcomp', 'withdrawn' ]
external = ['attprob', 'aggbehav']
other = ['sleepprob', 'otherprob' ]
dsm  = ['affprob', 'anxprob', 'pervdevprob', 'adhd', 'oppdef']

newt_checkboxes = [ 'emoreact', 'sleepprob', 'pervdevprob', 'anxdep', 'withdrawn', 'somcomp', None, None, 'attprob', None, 'aggbehav',
    'intprob', 'extprob', 'totprob', 'affprob', 'anxprob', None, 'adhd', 'oppdef', None]
NT_checkboxes = internal + ['sleepprob'] + external + [ 'intprob', 'extprob', 'totprob'] + dsm

def get_tscore(cat_map, raw_score):
    return cat_map['tscores'][int(raw_score)]


def get_pctile(tscore):
    pctile_idx = tscore - 50
    if tscore == 50:
        return '<= 50'
    elif pctile_idx >= len(cbcl.pctiles):
        return '> 98'
    else:
        return cbcl.pctiles[pctile_idx]


def score_category(row, category, category_map, col_prefix, cols):
    raw_col = raw_col_template.format(col_prefix, category)
    tscore_col = tscore_col_template.format(col_prefix, category)
    pctile_col = pctile_col_template.format(col_prefix, category)

    if not raw_col in row.axes[0].tolist():
        raw_score = row[cols].sum()
        row[raw_col] = raw_score

    if category != 'otherprob':
        tscore = get_tscore(category_map, row[raw_col])
        row[tscore_col] = tscore
        if category not in ['intprob', 'extprob', 'totprob']:
            row[pctile_col] = get_pctile(tscore)

        checkboxes = NT_checkboxes if not col_prefix else newt_checkboxes
        checkbox = checkboxes.index(category) + 1
        col_prefix = 'y' if not col_prefix else col_prefix
        borderline_col = borderline_col_template.format(col_prefix, checkbox)
        clinical_col = clinical_col_template.format(col_prefix, checkbox)
        row[borderline_col] = 1 if row[tscore_col] >= cbcl.borderline else 0
        row[clinical_col] = 1 if row[tscore_col] >= cbcl.clinical else 0

    return row

def score_all_categories_df(row, category, col_prefix):
    cat_map = cbcl.category_map[category]
    cols = [ col_prefix + 'ycbcl_q' + str(x) for x in cat_map['qs'] ]
    return score_category(row, category, cat_map, col_prefix, cols)


def score_empirical_df(row, col_prefix):
    categories = {
        'intprob': internal,
        'extprob': external,
        'totprob': internal + external + other
    }

    for emp_cat, cat_list  in categories.items():
        cols = [ raw_col_template.format(col_prefix, cat) for cat in cat_list ]
        row = score_category(row, emp_cat, cbcl.category_map[emp_cat], col_prefix, cols)

    return row


def score_ycbcl(outfile, infile=None, session=None):
    index_col = 0 if session else [0,1]
    df = pd.read_csv(infile, index_col=index_col)
    ybcl_cols = [ col for col in df.columns if 'ycbcl' in col ]
    df = df[df['ycbcl_age_5_complete'] != 0] if 'ycbcl_age_5_complete' in df.columns else df.dropna(subset=ybcl_cols, how='all')

    all_categories = internal + external + other + dsm

    col_prefix = 's{}_'.format(session) if session else ''
    for cat in all_categories:
        df = df.apply(score_all_categories_df, args=(cat, col_prefix), axis=1)
    df = df.apply(score_empirical_df, args=(col_prefix,), axis=1)

    df = df.rename(columns={col_prefix + 'ycbcl_t_sleepprob': col_prefix + 'ycbcl_t_sleeepprob', col_prefix + 'ycbcl_per_sleepprob': col_prefix + 'ycbcl_per_sleeprob'})

    keep_cols = [ col for col in df.columns if re.search('cbcl_(raw|t|per|borderline|clinical)', col) and 'otherprob' not in col ]
    df = df[keep_cols]
    df['ycbcl_age_5_yn'] = 1
    df['ycbcl_age_5_scores_complete'] = 2
    df.to_csv(outfile)

@Gooey
def parse_args():
    parser = GooeyParser()
    parser.add_argument('input_file', widget='FileChooser', help='REDCap export file containing yCBCL questionaire responses')
    parser.add_argument('output_file', widget='FileChooser', help='CSV file to save scored results to')
    parser.add_argument('--session', metavar='session', type=int, help='Session number (for REDCap non-longitudinal studies only)')
    return parser.parse_args()

if __name__ == '__main__':
    args = parse_args()

    score_ycbcl(args.output_file, args.input_file, args.session)
